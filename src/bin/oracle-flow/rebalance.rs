use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anchor_client::{
    Program,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        instruction::Instruction,
        message::{VersionedMessage, v0::Message as MessageV0},
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
        transaction::VersionedTransaction,
    },
};
use anchor_lang::solana_program::program_pack::Pack;
use anchor_spl::{
    associated_token::get_associated_token_address_with_program_id,
    token::spl_token::state::Account as SplTokenAccount,
};
use anyhow::{Context, ensure};
use solana_rpc_client_types::config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig};
use tokio::time::sleep;
use tracing::{Instrument, info, info_span, warn};
use twob_market_making::{
    ARRAY_LENGTH, AccountResolver, LIQUIDITY_AMPLIFICATION, LiquidityPositionBalances, MarketState,
    build_add_liquidity_instruction, build_update_liquidity_flows_instruction,
    build_withdraw_liquidity_instruction, get_token_program_id,
};

use crate::{
    config::JupiterConfig,
    jupiter::{
        BuiltSwap, JupiterSwapClient, SwapDirection, has_compute_unit_price,
        without_compute_unit_limit,
    },
    price::PriceData,
    quote_guard::validate_quote,
    telemetry,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceOutcome {
    Skipped,
    Executed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RebalancePlan {
    direction: SwapDirection,
    withdraw_base_lamports: u64,
    withdraw_quote_lamports: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SafeQuote {
    base_flow: u64,
    quote_flow: u64,
}

impl RebalancePlan {
    fn input_amount(self) -> u64 {
        match self.direction {
            SwapDirection::BaseToQuote => self.withdraw_base_lamports,
            SwapDirection::QuoteToBase => self.withdraw_quote_lamports,
        }
    }
}

/// Check if inventory needs rebalancing based on price and current balances.
pub fn needs_rebalance(
    price: &PriceData,
    balances: &LiquidityPositionBalances,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    threshold_bps: u64,
) -> bool {
    if price.price <= 0.0 {
        warn!(
            event.name = "rebalance_evaluate_skipped",
            rebalance.reason = "non_positive_oracle_price",
            price.oracle = price.price,
        );
        return false;
    }

    if balances.base_balance == 0 || balances.quote_balance == 0 {
        info!(
            event.name = "rebalance_evaluate",
            rebalance.reason = "zero_inventory_side",
            rebalance.outcome = "needed",
            position.base_balance.raw = balances.base_balance,
            position.quote_balance.raw = balances.quote_balance,
        );
        return true;
    }

    let base_ui = balances.base_balance as f64 / 10f64.powi(i32::from(base_token_decimals));
    let quote_ui = balances.quote_balance as f64 / 10f64.powi(i32::from(quote_token_decimals));

    if base_ui <= 0.0 || quote_ui <= 0.0 {
        return true;
    }

    let inventory_price = quote_ui / base_ui;
    let deviation_bps = ((inventory_price - price.price).abs() / price.price) * 10_000.0;

    info!(
        event.name = "rebalance_evaluate",
        price.inventory = inventory_price,
        price.oracle = price.price,
        inventory_deviation_bps = deviation_bps,
        rebalance.threshold_bps = threshold_bps,
        rebalance.outcome = if deviation_bps > threshold_bps as f64 {
            "needed"
        } else {
            "ok"
        },
    );

    deviation_bps > threshold_bps as f64
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_rebalance(
    program: &Program<Arc<Keypair>>,
    http_client: &reqwest::Client,
    market_id: u64,
    market_state: &MarketState,
    price: &PriceData,
    balances: &LiquidityPositionBalances,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    current_base_flow: u64,
    current_quote_flow: u64,
    liquidity_provider: Arc<Keypair>,
    jupiter_config: &JupiterConfig,
    min_rebalance_value_usd: f64,
    max_quote_price_deviation_bps: u64,
    max_oracle_age_secs: u64,
    max_oracle_future_skew_secs: u64,
    is_devnet: bool,
    cycle_id: &str,
    attempt_id: &str,
) -> anyhow::Result<RebalanceOutcome> {
    let mut previous_wallet_snapshot = None;
    log_wallet_balance_snapshot(
        program,
        market_state,
        &liquidity_provider.pubkey(),
        balances,
        "rebalance_pre_plan",
        cycle_id,
        attempt_id,
        &mut previous_wallet_snapshot,
    )
    .await;

    if is_devnet {
        info!(
            event.name = "rebalance_skipped",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %liquidity_provider.pubkey(),
            rebalance.attempt_id = %attempt_id,
            rebalance.reason = "devnet_noop",
            rebalance.outcome = "skipped",
        );
        return Ok(RebalanceOutcome::Skipped);
    }

    if balances.base_debt > 0 || balances.quote_debt > 0 {
        warn!(
            event.name = "rebalance_skipped",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %liquidity_provider.pubkey(),
            rebalance.attempt_id = %attempt_id,
            rebalance.reason = "liquidity_position_unhealthy",
            rebalance.outcome = "skipped",
            position.base_debt.raw = balances.base_debt,
            position.quote_debt.raw = balances.quote_debt,
        );
        return Ok(RebalanceOutcome::Skipped);
    }

    let Some(uncapped_plan) = plan_rebalance(
        price,
        balances,
        base_token_decimals,
        quote_token_decimals,
        min_rebalance_value_usd,
    ) else {
        info!(
            event.name = "rebalance_skipped",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %liquidity_provider.pubkey(),
            rebalance.attempt_id = %attempt_id,
            rebalance.reason = "planned_input_below_minimum",
            rebalance.outcome = "skipped",
        );
        return Ok(RebalanceOutcome::Skipped);
    };
    info!(
        event.name = "rebalance_planned",
        cycle.id = %cycle_id,
        market.id = market_id,
        lp.authority = %liquidity_provider.pubkey(),
        rebalance.attempt_id = %attempt_id,
        rebalance.direction = uncapped_plan.direction.label(),
        rebalance.planned_base_withdraw.raw = uncapped_plan.withdraw_base_lamports,
        rebalance.planned_quote_withdraw.raw = uncapped_plan.withdraw_quote_lamports,
        rebalance.planned_input.raw = uncapped_plan.input_amount(),
    );
    let safe_quote = plan_atomic_safe_quote(
        price,
        balances,
        uncapped_plan,
        current_base_flow,
        current_quote_flow,
        base_token_decimals,
        quote_token_decimals,
    )?;
    let Some(plan) = cap_rebalance_to_withdrawable(
        uncapped_plan,
        balances,
        safe_quote.base_flow,
        safe_quote.quote_flow,
    ) else {
        anyhow::bail!("atomic safe quote left no withdrawable liquidity");
    };
    ensure!(
        plan == uncapped_plan,
        "atomic safe quote could not preserve the planned withdrawal"
    );
    info!(
        event.name = "rebalance_safe_quote_planned",
        cycle.id = %cycle_id,
        market.id = market_id,
        lp.authority = %liquidity_provider.pubkey(),
        rebalance.attempt_id = %attempt_id,
        rebalance.direction = plan.direction.label(),
        rebalance.planned_input.raw = plan.input_amount(),
        rebalance.planned_base_withdraw.raw = plan.withdraw_base_lamports,
        rebalance.planned_quote_withdraw.raw = plan.withdraw_quote_lamports,
        quote.previous_base_flow = current_base_flow,
        quote.previous_quote_flow = current_quote_flow,
        quote.safe_base_flow = safe_quote.base_flow,
        quote.safe_quote_flow = safe_quote.quote_flow,
        price.oracle = price.price,
    );
    let (input_mint, output_mint) = match plan.direction {
        SwapDirection::BaseToQuote => (
            market_state.market.base_mint,
            market_state.market.quote_mint,
        ),
        SwapDirection::QuoteToBase => (
            market_state.market.quote_mint,
            market_state.market.base_mint,
        ),
    };

    let input_token_program = get_token_program_id(program, &input_mint).await?;
    let output_token_program = get_token_program_id(program, &output_mint).await?;
    let lp_input_ata = get_associated_token_address_with_program_id(
        &liquidity_provider.pubkey(),
        &input_mint,
        &input_token_program,
    );
    let lp_output_ata = get_associated_token_address_with_program_id(
        &liquidity_provider.pubkey(),
        &output_mint,
        &output_token_program,
    );
    let target_swap_amount = plan.input_amount();
    let input_ata_balance_before = read_ata_balance_or_zero(program, &lp_input_ata).await?;
    let output_ata_balance_before = read_ata_balance_or_zero(program, &lp_output_ata).await?;
    let withdraw_amount = target_swap_amount;
    let withdraw_plan = plan;

    info!(
        event.name = "rebalance_atomic_execution_planned",
        cycle.id = %cycle_id,
        market.id = market_id,
        lp.authority = %liquidity_provider.pubkey(),
        rebalance.attempt_id = %attempt_id,
        rebalance.direction = plan.direction.label(),
        rebalance.planned_input.raw = target_swap_amount,
        rebalance.input_ata_before.raw = input_ata_balance_before,
        rebalance.output_ata_before.raw = output_ata_balance_before,
        rebalance.withdrawn_input.raw = withdraw_amount,
        rebalance.withdraw_base.raw = withdraw_plan.withdraw_base_lamports,
        rebalance.withdraw_quote.raw = withdraw_plan.withdraw_quote_lamports,
        rebalance.atomic = true,
    );
    log_rebalance_transfer_accounts(
        program,
        market_id,
        market_state,
        liquidity_provider.pubkey(),
        withdraw_plan,
    )
    .await;

    log_wallet_balance_snapshot(
        program,
        market_state,
        &liquidity_provider.pubkey(),
        balances,
        "pre_atomic_rebalance",
        cycle_id,
        attempt_id,
        &mut previous_wallet_snapshot,
    )
    .await;

    if input_ata_balance_before > dust_threshold_for_mint(&input_mint) {
        warn!(
            event.name = "rebalance_ignoring_existing_wallet_input",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %liquidity_provider.pubkey(),
            rebalance.attempt_id = %attempt_id,
            rebalance.direction = plan.direction.label(),
            rebalance.input_ata_before.raw = input_ata_balance_before,
            rebalance.planned_input.raw = target_swap_amount,
            "existing input ATA balance is ignored; atomic rebalance swaps the exact withdrawn amount"
        );
    }

    let reference_index =
        oracle_flow_reference_index(program, market_state.market.end_slot_interval).await?;
    let safe_quote_ix = build_update_liquidity_flows_instruction(
        program,
        market_id,
        crate::twob_anchor::client::args::UpdateLiquidityFlows {
            reference_index,
            base_flow_u64: safe_quote.base_flow,
            quote_flow_u64: safe_quote.quote_flow,
        },
    );
    let withdraw_ix = build_withdraw_liquidity_instruction(
        program,
        market_id,
        crate::twob_anchor::client::args::WithdrawLiquidity {
            reference_index,
            base_lamports: withdraw_plan.withdraw_base_lamports,
            quote_lamports: withdraw_plan.withdraw_quote_lamports,
        },
    )
    .await
    .context("Failed to build withdraw_liquidity instruction for atomic rebalance")?;

    let built_swap = JupiterSwapClient::new(http_client, jupiter_config)
        .build_exact_in(
            liquidity_provider.clone(),
            input_mint,
            output_mint,
            lp_output_ata,
            withdraw_amount,
        )
        .instrument(info_span!(
            "jupiter.build",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %liquidity_provider.pubkey(),
            rebalance.attempt_id = %attempt_id,
            rebalance.direction = plan.direction.label(),
            jupiter.input_mint = %input_mint,
            jupiter.output_mint = %output_mint,
            rebalance.swap_input_requested.raw = withdraw_amount,
        ))
        .await
        .with_context(|| format!("Failed to build Jupiter swap {}", plan.direction.label()))?;

    let (deposit_base_lamports, deposit_quote_lamports) = match plan.direction {
        SwapDirection::BaseToQuote => (0, built_swap.minimum_output),
        SwapDirection::QuoteToBase => (built_swap.minimum_output, 0),
    };
    ensure!(
        deposit_base_lamports > 0 || deposit_quote_lamports > 0,
        "Jupiter build produced no minimum output to add back after rebalance"
    );

    let add_ix = build_add_liquidity_instruction(
        program,
        market_id,
        crate::twob_anchor::client::args::AddLiquidity {
            reference_index,
            base_lamports: deposit_base_lamports,
            quote_lamports: deposit_quote_lamports,
        },
    )
    .await
    .context("Failed to build add_liquidity instruction for atomic rebalance")?;

    let atomic_instructions =
        compose_atomic_rebalance_body(&built_swap, safe_quote_ix, withdraw_ix, add_ix);

    info!(
        event.name = "rebalance_atomic_transaction_planned",
        cycle.id = %cycle_id,
        market.id = market_id,
        lp.authority = %liquidity_provider.pubkey(),
        rebalance.attempt_id = %attempt_id,
        rebalance.direction = plan.direction.label(),
        twob.reference_index = reference_index,
        rebalance.withdraw_base.raw = withdraw_plan.withdraw_base_lamports,
        rebalance.withdraw_quote.raw = withdraw_plan.withdraw_quote_lamports,
        rebalance.deposit_base.raw = deposit_base_lamports,
        rebalance.deposit_quote.raw = deposit_quote_lamports,
        rebalance.withdrawn_input.raw = withdraw_amount,
        rebalance.swap_input_requested.raw = built_swap.input_amount,
        quote.safe_base_flow = safe_quote.base_flow,
        quote.safe_quote_flow = safe_quote.quote_flow,
        jupiter.expected_output.raw = built_swap.expected_output,
        jupiter.minimum_output.raw = built_swap.minimum_output,
        jupiter.slippage_bps = built_swap.slippage_bps,
        jupiter.price_impact_bps = ?built_swap.price_impact_bps,
        jupiter.route_labels = ?built_swap.route_labels,
        transaction.instruction_count = atomic_instructions.len(),
        transaction.lookup_table_count = built_swap.address_lookup_tables.len(),
    );

    let execution = execute_atomic_rebalance_transaction(
        program,
        liquidity_provider.clone(),
        &built_swap,
        atomic_instructions,
        jupiter_config,
        cycle_id,
        attempt_id,
        safe_quote,
        price,
        base_token_decimals,
        quote_token_decimals,
        max_quote_price_deviation_bps,
        max_oracle_age_secs,
        max_oracle_future_skew_secs,
    )
    .instrument(info_span!(
        "rebalance.atomic_transaction",
        cycle.id = %cycle_id,
        market.id = market_id,
        lp.authority = %liquidity_provider.pubkey(),
        rebalance.attempt_id = %attempt_id,
        rebalance.direction = plan.direction.label(),
    ))
    .await?;

    log_wallet_balance_snapshot(
        program,
        market_state,
        &liquidity_provider.pubkey(),
        balances,
        "post_atomic_rebalance",
        cycle_id,
        attempt_id,
        &mut previous_wallet_snapshot,
    )
    .await;

    info!(
        event.name = "rebalance_swap_completed",
        cycle.id = %cycle_id,
        market.id = market_id,
        rebalance.attempt_id = %attempt_id,
        rebalance.direction = plan.direction.label(),
        rebalance.outcome = "executed",
        twob.signature = %execution.signature,
        jupiter.slippage_bps = built_swap.slippage_bps,
        jupiter.price_impact_bps = ?built_swap.price_impact_bps,
        jupiter.input_consumed.raw = built_swap.input_amount,
        jupiter.minimum_output.raw = built_swap.minimum_output,
        jupiter.expected_output.raw = built_swap.expected_output,
        rebalance.deposit_base.raw = deposit_base_lamports,
        rebalance.deposit_quote.raw = deposit_quote_lamports,
        transaction.compute_unit_limit = execution.compute_unit_limit,
        transaction.compute_units_consumed = ?execution.compute_units_consumed,
        monotonic_counter.jupiter_executes_total = 1_u64,
    );

    Ok(RebalanceOutcome::Executed)
}

fn compose_atomic_rebalance_body(
    built_swap: &BuiltSwap,
    safe_quote_ix: Instruction,
    withdraw_ix: Instruction,
    add_ix: Instruction,
) -> Vec<Instruction> {
    let mut instructions = Vec::with_capacity(
        built_swap.setup_instructions.len()
            + built_swap.other_instructions.len()
            + usize::from(built_swap.cleanup_instruction.is_some())
            + usize::from(built_swap.tip_instruction.is_some())
            + 4,
    );
    instructions.extend(built_swap.setup_instructions.iter().cloned());
    instructions.push(safe_quote_ix);
    instructions.push(withdraw_ix);
    instructions.push(built_swap.swap_instruction.clone());
    instructions.push(add_ix);
    if let Some(cleanup_ix) = &built_swap.cleanup_instruction {
        instructions.push(cleanup_ix.clone());
    }
    instructions.extend(built_swap.other_instructions.iter().cloned());
    if let Some(tip_ix) = &built_swap.tip_instruction {
        instructions.push(tip_ix.clone());
    }
    instructions
}

#[derive(Debug, Clone)]
struct AtomicRebalanceExecution {
    signature: Signature,
    compute_unit_limit: u32,
    compute_units_consumed: Option<u64>,
}

#[allow(clippy::too_many_arguments)]
async fn execute_atomic_rebalance_transaction(
    program: &Program<Arc<Keypair>>,
    signer: Arc<Keypair>,
    built_swap: &BuiltSwap,
    body_instructions: Vec<Instruction>,
    jupiter_config: &JupiterConfig,
    cycle_id: &str,
    attempt_id: &str,
    safe_quote: SafeQuote,
    price: &PriceData,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    max_quote_price_deviation_bps: u64,
    max_oracle_age_secs: u64,
    max_oracle_future_skew_secs: u64,
) -> anyhow::Result<AtomicRebalanceExecution> {
    let compute_price_instructions = compute_price_instructions(
        built_swap,
        jupiter_config.fallback_compute_unit_price_micro_lamports,
    );
    let (recent_blockhash, last_valid_block_height) = program
        .rpc()
        .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
        .await
        .context("Failed to fetch latest blockhash for atomic rebalance")?;

    let simulation_limit = MAX_COMPUTE_UNIT_LIMIT;
    let simulation_instructions = atomic_transaction_instructions(
        simulation_limit,
        &compute_price_instructions,
        &body_instructions,
    );
    let simulation_tx = build_versioned_transaction(
        signer.as_ref(),
        &simulation_instructions,
        &built_swap.address_lookup_tables,
        recent_blockhash,
    )?;
    let simulation = program
        .rpc()
        .simulate_transaction_with_config(
            &simulation_tx,
            RpcSimulateTransactionConfig {
                sig_verify: false,
                replace_recent_blockhash: false,
                commitment: Some(CommitmentConfig::confirmed()),
                ..RpcSimulateTransactionConfig::default()
            },
        )
        .await
        .context("Failed to simulate atomic rebalance transaction")?;

    if let Some(error) = simulation.value.err {
        anyhow::bail!(
            "Atomic rebalance simulation failed. err={:?} logs={:?}",
            error,
            simulation.value.logs
        );
    }

    let compute_unit_limit = estimated_compute_unit_limit(simulation.value.units_consumed);
    info!(
        event.name = "rebalance_atomic_transaction_simulated",
        cycle.id = %cycle_id,
        rebalance.attempt_id = %attempt_id,
        transaction.compute_unit_limit_simulation = simulation_limit,
        transaction.compute_unit_limit = compute_unit_limit,
        transaction.compute_units_consumed = ?simulation.value.units_consumed,
        transaction.loaded_accounts_data_size = ?simulation.value.loaded_accounts_data_size,
    );

    let final_instructions = atomic_transaction_instructions(
        compute_unit_limit,
        &compute_price_instructions,
        &body_instructions,
    );
    let final_tx = build_versioned_transaction(
        signer.as_ref(),
        &final_instructions,
        &built_swap.address_lookup_tables,
        recent_blockhash,
    )?;
    let final_simulation = program
        .rpc()
        .simulate_transaction_with_config(
            &final_tx,
            RpcSimulateTransactionConfig {
                sig_verify: false,
                replace_recent_blockhash: false,
                commitment: Some(CommitmentConfig::confirmed()),
                ..RpcSimulateTransactionConfig::default()
            },
        )
        .await
        .context("Failed to simulate final atomic rebalance transaction")?;

    if let Some(error) = final_simulation.value.err {
        anyhow::bail!(
            "Final atomic rebalance simulation failed. err={:?} logs={:?}",
            error,
            final_simulation.value.logs
        );
    }

    if jupiter_config.dry_run {
        anyhow::bail!("JUPITER_DRY_RUN=true prevents sending atomic rebalance transactions");
    }
    let now_timestamp = u64::try_from(chrono::Utc::now().timestamp())
        .context("system clock is before the Unix epoch")?;
    validate_quote(
        safe_quote.base_flow,
        safe_quote.quote_flow,
        base_token_decimals,
        quote_token_decimals,
        price.price,
        price.timestamp,
        now_timestamp,
        max_quote_price_deviation_bps,
        max_oracle_age_secs,
        max_oracle_future_skew_secs,
    )
    .context("atomic rebalance quote failed final safety validation")?;

    let signature = send_and_confirm_atomic_transaction(
        program,
        &final_tx,
        last_valid_block_height,
        cycle_id,
        attempt_id,
    )
    .await?;

    Ok(AtomicRebalanceExecution {
        signature,
        compute_unit_limit,
        compute_units_consumed: final_simulation.value.units_consumed,
    })
}

const MAX_COMPUTE_UNIT_LIMIT: u32 = 1_400_000;
const MAX_TRANSACTION_SIZE_BYTES: u64 = 1_232;
const COMPUTE_UNIT_LIMIT_HEADROOM_NUMERATOR: u64 = 12;
const COMPUTE_UNIT_LIMIT_HEADROOM_DENOMINATOR: u64 = 10;
const CONFIRM_POLL_DELAY: Duration = Duration::from_millis(400);
const RESEND_INTERVAL: Duration = Duration::from_secs(2);

fn compute_price_instructions(
    built_swap: &BuiltSwap,
    fallback_compute_unit_price_micro_lamports: u64,
) -> Vec<Instruction> {
    let mut instructions = without_compute_unit_limit(&built_swap.compute_budget_instructions);
    if !has_compute_unit_price(&instructions) {
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
            fallback_compute_unit_price_micro_lamports,
        ));
    }
    instructions
}

fn atomic_transaction_instructions(
    compute_unit_limit: u32,
    compute_price_instructions: &[Instruction],
    body_instructions: &[Instruction],
) -> Vec<Instruction> {
    let mut instructions =
        Vec::with_capacity(1 + compute_price_instructions.len() + body_instructions.len());
    instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
        compute_unit_limit,
    ));
    instructions.extend(compute_price_instructions.iter().cloned());
    instructions.extend(body_instructions.iter().cloned());
    instructions
}

fn estimated_compute_unit_limit(units_consumed: Option<u64>) -> u32 {
    let estimated = units_consumed.unwrap_or(u64::from(MAX_COMPUTE_UNIT_LIMIT));
    let with_headroom = estimated
        .saturating_mul(COMPUTE_UNIT_LIMIT_HEADROOM_NUMERATOR)
        .saturating_add(COMPUTE_UNIT_LIMIT_HEADROOM_DENOMINATOR - 1)
        / COMPUTE_UNIT_LIMIT_HEADROOM_DENOMINATOR;
    with_headroom.clamp(1, u64::from(MAX_COMPUTE_UNIT_LIMIT)) as u32
}

fn build_versioned_transaction(
    signer: &Keypair,
    instructions: &[Instruction],
    lookup_tables: &[anchor_client::solana_sdk::address_lookup_table::AddressLookupTableAccount],
    recent_blockhash: anchor_client::solana_sdk::hash::Hash,
) -> anyhow::Result<VersionedTransaction> {
    let message = MessageV0::try_compile(
        &signer.pubkey(),
        instructions,
        lookup_tables,
        recent_blockhash,
    )
    .context("Failed to compile atomic rebalance v0 message")?;
    let signers: [&dyn Signer; 1] = [signer];
    let transaction = VersionedTransaction::try_new(VersionedMessage::V0(message), &signers)
        .context("Failed to sign atomic rebalance transaction")?;
    let transaction_size = bincode::serialized_size(&transaction)
        .context("Failed to measure atomic rebalance transaction")?;
    ensure!(
        transaction_size <= MAX_TRANSACTION_SIZE_BYTES,
        "Atomic rebalance transaction is too large: {} > {} bytes",
        transaction_size,
        MAX_TRANSACTION_SIZE_BYTES,
    );

    Ok(transaction)
}

async fn send_and_confirm_atomic_transaction(
    program: &Program<Arc<Keypair>>,
    transaction: &VersionedTransaction,
    last_valid_block_height: u64,
    cycle_id: &str,
    attempt_id: &str,
) -> anyhow::Result<Signature> {
    let send_config = RpcSendTransactionConfig {
        skip_preflight: true,
        max_retries: Some(0),
        preflight_commitment: Some(CommitmentConfig::confirmed().commitment),
        ..RpcSendTransactionConfig::default()
    };

    let signature = program
        .rpc()
        .send_transaction_with_config(transaction, send_config)
        .await
        .context("Failed to send atomic rebalance transaction")?;
    info!(
        event.name = "rebalance_atomic_transaction_submitted",
        cycle.id = %cycle_id,
        rebalance.attempt_id = %attempt_id,
        twob.signature = %signature,
        transaction.last_valid_block_height = last_valid_block_height,
    );

    let mut last_resend_at = Instant::now();
    loop {
        if let Some(status) = program
            .rpc()
            .get_signature_status_with_commitment(&signature, CommitmentConfig::confirmed())
            .await
            .context("Failed to fetch atomic rebalance signature status")?
        {
            if let Err(error) = status {
                anyhow::bail!(
                    "Atomic rebalance transaction failed: signature={} error={:?}",
                    signature,
                    error
                );
            }
            info!(
                event.name = "rebalance_atomic_transaction_confirmed",
                cycle.id = %cycle_id,
                rebalance.attempt_id = %attempt_id,
                twob.signature = %signature,
            );
            return Ok(signature);
        }

        let current_block_height = program
            .rpc()
            .get_block_height()
            .await
            .context("Failed to fetch block height while confirming atomic rebalance")?;
        if current_block_height > last_valid_block_height {
            anyhow::bail!(
                "Atomic rebalance transaction expired before confirmation: signature={} current_block_height={} last_valid_block_height={}",
                signature,
                current_block_height,
                last_valid_block_height
            );
        }

        if last_resend_at.elapsed() >= RESEND_INTERVAL {
            match program
                .rpc()
                .send_transaction_with_config(transaction, send_config)
                .await
            {
                Ok(_) => {
                    info!(
                        event.name = "rebalance_atomic_transaction_resent",
                        cycle.id = %cycle_id,
                        rebalance.attempt_id = %attempt_id,
                        twob.signature = %signature,
                    );
                }
                Err(error) => {
                    warn!(
                        event.name = "rebalance_atomic_transaction_resend_failed",
                        cycle.id = %cycle_id,
                        rebalance.attempt_id = %attempt_id,
                        twob.signature = %signature,
                        ?error,
                    );
                }
            }
            last_resend_at = Instant::now();
        }

        sleep(CONFIRM_POLL_DELAY).await;
    }
}

async fn log_rebalance_transfer_accounts(
    program: &Program<Arc<Keypair>>,
    market_id: u64,
    market_state: &MarketState,
    authority: anchor_client::solana_sdk::pubkey::Pubkey,
    plan: RebalancePlan,
) {
    let resolver = AccountResolver::new(crate::twob_anchor::ID);
    let market_pda = resolver.market_pda(market_id).address();

    let base_token_program =
        match get_token_program_id(program, &market_state.market.base_mint).await {
            Ok(program_id) => program_id,
            Err(error) => {
                warn!(
                    event.name = "rebalance_transfer_account_debug_failed",
                    market.id = market_id,
                    lp.authority = %authority,
                    base.mint = %market_state.market.base_mint,
                    ?error,
                    "failed to fetch base token program"
                );
                return;
            }
        };
    let quote_token_program =
        match get_token_program_id(program, &market_state.market.quote_mint).await {
            Ok(program_id) => program_id,
            Err(error) => {
                warn!(
                    event.name = "rebalance_transfer_account_debug_failed",
                    market.id = market_id,
                    lp.authority = %authority,
                    quote.mint = %market_state.market.quote_mint,
                    ?error,
                    "failed to fetch quote token program"
                );
                return;
            }
        };

    let base_vault = get_associated_token_address_with_program_id(
        &market_pda,
        &market_state.market.base_mint,
        &base_token_program,
    );
    let quote_vault = get_associated_token_address_with_program_id(
        &market_pda,
        &market_state.market.quote_mint,
        &quote_token_program,
    );
    let authority_base_token_account = get_associated_token_address_with_program_id(
        &authority,
        &market_state.market.base_mint,
        &base_token_program,
    );
    let authority_quote_token_account = get_associated_token_address_with_program_id(
        &authority,
        &market_state.market.quote_mint,
        &quote_token_program,
    );

    info!(
        event.name = "rebalance_transfer_accounts",
        market.id = market_id,
        lp.authority = %authority,
        market.pda = %market_pda,
        rebalance.direction = plan.direction.label(),
        base.token_program = %base_token_program,
        quote.token_program = %quote_token_program,
    );

    match plan.direction {
        SwapDirection::BaseToQuote => {
            log_token_account_state(program, "source(base_vault)", base_vault).await;
            log_token_account_state(
                program,
                "destination(authority_base_token_account)",
                authority_base_token_account,
            )
            .await;
        }
        SwapDirection::QuoteToBase => {
            log_token_account_state(program, "source(quote_vault)", quote_vault).await;
            log_token_account_state(
                program,
                "destination(authority_quote_token_account)",
                authority_quote_token_account,
            )
            .await;
        }
    }
}

async fn log_token_account_state(
    program: &Program<Arc<Keypair>>,
    label: &str,
    token_account: anchor_client::solana_sdk::pubkey::Pubkey,
) {
    let account = match program.rpc().get_account(&token_account).await {
        Ok(account) => account,
        Err(error) => {
            warn!(
                event.name = "token_account_debug_failed",
                token_account.label = label,
                token_account.address = %token_account,
                ?error,
                "failed to fetch token account"
            );
            return;
        }
    };

    let token_state = match SplTokenAccount::unpack(&account.data) {
        Ok(state) => state,
        Err(error) => {
            warn!(
                event.name = "token_account_debug_failed",
                token_account.label = label,
                token_account.address = %token_account,
                ?error,
                "failed to decode token account"
            );
            return;
        }
    };

    info!(
        event.name = "token_account_debug",
        token_account.label = label,
        token_account.address = %token_account,
        token_account.owner_program = %account.owner,
        token_account.owner = %token_state.owner,
        token_account.mint = %token_state.mint,
        token_account.amount.raw = token_state.amount,
        token_account.lamports = account.lamports,
        token_account.is_native = token_state.is_native(),
    );
}

async fn oracle_flow_reference_index(
    program: &Program<Arc<Keypair>>,
    end_slot_interval: u64,
) -> anyhow::Result<u64> {
    let current_slot = program.rpc().get_slot().await?;
    let reference_index = (current_slot + ARRAY_LENGTH / 2) / ARRAY_LENGTH / end_slot_interval;
    ensure!(
        reference_index > 0,
        "Oracle-flow rebalance requires reference_index > 0 with the current calculation"
    );
    Ok(reference_index)
}

fn plan_rebalance(
    price: &PriceData,
    balances: &LiquidityPositionBalances,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    min_rebalance_value_usd: f64,
) -> Option<RebalancePlan> {
    if !price.price.is_finite() || price.price <= 0.0 {
        return None;
    }

    let base_ui = balances.base_balance as f64 / 10f64.powi(i32::from(base_token_decimals));
    let quote_ui = balances.quote_balance as f64 / 10f64.powi(i32::from(quote_token_decimals));

    // One side fully depleted: swap half of the available side to restore the missing one.
    if balances.base_balance == 0 && balances.quote_balance > 0 {
        let withdraw_quote_lamports = balances.quote_balance / 2;
        info!(
            event.name = "rebalance_plan_selected",
            rebalance.reason = "base_depleted",
            rebalance.direction = SwapDirection::QuoteToBase.label(),
            rebalance.planned_quote_withdraw.raw = withdraw_quote_lamports,
        );
        return Some(RebalancePlan {
            direction: SwapDirection::QuoteToBase,
            withdraw_base_lamports: 0,
            withdraw_quote_lamports,
        });
    }
    if balances.quote_balance == 0 && balances.base_balance > 0 {
        let withdraw_base_lamports = balances.base_balance / 2;
        info!(
            event.name = "rebalance_plan_selected",
            rebalance.reason = "quote_depleted",
            rebalance.direction = SwapDirection::BaseToQuote.label(),
            rebalance.planned_base_withdraw.raw = withdraw_base_lamports,
        );
        return Some(RebalancePlan {
            direction: SwapDirection::BaseToQuote,
            withdraw_base_lamports,
            withdraw_quote_lamports: 0,
        });
    }

    if !base_ui.is_finite() || !quote_ui.is_finite() || base_ui <= 0.0 || quote_ui <= 0.0 {
        warn!(
            event.name = "rebalance_plan_rejected",
            rebalance.reason = "invalid_ui_amounts",
            rebalance.base_ui = base_ui,
            rebalance.quote_ui = quote_ui,
        );
        return None;
    }

    info!(
        event.name = "rebalance_plan_inputs",
        rebalance.base_ui = base_ui,
        rebalance.quote_ui = quote_ui,
        price.oracle = price.price,
        rebalance.ideal_quote_ui = base_ui * price.price,
        rebalance.quote_excess_ui = (quote_ui - base_ui * price.price).max(0.0),
        rebalance.base_excess_ui = (base_ui - quote_ui / price.price).max(0.0),
    );

    let quote_excess_ui = (quote_ui - base_ui * price.price).max(0.0);
    if quote_excess_ui > 0.0 {
        let withdraw_quote_ui = quote_excess_ui / 2.0;
        let withdraw_value_usd = withdraw_quote_ui; // quote is USD-denominated
        if withdraw_value_usd < min_rebalance_value_usd {
            info!(
                event.name = "rebalance_plan_rejected",
                rebalance.reason = "quote_withdraw_below_minimum",
                rebalance.withdraw_value_usd = withdraw_value_usd,
                rebalance.min_value_usd = min_rebalance_value_usd,
            );
            return None;
        }
        let withdraw_quote_lamports =
            ui_amount_to_lamports(withdraw_quote_ui, quote_token_decimals);
        if withdraw_quote_lamports > 0 {
            return Some(RebalancePlan {
                direction: SwapDirection::QuoteToBase,
                withdraw_base_lamports: 0,
                withdraw_quote_lamports,
            });
        }
        info!(
            event.name = "rebalance_plan_rejected",
            rebalance.reason = "quote_excess_rounds_to_zero",
            rebalance.withdraw_quote_ui = withdraw_quote_ui,
        );
    }

    let base_excess_ui = (base_ui - quote_ui / price.price).max(0.0);
    let withdraw_base_ui = base_excess_ui / 2.0;
    let withdraw_value_usd = withdraw_base_ui * price.price;
    if withdraw_base_ui > 0.0 && withdraw_value_usd < min_rebalance_value_usd {
        info!(
            event.name = "rebalance_plan_rejected",
            rebalance.reason = "base_withdraw_below_minimum",
            rebalance.withdraw_value_usd = withdraw_value_usd,
            rebalance.min_value_usd = min_rebalance_value_usd,
        );
        return None;
    }
    let withdraw_base_lamports = ui_amount_to_lamports(withdraw_base_ui, base_token_decimals);
    if withdraw_base_lamports > 0 {
        return Some(RebalancePlan {
            direction: SwapDirection::BaseToQuote,
            withdraw_base_lamports,
            withdraw_quote_lamports: 0,
        });
    }

    info!(
        event.name = "rebalance_plan_rejected",
        rebalance.reason = "base_excess_rounds_to_zero",
        rebalance.withdraw_base_ui = withdraw_base_ui,
    );
    None
}

const ORACLE_PRICE_SCALE: u64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OracleFlowRatio {
    numerator: u128,
    denominator: u128,
}

fn oracle_flow_ratio(
    oracle_price: f64,
    base_token_decimals: u8,
    quote_token_decimals: u8,
) -> anyhow::Result<OracleFlowRatio> {
    ensure!(
        oracle_price.is_finite() && oracle_price > 0.0,
        "atomic safe quote requires a finite positive oracle price"
    );

    let scaled_price = oracle_price * ORACLE_PRICE_SCALE as f64;
    ensure!(
        scaled_price.is_finite() && scaled_price >= 1.0 && scaled_price < u64::MAX as f64,
        "oracle price cannot be represented at atomic safe quote precision"
    );
    let price_atoms = scaled_price.floor() as u64;
    ensure!(
        price_atoms > 0,
        "oracle price rounds to zero at atomic safe quote precision"
    );

    let base_scale = 10_u128
        .checked_pow(u32::from(base_token_decimals))
        .context("base token decimal scale overflowed while planning atomic safe quote")?;
    let quote_scale = 10_u128
        .checked_pow(u32::from(quote_token_decimals))
        .context("quote token decimal scale overflowed while planning atomic safe quote")?;
    let numerator = u128::from(price_atoms)
        .checked_mul(quote_scale)
        .context("oracle flow ratio numerator overflowed")?;
    let denominator = u128::from(ORACLE_PRICE_SCALE)
        .checked_mul(base_scale)
        .context("oracle flow ratio denominator overflowed")?;

    Ok(OracleFlowRatio {
        numerator,
        denominator,
    })
}

#[allow(clippy::too_many_arguments)]
fn plan_atomic_safe_quote(
    price: &PriceData,
    balances: &LiquidityPositionBalances,
    plan: RebalancePlan,
    current_base_flow: u64,
    current_quote_flow: u64,
    base_token_decimals: u8,
    quote_token_decimals: u8,
) -> anyhow::Result<SafeQuote> {
    ensure!(
        balances.base_debt == 0 && balances.quote_debt == 0,
        "atomic safe quote requires a debt-free liquidity position"
    );
    ensure!(
        current_base_flow > 0 && current_quote_flow > 0,
        "atomic safe quote cannot recover a zero flow side"
    );

    let remaining_base = balances
        .base_balance
        .checked_sub(plan.withdraw_base_lamports)
        .context("planned base withdrawal exceeds available liquidity")?;
    let remaining_quote = balances
        .quote_balance
        .checked_sub(plan.withdraw_quote_lamports)
        .context("planned quote withdrawal exceeds available liquidity")?;
    ensure!(
        remaining_base > 0 && remaining_quote > 0,
        "atomic safe quote requires positive inventory on both sides after withdrawal"
    );

    let max_base_by_inventory = remaining_base
        .checked_mul(LIQUIDITY_AMPLIFICATION)
        .context("post-withdraw base flow capacity overflowed")?;
    let max_quote_by_inventory = remaining_quote
        .checked_mul(LIQUIDITY_AMPLIFICATION)
        .context("post-withdraw quote flow capacity overflowed")?;
    let max_base_flow = current_base_flow.min(max_base_by_inventory);
    let max_quote_flow = current_quote_flow.min(max_quote_by_inventory);
    ensure!(
        max_base_flow > 0 && max_quote_flow > 0,
        "atomic safe quote has no positive post-withdraw depth"
    );

    let oracle_ratio = oracle_flow_ratio(price.price, base_token_decimals, quote_token_decimals)?;
    let max_base_from_quote = u128::from(max_quote_flow)
        .checked_mul(oracle_ratio.denominator)
        .context("quote-constrained base flow calculation overflowed")?
        / oracle_ratio.numerator;
    let base_flow_u128 = u128::from(max_base_flow).min(max_base_from_quote);
    ensure!(
        base_flow_u128 > 0 && base_flow_u128 <= u128::from(u64::MAX),
        "atomic safe base flow is not representable"
    );
    let quote_flow_u128 = base_flow_u128
        .checked_mul(oracle_ratio.numerator)
        .context("atomic safe quote flow calculation overflowed")?
        / oracle_ratio.denominator;
    ensure!(
        quote_flow_u128 > 0
            && quote_flow_u128 <= u128::from(max_quote_flow)
            && quote_flow_u128 <= u128::from(u64::MAX),
        "atomic safe quote flow is not representable"
    );

    let safe_quote = SafeQuote {
        base_flow: base_flow_u128 as u64,
        quote_flow: quote_flow_u128 as u64,
    };
    ensure!(
        safe_quote.base_flow <= current_base_flow && safe_quote.quote_flow <= current_quote_flow,
        "atomic safe quote must not increase depth during rebalance"
    );

    Ok(safe_quote)
}

fn cap_rebalance_to_withdrawable(
    plan: RebalancePlan,
    balances: &LiquidityPositionBalances,
    current_base_flow: u64,
    current_quote_flow: u64,
) -> Option<RebalancePlan> {
    let withdrawable_base_lamports = balances
        .base_balance
        .saturating_sub(current_base_flow / LIQUIDITY_AMPLIFICATION);
    let withdrawable_quote_lamports = balances
        .quote_balance
        .saturating_sub(current_quote_flow / LIQUIDITY_AMPLIFICATION);

    let capped = RebalancePlan {
        direction: plan.direction,
        withdraw_base_lamports: plan.withdraw_base_lamports.min(withdrawable_base_lamports),
        withdraw_quote_lamports: plan
            .withdraw_quote_lamports
            .min(withdrawable_quote_lamports),
    };

    if capped.withdraw_base_lamports == 0 && capped.withdraw_quote_lamports == 0 {
        None
    } else {
        Some(capped)
    }
}

/// Pubkey of the native SOL mint (wSOL).
const NATIVE_SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const DEFAULT_TOKEN_DUST_RAW: u64 = 10;
const NATIVE_SOL_DUST_LAMPORTS: u64 = 10_000;

#[derive(Debug, Clone, Copy)]
struct WalletBalanceSnapshot {
    native_sol_lamports: u64,
    base_ata: Pubkey,
    base_ata_raw: u64,
    quote_ata: Pubkey,
    quote_ata_raw: u64,
}

#[allow(clippy::too_many_arguments)]
async fn log_wallet_balance_snapshot(
    program: &Program<Arc<Keypair>>,
    market_state: &MarketState,
    owner: &Pubkey,
    balances: &LiquidityPositionBalances,
    stage: &str,
    cycle_id: &str,
    attempt_id: &str,
    previous: &mut Option<WalletBalanceSnapshot>,
) {
    let snapshot = match read_wallet_balance_snapshot(program, market_state, owner).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(
                event.name = "wallet_balance_snapshot_error",
                snapshot.stage = stage,
                cycle.id = %cycle_id,
                market.id = market_state.market.id,
                lp.authority = %owner,
                rebalance.attempt_id = %attempt_id,
                ?error,
                "failed to read wallet balance snapshot"
            );
            return;
        }
    };

    let (native_sol_delta, base_delta, quote_delta) =
        previous.map_or((0_i64, 0_i64, 0_i64), |previous_snapshot| {
            (
                clamp_delta(telemetry::balance_delta(
                    snapshot.native_sol_lamports,
                    previous_snapshot.native_sol_lamports,
                )),
                clamp_delta(telemetry::balance_delta(
                    snapshot.base_ata_raw,
                    previous_snapshot.base_ata_raw,
                )),
                clamp_delta(telemetry::balance_delta(
                    snapshot.quote_ata_raw,
                    previous_snapshot.quote_ata_raw,
                )),
            )
        });

    info!(
        event.name = "wallet_balance_snapshot",
        snapshot.stage = stage,
        cycle.id = %cycle_id,
        slot.current = market_state.current_slot,
        market.id = market_state.market.id,
        lp.authority = %owner,
        rebalance.attempt_id = %attempt_id,
        base.mint = %market_state.market.base_mint,
        quote.mint = %market_state.market.quote_mint,
        position.base_balance.raw = balances.base_balance,
        position.quote_balance.raw = balances.quote_balance,
        position.base_debt.raw = balances.base_debt,
        position.quote_debt.raw = balances.quote_debt,
        wallet.native_sol.lamports = snapshot.native_sol_lamports,
        wallet.base_ata.address = %snapshot.base_ata,
        wallet.base_ata.raw = snapshot.base_ata_raw,
        wallet.quote_ata.address = %snapshot.quote_ata,
        wallet.quote_ata.raw = snapshot.quote_ata_raw,
        wallet.native_sol_delta.raw = native_sol_delta,
        wallet.base_delta.raw = base_delta,
        wallet.quote_delta.raw = quote_delta,
        gauge.wallet_native_sol_lamports = snapshot.native_sol_lamports as f64,
        gauge.wallet_base_balance_raw = snapshot.base_ata_raw as f64,
        gauge.wallet_quote_balance_raw = snapshot.quote_ata_raw as f64,
    );

    *previous = Some(snapshot);
}

async fn read_wallet_balance_snapshot(
    program: &Program<Arc<Keypair>>,
    market_state: &MarketState,
    owner: &Pubkey,
) -> anyhow::Result<WalletBalanceSnapshot> {
    let base_token_program = get_token_program_id(program, &market_state.market.base_mint).await?;
    let quote_token_program =
        get_token_program_id(program, &market_state.market.quote_mint).await?;
    let base_ata = get_associated_token_address_with_program_id(
        owner,
        &market_state.market.base_mint,
        &base_token_program,
    );
    let quote_ata = get_associated_token_address_with_program_id(
        owner,
        &market_state.market.quote_mint,
        &quote_token_program,
    );
    let native_sol_lamports = program
        .rpc()
        .get_balance(owner)
        .await
        .context("Failed to read LP native SOL balance")?;
    let base_ata_raw = read_ata_balance_or_zero(program, &base_ata).await?;
    let quote_ata_raw = read_ata_balance_or_zero(program, &quote_ata).await?;

    Ok(WalletBalanceSnapshot {
        native_sol_lamports,
        base_ata,
        base_ata_raw,
        quote_ata,
        quote_ata_raw,
    })
}

fn clamp_delta(delta: i128) -> i64 {
    delta.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

async fn read_ata_balance_or_zero(
    program: &Program<Arc<Keypair>>,
    token_account: &Pubkey,
) -> anyhow::Result<u64> {
    let Some(account) = read_token_account(program, token_account).await? else {
        return Ok(0);
    };
    let state = SplTokenAccount::unpack(&account.data)
        .with_context(|| format!("Failed to decode ATA {}", token_account))?;
    Ok(state.amount)
}

async fn read_token_account(
    program: &Program<Arc<Keypair>>,
    token_account: &Pubkey,
) -> anyhow::Result<Option<anchor_client::solana_sdk::account::Account>> {
    let response = program
        .rpc()
        .get_account_with_commitment(token_account, CommitmentConfig::confirmed())
        .await
        .with_context(|| format!("Failed to fetch token account {}", token_account))?;
    Ok(response.value)
}

fn native_sol_mint() -> Pubkey {
    NATIVE_SOL_MINT.parse().expect("hardcoded native mint")
}

fn is_native_sol_mint(mint: &Pubkey) -> bool {
    *mint == native_sol_mint()
}

fn dust_threshold_for_mint(mint: &Pubkey) -> u64 {
    if is_native_sol_mint(mint) {
        NATIVE_SOL_DUST_LAMPORTS
    } else {
        DEFAULT_TOKEN_DUST_RAW
    }
}

fn ui_amount_to_lamports(amount_ui: f64, decimals: u8) -> u64 {
    if !amount_ui.is_finite() || amount_ui <= 0.0 {
        return 0;
    }

    let scaled = amount_ui * 10f64.powi(i32::from(decimals));
    if !scaled.is_finite() || scaled <= 0.0 {
        return 0;
    }

    scaled.floor().clamp(0.0, u64::MAX as f64) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker_instruction(marker: u8) -> Instruction {
        Instruction {
            program_id: Pubkey::new_from_array([marker; 32]),
            accounts: Vec::new(),
            data: vec![marker],
        }
    }

    fn sample_built_swap() -> BuiltSwap {
        BuiltSwap {
            input_amount: 1,
            expected_output: 1,
            minimum_output: 1,
            slippage_bps: 0,
            price_impact_bps: None,
            route_labels: Vec::new(),
            compute_budget_instructions: Vec::new(),
            setup_instructions: vec![marker_instruction(1), marker_instruction(2)],
            swap_instruction: marker_instruction(5),
            cleanup_instruction: Some(marker_instruction(7)),
            other_instructions: vec![marker_instruction(8)],
            tip_instruction: Some(marker_instruction(9)),
            address_lookup_tables: Vec::new(),
        }
    }

    fn sample_balances(base_balance: u64, quote_balance: u64) -> LiquidityPositionBalances {
        LiquidityPositionBalances {
            base_balance,
            quote_balance,
            base_debt: 0,
            quote_debt: 0,
        }
    }

    #[test]
    fn returns_false_when_within_threshold() {
        // 1.0 SOL (9 decimals), 84.5 USDC (6 decimals) => 84.5 USDC/SOL
        let balances = sample_balances(1_000_000_000, 84_500_000);
        let price = PriceData {
            price: 84.0,
            timestamp: 0,
        };

        let should_rebalance = needs_rebalance(&price, &balances, 9, 6, 100);
        assert!(!should_rebalance);
    }

    #[test]
    fn returns_true_when_deviation_exceeds_threshold() {
        // 1.0 SOL, 100 USDC => 100 USDC/SOL
        let balances = sample_balances(1_000_000_000, 100_000_000);
        let price = PriceData {
            price: 84.0,
            timestamp: 0,
        };

        let should_rebalance = needs_rebalance(&price, &balances, 9, 6, 100);
        assert!(should_rebalance);
    }

    #[test]
    fn returns_true_when_any_side_is_zero() {
        let balances = sample_balances(1_000_000_000, 0);
        let price = PriceData {
            price: 84.0,
            timestamp: 0,
        };

        let should_rebalance = needs_rebalance(&price, &balances, 9, 6, 100);
        assert!(should_rebalance);
    }

    #[test]
    fn plans_quote_to_base_rebalance_using_half_the_unused_quote() {
        let balances = sample_balances(1_000_000_000, 100_000_000);
        let price = PriceData {
            price: 92.0,
            timestamp: 0,
        };

        let plan = plan_rebalance(&price, &balances, 9, 6, 0.0).unwrap();
        assert_eq!(plan.direction, SwapDirection::QuoteToBase);
        assert_eq!(plan.withdraw_base_lamports, 0);
        assert_eq!(plan.withdraw_quote_lamports, 4_000_000);
    }

    #[test]
    fn plans_base_to_quote_rebalance_using_half_the_unused_base() {
        let balances = sample_balances(2_000_000_000, 100_000_000);
        let price = PriceData {
            price: 100.0,
            timestamp: 0,
        };

        let plan = plan_rebalance(&price, &balances, 9, 6, 0.0).unwrap();
        assert_eq!(plan.direction, SwapDirection::BaseToQuote);
        assert_eq!(plan.withdraw_base_lamports, 500_000_000);
        assert_eq!(plan.withdraw_quote_lamports, 0);
    }

    #[test]
    fn plans_quote_to_base_when_base_is_fully_depleted() {
        // base=0, quote=355_440_173 → should swap half the quote to base
        let balances = sample_balances(0, 355_440_173);
        let price = PriceData {
            price: 84.0,
            timestamp: 0,
        };

        let plan = plan_rebalance(&price, &balances, 9, 6, 0.0).unwrap();
        assert_eq!(plan.direction, SwapDirection::QuoteToBase);
        assert_eq!(plan.withdraw_base_lamports, 0);
        assert_eq!(plan.withdraw_quote_lamports, 177_720_086);
    }

    #[test]
    fn plans_base_to_quote_when_quote_is_fully_depleted() {
        // base=1_000_000_000, quote=0 → should swap half the base to quote
        let balances = sample_balances(1_000_000_000, 0);
        let price = PriceData {
            price: 84.0,
            timestamp: 0,
        };

        let plan = plan_rebalance(&price, &balances, 9, 6, 0.0).unwrap();
        assert_eq!(plan.direction, SwapDirection::BaseToQuote);
        assert_eq!(plan.withdraw_base_lamports, 500_000_000);
        assert_eq!(plan.withdraw_quote_lamports, 0);
    }

    #[test]
    fn returns_none_when_half_unused_inventory_rounds_to_zero() {
        let balances = sample_balances(1_000_000_000, 92_000_001);
        let price = PriceData {
            price: 92.0,
            timestamp: 0,
        };

        assert!(plan_rebalance(&price, &balances, 9, 6, 0.0).is_none());
    }

    #[test]
    fn caps_rebalance_to_withdrawable_inventory() {
        let plan = RebalancePlan {
            direction: SwapDirection::BaseToQuote,
            withdraw_base_lamports: 900,
            withdraw_quote_lamports: 0,
        };
        let balances = sample_balances(1_000, 2_000);

        let capped = cap_rebalance_to_withdrawable(plan, &balances, 600, 0).unwrap();
        // withdrawable = 1_000 - (600 / LIQUIDITY_AMPLIFICATION=2) = 700; plan wants 900 → capped to 700
        assert_eq!(capped.withdraw_base_lamports, 700);
        assert_eq!(capped.withdraw_quote_lamports, 0);
    }

    #[test]
    fn composes_safe_quote_before_withdraw_and_swap() {
        let instructions = compose_atomic_rebalance_body(
            &sample_built_swap(),
            marker_instruction(3),
            marker_instruction(4),
            marker_instruction(6),
        );

        let markers = instructions
            .iter()
            .map(|instruction| instruction.data[0])
            .collect::<Vec<_>>();
        assert_eq!(markers, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn safe_quote_is_oracle_bounded_and_never_increases_depth() {
        let balances = sample_balances(2_000_000_000, 100_000_000);
        let price = PriceData {
            price: 100.0,
            timestamp: 0,
        };
        let plan = RebalancePlan {
            direction: SwapDirection::BaseToQuote,
            withdraw_base_lamports: 500_000_000,
            withdraw_quote_lamports: 0,
        };

        let safe_quote =
            plan_atomic_safe_quote(&price, &balances, plan, 1_200_000_000, 100_000_000, 9, 6)
                .unwrap();

        assert_eq!(
            safe_quote,
            SafeQuote {
                base_flow: 1_000_000_000,
                quote_flow: 100_000_000,
            }
        );
        assert_eq!(
            cap_rebalance_to_withdrawable(
                plan,
                &balances,
                safe_quote.base_flow,
                safe_quote.quote_flow,
            ),
            Some(plan)
        );
    }

    #[test]
    fn safe_quote_caps_depth_to_post_withdraw_inventory() {
        let balances = sample_balances(2_000_000_000, 100_000_000);
        let price = PriceData {
            price: 100.0,
            timestamp: 0,
        };
        let plan = RebalancePlan {
            direction: SwapDirection::BaseToQuote,
            withdraw_base_lamports: 1_400_000_000,
            withdraw_quote_lamports: 0,
        };

        let safe_quote =
            plan_atomic_safe_quote(&price, &balances, plan, 2_000_000_000, 200_000_000, 9, 6)
                .unwrap();

        assert_eq!(
            safe_quote,
            SafeQuote {
                base_flow: 1_200_000_000,
                quote_flow: 120_000_000,
            }
        );
        assert!(safe_quote.base_flow / LIQUIDITY_AMPLIFICATION <= 600_000_000);
        assert!(safe_quote.quote_flow / LIQUIDITY_AMPLIFICATION <= 100_000_000);
    }

    #[test]
    fn safe_quote_rounds_ratio_down_from_oracle() {
        let balances = sample_balances(1_000_000_000, 100_000_000);
        let price = PriceData {
            price: 97.660_036_84,
            timestamp: 0,
        };
        let plan = RebalancePlan {
            direction: SwapDirection::BaseToQuote,
            withdraw_base_lamports: 1,
            withdraw_quote_lamports: 0,
        };

        let safe_quote =
            plan_atomic_safe_quote(&price, &balances, plan, 1_000_000_000, 100_000_000, 9, 6)
                .unwrap();
        let implied_price =
            safe_quote.quote_flow as f64 / safe_quote.base_flow as f64 * 10f64.powi(3);

        assert_eq!(safe_quote.quote_flow, 97_660_036);
        assert!(implied_price <= price.price);
    }

    #[test]
    fn safe_quote_fails_closed_for_zero_flow_side() {
        let balances = sample_balances(1_000_000_000, 100_000_000);
        let price = PriceData {
            price: 100.0,
            timestamp: 0,
        };
        let plan = RebalancePlan {
            direction: SwapDirection::BaseToQuote,
            withdraw_base_lamports: 1,
            withdraw_quote_lamports: 0,
        };

        assert!(plan_atomic_safe_quote(&price, &balances, plan, 0, 100_000_000, 9, 6).is_err());
        assert!(plan_atomic_safe_quote(&price, &balances, plan, 1_000_000_000, 0, 9, 6).is_err());
    }

    #[test]
    fn safe_quote_fails_closed_for_zero_post_withdraw_inventory() {
        let balances = sample_balances(1_000_000_000, 100_000_000);
        let price = PriceData {
            price: 100.0,
            timestamp: 0,
        };
        let plan = RebalancePlan {
            direction: SwapDirection::BaseToQuote,
            withdraw_base_lamports: balances.base_balance,
            withdraw_quote_lamports: 0,
        };

        assert!(
            plan_atomic_safe_quote(&price, &balances, plan, 1_000_000_000, 100_000_000, 9, 6,)
                .is_err()
        );
    }
}
