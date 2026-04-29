use std::{sync::Arc, time::Duration};

use anchor_client::{
    Program,
    solana_sdk::{
        commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Keypair, signer::Signer,
    },
};
use anchor_lang::solana_program::{program_pack::Pack, system_instruction};
use anchor_spl::{
    associated_token::get_associated_token_address_with_program_id,
    token::spl_token::state::Account as SplTokenAccount,
};
use anyhow::{Context, ensure};
use tokio::time::sleep;
use tracing::{Instrument, info, info_span, warn};
use twob_market_making::{
    ARRAY_LENGTH, AccountResolver, LIQUIDITY_AMPLIFICATION, LiquidityPositionBalances, MarketState,
    build_withdraw_liquidity_instruction, execute_add_liquidity, execute_withdraw_liquidity,
    get_token_program_id,
};

use crate::{
    config::JupiterConfig,
    jupiter::{JupiterUltraClient, SwapDirection},
    price::PriceData,
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

impl RebalancePlan {
    fn input_amount(self) -> u64 {
        match self.direction {
            SwapDirection::BaseToQuote => self.withdraw_base_lamports,
            SwapDirection::QuoteToBase => self.withdraw_quote_lamports,
        }
    }

    fn with_input_amount(self, amount: u64) -> Self {
        match self.direction {
            SwapDirection::BaseToQuote => Self {
                withdraw_base_lamports: amount,
                withdraw_quote_lamports: 0,
                ..self
            },
            SwapDirection::QuoteToBase => Self {
                withdraw_base_lamports: 0,
                withdraw_quote_lamports: amount,
                ..self
            },
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
    _reduction_factor: f64,
    _max_reduction_attempts: usize,
    min_rebalance_value_usd: f64,
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
    let Some(plan) = cap_rebalance_to_withdrawable(
        uncapped_plan,
        balances,
        current_base_flow,
        current_quote_flow,
    ) else {
        info!(
            event.name = "rebalance_skipped",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %liquidity_provider.pubkey(),
            rebalance.attempt_id = %attempt_id,
            rebalance.reason = "no_withdrawable_liquidity",
            rebalance.outcome = "skipped",
            position.base_balance.raw = balances.base_balance,
            position.quote_balance.raw = balances.quote_balance,
            position.base_flow.raw = current_base_flow,
            position.quote_flow.raw = current_quote_flow,
        );
        return Ok(RebalanceOutcome::Skipped);
    };
    info!(
        event.name = "rebalance_capped",
        cycle.id = %cycle_id,
        market.id = market_id,
        lp.authority = %liquidity_provider.pubkey(),
        rebalance.attempt_id = %attempt_id,
        rebalance.direction = plan.direction.label(),
        rebalance.planned_input.raw = plan.input_amount(),
        rebalance.planned_base_withdraw.raw = plan.withdraw_base_lamports,
        rebalance.planned_quote_withdraw.raw = plan.withdraw_quote_lamports,
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
    let lp_input_ata = get_associated_token_address_with_program_id(
        &liquidity_provider.pubkey(),
        &input_mint,
        &input_token_program,
    );
    let target_swap_amount = plan.input_amount();
    let existing_input_balance = read_swap_input_balance(
        program,
        &input_mint,
        &lp_input_ata,
        &liquidity_provider.pubkey(),
    )
    .await?;
    let withdraw_amount = target_swap_amount.saturating_sub(existing_input_balance);
    let withdraw_plan = plan.with_input_amount(withdraw_amount);

    info!(
        event.name = "rebalance_execution_planned",
        cycle.id = %cycle_id,
        market.id = market_id,
        lp.authority = %liquidity_provider.pubkey(),
        rebalance.attempt_id = %attempt_id,
        rebalance.direction = plan.direction.label(),
        rebalance.planned_input.raw = target_swap_amount,
        rebalance.wallet_input_before.raw = existing_input_balance,
        rebalance.withdrawn_input.raw = withdraw_amount,
        rebalance.withdraw_base.raw = withdraw_plan.withdraw_base_lamports,
        rebalance.withdraw_quote.raw = withdraw_plan.withdraw_quote_lamports,
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
        "pre_withdraw",
        cycle_id,
        attempt_id,
        &mut previous_wallet_snapshot,
    )
    .await;

    if withdraw_amount > 0 {
        let withdraw_reference_index =
            oracle_flow_reference_index(program, market_state.market.end_slot_interval).await?;
        execute_exact_withdraw_liquidity(
            program,
            market_id,
            withdraw_reference_index,
            liquidity_provider.clone(),
            withdraw_plan,
        )
        .instrument(info_span!(
            "twob.withdraw_liquidity",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %liquidity_provider.pubkey(),
            rebalance.attempt_id = %attempt_id,
            twob.instruction = "withdraw_liquidity",
            twob.reference_index = withdraw_reference_index,
            rebalance.withdraw_base.raw = withdraw_plan.withdraw_base_lamports,
            rebalance.withdraw_quote.raw = withdraw_plan.withdraw_quote_lamports,
        ))
        .await
        .context("Failed to withdraw liquidity for rebalance")?;
    } else {
        warn!(
            event.name = "rebalance_using_existing_wallet_input",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %liquidity_provider.pubkey(),
            rebalance.attempt_id = %attempt_id,
            rebalance.direction = plan.direction.label(),
            rebalance.wallet_input_before.raw = existing_input_balance,
            rebalance.planned_input.raw = target_swap_amount,
            "using existing wallet input balance; no additional withdraw needed"
        );
    }

    log_wallet_balance_snapshot(
        program,
        market_state,
        &liquidity_provider.pubkey(),
        balances,
        "post_withdraw",
        cycle_id,
        attempt_id,
        &mut previous_wallet_snapshot,
    )
    .await;

    let expected_input_balance = existing_input_balance.saturating_add(withdraw_amount);
    let actual_input_balance = wait_for_swap_input_balance(
        program,
        &input_mint,
        &lp_input_ata,
        &liquidity_provider.pubkey(),
        expected_input_balance.min(target_swap_amount),
    )
    .await?;
    let swap_amount = actual_input_balance.min(target_swap_amount);

    info!(
        event.name = "rebalance_input_balance_ready",
        cycle.id = %cycle_id,
        market.id = market_id,
        lp.authority = %liquidity_provider.pubkey(),
        rebalance.attempt_id = %attempt_id,
        rebalance.planned_input.raw = target_swap_amount,
        rebalance.actual_input_balance.raw = actual_input_balance,
        rebalance.swap_input_requested.raw = swap_amount,
    );

    if swap_amount == 0 {
        if withdraw_amount > 0 {
            anyhow::bail!("withdraw succeeded but no Jupiter input balance is visible");
        }
        info!(
            event.name = "rebalance_skipped",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %liquidity_provider.pubkey(),
            rebalance.attempt_id = %attempt_id,
            rebalance.reason = "no_jupiter_input_balance",
            rebalance.outcome = "skipped",
        );
        return Ok(RebalanceOutcome::Skipped);
    }

    // Jupiter Ultra validates the LP's *native SOL wallet balance* for wSOL swaps,
    // not the SPL token ATA balance. Always unwrap wSOL input before ordering, otherwise
    // Jupiter can spend fee-wallet SOL while the withdrawn wSOL stays stranded outside
    // the liquidity position.
    prepare_wsol_input_for_jupiter(
        program,
        &input_mint,
        &lp_input_ata,
        swap_amount,
        liquidity_provider.clone(),
    )
    .await?;

    log_wallet_balance_snapshot(
        program,
        market_state,
        &liquidity_provider.pubkey(),
        balances,
        "pre_jupiter_order",
        cycle_id,
        attempt_id,
        &mut previous_wallet_snapshot,
    )
    .await;

    let swap_execution = JupiterUltraClient::new(http_client, jupiter_config)
        .swap_exact_in(
            liquidity_provider.clone(),
            input_mint,
            output_mint,
            swap_amount,
        )
        .instrument(info_span!(
            "jupiter.execute",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %liquidity_provider.pubkey(),
            rebalance.attempt_id = %attempt_id,
            rebalance.direction = plan.direction.label(),
            jupiter.input_mint = %input_mint,
            jupiter.output_mint = %output_mint,
            rebalance.swap_input_requested.raw = swap_amount,
        ))
        .await
        .with_context(|| {
            format!(
                "Failed to execute Jupiter Ultra swap {}",
                plan.direction.label()
            )
        })?;

    log_wallet_balance_snapshot(
        program,
        market_state,
        &liquidity_provider.pubkey(),
        balances,
        "post_jupiter_execute",
        cycle_id,
        attempt_id,
        &mut previous_wallet_snapshot,
    )
    .await;

    let prior_pending_input = 0_u64;
    let available_budget = withdraw_amount.saturating_add(prior_pending_input);
    let external_wallet_input_estimated =
        telemetry::external_wallet_input_estimated(swap_execution.input_consumed, available_budget);
    info!(
        event.name = "jupiter_swap_completed",
        cycle.id = %cycle_id,
        market.id = market_id,
        lp.authority = %liquidity_provider.pubkey(),
        rebalance.attempt_id = %attempt_id,
        rebalance.direction = plan.direction.label(),
        rebalance.available_budget.raw = available_budget,
        rebalance.withdrawn_input.raw = withdraw_amount,
        rebalance.prior_pending_input.raw = prior_pending_input,
        rebalance.wallet_input_before.raw = existing_input_balance,
        rebalance.swap_input_requested.raw = swap_amount,
        jupiter.input_consumed.raw = swap_execution.input_consumed,
        jupiter.output_received.raw = swap_execution.output_received,
        jupiter.external_wallet_input_estimated.raw = external_wallet_input_estimated,
        jupiter.request_id = ?swap_execution.request_id,
        jupiter.router = ?swap_execution.router,
        jupiter.slippage_bps = ?swap_execution.slippage_bps,
        jupiter.price_impact_bps = ?swap_execution.price_impact_bps,
        jupiter.signature = ?swap_execution.signature,
        monotonic_counter.jupiter_executes_total = 1_u64,
    );
    if external_wallet_input_estimated > dust_threshold_for_mint(&input_mint) {
        warn!(
            event.name = "wallet_external_spend_suspected",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %liquidity_provider.pubkey(),
            rebalance.attempt_id = %attempt_id,
            rebalance.direction = plan.direction.label(),
            rebalance.available_budget.raw = available_budget,
            rebalance.withdrawn_input.raw = withdraw_amount,
            rebalance.wallet_input_before.raw = existing_input_balance,
            rebalance.swap_input_requested.raw = swap_amount,
            jupiter.input_consumed.raw = swap_execution.input_consumed,
            jupiter.external_wallet_input_estimated.raw = external_wallet_input_estimated,
            monotonic_counter.wallet_external_spend_suspected_total = 1_u64,
            "Jupiter consumed more input than the amount withdrawn by this rebalance"
        );
    }

    let (mut deposit_base_lamports, mut deposit_quote_lamports) = match plan.direction {
        SwapDirection::BaseToQuote => (
            swap_amount.saturating_sub(swap_execution.input_consumed),
            swap_execution.output_received,
        ),
        SwapDirection::QuoteToBase => (
            swap_execution.output_received,
            swap_amount.saturating_sub(swap_execution.input_consumed),
        ),
    };

    ensure!(
        deposit_base_lamports > 0 || deposit_quote_lamports > 0,
        "Jupiter swap produced no liquidity to add back after rebalance"
    );

    deposit_base_lamports = prepare_deposit_balance(
        program,
        &market_state.market.base_mint,
        &liquidity_provider.pubkey(),
        deposit_base_lamports,
        liquidity_provider.clone(),
    )
    .await?;
    deposit_quote_lamports = prepare_deposit_balance(
        program,
        &market_state.market.quote_mint,
        &liquidity_provider.pubkey(),
        deposit_quote_lamports,
        liquidity_provider.clone(),
    )
    .await?;

    ensure!(
        deposit_base_lamports > 0 || deposit_quote_lamports > 0,
        "No settled Jupiter output is available to add back after rebalance"
    );

    log_wallet_balance_snapshot(
        program,
        market_state,
        &liquidity_provider.pubkey(),
        balances,
        "pre_add_liquidity",
        cycle_id,
        attempt_id,
        &mut previous_wallet_snapshot,
    )
    .await;

    let add_reference_index =
        oracle_flow_reference_index(program, market_state.market.end_slot_interval).await?;
    execute_add_liquidity(
        program,
        market_id,
        deposit_base_lamports,
        deposit_quote_lamports,
        add_reference_index,
        liquidity_provider.clone(),
    )
    .instrument(info_span!(
        "twob.add_liquidity",
        cycle.id = %cycle_id,
        market.id = market_id,
        rebalance.attempt_id = %attempt_id,
        twob.instruction = "add_liquidity",
        twob.reference_index = add_reference_index,
        rebalance.deposit_base.raw = deposit_base_lamports,
        rebalance.deposit_quote.raw = deposit_quote_lamports,
    ))
    .await
    .context("Failed to add rebalanced liquidity back to the position")?;

    log_wallet_balance_snapshot(
        program,
        market_state,
        &liquidity_provider.pubkey(),
        balances,
        "post_add_liquidity",
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
        jupiter.request_id = ?swap_execution.request_id,
        jupiter.router = ?swap_execution.router,
        jupiter.slippage_bps = ?swap_execution.slippage_bps,
        jupiter.price_impact_bps = ?swap_execution.price_impact_bps,
        jupiter.signature = ?swap_execution.signature,
        jupiter.input_consumed.raw = swap_execution.input_consumed,
        jupiter.output_received.raw = swap_execution.output_received,
        rebalance.deposit_base.raw = deposit_base_lamports,
        rebalance.deposit_quote.raw = deposit_quote_lamports,
    );

    Ok(RebalanceOutcome::Executed)
}

#[allow(clippy::too_many_arguments)]
async fn execute_exact_withdraw_liquidity(
    program: &Program<Arc<Keypair>>,
    market_id: u64,
    reference_index: u64,
    signer: Arc<Keypair>,
    plan: RebalancePlan,
) -> anyhow::Result<RebalancePlan> {
    let ix = build_withdraw_liquidity_instruction(
        program,
        market_id,
        crate::twob_anchor::client::args::WithdrawLiquidity {
            reference_index,
            base_lamports: plan.withdraw_base_lamports,
            quote_lamports: plan.withdraw_quote_lamports,
        },
    )
    .await?;

    let signed_tx = program
        .request()
        .instruction(ix)
        .signer(signer.clone())
        .signed_transaction()
        .await?;

    let simulation = program.rpc().simulate_transaction(&signed_tx).await?;
    if let Some(err) = simulation.value.err {
        anyhow::bail!(
            "Withdraw simulation failed. err={:?} logs={:?}",
            err,
            simulation.value.logs
        );
    }

    execute_withdraw_liquidity(
        program,
        market_id,
        plan.withdraw_base_lamports,
        plan.withdraw_quote_lamports,
        reference_index,
        signer,
    )
    .await?;

    Ok(plan)
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
/// Lamports kept in the native wallet to cover transaction fees when we close the ATA.
const FEE_RESERVE_LAMPORTS: u64 = 10_000_000; // 0.01 SOL
const BALANCE_POLL_ATTEMPTS: usize = 8;
const BALANCE_POLL_DELAY: Duration = Duration::from_millis(750);
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

async fn prepare_wsol_input_for_jupiter(
    program: &Program<Arc<Keypair>>,
    input_mint: &Pubkey,
    wsol_ata: &Pubkey,
    swap_amount: u64,
    signer: Arc<Keypair>,
) -> anyhow::Result<()> {
    if !is_native_sol_mint(input_mint) {
        return Ok(());
    }

    let lp_pubkey = signer.pubkey();
    let wsol_balance = read_ata_balance_or_zero(program, wsol_ata).await?;

    if wsol_balance > 0 {
        info!(
            event.name = "jupiter_wsol_input_unwrap",
            wallet.wsol_ata.address = %wsol_ata,
            wallet.wsol_ata.raw = wsol_balance,
            rebalance.swap_input_requested.raw = swap_amount,
        );
        let close_ix = anchor_spl::token::spl_token::instruction::close_account(
            &anchor_spl::token::spl_token::ID,
            wsol_ata,
            &lp_pubkey,
            &lp_pubkey,
            &[],
        )
        .map_err(|e| anyhow::anyhow!("Failed to build close_account instruction: {e}"))?;

        program
            .request()
            .instruction(close_ix)
            .signer(signer.clone())
            .send()
            .await
            .context("Failed to close wSOL ATA")?;
    }

    let required_native = swap_amount
        .checked_add(FEE_RESERVE_LAMPORTS)
        .context("SOL swap amount plus fee reserve overflowed")?;
    let native_balance = wait_for_native_balance_at_least(program, &lp_pubkey, required_native)
        .await
        .context("Failed to confirm native SOL balance before Jupiter swap")?;

    info!(
        event.name = "jupiter_native_sol_input_ready",
        wallet.native_sol.lamports = native_balance,
        rebalance.swap_input_requested.raw = swap_amount,
        wallet.native_sol_fee_reserve.lamports = FEE_RESERVE_LAMPORTS,
    );

    ensure!(
        native_balance >= required_native,
        "Insufficient native SOL for Jupiter swap: balance={} required={}",
        native_balance,
        required_native
    );

    Ok(())
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

async fn read_swap_input_balance(
    program: &Program<Arc<Keypair>>,
    input_mint: &Pubkey,
    input_ata: &Pubkey,
    owner: &Pubkey,
) -> anyhow::Result<u64> {
    if is_native_sol_mint(input_mint) {
        let native_balance = program
            .rpc()
            .get_balance(owner)
            .await
            .context("Failed to read LP native SOL balance")?
            .saturating_sub(FEE_RESERVE_LAMPORTS);
        let wsol_balance = read_ata_balance_or_zero(program, input_ata).await?;
        return Ok(native_balance.saturating_add(wsol_balance));
    }

    read_ata_balance_or_zero(program, input_ata).await
}

async fn wait_for_swap_input_balance(
    program: &Program<Arc<Keypair>>,
    input_mint: &Pubkey,
    input_ata: &Pubkey,
    owner: &Pubkey,
    expected_amount: u64,
) -> anyhow::Result<u64> {
    let mut last_balance = 0;
    for attempt in 0..BALANCE_POLL_ATTEMPTS {
        last_balance = read_swap_input_balance(program, input_mint, input_ata, owner).await?;
        if last_balance >= expected_amount {
            return Ok(last_balance);
        }
        if attempt + 1 < BALANCE_POLL_ATTEMPTS {
            sleep(BALANCE_POLL_DELAY).await;
        }
    }
    Ok(last_balance)
}

async fn wait_for_ata_balance_at_least(
    program: &Program<Arc<Keypair>>,
    token_account: &Pubkey,
    expected_amount: u64,
) -> anyhow::Result<u64> {
    let mut last_balance = 0;
    for attempt in 0..BALANCE_POLL_ATTEMPTS {
        last_balance = read_ata_balance_or_zero(program, token_account).await?;
        if last_balance >= expected_amount {
            return Ok(last_balance);
        }
        if attempt + 1 < BALANCE_POLL_ATTEMPTS {
            sleep(BALANCE_POLL_DELAY).await;
        }
    }
    Ok(last_balance)
}

async fn wait_for_native_balance_at_least(
    program: &Program<Arc<Keypair>>,
    owner: &Pubkey,
    expected_amount: u64,
) -> anyhow::Result<u64> {
    let mut last_balance = 0;
    for attempt in 0..BALANCE_POLL_ATTEMPTS {
        last_balance = program
            .rpc()
            .get_balance(owner)
            .await
            .context("Failed to read native SOL balance")?;
        if last_balance >= expected_amount {
            return Ok(last_balance);
        }
        if attempt + 1 < BALANCE_POLL_ATTEMPTS {
            sleep(BALANCE_POLL_DELAY).await;
        }
    }
    Ok(last_balance)
}

async fn prepare_deposit_balance(
    program: &Program<Arc<Keypair>>,
    mint: &Pubkey,
    owner: &Pubkey,
    requested_amount: u64,
    signer: Arc<Keypair>,
) -> anyhow::Result<u64> {
    if requested_amount == 0 {
        return Ok(0);
    }

    let token_program = get_token_program_id(program, mint).await?;
    let ata = get_associated_token_address_with_program_id(owner, mint, &token_program);

    if is_native_sol_mint(mint) {
        return prepare_wsol_deposit_balance(program, &ata, requested_amount, signer).await;
    }

    let available = wait_for_ata_balance_at_least(program, &ata, requested_amount).await?;
    if available < requested_amount {
        warn!(
            event.name = "rebalance_deposit_capped",
            token.mint = %mint,
            rebalance.deposit_requested.raw = requested_amount,
            rebalance.deposit_available.raw = available,
        );
    }
    Ok(available.min(requested_amount))
}

async fn prepare_wsol_deposit_balance(
    program: &Program<Arc<Keypair>>,
    wsol_ata: &Pubkey,
    requested_amount: u64,
    signer: Arc<Keypair>,
) -> anyhow::Result<u64> {
    let current_wsol = read_ata_balance_or_zero(program, wsol_ata).await?;
    if current_wsol >= requested_amount {
        return Ok(requested_amount);
    }

    let owner = signer.pubkey();
    let deficit = requested_amount - current_wsol;
    let required_native = deficit
        .checked_add(FEE_RESERVE_LAMPORTS)
        .context("wSOL wrap amount plus fee reserve overflowed")?;
    let native_balance = wait_for_native_balance_at_least(program, &owner, required_native).await?;
    let wrap_amount = native_balance
        .saturating_sub(FEE_RESERVE_LAMPORTS)
        .min(deficit);

    if wrap_amount == 0 {
        warn!(
            event.name = "rebalance_wsol_deposit_capped",
            rebalance.deposit_requested.raw = requested_amount,
            rebalance.deposit_available.raw = current_wsol,
            wallet.native_sol.lamports = native_balance,
        );
        return Ok(current_wsol.min(requested_amount));
    }

    let native_mint = native_sol_mint();
    let create_ata_ix =
        anchor_spl::associated_token::spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &owner,
            &owner,
            &native_mint,
            &anchor_spl::token::spl_token::ID,
        );
    let transfer_ix = system_instruction::transfer(&owner, wsol_ata, wrap_amount);
    let sync_ix = anchor_spl::token::spl_token::instruction::sync_native(
        &anchor_spl::token::spl_token::ID,
        wsol_ata,
    )
    .map_err(|e| anyhow::anyhow!("Failed to build sync_native instruction: {e}"))?;

    program
        .request()
        .instruction(create_ata_ix)
        .instruction(transfer_ix)
        .instruction(sync_ix)
        .signer(signer)
        .send()
        .await
        .context("Failed to wrap native SOL for add_liquidity")?;

    let expected_wsol = current_wsol.saturating_add(wrap_amount);
    let final_wsol = wait_for_ata_balance_at_least(program, wsol_ata, expected_wsol).await?;
    if final_wsol < requested_amount {
        warn!(
            event.name = "rebalance_wsol_deposit_capped_after_wrap",
            wallet.wsol_ata.address = %wsol_ata,
            rebalance.deposit_requested.raw = requested_amount,
            rebalance.deposit_available.raw = final_wsol,
        );
    }
    Ok(final_wsol.min(requested_amount))
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
}
