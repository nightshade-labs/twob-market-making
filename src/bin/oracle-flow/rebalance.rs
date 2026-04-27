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
use twob_market_making::{
    ARRAY_LENGTH, AccountResolver, LIQUIDITY_AMPLIFICATION, LiquidityPositionBalances, MarketState,
    build_withdraw_liquidity_instruction, execute_add_liquidity, execute_withdraw_liquidity,
    get_token_program_id,
};

use crate::{
    config::JupiterConfig,
    jupiter::{JupiterUltraClient, SwapDirection},
    price::PriceData,
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
        eprintln!(
            "Oracle price is non-positive ({}), skipping rebalance",
            price.price
        );
        return false;
    }

    if balances.base_balance == 0 || balances.quote_balance == 0 {
        println!(
            "[rebalance] one side is zero (base={} quote={}) — rebalance needed",
            balances.base_balance, balances.quote_balance,
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

    println!(
        "[rebalance] inventory_price={:.6} oracle_price={:.6} deviation={:.2} bps threshold={} bps — {}",
        inventory_price,
        price.price,
        deviation_bps,
        threshold_bps,
        if deviation_bps > threshold_bps as f64 {
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
) -> anyhow::Result<RebalanceOutcome> {
    if is_devnet {
        println!("Devnet detected. Skipping execute_rebalance (no-op).");
        return Ok(RebalanceOutcome::Skipped);
    }

    if balances.base_debt > 0 || balances.quote_debt > 0 {
        println!(
            "Skipping rebalance because the liquidity position is unhealthy: base_debt={} quote_debt={}",
            balances.base_debt, balances.quote_debt
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
        println!(
            "[rebalance] skipping — computed withdraw amount rounds to zero or is below minimum"
        );
        return Ok(RebalanceOutcome::Skipped);
    };
    println!(
        "[rebalance] planned {} withdraw: base={} quote={}",
        uncapped_plan.direction.label(),
        uncapped_plan.withdraw_base_lamports,
        uncapped_plan.withdraw_quote_lamports,
    );
    let Some(plan) = cap_rebalance_to_withdrawable(
        uncapped_plan,
        balances,
        current_base_flow,
        current_quote_flow,
    ) else {
        println!(
            "[rebalance] skipping — no withdrawable liquidity after capping to available balance \
             (base_balance={} quote_balance={} base_flow={} quote_flow={})",
            balances.base_balance, balances.quote_balance, current_base_flow, current_quote_flow,
        );
        return Ok(RebalanceOutcome::Skipped);
    };
    println!(
        "[rebalance] capped {} withdraw: base={} quote={}",
        plan.direction.label(),
        plan.withdraw_base_lamports,
        plan.withdraw_quote_lamports,
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

    println!(
        "Executing rebalance {}: target_swap={} wallet_input={} withdraw_base={} withdraw_quote={}",
        plan.direction.label(),
        target_swap_amount,
        existing_input_balance,
        withdraw_plan.withdraw_base_lamports,
        withdraw_plan.withdraw_quote_lamports
    );
    log_rebalance_transfer_accounts(
        program,
        market_id,
        market_state,
        liquidity_provider.pubkey(),
        withdraw_plan,
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
        .await
        .context("Failed to withdraw liquidity for rebalance")?;
    } else {
        println!("[rebalance] using wallet input balance; no additional withdraw needed");
    }

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

    println!(
        "[jupiter] input balance after withdrawal: target={} actual={} using={}",
        target_swap_amount, actual_input_balance, swap_amount,
    );

    if swap_amount == 0 {
        if withdraw_amount > 0 {
            anyhow::bail!("withdraw succeeded but no Jupiter input balance is visible");
        }
        println!("[rebalance] no Jupiter input balance available — skipping swap");
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

    let swap_execution = JupiterUltraClient::new(http_client, jupiter_config)
        .swap_exact_in(
            liquidity_provider.clone(),
            input_mint,
            output_mint,
            swap_amount,
        )
        .await
        .with_context(|| {
            format!(
                "Failed to execute Jupiter Ultra swap {}",
                plan.direction.label()
            )
        })?;

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

    let add_reference_index =
        oracle_flow_reference_index(program, market_state.market.end_slot_interval).await?;
    execute_add_liquidity(
        program,
        market_id,
        deposit_base_lamports,
        deposit_quote_lamports,
        add_reference_index,
        liquidity_provider,
    )
    .await
    .context("Failed to add rebalanced liquidity back to the position")?;

    println!(
        "Rebalance swap completed: signature={:?} input_consumed={} output_received={} deposit_base={} deposit_quote={}",
        swap_execution.signature,
        swap_execution.input_consumed,
        swap_execution.output_received,
        deposit_base_lamports,
        deposit_quote_lamports
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
                eprintln!(
                    "Withdraw debug: failed to fetch base token program: {:#}",
                    error
                );
                return;
            }
        };
    let quote_token_program =
        match get_token_program_id(program, &market_state.market.quote_mint).await {
            Ok(program_id) => program_id,
            Err(error) => {
                eprintln!(
                    "Withdraw debug: failed to fetch quote token program: {:#}",
                    error
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

    println!(
        "Withdraw debug: market_pda={} direction={} base_token_program={} quote_token_program={}",
        market_pda,
        plan.direction.label(),
        base_token_program,
        quote_token_program
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
            eprintln!(
                "Withdraw debug {}: failed to fetch account {}: {}",
                label, token_account, error
            );
            return;
        }
    };

    let token_state = match SplTokenAccount::unpack(&account.data) {
        Ok(state) => state,
        Err(error) => {
            eprintln!(
                "Withdraw debug {}: failed to decode token account {}: {}",
                label, token_account, error
            );
            return;
        }
    };

    println!(
        "Withdraw debug {}: pubkey={} owner_program={} token_owner={} mint={} amount={} lamports={} is_native={}",
        label,
        token_account,
        account.owner,
        token_state.owner,
        token_state.mint,
        token_state.amount,
        account.lamports,
        token_state.is_native()
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
        println!(
            "[rebalance] plan: base depleted — swapping half of quote to base \
             (withdraw_quote={})",
            withdraw_quote_lamports,
        );
        return Some(RebalancePlan {
            direction: SwapDirection::QuoteToBase,
            withdraw_base_lamports: 0,
            withdraw_quote_lamports,
        });
    }
    if balances.quote_balance == 0 && balances.base_balance > 0 {
        let withdraw_base_lamports = balances.base_balance / 2;
        println!(
            "[rebalance] plan: quote depleted — swapping half of base to quote \
             (withdraw_base={})",
            withdraw_base_lamports,
        );
        return Some(RebalancePlan {
            direction: SwapDirection::BaseToQuote,
            withdraw_base_lamports,
            withdraw_quote_lamports: 0,
        });
    }

    if !base_ui.is_finite() || !quote_ui.is_finite() || base_ui <= 0.0 || quote_ui <= 0.0 {
        println!(
            "[rebalance] plan: cannot plan — invalid UI amounts (base={} quote={})",
            base_ui, quote_ui,
        );
        return None;
    }

    println!(
        "[rebalance] plan: base={:.6} quote={:.6} oracle={:.6} \
         ideal_quote={:.6} quote_excess={:.6} base_excess={:.6}",
        base_ui,
        quote_ui,
        price.price,
        base_ui * price.price,
        (quote_ui - base_ui * price.price).max(0.0),
        (base_ui - quote_ui / price.price).max(0.0),
    );

    let quote_excess_ui = (quote_ui - base_ui * price.price).max(0.0);
    if quote_excess_ui > 0.0 {
        let withdraw_quote_ui = quote_excess_ui / 2.0;
        let withdraw_value_usd = withdraw_quote_ui; // quote is USD-denominated
        if withdraw_value_usd < min_rebalance_value_usd {
            println!(
                "[rebalance] plan: quote withdraw ${:.4} is below minimum ${:.2} — no plan",
                withdraw_value_usd, min_rebalance_value_usd,
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
        println!(
            "[rebalance] plan: quote excess {:.6} rounds to 0 lamports — no plan",
            withdraw_quote_ui,
        );
    }

    let base_excess_ui = (base_ui - quote_ui / price.price).max(0.0);
    let withdraw_base_ui = base_excess_ui / 2.0;
    let withdraw_value_usd = withdraw_base_ui * price.price;
    if withdraw_base_ui > 0.0 && withdraw_value_usd < min_rebalance_value_usd {
        println!(
            "[rebalance] plan: base withdraw ${:.4} is below minimum ${:.2} — no plan",
            withdraw_value_usd, min_rebalance_value_usd,
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

    println!(
        "[rebalance] plan: base excess {:.6} rounds to 0 lamports — no plan",
        withdraw_base_ui,
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
        println!(
            "[jupiter] unwrapping {} wSOL lamports from {} before swap",
            wsol_balance, wsol_ata,
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

    println!(
        "[jupiter] native SOL wallet: {} lamports (swap needs {} + {} fee reserve)",
        native_balance, swap_amount, FEE_RESERVE_LAMPORTS,
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
        eprintln!(
            "[rebalance] capping deposit for mint {}: requested={} available={}",
            mint, requested_amount, available,
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
        eprintln!(
            "[rebalance] capping wSOL deposit: requested={} available={}",
            requested_amount, current_wsol,
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
        eprintln!(
            "[rebalance] capping wSOL deposit after wrap: requested={} available={}",
            requested_amount, final_wsol,
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
