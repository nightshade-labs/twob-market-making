use std::sync::Arc;

use anchor_client::{
    Program,
    solana_sdk::{signature::Keypair, signer::Signer},
};
use anchor_lang::solana_program::program_pack::Pack;
use anchor_spl::{
    associated_token::get_associated_token_address_with_program_id,
    token::spl_token::state::Account as SplTokenAccount,
};
use anyhow::{Context, ensure};
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
        if deviation_bps > threshold_bps as f64 { "needed" } else { "ok" },
    );

    deviation_bps > threshold_bps as f64
}

/// Execute the rebalancing operation.
///
/// TODO: Replace with actual rebalancing logic.
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

    let Some(uncapped_plan) =
        plan_rebalance(price, balances, base_token_decimals, quote_token_decimals)
    else {
        println!("[rebalance] skipping — computed withdraw amount rounds to zero");
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
            balances.base_balance,
            balances.quote_balance,
            current_base_flow,
            current_quote_flow,
        );
        return Ok(RebalanceOutcome::Skipped);
    };
    println!(
        "[rebalance] capped {} withdraw: base={} quote={}",
        plan.direction.label(),
        plan.withdraw_base_lamports,
        plan.withdraw_quote_lamports,
    );
    let withdraw_reference_index =
        oracle_flow_reference_index(program, market_state.market.end_slot_interval).await?;

    println!(
        "Executing rebalance {}: withdraw_base={} withdraw_quote={}",
        plan.direction.label(),
        plan.withdraw_base_lamports,
        plan.withdraw_quote_lamports
    );
    log_rebalance_transfer_accounts(
        program,
        market_id,
        market_state,
        liquidity_provider.pubkey(),
        plan,
    )
    .await;

    let executed_plan = execute_exact_withdraw_liquidity(
        program,
        market_id,
        withdraw_reference_index,
        liquidity_provider.clone(),
        plan,
    )
    .await
    .context("Failed to withdraw liquidity for rebalance")?;

    let (input_mint, output_mint) = match executed_plan.direction {
        SwapDirection::BaseToQuote => (
            market_state.market.base_mint,
            market_state.market.quote_mint,
        ),
        SwapDirection::QuoteToBase => (
            market_state.market.quote_mint,
            market_state.market.base_mint,
        ),
    };

    let swap_execution = JupiterUltraClient::new(http_client, jupiter_config)
        .swap_exact_in(
            liquidity_provider.clone(),
            input_mint,
            output_mint,
            executed_plan.input_amount(),
        )
        .await
        .with_context(|| {
            format!(
                "Failed to execute Jupiter Ultra swap {}",
                executed_plan.direction.label()
            )
        })?;

    let (deposit_base_lamports, deposit_quote_lamports) = match executed_plan.direction {
        SwapDirection::BaseToQuote => (
            executed_plan
                .withdraw_base_lamports
                .saturating_sub(swap_execution.input_consumed),
            swap_execution.output_received,
        ),
        SwapDirection::QuoteToBase => (
            swap_execution.output_received,
            executed_plan
                .withdraw_quote_lamports
                .saturating_sub(swap_execution.input_consumed),
        ),
    };

    ensure!(
        deposit_base_lamports > 0 || deposit_quote_lamports > 0,
        "Jupiter swap produced no liquidity to add back after rebalance"
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
) -> Option<RebalancePlan> {
    if !price.price.is_finite() || price.price <= 0.0 {
        return None;
    }

    let base_ui = balances.base_balance as f64 / 10f64.powi(i32::from(base_token_decimals));
    let quote_ui = balances.quote_balance as f64 / 10f64.powi(i32::from(quote_token_decimals));
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
        let withdraw_quote_lamports =
            ui_amount_to_lamports(quote_excess_ui / 2.0, quote_token_decimals);
        if withdraw_quote_lamports > 0 {
            return Some(RebalancePlan {
                direction: SwapDirection::QuoteToBase,
                withdraw_base_lamports: 0,
                withdraw_quote_lamports,
            });
        }
        println!(
            "[rebalance] plan: quote excess {:.6} rounds to 0 lamports — no plan",
            quote_excess_ui / 2.0,
        );
    }

    let base_excess_ui = (base_ui - quote_ui / price.price).max(0.0);
    let withdraw_base_lamports = ui_amount_to_lamports(base_excess_ui / 2.0, base_token_decimals);
    if withdraw_base_lamports > 0 {
        return Some(RebalancePlan {
            direction: SwapDirection::BaseToQuote,
            withdraw_base_lamports,
            withdraw_quote_lamports: 0,
        });
    }

    println!(
        "[rebalance] plan: base excess {:.6} rounds to 0 lamports — no plan",
        base_excess_ui / 2.0,
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

        let plan = plan_rebalance(&price, &balances, 9, 6).unwrap();
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

        let plan = plan_rebalance(&price, &balances, 9, 6).unwrap();
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

        assert!(plan_rebalance(&price, &balances, 9, 6).is_none());
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
        assert_eq!(capped.withdraw_base_lamports, 880);
        assert_eq!(capped.withdraw_quote_lamports, 0);
    }
}
