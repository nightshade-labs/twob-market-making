use std::sync::Arc;

use anchor_client::{Program, solana_sdk::signature::Keypair};
use twob_market_making::LiquidityPositionBalances;

use crate::price::PriceData;

/// Check if inventory needs rebalancing based on price and current balances.
pub fn needs_rebalance(
    price: &PriceData,
    balances: &LiquidityPositionBalances,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    threshold_bps: u64,
) -> bool {
    if price.price <= 0.0 {
        eprintln!("Oracle price is non-positive ({}), skipping rebalance", price.price);
        return false;
    }

    if balances.base_balance == 0 || balances.quote_balance == 0 {
        return true;
    }

    let base_ui =
        balances.base_balance as f64 / 10f64.powi(i32::from(base_token_decimals));
    let quote_ui =
        balances.quote_balance as f64 / 10f64.powi(i32::from(quote_token_decimals));

    if base_ui <= 0.0 || quote_ui <= 0.0 {
        return true;
    }

    let inventory_price = quote_ui / base_ui;
    let deviation_bps = ((inventory_price - price.price).abs() / price.price) * 10_000.0;

    println!(
        "Rebalance check: inventory_price={} oracle_price={} deviation_bps={:.2} threshold_bps={}",
        inventory_price, price.price, deviation_bps, threshold_bps
    );

    deviation_bps > threshold_bps as f64
}

/// Execute the rebalancing operation.
///
/// TODO: Replace with actual rebalancing logic.
pub async fn execute_rebalance(
    _program: &Program<Arc<Keypair>>,
    _market_id: u64,
    _price: &PriceData,
    _balances: &LiquidityPositionBalances,
    _liquidity_provider: Arc<Keypair>,
) -> anyhow::Result<()> {
    anyhow::bail!("execute_rebalance is not implemented yet")
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
}
