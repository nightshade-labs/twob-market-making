use twob_market_making::FLOW_PRECISION;
use twob_market_making::{
    LiquidityPositionBalances, MarketState, twob_anchor::accounts::LiquidityPosition,
};

use crate::price::PriceData;

#[derive(Debug, Clone)]
pub struct OptimalQuote {
    pub base_flow: u64,
    pub quote_flow: u64,
}

/// Calculate the optimal quote based on oracle price and inventory-implied price.
pub fn calculate_optimal_quote(
    price: &PriceData,
    position: &LiquidityPosition,
    market_state: &MarketState,
    balances: &LiquidityPositionBalances,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    weight: f64,
) -> OptimalQuote {
    let fallback = OptimalQuote {
        base_flow: position.base_flow_u64.max(1),
        quote_flow: position.quote_flow_u64.max(1),
    };

    let oracle_price = price.price;
    if !oracle_price.is_finite() || oracle_price <= 0.0 {
        eprintln!(
            "Oracle price is invalid ({}). Keeping current quote.",
            oracle_price
        );
        return fallback;
    }

    let lp_price = liquidity_position_price(balances, base_token_decimals, quote_token_decimals);
    let market_price = market_price_excluding_position(
        position,
        market_state,
        base_token_decimals,
        quote_token_decimals,
    );

    let normalized_weight = sanitize_weight(weight);
    let target_quote_price = match lp_price {
        Some(position_price) => {
            // Weighted blend between oracle and inventory-implied price.
            (oracle_price + normalized_weight * position_price) / (1.0 + normalized_weight)
        }
        None => oracle_price,
    };

    let Some(inventory_price) = lp_price else {
        eprintln!("Liquidity-position price is unavailable. Keeping current quote.");
        return fallback;
    };

    let Some(target_flows) = compute_target_flows(
        balances,
        target_quote_price,
        inventory_price,
        base_token_decimals,
        quote_token_decimals,
    ) else {
        eprintln!("Failed to compute inventory-constrained flows. Keeping current quote.");
        return fallback;
    };

    println!(
        "Quote calc: oracle_price={} lp_price={:?} market_price={:?} weight={} target_quote_price={} inventory_price={} target_base_flow={} target_quote_flow={}",
        oracle_price,
        lp_price,
        market_price,
        normalized_weight,
        target_quote_price,
        inventory_price,
        target_flows.base_flow,
        target_flows.quote_flow
    );

    target_flows
}

/// Check if the current quote deviates from optimal by more than the threshold.
///
/// Returns true if an update is needed.
pub fn should_update_quote(
    current_base_flow: u64,
    current_quote_flow: u64,
    optimal: &OptimalQuote,
    threshold_bps: u64,
) -> bool {
    if current_base_flow == 0 || current_quote_flow == 0 {
        return optimal.base_flow > 0 && optimal.quote_flow > 0;
    }

    if optimal.base_flow == 0 || optimal.quote_flow == 0 {
        return false;
    }

    // Compare ratios: current_base/current_quote vs optimal_base/optimal_quote
    // Using cross multiplication to avoid floating point: a/b vs c/d => a*d vs b*c
    let current_ratio = current_base_flow as u128 * optimal.quote_flow as u128;
    let optimal_ratio = optimal.base_flow as u128 * current_quote_flow as u128;

    let (larger, smaller) = if current_ratio > optimal_ratio {
        (current_ratio, optimal_ratio)
    } else {
        (optimal_ratio, current_ratio)
    };

    // deviation_bps = (larger - smaller) * 10000 / smaller
    let deviation_bps = (larger - smaller) * 10_000 / smaller;

    deviation_bps > threshold_bps as u128
}

fn sanitize_weight(weight: f64) -> f64 {
    if weight.is_finite() && weight >= 0.0 {
        weight
    } else {
        0.0
    }
}

fn liquidity_position_price(
    balances: &LiquidityPositionBalances,
    base_token_decimals: u8,
    quote_token_decimals: u8,
) -> Option<f64> {
    if balances.base_balance == 0 || balances.quote_balance == 0 {
        return None;
    }

    let base_ui = balances.base_balance as f64 / 10f64.powi(i32::from(base_token_decimals));
    let quote_ui = balances.quote_balance as f64 / 10f64.powi(i32::from(quote_token_decimals));
    if !base_ui.is_finite() || !quote_ui.is_finite() || base_ui <= 0.0 || quote_ui <= 0.0 {
        return None;
    }

    Some(quote_ui / base_ui)
}

fn market_price_excluding_position(
    position: &LiquidityPosition,
    market_state: &MarketState,
    base_token_decimals: u8,
    quote_token_decimals: u8,
) -> Option<f64> {
    let own_base_flow = position.base_flow_u64 as u128 * FLOW_PRECISION;
    let own_quote_flow = position.quote_flow_u64 as u128 * FLOW_PRECISION;

    if market_state.market.base_flow <= own_base_flow || market_state.market.quote_flow <= own_quote_flow
    {
        return None;
    }

    let market_base_flow = market_state.market.base_flow - own_base_flow;
    let market_quote_flow = market_state.market.quote_flow - own_quote_flow;
    if market_base_flow == 0 || market_quote_flow == 0 {
        return None;
    }

    let native_ratio = market_quote_flow as f64 / market_base_flow as f64;
    if !native_ratio.is_finite() || native_ratio <= 0.0 {
        return None;
    }

    let base_scale = 10f64.powi(i32::from(base_token_decimals));
    let quote_scale = 10f64.powi(i32::from(quote_token_decimals));
    Some(native_ratio * base_scale / quote_scale)
}

fn compute_target_flows(
    balances: &LiquidityPositionBalances,
    target_quote_price: f64,
    inventory_quote_price: f64,
    base_token_decimals: u8,
    quote_token_decimals: u8,
) -> Option<OptimalQuote> {
    if balances.base_balance == 0 || balances.quote_balance == 0 {
        return None;
    }
    if !target_quote_price.is_finite()
        || target_quote_price <= 0.0
        || !inventory_quote_price.is_finite()
        || inventory_quote_price <= 0.0
    {
        return None;
    }

    // If target price is above inventory-implied price, quote side is limiting.
    // Keep quote flow at max available and solve base from price.
    if target_quote_price >= inventory_quote_price {
        let quote_flow = balances.quote_balance;
        let base_flow = base_flow_for_price(
            quote_flow,
            target_quote_price,
            base_token_decimals,
            quote_token_decimals,
        )?
        .clamp(1, balances.base_balance);

        return Some(OptimalQuote {
            base_flow,
            quote_flow,
        });
    }

    // If target price is below inventory-implied price, base side is limiting.
    // Keep base flow at max available and solve quote from price.
    let base_flow = balances.base_balance;
    let quote_flow = quote_flow_for_price(
        base_flow,
        target_quote_price,
        base_token_decimals,
        quote_token_decimals,
    )?
    .clamp(1, balances.quote_balance);

    Some(OptimalQuote {
        base_flow,
        quote_flow,
    })
}

fn quote_flow_for_price(
    base_flow: u64,
    target_quote_price: f64,
    base_token_decimals: u8,
    quote_token_decimals: u8,
) -> Option<u64> {
    if base_flow == 0 || !target_quote_price.is_finite() || target_quote_price <= 0.0 {
        return None;
    }

    let base_scale = 10f64.powi(i32::from(base_token_decimals));
    let quote_scale = 10f64.powi(i32::from(quote_token_decimals));
    let quote_per_base_native = target_quote_price * quote_scale / base_scale;
    if !quote_per_base_native.is_finite() || quote_per_base_native <= 0.0 {
        return None;
    }

    let raw = (base_flow as f64) * quote_per_base_native;
    if !raw.is_finite() || raw <= 0.0 {
        return None;
    }

    Some(raw.floor().clamp(1.0, u64::MAX as f64) as u64)
}

fn base_flow_for_price(
    quote_flow: u64,
    target_quote_price: f64,
    base_token_decimals: u8,
    quote_token_decimals: u8,
) -> Option<u64> {
    if quote_flow == 0 || !target_quote_price.is_finite() || target_quote_price <= 0.0 {
        return None;
    }

    let base_scale = 10f64.powi(i32::from(base_token_decimals));
    let quote_scale = 10f64.powi(i32::from(quote_token_decimals));
    let base_per_quote_native = base_scale / (target_quote_price * quote_scale);
    if !base_per_quote_native.is_finite() || base_per_quote_native <= 0.0 {
        return None;
    }

    let raw = (quote_flow as f64) * base_per_quote_native;
    if !raw.is_finite() || raw <= 0.0 {
        return None;
    }

    Some(raw.floor().clamp(1.0, u64::MAX as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use twob_market_making::LiquidityPositionBalances;

    #[test]
    fn weighted_quote_price_is_oracle_dominant_with_small_weight() {
        let oracle = 100.0;
        let lp = 80.0;
        let weight = sanitize_weight(0.1);

        let blended = (oracle + weight * lp) / (1.0 + weight);
        assert!((blended - 98.1818181818).abs() < 1e-9);
    }

    #[test]
    fn quote_flow_conversion_respects_decimals() {
        // 1 SOL flow (1e9 lamports) at 84 USDC/SOL should be 84e6 micro-USDC flow.
        let quote_flow = quote_flow_for_price(1_000_000_000, 84.0, 9, 6).unwrap();
        assert_eq!(quote_flow, 84_000_000);
    }

    #[test]
    fn base_flow_conversion_respects_decimals() {
        // 100 USDC flow at 101 USDC/SOL should be ~0.990099009 SOL flow.
        let base_flow = base_flow_for_price(100_000_000, 101.0, 9, 6).unwrap();
        assert_eq!(base_flow, 990_099_009);
    }

    #[test]
    fn liquidity_position_price_uses_ui_units() {
        let balances = LiquidityPositionBalances {
            base_balance: 2_000_000_000, // 2 SOL
            quote_balance: 168_000_000,  // 168 USDC
            base_debt: 0,
            quote_debt: 0,
        };

        let lp_price = liquidity_position_price(&balances, 9, 6).unwrap();
        assert!((lp_price - 84.0).abs() < 1e-9);
    }

    #[test]
    fn target_above_inventory_anchors_quote_flow() {
        let balances = LiquidityPositionBalances {
            base_balance: 1_000_000_000, // 1 SOL
            quote_balance: 100_000_000,  // 100 USDC
            base_debt: 0,
            quote_debt: 0,
        };

        let optimal = compute_target_flows(&balances, 101.0, 100.0, 9, 6).unwrap();
        assert_eq!(optimal.quote_flow, 100_000_000);
        assert_eq!(optimal.base_flow, 990_099_009);
    }

    #[test]
    fn target_below_inventory_anchors_base_flow() {
        let balances = LiquidityPositionBalances {
            base_balance: 1_000_000_000, // 1 SOL
            quote_balance: 100_000_000,  // 100 USDC
            base_debt: 0,
            quote_debt: 0,
        };

        let optimal = compute_target_flows(&balances, 99.0, 100.0, 9, 6).unwrap();
        assert_eq!(optimal.base_flow, 1_000_000_000);
        assert_eq!(optimal.quote_flow, 99_000_000);
    }
}
