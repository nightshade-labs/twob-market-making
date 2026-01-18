use twob_market_making::{
    LiquidityPositionBalances, MarketState, twob_anchor::accounts::LiquidityPosition,
};

use crate::price::PriceData;

#[derive(Debug, Clone)]
pub struct OptimalQuote {
    pub base_flow: u64,
    pub quote_flow: u64,
}

/// Calculate the optimal quote based on external price and position inventory.
///
/// TODO: Replace this placeholder with your actual formula.
pub fn calculate_optimal_quote(
    _price: &PriceData,
    _position: &LiquidityPosition,
    _market_state: &MarketState,
    _balances: &LiquidityPositionBalances,
) -> OptimalQuote {
    // Placeholder: returns current flows unchanged
    // Replace with your formula that considers:
    // - External price (price.price)
    // - Position balances (balances.base_balance, balances.quote_balance)
    // - Market state (market_state.market.base_flow, market_state.market.quote_flow)
    todo!("Implement optimal quote calculation formula")
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
