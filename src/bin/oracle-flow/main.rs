// On start up
// - check flow of liquidity position and store in memory
// - check inventory and calculate at which slot inventory should be rebalanced

// Listen to twob market price updates
// - recalculate at which slot inventory should be rebalanced

// When rebalance timer is triggered, rebalance

// Loop
// - get SOL/USDC binance price every second
// - create target price, which is a function of inventory and binance price
// - if target price differs to much from current flow price, update flows
// no need to recalculate rebalance slot as this will update market price which triggers this calculation

fn main() {}
