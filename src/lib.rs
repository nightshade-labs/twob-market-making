use anchor_lang::prelude::*;

pub mod accounts;
pub mod constants;
pub mod instructions;

// Re-export commonly used types
pub use accounts::{AccountResolver, PdaResult};
pub use constants::*;
pub use instructions::*;

declare_program!(twob_anchor);
use twob_anchor::accounts::{Bookkeeping, LiquidityPosition, Market};

/// The TwoB Anchor program ID
pub const TWOB_PROGRAM_ID: &str = "DkjFmy1YNDDDaXoy3ZvuCnpb294UDbpbT457gUyiFS5V";

/// Parse the program ID from the constant string
pub fn program_id() -> anchor_lang::prelude::Pubkey {
    TWOB_PROGRAM_ID.parse().expect("Invalid program ID")
}

pub struct LiquidityPositionBalances {
    pub base_balance: u64,
    pub quote_balance: u64,
    pub base_debt: u64,
    pub quote_debt: u64,
}

pub fn get_liquidity_position_balances(
    liquidity_position: LiquidityPosition,
    bookkeeping: Bookkeeping,
    market: Market,
    current_slot: u64,
) -> LiquidityPositionBalances {
    let inactive_slots =
        bookkeeping.slots_without_trade - liquidity_position.slots_without_trade_snapshot;

    // Base token outflow since last update slot
    let accumulated_base_outflow = BOOKKEEPING_PRECISION_FACTOR
        * (current_slot - liquidity_position.last_update_slot - inactive_slots) as u128
        * liquidity_position.base_flow_u64 as u128;

    // Base token inflow since last update slot
    let accumulated_base_inflow = (bookkeeping.base_per_quote
        + BOOKKEEPING_PRECISION_FACTOR / FLOW_PRECISION * market.base_flow / market.quote_flow
            * FLOW_PRECISION
            * (current_slot - bookkeeping.last_update_slot) as u128
        - liquidity_position.base_per_quote_snapshot)
        * liquidity_position.quote_flow_u64 as u128;

    // Quote token outflow since last update slot
    let accumulated_quote_outflow = BOOKKEEPING_PRECISION_FACTOR
        * (current_slot - liquidity_position.last_update_slot - inactive_slots) as u128
        * liquidity_position.quote_flow_u64 as u128;

    // Quote token inflow since last update slot
    let accumulated_quote_inflow = (bookkeeping.quote_per_base
        + BOOKKEEPING_PRECISION_FACTOR / FLOW_PRECISION * market.quote_flow / market.base_flow
            * FLOW_PRECISION
            * (current_slot - bookkeeping.last_update_slot) as u128
        - liquidity_position.quote_per_base_snapshot)
        * liquidity_position.base_flow_u64 as u128;

    let base_balance;
    let base_debt;
    if accumulated_base_outflow > liquidity_position.base_balance + accumulated_base_inflow {
        base_balance = 0;
        base_debt =
            (accumulated_base_outflow - liquidity_position.base_balance - accumulated_base_inflow)
                / BOOKKEEPING_PRECISION_FACTOR;
    } else {
        base_balance = (liquidity_position.base_balance + accumulated_base_inflow
            - accumulated_base_outflow)
            / BOOKKEEPING_PRECISION_FACTOR;
        base_debt = 0;
    }

    let quote_balance;
    let quote_debt;
    if accumulated_quote_outflow > liquidity_position.quote_balance + accumulated_quote_inflow {
        quote_balance = 0;
        quote_debt = (accumulated_quote_outflow
            - liquidity_position.quote_balance
            - accumulated_quote_inflow)
            / BOOKKEEPING_PRECISION_FACTOR;
    } else {
        quote_balance = (liquidity_position.quote_balance + accumulated_quote_inflow
            - accumulated_quote_outflow)
            / BOOKKEEPING_PRECISION_FACTOR;
        quote_debt = 0;
    }

    LiquidityPositionBalances {
        base_balance: base_balance as u64,
        quote_balance: quote_balance as u64,
        base_debt: base_debt as u64,
        quote_debt: quote_debt as u64,
    }
}
