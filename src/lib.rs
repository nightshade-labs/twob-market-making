use std::sync::Arc;

use anchor_client::{Program, solana_sdk::signature::Keypair};
use anchor_lang::prelude::*;

pub mod accounts;
pub mod constants;
pub mod instructions;
pub mod state;

// Re-export commonly used types
pub use accounts::{AccountResolver, PdaResult};
pub use constants::*;
pub use instructions::*;
pub use state::{MarketState, fetch_liquidity_position, fetch_market_state};

declare_program!(twob_anchor);
use twob_anchor::accounts::{Bookkeeping, LiquidityPosition, Market};

use crate::twob_anchor::accounts::Exits;

/// The TwoB Anchor program ID
pub const TWOB_PROGRAM_ID: &str = "DkjFmy1YNDDDaXoy3ZvuCnpb294UDbpbT457gUyiFS5V";

/// Parse the program ID from the constant string
pub fn program_id() -> anchor_lang::prelude::Pubkey {
    TWOB_PROGRAM_ID.parse().expect("Invalid program ID")
}

pub async fn get_token_program_id(
    program: &Program<Arc<Keypair>>,
    mint: &Pubkey,
) -> anyhow::Result<Pubkey> {
    let account = program
        .rpc()
        .get_account(mint)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to fetch mint account: {}", e))?;

    Ok(account.owner)
}

pub struct LiquidityPositionBalances {
    pub base_balance: u64,
    pub quote_balance: u64,
    pub base_debt: u64,
    pub quote_debt: u64,
}
pub async fn get_liquidity_position_balances(
    program: &Program<Arc<Keypair>>,
    liquidity_position: LiquidityPosition,
    bookkeeping: Bookkeeping,
    market: Market,
    current_slot: u64,
) -> LiquidityPositionBalances {
    let resolver = AccountResolver::new(twob_anchor::ID);
    let market_pda = resolver.market_pda(market.id);

    let inactive_slots =
        bookkeeping.slots_without_trade - liquidity_position.slots_without_trade_snapshot;

    // For debugging since sometimes it failed because substraction overflow
    println!(
        "Base balances {:?} | Base debt {:?} | Quote balances {:?} | Quote debt {:?}",
        liquidity_position.base_balance,
        liquidity_position.base_debt,
        liquidity_position.quote_balance,
        liquidity_position.quote_debt
    );

    println!("Current slot {}", current_slot);
    println!(
        "LP last update slot {}",
        liquidity_position.last_update_slot
    );
    println!("Inactive slots {}", inactive_slots);
    // Base token outflow since last update slot
    let accumulated_base_outflow = BOOKKEEPING_PRECISION_FACTOR
        * (current_slot - liquidity_position.last_update_slot - inactive_slots) as u128
        * liquidity_position.base_flow_u64 as u128;

    // Quote token outflow since last update slot
    let accumulated_quote_outflow = BOOKKEEPING_PRECISION_FACTOR
        * (current_slot - liquidity_position.last_update_slot - inactive_slots) as u128
        * liquidity_position.quote_flow_u64 as u128;

    // Cacluclation token inflow is a bit tricky since we only have data up to bookkeeping last update slot.
    // We need to go from there till current slot and loop through the exits accounts to adapt market flows
    // First calculate correct base_per_quote and use that then.
    let base_per_quote = {
        let mut base_per_quote = bookkeeping.base_per_quote;
        let mut market_base_flow = market.base_flow;
        let mut market_quote_flow = market.quote_flow;
        let mut last_update_slot = bookkeeping.last_update_slot;

        let last_update_index = last_update_slot / ARRAY_LENGTH / market.end_slot_interval;
        let current_slot_index = current_slot / ARRAY_LENGTH / market.end_slot_interval;

        // This will sum up all prices up to the last index of the last exits account
        // After that we still need to sum up prices from that point until the current slot
        for exits_index in last_update_index..=current_slot_index {
            let exits_account_pda = resolver.exits_pda(&market_pda.address(), exits_index);

            let exits_account = program.account::<Exits>(exits_account_pda.address()).await;

            let start_index = if exits_index == last_update_index {
                (bookkeeping.last_update_slot
                    - last_update_index * market.end_slot_interval * ARRAY_LENGTH)
                    / market.end_slot_interval
                    + 1
            } else {
                0
            };

            let end_index = if exits_index == current_slot_index {
                (current_slot - current_slot_index * market.end_slot_interval * ARRAY_LENGTH)
                    / market.end_slot_interval
            } else {
                ARRAY_LENGTH - 1
            };

            for i in start_index..=end_index {
                let slot = i * market.end_slot_interval
                    + exits_index * market.end_slot_interval * ARRAY_LENGTH;
                let slot_diff = slot - last_update_slot;
                last_update_slot = slot;

                if market_base_flow == 0 || market_quote_flow == 0 {
                    continue;
                }
                base_per_quote += BOOKKEEPING_PRECISION_FACTOR * market_base_flow
                    / market_quote_flow
                    * slot_diff as u128;

                let base_exit = match exits_account {
                    Ok(exits) => exits.base_exits[i as usize],
                    Err(_) => 0,
                };
                let quote_exit = match exits_account {
                    Ok(exits) => exits.quote_exits[i as usize],
                    Err(_) => 0,
                };
                market_base_flow -= base_exit;
                market_quote_flow -= quote_exit;
            }

            // After we went to all exits account we need to sum up prices up to current slot
            if exits_index == current_slot_index {
                let slot_diff = current_slot - last_update_slot;
                if market_base_flow == 0 || market_quote_flow == 0 {
                    continue;
                }
                base_per_quote += BOOKKEEPING_PRECISION_FACTOR * market_base_flow
                    / market_quote_flow
                    * slot_diff as u128;
            }
        }
        base_per_quote
    };

    let quote_per_base = {
        let mut quote_per_base = bookkeeping.quote_per_base;
        let mut market_base_flow = market.base_flow;
        let mut market_quote_flow = market.quote_flow;
        let mut last_update_slot = bookkeeping.last_update_slot;

        let last_update_index = last_update_slot / ARRAY_LENGTH / market.end_slot_interval;
        let current_slot_index = current_slot / ARRAY_LENGTH / market.end_slot_interval;

        for exits_index in last_update_index..=current_slot_index {
            let exits_account_pda = resolver.exits_pda(&market_pda.address(), exits_index);
            let exits_account = program.account::<Exits>(exits_account_pda.address()).await;

            let start_index = if exits_index == last_update_index {
                (bookkeeping.last_update_slot
                    - last_update_index * market.end_slot_interval * ARRAY_LENGTH)
                    / market.end_slot_interval
                    + 1
            } else {
                0
            };

            let end_index = if exits_index == current_slot_index {
                (current_slot - current_slot_index * market.end_slot_interval * ARRAY_LENGTH)
                    / market.end_slot_interval
            } else {
                ARRAY_LENGTH - 1
            };

            for i in start_index..=end_index {
                let slot = i * market.end_slot_interval
                    + exits_index * market.end_slot_interval * ARRAY_LENGTH;
                let slot_diff = slot - last_update_slot;
                last_update_slot = slot;
                if market_base_flow == 0 || market_quote_flow == 0 {
                    continue;
                }
                quote_per_base += BOOKKEEPING_PRECISION_FACTOR * market_quote_flow
                    / market_base_flow
                    * slot_diff as u128;

                let base_exit = match exits_account {
                    Ok(exits) => exits.base_exits[i as usize],
                    Err(_) => 0,
                };
                let quote_exit = match exits_account {
                    Ok(exits) => exits.quote_exits[i as usize],
                    Err(_) => 0,
                };
                market_base_flow -= base_exit;
                market_quote_flow -= quote_exit;
            }

            // After we went to all exits account we need to sum up prices up to current slot
            if exits_index == current_slot_index {
                let slot_diff = current_slot - last_update_slot;
                if market_base_flow == 0 || market_quote_flow == 0 {
                    continue;
                }
                quote_per_base += BOOKKEEPING_PRECISION_FACTOR * market_quote_flow
                    / market_base_flow
                    * slot_diff as u128;
            }
        }
        quote_per_base
    };

    // Base token inflow since last update slot
    let accumulated_base_inflow = (base_per_quote - liquidity_position.base_per_quote_snapshot)
        * liquidity_position.quote_flow_u64 as u128;

    // Quote token inflow since last update slot
    let accumulated_quote_inflow = (quote_per_base - liquidity_position.quote_per_base_snapshot)
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
