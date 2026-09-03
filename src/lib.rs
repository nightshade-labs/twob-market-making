use std::sync::Arc;

use anchor_client::{
    Program,
    solana_sdk::{account::Account, commitment_config::CommitmentConfig, signature::Keypair},
};
use anchor_lang::prelude::*;
use anyhow::{Context, ensure};
use tracing::info;

pub mod accounts;
pub mod constants;
pub mod instructions;
pub mod state;

// Re-export commonly used types
pub use accounts::{AccountResolver, PdaResult};
pub use constants::*;
pub use instructions::*;
pub use state::{
    MarketState, fetch_liquidity_position, fetch_market_position_state, fetch_market_state,
};

declare_program!(twob_anchor);
use twob_anchor::accounts::{Bookkeeping, LiquidityPosition, Market};

use crate::twob_anchor::accounts::Exits;

/// The TwoB Anchor program ID
pub const TWOB_PROGRAM_ID: &str = "CCAmAqvza37EWzou7LoYCaGKzdJsCu1CLPMp3Wvx3Bc5";

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

const EXITS_BUCKET_COUNT: usize = ARRAY_LENGTH as usize;

struct ExitSchedule {
    base_exits: [u128; EXITS_BUCKET_COUNT],
    quote_exits: [u128; EXITS_BUCKET_COUNT],
    index: u64,
}

impl ExitSchedule {
    fn empty(index: u64) -> Self {
        Self {
            base_exits: [0; EXITS_BUCKET_COUNT],
            quote_exits: [0; EXITS_BUCKET_COUNT],
            index,
        }
    }
}
pub async fn get_liquidity_position_balances(
    program: &Program<Arc<Keypair>>,
    liquidity_position: LiquidityPosition,
    bookkeeping: Bookkeeping,
    market: Market,
    current_slot: u64,
) -> anyhow::Result<LiquidityPositionBalances> {
    let resolver = AccountResolver::new(twob_anchor::ID);
    let market_pda = resolver.market_pda(market.id);

    let elapsed_slots = current_slot
        .checked_sub(liquidity_position.last_update_slot)
        .context("Current slot precedes liquidity position last update slot")?;
    let raw_inactive = bookkeeping
        .slots_without_trade
        .checked_sub(liquidity_position.slots_without_trade_snapshot)
        .context("Bookkeeping inactive-slot counter precedes liquidity position snapshot")?;
    let active_slots = elapsed_slots
        .checked_sub(raw_inactive)
        .context("Inactive slots exceed elapsed liquidity position slots")?;

    info!(
        event.name = "liquidity_position_balance_slots",
        slot.current = current_slot,
        lp.last_update_slot = liquidity_position.last_update_slot,
        lp.elapsed_slots = elapsed_slots,
        lp.inactive_slots = raw_inactive,
        lp.active_slots = active_slots,
    );
    info!(
        event.name = "liquidity_position_on_chain_balances",
        position.base_balance.raw = liquidity_position.base_balance,
        position.base_debt.raw = liquidity_position.base_debt,
        position.quote_balance.raw = liquidity_position.quote_balance,
        position.quote_debt.raw = liquidity_position.quote_debt,
        position.base_flow.raw = liquidity_position.base_flow_u64,
        position.quote_flow.raw = liquidity_position.quote_flow_u64,
    );

    // Base token outflow since last update slot
    let accumulated_base_outflow = BOOKKEEPING_PRECISION_FACTOR
        .checked_mul(active_slots as u128)
        .and_then(|value| value.checked_mul(liquidity_position.base_flow_u64 as u128))
        .context("Accumulated base outflow overflowed")?;

    // Quote token outflow since last update slot
    let accumulated_quote_outflow = BOOKKEEPING_PRECISION_FACTOR
        .checked_mul(active_slots as u128)
        .and_then(|value| value.checked_mul(liquidity_position.quote_flow_u64 as u128))
        .context("Accumulated quote outflow overflowed")?;

    ensure!(
        market.end_slot_interval > 0,
        "Market end slot interval must be greater than zero"
    );
    ensure!(
        current_slot >= bookkeeping.last_update_slot,
        "Current slot precedes bookkeeping last update slot"
    );

    let exits_interval = market
        .end_slot_interval
        .checked_mul(ARRAY_LENGTH)
        .context("Exits interval overflowed")?;
    let last_update_index = bookkeeping.last_update_slot / exits_interval;
    let current_slot_index = current_slot / exits_interval;
    let exits_account_count = current_slot_index
        .checked_sub(last_update_index)
        .and_then(|value| value.checked_add(1))
        .context("Exits account range overflowed")?;
    let exits_account_capacity =
        usize::try_from(exits_account_count).context("Exits account range is too large")?;
    let mut exits_addresses = Vec::new();
    exits_addresses
        .try_reserve_exact(exits_account_capacity)
        .context("Failed to reserve Exits account range")?;

    for exits_index in last_update_index..=current_slot_index {
        let exits_account_pda = resolver.exits_pda(&market_pda.address(), exits_index);
        exits_addresses.push((exits_index, exits_account_pda.address()));
    }
    let exit_pubkeys = exits_addresses
        .iter()
        .map(|(_, address)| *address)
        .collect::<Vec<_>>();
    let exits_response = program
        .rpc()
        .get_multiple_accounts_with_commitment(&exit_pubkeys, CommitmentConfig::confirmed())
        .await
        .context("Failed to fetch Exits account range")?;
    ensure!(
        exits_response.context.slot >= current_slot,
        "Exits snapshot slot {} precedes core snapshot slot {}",
        exits_response.context.slot,
        current_slot,
    );
    ensure!(
        exits_response.value.len() == exits_addresses.len(),
        "Expected {} Exits account responses, received {}",
        exits_addresses.len(),
        exits_response.value.len(),
    );
    let exits_accounts = exits_addresses
        .into_iter()
        .zip(exits_response.value)
        .map(|((index, address), account)| decode_exit_schedule(account, address, index))
        .collect::<anyhow::Result<Vec<_>>>()?;

    let (base_per_quote, quote_per_base) = project_bookkeeping_prices(
        &bookkeeping,
        &market,
        current_slot,
        last_update_index,
        &exits_accounts,
    )?;

    // Base token inflow since last update slot
    let accumulated_base_inflow = base_per_quote
        .checked_sub(liquidity_position.base_per_quote_snapshot)
        .and_then(|value| value.checked_mul(liquidity_position.quote_flow_u64 as u128))
        .context("Accumulated base inflow overflowed or preceded its position snapshot")?;

    // Quote token inflow since last update slot
    let accumulated_quote_inflow = quote_per_base
        .checked_sub(liquidity_position.quote_per_base_snapshot)
        .and_then(|value| value.checked_mul(liquidity_position.base_flow_u64 as u128))
        .context("Accumulated quote inflow overflowed or preceded its position snapshot")?;

    info!(
        event.name = "liquidity_position_computed_flows",
        position.base_outflow.raw = accumulated_base_outflow / BOOKKEEPING_PRECISION_FACTOR,
        position.base_inflow.raw = accumulated_base_inflow / BOOKKEEPING_PRECISION_FACTOR,
        position.quote_outflow.raw = accumulated_quote_outflow / BOOKKEEPING_PRECISION_FACTOR,
        position.quote_inflow.raw = accumulated_quote_inflow / BOOKKEEPING_PRECISION_FACTOR,
    );

    let base_balance;
    let base_debt;
    let available_base = liquidity_position
        .base_balance
        .checked_add(accumulated_base_inflow)
        .context("Available base balance overflowed")?;
    if accumulated_base_outflow > available_base {
        base_balance = 0;
        base_debt = accumulated_base_outflow
            .checked_sub(available_base)
            .context("Base debt underflowed")?
            / BOOKKEEPING_PRECISION_FACTOR;
    } else {
        base_balance = available_base
            .checked_sub(accumulated_base_outflow)
            .context("Base balance underflowed")?
            / BOOKKEEPING_PRECISION_FACTOR;
        base_debt = 0;
    }

    let quote_balance;
    let quote_debt;
    let available_quote = liquidity_position
        .quote_balance
        .checked_add(accumulated_quote_inflow)
        .context("Available quote balance overflowed")?;
    if accumulated_quote_outflow > available_quote {
        quote_balance = 0;
        quote_debt = accumulated_quote_outflow
            .checked_sub(available_quote)
            .context("Quote debt underflowed")?
            / BOOKKEEPING_PRECISION_FACTOR;
    } else {
        quote_balance = available_quote
            .checked_sub(accumulated_quote_outflow)
            .context("Quote balance underflowed")?
            / BOOKKEEPING_PRECISION_FACTOR;
        quote_debt = 0;
    }

    info!(
        event.name = "liquidity_position_computed_balances",
        position.base_balance.raw = base_balance,
        position.base_debt.raw = base_debt,
        position.quote_balance.raw = quote_balance,
        position.quote_debt.raw = quote_debt,
    );

    Ok(LiquidityPositionBalances {
        base_balance: u64::try_from(base_balance).context("Base balance exceeds u64")?,
        quote_balance: u64::try_from(quote_balance).context("Quote balance exceeds u64")?,
        base_debt: u64::try_from(base_debt).context("Base debt exceeds u64")?,
        quote_debt: u64::try_from(quote_debt).context("Quote debt exceeds u64")?,
    })
}

fn project_bookkeeping_prices(
    bookkeeping: &Bookkeeping,
    market: &Market,
    current_slot: u64,
    first_exits_index: u64,
    exits_accounts: &[ExitSchedule],
) -> anyhow::Result<(u128, u128)> {
    ensure!(
        market.end_slot_interval > 0,
        "Market end slot interval must be greater than zero"
    );

    let exits_interval = market
        .end_slot_interval
        .checked_mul(ARRAY_LENGTH)
        .context("Exits interval overflowed")?;
    ensure!(
        current_slot >= bookkeeping.last_update_slot,
        "Current slot precedes bookkeeping last update slot"
    );
    let expected_first_exits_index = bookkeeping.last_update_slot / exits_interval;
    ensure!(
        first_exits_index == expected_first_exits_index,
        "First Exits index {} does not match bookkeeping index {}",
        first_exits_index,
        expected_first_exits_index,
    );
    let current_slot_index = current_slot / exits_interval;
    let expected_exits_count = current_slot_index
        .checked_sub(first_exits_index)
        .and_then(|value| value.checked_add(1))
        .context("Expected Exits account range overflowed")?;
    ensure!(
        exits_accounts.len()
            == usize::try_from(expected_exits_count)
                .context("Exits account range exceeds usize")?,
        "Expected {} Exits accounts, received {}",
        expected_exits_count,
        exits_accounts.len(),
    );
    let mut base_per_quote = bookkeeping.base_per_quote;
    let mut quote_per_base = bookkeeping.quote_per_base;
    let mut market_base_flow = market.base_flow;
    let mut market_quote_flow = market.quote_flow;
    let mut last_update_slot = bookkeeping.last_update_slot;

    for (offset, exits) in exits_accounts.iter().enumerate() {
        let exits_index = first_exits_index
            .checked_add(u64::try_from(offset).context("Exits account offset exceeds u64")?)
            .context("Exits account index overflowed")?;
        ensure!(
            exits.index == exits_index,
            "Exits account has index {}, expected {}",
            exits.index,
            exits_index,
        );

        let interval_start = exits_index
            .checked_mul(exits_interval)
            .context("Exits interval start overflowed")?;
        let start_index = if exits_index == first_exits_index {
            bookkeeping
                .last_update_slot
                .checked_sub(interval_start)
                .context("Bookkeeping last update slot precedes its exits interval")?
                / market.end_slot_interval
                + 1
        } else {
            0
        };
        let end_index = if exits_index == current_slot_index {
            current_slot
                .checked_sub(interval_start)
                .context("Current slot precedes its exits interval")?
                / market.end_slot_interval
        } else {
            ARRAY_LENGTH - 1
        };
        ensure!(
            end_index < ARRAY_LENGTH,
            "Exits end index {} is outside array length {}",
            end_index,
            ARRAY_LENGTH,
        );

        for i in start_index..=end_index {
            let slot = interval_start
                .checked_add(
                    i.checked_mul(market.end_slot_interval)
                        .context("Exit slot offset overflowed")?,
                )
                .context("Exit slot overflowed")?;
            let slot_diff = slot
                .checked_sub(last_update_slot)
                .context("Exit slot precedes previous projection slot")?;
            last_update_slot = slot;

            if market_base_flow != 0 && market_quote_flow != 0 {
                base_per_quote = base_per_quote
                    .checked_add(flow_price_increment(
                        market_base_flow,
                        market_quote_flow,
                        slot_diff,
                    )?)
                    .context("Accumulated base-per-quote overflowed")?;
                quote_per_base = quote_per_base
                    .checked_add(flow_price_increment(
                        market_quote_flow,
                        market_base_flow,
                        slot_diff,
                    )?)
                    .context("Accumulated quote-per-base overflowed")?;
            }

            let array_index = usize::try_from(i).context("Exit array index exceeds usize")?;
            market_base_flow = market_base_flow
                .checked_sub(exits.base_exits[array_index])
                .context("Scheduled base exit exceeds market base flow")?;
            market_quote_flow = market_quote_flow
                .checked_sub(exits.quote_exits[array_index])
                .context("Scheduled quote exit exceeds market quote flow")?;
        }
    }

    let slot_diff = current_slot
        .checked_sub(last_update_slot)
        .context("Current slot precedes final projection slot")?;
    if market_base_flow != 0 && market_quote_flow != 0 {
        base_per_quote = base_per_quote
            .checked_add(flow_price_increment(
                market_base_flow,
                market_quote_flow,
                slot_diff,
            )?)
            .context("Final base-per-quote accumulation overflowed")?;
        quote_per_base = quote_per_base
            .checked_add(flow_price_increment(
                market_quote_flow,
                market_base_flow,
                slot_diff,
            )?)
            .context("Final quote-per-base accumulation overflowed")?;
    }

    Ok((base_per_quote, quote_per_base))
}

fn decode_exit_schedule(
    account: Option<Account>,
    address: Pubkey,
    expected_index: u64,
) -> anyhow::Result<ExitSchedule> {
    let Some(account) = account else {
        return Ok(ExitSchedule::empty(expected_index));
    };
    ensure!(
        account.owner == twob_anchor::ID,
        "Exits account {} has unexpected owner {}",
        address,
        account.owner,
    );
    let mut data = account.data.as_slice();
    let exits = Exits::try_deserialize(&mut data)
        .with_context(|| format!("Failed to decode Exits account {address}"))?;
    ensure!(
        exits.index == expected_index,
        "Exits account {} has index {}, expected {}",
        address,
        exits.index,
        expected_index,
    );

    Ok(ExitSchedule {
        base_exits: exits.base_exits,
        quote_exits: exits.quote_exits,
        index: exits.index,
    })
}

fn flow_price_increment(
    numerator_flow: u128,
    denominator_flow: u128,
    slot_diff: u64,
) -> anyhow::Result<u128> {
    ensure!(denominator_flow > 0, "Flow price denominator is zero");

    BOOKKEEPING_PRECISION_FACTOR
        .checked_mul(numerator_flow)
        .and_then(|value| value.checked_div(denominator_flow))
        .and_then(|value| value.checked_mul(slot_diff as u128))
        .context("Flow price increment overflowed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_price_increment_matches_existing_operation_order() {
        let increment = flow_price_increment(200, 100, 3).unwrap();

        assert_eq!(increment, BOOKKEEPING_PRECISION_FACTOR * 2 * 3);
    }

    #[test]
    fn flow_price_increment_rejects_zero_denominator() {
        assert!(flow_price_increment(100, 0, 1).is_err());
    }

    #[test]
    fn flow_price_increment_rejects_overflow() {
        assert!(flow_price_increment(u128::MAX, 1, 1).is_err());
    }

    #[test]
    fn missing_exits_account_is_an_empty_schedule() {
        let schedule = decode_exit_schedule(None, Pubkey::new_unique(), 6_342_416).unwrap();

        assert_eq!(schedule.index, 6_342_416);
        assert_eq!(schedule.base_exits, [0; EXITS_BUCKET_COUNT]);
        assert_eq!(schedule.quote_exits, [0; EXITS_BUCKET_COUNT]);
    }

    #[test]
    fn malformed_existing_exits_account_fails_closed() {
        let address = Pubkey::new_unique();
        let account = Account {
            lamports: 1,
            data: vec![0; 8],
            owner: twob_anchor::ID,
            executable: false,
            rent_epoch: 0,
        };

        assert!(decode_exit_schedule(Some(account), address, 1).is_err());
    }
}
