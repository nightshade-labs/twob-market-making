use std::sync::Arc;

use anchor_client::{Program, solana_sdk::signature::Keypair};
use anchor_lang::prelude::Pubkey;
use twob_market_making::{
    ARRAY_LENGTH, LiquidityPositionBalances, MarketState, fetch_liquidity_position,
    fetch_market_state, get_liquidity_position_balances, twob_anchor::accounts::LiquidityPosition,
};

use crate::config::DelayConfig;

pub enum PositionAction {
    Stop {
        reference_index: u64,
    },
    UpdateFlows {
        base_flow: u64,
        quote_flow: u64,
        reference_index: u64,
    },
}

pub struct EvaluationResult {
    pub action: PositionAction,
    pub market_state: MarketState,
    pub position: LiquidityPosition,
    pub balances: LiquidityPositionBalances,
}

pub async fn evaluate_position(
    program: &Program<Arc<Keypair>>,
    market_id: u64,
    authority: &Pubkey,
    flow_divisor: u64,
) -> anyhow::Result<EvaluationResult> {
    let market_state = fetch_market_state(program, market_id).await?;
    let position = fetch_liquidity_position(program, market_id, authority).await?;

    let reference_index =
        market_state.current_slot / ARRAY_LENGTH / market_state.market.end_slot_interval;

    let balances = get_liquidity_position_balances(
        program,
        position,
        market_state.bookkeeping,
        market_state.market,
        market_state.current_slot,
    )
    .await;

    let action = if balances.base_debt > 0 || balances.quote_debt > 0 {
        PositionAction::Stop { reference_index }
    } else {
        PositionAction::UpdateFlows {
            base_flow: balances.base_balance / flow_divisor,
            quote_flow: balances.quote_balance / flow_divisor,
            reference_index,
        }
    };

    Ok(EvaluationResult {
        action,
        market_state,
        position,
        balances,
    })
}

pub fn calculate_update_delay(
    position: &LiquidityPosition,
    market_state: &MarketState,
    balances: &LiquidityPositionBalances,
    delay_config: &DelayConfig,
) -> u64 {
    let base_outflow = position.base_flow_u64 as u128;
    let quote_outflow = position.quote_flow_u64 as u128;

    if market_state.market.quote_flow == 0 || market_state.market.base_flow == 0 {
        return delay_config.normal_delay_ms as u64;
    }

    let base_inflow =
        quote_outflow * market_state.market.base_flow / market_state.market.quote_flow;
    let quote_inflow =
        base_outflow * market_state.market.quote_flow / market_state.market.base_flow;

    let slots_until_debt = if base_outflow > base_inflow {
        let delta = base_outflow - base_inflow;
        balances.base_balance as u128 / delta
    } else if quote_outflow > quote_inflow {
        let delta = quote_outflow - quote_inflow;
        balances.quote_balance as u128 / delta
    } else {
        u64::MAX as u128
    };

    println!("Slots until debt: {}", slots_until_debt);

    let delay = if slots_until_debt <= delay_config.critical_threshold {
        delay_config.critical_delay_ms
    } else if slots_until_debt <= delay_config.safe_threshold {
        delay_config.normal_delay_ms
    } else {
        let additional_slots = slots_until_debt
            .min(delay_config.safe_threshold + delay_config.max_additional_slots)
            - delay_config.safe_threshold;
        additional_slots * delay_config.delay_scale_factor + delay_config.normal_delay_ms
    };

    println!("Update flows in {}s", delay / 1000);
    delay as u64
}
