mod config;
mod price;
mod quote;
mod rebalance;

use std::{sync::Arc, time::Duration};

use anchor_client::{
    Client,
    solana_sdk::{commitment_config::CommitmentConfig, signer::Signer},
};
use config::Config;
use price::fetch_price;
use quote::{calculate_optimal_quote, should_update_quote};
use rebalance::{execute_rebalance, needs_rebalance};
use tokio::{signal, time::sleep};
use twob_market_making::{
    ARRAY_LENGTH, build_update_liquidity_flows_instruction, execute_update_flows,
    fetch_liquidity_position, fetch_market_state, get_liquidity_position_balances, twob_anchor,
};

const FLOW_REDUCTION_FACTOR: f64 = 0.99;
const MAX_FLOW_REDUCTION_ATTEMPTS: usize = 200;
const LIQUIDITY_POSITION_UNHEALTHY_ERROR_CODE: u32 = 6013;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let config = Config::from_env()?;

    let cluster = config.cluster();
    let market_id = config.market_id;
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let quote_threshold_bps = config.quote_threshold_bps;
    let rebalance_threshold_bps = config.rebalance_threshold_bps;
    let base_token_decimals = config.base_token_decimals;
    let quote_token_decimals = config.quote_token_decimals;
    let optimal_quote_weight = config.optimal_quote_weight;
    let is_devnet = config.rpc_url.contains("devnet");
    let price_feed_url = config.price_feed_url;
    let liquidity_provider = Arc::new(config.keypair);
    let client = Arc::new(Client::new_with_options(
        cluster,
        liquidity_provider.clone(),
        CommitmentConfig::confirmed(),
    ));

    let http_client = reqwest::Client::new();
    let program = client.program(twob_anchor::ID)?;
    let authority = liquidity_provider.pubkey();

    println!("Starting oracle-flow binary");
    println!("Market ID: {}", market_id);
    println!("Poll interval: {}s", poll_interval.as_secs());
    println!("Rebalance threshold: {} bps", rebalance_threshold_bps);
    println!("Quote threshold: {} bps", quote_threshold_bps);
    println!("Optimal quote weight: {}", optimal_quote_weight);
    println!("Devnet mode: {}", is_devnet);

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("Shutting down...");
                break;
            }
            _ = sleep(poll_interval) => {
                if let Err(e) = run_update_cycle(
                    &program,
                    &http_client,
                    &price_feed_url,
                    quote_threshold_bps,
                    rebalance_threshold_bps,
                    base_token_decimals,
                    quote_token_decimals,
                    optimal_quote_weight,
                    is_devnet,
                    market_id,
                    &authority,
                    liquidity_provider.clone(),
                ).await {
                    eprintln!("Update cycle error: {}", e);
                }
            }
        }
    }

    Ok(())
}

async fn run_update_cycle(
    program: &anchor_client::Program<Arc<anchor_client::solana_sdk::signature::Keypair>>,
    http_client: &reqwest::Client,
    price_feed_url: &str,
    quote_threshold_bps: u64,
    rebalance_threshold_bps: u64,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    optimal_quote_weight: f64,
    is_devnet: bool,
    market_id: u64,
    authority: &anchor_client::solana_sdk::pubkey::Pubkey,
    liquidity_provider: Arc<anchor_client::solana_sdk::signature::Keypair>,
) -> anyhow::Result<()> {
    // 1. Fetch external price
    let price_data = fetch_price(http_client, price_feed_url).await?;
    println!("Fetched price: {}", price_data.price);

    // 2. Fetch liquidity position and market state
    let mut market_state = fetch_market_state(program, market_id).await?;
    let mut position = fetch_liquidity_position(program, market_id, authority).await?;
    let mut balances = get_liquidity_position_balances(
        program,
        position,
        market_state.bookkeeping,
        market_state.market,
        market_state.current_slot,
    )
    .await;

    println!("Market state {:?}", market_state.market);
    println!("Position {:?}", position);

    // 3. Check if rebalance is needed
    if needs_rebalance(
        &price_data,
        &balances,
        base_token_decimals,
        quote_token_decimals,
        rebalance_threshold_bps,
    ) {
        println!("Inventory rebalance needed");
        execute_rebalance(
            program,
            market_id,
            &price_data,
            &balances,
            liquidity_provider.clone(),
            is_devnet,
        )
        .await?;

        // Re-fetch position data after rebalance
        market_state = fetch_market_state(program, market_id).await?;
        position = fetch_liquidity_position(program, market_id, authority).await?;
        balances = get_liquidity_position_balances(
            program,
            position,
            market_state.bookkeeping,
            market_state.market,
            market_state.current_slot,
        )
        .await;
        println!("Rebalance completed, position data refreshed");
    }

    // 4. Calculate optimal quote
    let optimal = calculate_optimal_quote(
        &price_data,
        &position,
        &market_state,
        &balances,
        base_token_decimals,
        quote_token_decimals,
        optimal_quote_weight,
    );

    // 4. Get current quote from position
    let current_base_flow = position.base_flow_u64;
    let current_quote_flow = position.quote_flow_u64;

    // 5. Check if update is needed
    if should_update_quote(
        current_base_flow,
        current_quote_flow,
        &optimal,
        quote_threshold_bps,
    ) {
        println!(
            "Quote deviation exceeds threshold. Updating flows: base={} quote={}",
            optimal.base_flow, optimal.quote_flow
        );

        let reference_index = (market_state.current_slot + ARRAY_LENGTH / 2)
            / ARRAY_LENGTH
            / market_state.market.end_slot_interval;

        let (final_base_flow, final_quote_flow) = execute_update_flows_with_backoff(
            program,
            market_id,
            optimal.base_flow,
            optimal.quote_flow,
            reference_index,
            liquidity_provider,
        )
        .await?;

        println!(
            "Flows updated successfully with final values: base={} quote={}",
            final_base_flow, final_quote_flow
        );
    } else {
        println!("Quote within threshold, no update needed");
    }

    Ok(())
}

async fn execute_update_flows_with_backoff(
    program: &anchor_client::Program<Arc<anchor_client::solana_sdk::signature::Keypair>>,
    market_id: u64,
    base_flow: u64,
    quote_flow: u64,
    reference_index: u64,
    signer: Arc<anchor_client::solana_sdk::signature::Keypair>,
) -> anyhow::Result<(u64, u64)> {
    let mut candidate_base_flow = base_flow.max(1);
    let mut candidate_quote_flow = quote_flow.max(1);

    for attempt in 0..MAX_FLOW_REDUCTION_ATTEMPTS {
        let ix = build_update_liquidity_flows_instruction(
            program,
            market_id,
            twob_anchor::client::args::UpdateLiquidityFlows {
                reference_index,
                base_flow_u64: candidate_base_flow,
                quote_flow_u64: candidate_quote_flow,
            },
        );

        let signed_tx = program
            .request()
            .instruction(ix)
            .signer(signer.clone())
            .signed_transaction()
            .await?;

        let simulation = program.rpc().simulate_transaction(&signed_tx).await?;
        if simulation.value.err.is_none() {
            execute_update_flows(
                program,
                market_id,
                candidate_base_flow,
                candidate_quote_flow,
                reference_index,
                signer,
            )
            .await?;
            return Ok((candidate_base_flow, candidate_quote_flow));
        }

        let err = &simulation.value.err;
        let logs = simulation.value.logs.as_deref();
        if is_liquidity_position_unhealthy(err, logs) {
            let next_base_flow = reduce_flow(candidate_base_flow, FLOW_REDUCTION_FACTOR);
            let next_quote_flow = reduce_flow(candidate_quote_flow, FLOW_REDUCTION_FACTOR);

            println!(
                "Simulation failed with LiquidityPositionUnhealthy on attempt {}. Reducing flows: base {} -> {}, quote {} -> {}",
                attempt + 1,
                candidate_base_flow,
                next_base_flow,
                candidate_quote_flow,
                next_quote_flow
            );

            if next_base_flow == candidate_base_flow && next_quote_flow == candidate_quote_flow {
                anyhow::bail!(
                    "Unable to reduce flows further after LiquidityPositionUnhealthy. Last attempted flows: base={}, quote={}",
                    candidate_base_flow,
                    candidate_quote_flow
                );
            }

            candidate_base_flow = next_base_flow;
            candidate_quote_flow = next_quote_flow;
            continue;
        }

        anyhow::bail!(
            "Update-flows simulation failed with non-retriable error. err={:?} logs={:?}",
            err,
            logs
        );
    }

    anyhow::bail!(
        "Failed to find healthy flows after {} attempts. Last attempted base={} quote={}",
        MAX_FLOW_REDUCTION_ATTEMPTS,
        candidate_base_flow,
        candidate_quote_flow
    )
}

fn is_liquidity_position_unhealthy(
    err: &Option<anchor_client::solana_sdk::transaction::TransactionError>,
    logs: Option<&[String]>,
) -> bool {
    let code_match = matches!(
        err,
        Some(anchor_client::solana_sdk::transaction::TransactionError::InstructionError(
            _,
            anchor_client::solana_sdk::instruction::InstructionError::Custom(code)
        )) if *code == LIQUIDITY_POSITION_UNHEALTHY_ERROR_CODE
    );

    if code_match {
        return true;
    }

    logs.map(|entries| {
        entries.iter().any(|line| {
            line.contains("LiquidityPositionUnhealthy")
                || line.contains("Liquidity position is unhealthy")
                || line.contains("custom program error: 0x177d")
        })
    })
    .unwrap_or(false)
}

fn reduce_flow(flow: u64, factor: f64) -> u64 {
    if flow <= 1 {
        return flow;
    }

    let reduced = ((flow as f64) * factor).floor() as u64;
    reduced.clamp(1, flow - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reduce_flow_always_makes_progress_when_possible() {
        assert_eq!(reduce_flow(100, 0.99), 99);
        assert_eq!(reduce_flow(2, 0.99), 1);
        assert_eq!(reduce_flow(1, 0.99), 1);
    }
}
