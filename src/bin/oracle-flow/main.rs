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
    ARRAY_LENGTH, execute_update_flows, fetch_liquidity_position, fetch_market_state,
    get_liquidity_position_balances, twob_anchor,
};

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

        execute_update_flows(
            program,
            market_id,
            optimal.base_flow,
            optimal.quote_flow,
            reference_index,
            liquidity_provider,
        )
        .await?;

        println!("Flows updated successfully");
    } else {
        println!("Quote within threshold, no update needed");
    }

    Ok(())
}
