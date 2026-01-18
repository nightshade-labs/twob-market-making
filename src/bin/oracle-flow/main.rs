mod config;
mod price;
mod quote;

use std::{sync::Arc, time::Duration};

use anchor_client::{
    Client,
    solana_sdk::{
        commitment_config::CommitmentConfig, signature::read_keypair_file, signer::Signer,
    },
};
use config::Config;
use price::fetch_price;
use quote::{calculate_optimal_quote, should_update_quote};
use tokio::{signal, time::sleep};
use twob_market_making::{
    ARRAY_LENGTH, execute_update_flows, fetch_liquidity_position, fetch_market_state, twob_anchor,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let config = Config::from_env()?;

    let liquidity_provider = read_keypair_file(&config.keypair_path).map_err(|e| {
        anyhow::anyhow!("Failed to read keypair from {}: {}", config.keypair_path, e)
    })?;

    let liquidity_provider = Arc::new(liquidity_provider);
    let client = Arc::new(Client::new_with_options(
        config.cluster(),
        liquidity_provider.clone(),
        CommitmentConfig::confirmed(),
    ));

    let http_client = reqwest::Client::new();
    let program = client.program(twob_anchor::ID)?;
    let authority = liquidity_provider.pubkey();
    let market_id = config.market_id;
    let poll_interval = Duration::from_secs(config.poll_interval_secs);

    println!("Starting oracle-flow binary");
    println!("Market ID: {}", market_id);
    println!("Poll interval: {}s", config.poll_interval_secs);
    println!("Quote threshold: {} bps", config.quote_threshold_bps);

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
                    &config,
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
    config: &Config,
    market_id: u64,
    authority: &anchor_client::solana_sdk::pubkey::Pubkey,
    liquidity_provider: Arc<anchor_client::solana_sdk::signature::Keypair>,
) -> anyhow::Result<()> {
    // 1. Fetch external price
    let price_data = fetch_price(http_client, &config.price_feed_url).await?;
    println!("Fetched price: {}", price_data.price);

    // 2. Fetch liquidity position and market state
    let market_state = fetch_market_state(program, market_id).await?;
    let position = fetch_liquidity_position(program, market_id, authority).await?;

    // 3. Calculate optimal quote
    let optimal = calculate_optimal_quote(&price_data, &position, &market_state);

    // 4. Get current quote from position
    let current_base_flow = position.base_flow_u64;
    let current_quote_flow = position.quote_flow_u64;

    // 5. Check if update is needed
    if should_update_quote(
        current_base_flow,
        current_quote_flow,
        &optimal,
        config.quote_threshold_bps,
    ) {
        println!(
            "Quote deviation exceeds threshold. Updating flows: base={} quote={}",
            optimal.base_flow, optimal.quote_flow
        );

        let reference_index =
            market_state.current_slot / ARRAY_LENGTH / market_state.market.end_slot_interval;

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
