// On start up
// - check inventory and calculate at which slot flows should be updated, maybe set a maximum duration

// Listen to twob market price updates
// - recalculate at which slot flows should be updated

use std::time::Duration;

// When update flow timer is triggered, update flows
use anchor_client::{
    Client, Cluster,
    solana_sdk::{
        commitment_config::CommitmentConfig, signature::read_keypair_file, signer::Signer,
    },
};
use anchor_lang::prelude::*;
use tokio::time::sleep;
use twob_market_making::{
    AccountResolver, LiquidityPositionBalances, build_update_liquidity_flows_instruction,
    get_liquidity_position_balances,
    twob_anchor::{
        accounts::{Bookkeeping, LiquidityPosition, Market},
        client::args::UpdateLiquidityFlows,
    },
};

declare_program!(twob_anchor);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let liquidity_provider = read_keypair_file("/Users/thgehr/.config/solana/lp1.json")
        .expect("Keypair file is required");
    let url = Cluster::Custom(
        "http://127.0.0.1:8899".to_string(),
        "ws://127.0.0.1:8900".to_string(),
    );

    let market_id = 1u64;

    let liquidity_provider = std::sync::Arc::new(liquidity_provider);
    let client = Client::new_with_options(
        url,
        liquidity_provider.clone(),
        CommitmentConfig::confirmed(),
    );

    let program = client.program(twob_anchor::ID)?;
    let resolver = AccountResolver::new(twob_anchor::ID);

    let market_pda = resolver.market_pda(market_id);
    let liquidity_position_pda =
        resolver.liquidity_position_pda(&market_pda.address(), &liquidity_provider.pubkey());
    let bookkeeping_pda = resolver.bookkeeping_pda(&market_pda.address());

    let market = program.account::<Market>(market_pda.address()).await?;
    let liquidity_position = program
        .account::<LiquidityPosition>(liquidity_position_pda.address())
        .await?;
    let bookkeeping = program
        .account::<Bookkeeping>(bookkeeping_pda.address())
        .await?;

    // Update flows every x minutes
    let update_flows_task = tokio::spawn(async move {
        loop {
            sleep(Duration::from_mins(1)).await;

            let liquidity_provider = liquidity_provider.clone();
            let program = client.program(twob_anchor::ID).unwrap();

            let current_slot = program.rpc().get_slot().await.unwrap();
            let reference_index = current_slot / 1000;

            let update_flows_args = UpdateLiquidityFlows {
                reference_index: reference_index,
                base_flow_u64: 1,
                quote_flow_u64: 1,
            };

            let update_flows_ix =
                build_update_liquidity_flows_instruction(&program, market_id, update_flows_args);

            if let Err(e) = program
                .request()
                .instruction(update_flows_ix)
                .signer(liquidity_provider)
                .send()
                .await
            {
                eprintln!("Failed to update flows: {}", e);
            };
        }
    });

    ////////
    // TODO: Update flows if slots until debt is smaller than y (maybe around 1000)
    ////////
    // Calculate balances
    let current_slot = program.rpc().get_slot().await?;

    let LiquidityPositionBalances {
        base_balance,
        quote_balance,
        base_debt,
        quote_debt,
    } = get_liquidity_position_balances(liquidity_position, bookkeeping, market, current_slot);

    println!("Base balance {}", base_balance);
    println!("Quote balance {}", quote_balance);
    println!("Base debt {}", base_debt);
    println!("Quote debt {}", quote_debt);

    // Calculate when to update flows

    let base_outflow = liquidity_position.base_flow_u64 as u128;
    let quote_outflow = liquidity_position.quote_flow_u64 as u128;
    let base_inflow = quote_outflow * market.base_flow / market.quote_flow;
    let quote_inflow = base_outflow * market.quote_flow / market.base_flow;

    println!("Base outflow {}", base_outflow);
    println!("Base iinflow {}", base_inflow);
    println!("Quote outflow {}", quote_outflow);
    println!("Quote iinflow {}", quote_inflow);

    if base_outflow > base_inflow {
        let delta_base_outflow = base_outflow - base_inflow;

        let slots_unit_debt = base_balance / delta_base_outflow as u64;

        println!("Slots until debt: {}", slots_unit_debt);
    } else if quote_outflow > quote_inflow {
        let delta_quote_outflow = quote_outflow - quote_inflow;

        let slots_unit_debt = quote_balance / delta_quote_outflow as u64;

        println!("Slots until debt: {}", slots_unit_debt);
    }

    println!("Current slot {}", current_slot);
    println!("Liquidity position {:?}", liquidity_position);
    println!("Bookkeeping {:?}", bookkeeping);

    tokio::try_join!(update_flows_task)?;
    Ok(())
}
