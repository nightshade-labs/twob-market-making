// On start up
// - check inventory and calculate at which slot flows should be updated, maybe set a maximum duration

// Listen to twob market price updates
// - recalculate at which slot flows should be updated

use std::{sync::Arc, time::Duration, u64};

// When update flow timer is triggered, update flows
use anchor_client::{
    Client, Cluster,
    solana_sdk::{
        commitment_config::CommitmentConfig, signature::read_keypair_file, signer::Signer,
    },
};

use tokio::{sync::mpsc, task::JoinHandle, time::sleep};
use twob_market_making::{
    ARRAY_LENGTH, AccountResolver, LiquidityPositionBalances,
    build_update_liquidity_flows_instruction, get_liquidity_position_balances,
    twob_anchor::{
        self,
        accounts::{Bookkeeping, LiquidityPosition, Market},
        client::args::UpdateLiquidityFlows,
        events::MarketUpdateEvent,
    },
};

// declare_program!(twob_anchor);

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
    let client = Arc::new(Client::new_with_options(
        url,
        liquidity_provider.clone(),
        CommitmentConfig::confirmed(),
    ));

    let program = client.program(twob_anchor::ID)?;
    let resolver = AccountResolver::new(twob_anchor::ID);

    let market_pda = resolver.market_pda(market_id);
    let liquidity_position_pda =
        resolver.liquidity_position_pda(&market_pda.address(), &liquidity_provider.pubkey());
    let bookkeeping_pda = resolver.bookkeeping_pda(&market_pda.address());

    let client_clone = client.clone();
    let liquidity_provider_clone = liquidity_provider.clone();
    // Update flows every x minutes
    let update_flows_task = tokio::spawn(async move {
        loop {
            let liquidity_provider = liquidity_provider_clone.clone();
            let program = client_clone.program(twob_anchor::ID).unwrap();

            let market = program
                .account::<Market>(market_pda.address())
                .await
                .unwrap();
            let liquidity_position = program
                .account::<LiquidityPosition>(liquidity_position_pda.address())
                .await
                .unwrap();
            let bookkeeping = program
                .account::<Bookkeeping>(bookkeeping_pda.address())
                .await
                .unwrap();

            let current_slot = program.rpc().get_slot().await.unwrap();
            let reference_index = current_slot / ARRAY_LENGTH / market.end_slot_interval;

            let LiquidityPositionBalances {
                base_balance,
                quote_balance,
                base_debt,
                quote_debt,
            } = get_liquidity_position_balances(
                &program,
                liquidity_position,
                bookkeeping,
                market,
                current_slot,
            )
            .await;

            if base_debt > 0 || quote_debt > 0 {
                // TODO: Stop liquidity position
                println!("🚨🚨🚨🚨, position has accumulated debt. Stop position");
            }

            let update_flows_args = UpdateLiquidityFlows {
                reference_index: reference_index,
                base_flow_u64: base_balance / 5,
                quote_flow_u64: quote_balance / 5,
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

            println!("Updated flow in regular loop");
            sleep(Duration::from_mins(5)).await;
        }
    });

    let (market_update_event_sender, mut market_update_event_receiver) = mpsc::unbounded_channel();
    let market_update_event_unsubscriber = program
        .on(move |event_ctx, event: MarketUpdateEvent| {
            if market_update_event_sender
                .send((event_ctx.signature, event_ctx.slot, event))
                .is_err()
            {
                println!("Error while transferring the event.")
            }
        })
        .await?;

    let mut current_task: Option<JoinHandle<()>> = None;

    while let Some(_event) = market_update_event_receiver.recv().await {
        if let Some(handle) = current_task.take() {
            println!("Aborting task");
            handle.abort();
        }
        let liquidity_provider = liquidity_provider.clone();
        let client = client.clone();

        let current_slot = program.rpc().get_slot().await.unwrap();

        let market = program
            .account::<Market>(market_pda.address())
            .await
            .unwrap();
        let liquidity_position = program
            .account::<LiquidityPosition>(liquidity_position_pda.address())
            .await
            .unwrap();
        let bookkeeping = program
            .account::<Bookkeeping>(bookkeeping_pda.address())
            .await
            .unwrap();

        let LiquidityPositionBalances {
            base_balance,
            quote_balance,
            base_debt,
            quote_debt,
        } = get_liquidity_position_balances(
            &program,
            liquidity_position,
            bookkeeping,
            market,
            current_slot,
        )
        .await;

        if base_debt > 0 || quote_debt > 0 {
            // TODO: Stop liquidity position
            println!("🚨🚨🚨🚨, position has accumulated debt. Stop position");
            return Ok(());
        }

        let base_outflow = liquidity_position.base_flow_u64 as u128;
        let quote_outflow = liquidity_position.quote_flow_u64 as u128;
        let base_inflow = quote_outflow * market.base_flow / market.quote_flow;
        let quote_inflow = base_outflow * market.quote_flow / market.base_flow;

        let slots_until_debt = if base_outflow > base_inflow {
            let delta_base_outflow = base_outflow - base_inflow;

            base_balance / delta_base_outflow as u64
        } else if quote_outflow > quote_inflow {
            let delta_quote_outflow = quote_outflow - quote_inflow;

            quote_balance / delta_quote_outflow as u64
        } else {
            u64::MAX
        };

        println!("Slots until debt: {}", slots_until_debt);

        // TODO: Need to analyse which numbers make sense
        let threshold = 10000;
        let delay = if slots_until_debt <= 25 {
            100
        } else if slots_until_debt <= threshold {
            2000
        } else {
            (slots_until_debt.min(threshold + 1000) - threshold) * 400 + 2000
        };

        println!("Update flows in {}s", delay / 1000);

        current_task = Some(tokio::spawn(async move {
            sleep(Duration::from_millis(delay)).await;
            println!("Update flows");

            let program = client.program(twob_anchor::ID).unwrap();

            let market = program
                .account::<Market>(market_pda.address())
                .await
                .unwrap();
            let liquidity_position = program
                .account::<LiquidityPosition>(liquidity_position_pda.address())
                .await
                .unwrap();
            let bookkeeping = program
                .account::<Bookkeeping>(bookkeeping_pda.address())
                .await
                .unwrap();

            let current_slot = program.rpc().get_slot().await.unwrap();
            let reference_index = current_slot / ARRAY_LENGTH / market.end_slot_interval;

            let LiquidityPositionBalances {
                base_balance,
                quote_balance,
                base_debt,
                quote_debt,
            } = get_liquidity_position_balances(
                &program,
                liquidity_position,
                bookkeeping,
                market,
                current_slot,
            )
            .await;

            if base_debt > 0 || quote_debt > 0 {
                // TODO: Stop liquidity position
                println!("🚨🚨🚨🚨, position has accumulated debt. Stop position");
            }

            let update_flows_args = UpdateLiquidityFlows {
                reference_index: reference_index,
                base_flow_u64: base_balance / 5,
                quote_flow_u64: quote_balance / 5,
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
        }));
    }

    market_update_event_unsubscriber.unsubscribe().await;
    tokio::try_join!(update_flows_task)?;
    Ok(())
}
