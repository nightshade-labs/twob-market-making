use std::{sync::Arc, time::Duration};

use anchor_client::{
    Client, Cluster, Program,
    solana_sdk::{
        commitment_config::CommitmentConfig, signature::Keypair, signature::read_keypair_file,
        signer::Signer,
    },
};
use anchor_lang::prelude::Pubkey;
use tokio::{sync::mpsc, task::JoinHandle, time::sleep};
use twob_market_making::{
    ARRAY_LENGTH, LiquidityPositionBalances, MarketState, execute_stop_position,
    execute_update_flows, fetch_liquidity_position, fetch_market_state,
    get_liquidity_position_balances,
    twob_anchor::{self, accounts::LiquidityPosition, events::MarketUpdateEvent},
};

enum PositionAction {
    Stop {
        reference_index: u64,
    },
    UpdateFlows {
        base_flow: u64,
        quote_flow: u64,
        reference_index: u64,
    },
}

async fn evaluate_position(
    program: &Program<Arc<Keypair>>,
    market_id: u64,
    authority: &Pubkey,
) -> anyhow::Result<(PositionAction, MarketState, LiquidityPosition)> {
    let market_state = fetch_market_state(program, market_id).await?;
    let position = fetch_liquidity_position(program, market_id, authority).await?;

    let reference_index =
        market_state.current_slot / ARRAY_LENGTH / market_state.market.end_slot_interval;

    let LiquidityPositionBalances {
        base_balance,
        quote_balance,
        base_debt,
        quote_debt,
    } = get_liquidity_position_balances(
        program,
        position,
        market_state.bookkeeping,
        market_state.market,
        market_state.current_slot,
    )
    .await;

    let action = if base_debt > 0 || quote_debt > 0 {
        PositionAction::Stop { reference_index }
    } else {
        PositionAction::UpdateFlows {
            base_flow: base_balance / 5,
            quote_flow: quote_balance / 5,
            reference_index,
        }
    };

    Ok((action, market_state, position))
}

fn calculate_update_delay(
    position: &LiquidityPosition,
    market_state: &MarketState,
    balances: &LiquidityPositionBalances,
) -> u64 {
    let base_outflow = position.base_flow_u64 as u128;
    let quote_outflow = position.quote_flow_u64 as u128;

    if market_state.market.quote_flow == 0 || market_state.market.base_flow == 0 {
        return 2000;
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

    // TODO: Analyze which numbers make sense for production
    let threshold = 10000u128;
    let delay = if slots_until_debt <= 25 {
        100
    } else if slots_until_debt <= threshold {
        2000
    } else {
        (slots_until_debt.min(threshold + 1000) - threshold) * 400 + 2000
    };

    println!("Update flows in {}s", delay / 1000);
    delay as u64
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let liquidity_provider = read_keypair_file("/Users/thgehr/.config/solana/lp1.json")
        .expect("Keypair file is required");
    let url = Cluster::Custom(
        "http://127.0.0.1:8899".to_string(),
        "ws://127.0.0.1:8900".to_string(),
    );
    let market_id = 1u64;

    let liquidity_provider = Arc::new(liquidity_provider);
    let client = Arc::new(Client::new_with_options(
        url,
        liquidity_provider.clone(),
        CommitmentConfig::confirmed(),
    ));

    let program = client.program(twob_anchor::ID)?;
    let authority = liquidity_provider.pubkey();

    // Periodic update task
    let client_periodic = client.clone();
    let lp_periodic = liquidity_provider.clone();
    let update_flows_task = tokio::spawn(async move {
        loop {
            let program = client_periodic.program(twob_anchor::ID).unwrap();

            match evaluate_position(&program, market_id, &lp_periodic.pubkey()).await {
                Ok((action, _, _)) => match action {
                    PositionAction::Stop { reference_index } => {
                        if let Err(e) = execute_stop_position(
                            &program,
                            market_id,
                            reference_index,
                            lp_periodic.clone(),
                        )
                        .await
                        {
                            eprintln!("Failed to stop position: {}", e);
                        }
                        return;
                    }
                    PositionAction::UpdateFlows {
                        base_flow,
                        quote_flow,
                        reference_index,
                    } => {
                        if let Err(e) = execute_update_flows(
                            &program,
                            market_id,
                            base_flow,
                            quote_flow,
                            reference_index,
                            lp_periodic.clone(),
                        )
                        .await
                        {
                            eprintln!("Failed to update flows: {}", e);
                        }
                        println!("Updated flow in regular loop");
                    }
                },
                Err(e) => eprintln!("Failed to evaluate position: {}", e),
            }

            sleep(Duration::from_mins(5)).await;
        }
    });

    // Event-driven updates
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _unsubscriber = program
        .on(move |ctx, event: MarketUpdateEvent| {
            let _ = tx.send((ctx.signature, ctx.slot, event));
        })
        .await?;

    let mut current_task: Option<JoinHandle<()>> = None;

    while let Some(_event) = rx.recv().await {
        if let Some(handle) = current_task.take() {
            handle.abort();
        }

        let client = client.clone();
        let lp = liquidity_provider.clone();

        let program = client.program(twob_anchor::ID)?;

        match evaluate_position(&program, market_id, &authority).await {
            Ok((action, market_state, position)) => match action {
                PositionAction::Stop { reference_index } => {
                    if let Err(e) =
                        execute_stop_position(&program, market_id, reference_index, lp).await
                    {
                        eprintln!("Failed to stop position: {}", e);
                    }
                    return Ok(());
                }
                PositionAction::UpdateFlows {
                    base_flow,
                    quote_flow,
                    ..
                } => {
                    let balances = LiquidityPositionBalances {
                        base_balance: base_flow * 5,
                        quote_balance: quote_flow * 5,
                        base_debt: 0,
                        quote_debt: 0,
                    };
                    let delay = calculate_update_delay(&position, &market_state, &balances);

                    current_task = Some(tokio::spawn(async move {
                        sleep(Duration::from_millis(delay)).await;

                        let program = client.program(twob_anchor::ID).unwrap();

                        match evaluate_position(&program, market_id, &lp.pubkey()).await {
                            Ok((action, _, _)) => match action {
                                PositionAction::Stop { reference_index } => {
                                    if let Err(e) = execute_stop_position(
                                        &program,
                                        market_id,
                                        reference_index,
                                        lp,
                                    )
                                    .await
                                    {
                                        eprintln!("Failed to stop position: {}", e);
                                    }
                                }
                                PositionAction::UpdateFlows {
                                    base_flow,
                                    quote_flow,
                                    reference_index,
                                } => {
                                    if let Err(e) = execute_update_flows(
                                        &program,
                                        market_id,
                                        base_flow,
                                        quote_flow,
                                        reference_index,
                                        lp,
                                    )
                                    .await
                                    {
                                        eprintln!("Failed to update flows: {}", e);
                                    }
                                }
                            },
                            Err(e) => eprintln!("Failed to evaluate position: {}", e),
                        }
                    }));
                }
            },
            Err(e) => eprintln!("Failed to evaluate position: {}", e),
        }
    }

    tokio::try_join!(update_flows_task)?;
    Ok(())
}
