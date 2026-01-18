mod position;

use std::{sync::Arc, time::Duration};

use anchor_client::{
    Client, Cluster,
    solana_sdk::{
        commitment_config::CommitmentConfig, signature::read_keypair_file, signer::Signer,
    },
};
use position::{PositionAction, calculate_update_delay, evaluate_position};
use tokio::{sync::mpsc, task::JoinHandle, time::sleep};
use twob_market_making::{
    LiquidityPositionBalances, execute_stop_position, execute_update_flows,
    twob_anchor::{self, events::MarketUpdateEvent},
};

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
