mod config;
mod position;

use std::{sync::Arc, time::Duration};

use anchor_client::{
    Client,
    solana_sdk::{commitment_config::CommitmentConfig, signer::Signer},
};
use config::{Config, DelayConfig};
use position::{EvaluationResult, PositionAction, calculate_update_delay, evaluate_position};
use tokio::{signal, sync::mpsc, task::JoinHandle, time::sleep};
use twob_market_making::{
    execute_stop_position, execute_update_flows,
    twob_anchor::{self, events::MarketUpdateEvent},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let config = Config::from_env()?;
    let delay_config = DelayConfig::default();

    let cluster = config.cluster();
    let market_id = config.market_id;
    let flow_divisor = config.flow_divisor;
    let liquidity_provider = Arc::new(config.keypair);
    let client = Arc::new(Client::new_with_options(
        cluster,
        liquidity_provider.clone(),
        CommitmentConfig::confirmed(),
    ));

    let mut subscription_program = client.program(twob_anchor::ID)?;
    let authority = liquidity_provider.pubkey();

    // Periodic update task
    // Keeps inventory balanced within acceptable bounds
    let client_periodic = client.clone();
    let lp_periodic = liquidity_provider.clone();
    let mut update_flows_task = tokio::spawn(async move {
        loop {
            let program = match client_periodic.program(twob_anchor::ID) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Failed to get program client: {}", e);
                    sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            match evaluate_position(&program, market_id, &lp_periodic.pubkey(), flow_divisor).await
            {
                Ok(EvaluationResult { action, .. }) => match action {
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

            sleep(Duration::from_secs(5 * 60)).await;
        }
    });

    // Event-driven updates
    // Recalculates update timing when market state changes
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut event_unsubscriber = Some(
        subscription_program
            .on(move |ctx, event: MarketUpdateEvent| {
                let _ = tx.send((ctx.signature, ctx.slot, event));
            })
            .await?,
    );

    let mut current_task: Option<JoinHandle<()>> = None;

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("Shutting down...");
                break;
            }
            result = &mut update_flows_task => {
                match result {
                    Ok(_) => println!("Periodic task completed"),
                    Err(e) if e.is_panic() => eprintln!("Periodic task panicked: {}", e),
                    Err(e) => eprintln!("Periodic task error: {}", e),
                }
                break;
            }
            event = rx.recv() => {
                let Some(_event) = event else {
                    println!("Event channel closed; attempting to resubscribe");

                    if let Some(handle) = current_task.take() {
                        handle.abort();
                    }

                    if let Some(unsubscriber) = event_unsubscriber.take() {
                        drop(unsubscriber);
                    }

                    loop {
                        subscription_program = match client.program(twob_anchor::ID) {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("Failed to get program client for resubscribe: {}", e);
                                sleep(Duration::from_secs(5)).await;
                                continue;
                            }
                        };

                        let (new_tx, new_rx) = mpsc::unbounded_channel();
                        match subscription_program
                            .on(move |ctx, event: MarketUpdateEvent| {
                                let _ = new_tx.send((ctx.signature, ctx.slot, event));
                            })
                            .await
                        {
                            Ok(unsubscriber) => {
                                rx = new_rx;
                                event_unsubscriber = Some(unsubscriber);
                                println!("Resubscribed to market updates");
                                break;
                            }
                            Err(e) => {
                                eprintln!("Failed to resubscribe to market updates: {}", e);
                                sleep(Duration::from_secs(5)).await;
                            }
                        }
                    }

                    continue;
                };

                if let Some(handle) = current_task.take() {
                    handle.abort();
                }

                let client = client.clone();
                let lp = liquidity_provider.clone();

                let program = match client.program(twob_anchor::ID) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("Failed to get program client: {}", e);
                        continue;
                    }
                };

                match evaluate_position(&program, market_id, &authority, flow_divisor).await {
                    Ok(result) => match result.action {
                        PositionAction::Stop { reference_index } => {
                            if let Err(e) =
                                execute_stop_position(&program, market_id, reference_index, lp).await
                            {
                                eprintln!("Failed to stop position: {}", e);
                            }
                            break;
                        }
                        PositionAction::UpdateFlows { .. } => {
                            let delay = calculate_update_delay(
                                &result.position,
                                &result.market_state,
                                &result.balances,
                                &delay_config,
                            );

                            current_task = Some(tokio::spawn(async move {
                                sleep(Duration::from_millis(delay)).await;

                                let program = match client.program(twob_anchor::ID) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        eprintln!("Failed to get program client: {}", e);
                                        return;
                                    }
                                };

                                match evaluate_position(&program, market_id, &lp.pubkey(), flow_divisor)
                                    .await
                                {
                                    Ok(EvaluationResult { action, .. }) => match action {
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
        }
    }

    // Cleanup
    if let Some(task) = current_task.take() {
        task.abort();
    }
    update_flows_task.abort();

    Ok(())
}
