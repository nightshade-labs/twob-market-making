mod config;
mod jupiter;
mod price;
mod quote;
mod rebalance;
mod telemetry;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anchor_client::{
    Client,
    solana_sdk::{commitment_config::CommitmentConfig, signer::Signer},
};
use config::{Config, JupiterConfig};
use price::fetch_price;
use quote::{calculate_optimal_quote, should_update_quote};
use rebalance::{RebalanceOutcome, execute_rebalance, needs_rebalance};
use tokio::{signal, time::sleep};
use tracing::{Instrument, error, info, info_span, warn};
use twob_market_making::{
    ARRAY_LENGTH, LiquidityPositionBalances, MarketState, build_update_liquidity_flows_instruction,
    execute_update_flows, fetch_liquidity_position, fetch_market_state,
    get_liquidity_position_balances,
    twob_anchor::{self, accounts::LiquidityPosition},
};

const LIQUIDITY_POSITION_UNHEALTHY_ERROR_CODE: u32 = 6013;
type OracleProgram = anchor_client::Program<Arc<anchor_client::solana_sdk::signature::Keypair>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let config = Config::from_env()?;

    let telemetry_config = config.telemetry.clone();
    let rpc_url = config.rpc_url.clone();
    let cluster = config.cluster();
    let market_id = config.market_id;
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    let quote_threshold_bps = config.quote_threshold_bps;
    let rebalance_threshold_bps = config.rebalance_threshold_bps;
    let base_token_decimals = config.base_token_decimals;
    let quote_token_decimals = config.quote_token_decimals;
    let optimal_quote_weight = config.optimal_quote_weight;
    let flow_reduction_factor = config.flow_reduction_factor;
    let max_flow_reduction_attempts = config.max_flow_reduction_attempts;
    let rebalance_cooldown = Duration::from_secs(config.rebalance_cooldown_secs);
    let min_rebalance_value_usd = config.min_rebalance_value_usd;
    let is_devnet = config.rpc_url.contains("devnet");
    let price_feed_url = config.price_feed_url;
    let jupiter_config = config.jupiter.clone();
    let liquidity_provider = Arc::new(config.keypair);
    let client = Arc::new(Client::new_with_options(
        cluster,
        liquidity_provider.clone(),
        CommitmentConfig::confirmed(),
    ));

    let http_client = reqwest::Client::new();
    let program = client.program(twob_anchor::ID)?;
    let authority = liquidity_provider.pubkey();
    let _telemetry_guard = telemetry::init_telemetry(telemetry::TelemetryInitConfig {
        service_name: telemetry_config.service_name.clone(),
        stdout_json: telemetry_config.stdout_json,
        market_id,
        authority: authority.to_string(),
        rpc_url,
        program_id: twob_anchor::ID.to_string(),
    })?;

    info!(
        event.name = "oracle_flow_started",
        market.id = market_id,
        lp.authority = %authority,
        poll_interval_secs = poll_interval.as_secs(),
        rebalance.threshold_bps = rebalance_threshold_bps,
        quote.threshold_bps = quote_threshold_bps,
        quote.optimal_weight = optimal_quote_weight,
        jupiter.api_key_configured = jupiter_config.api_key.is_some(),
        jupiter.dry_run = jupiter_config.dry_run,
        solana.devnet_mode = is_devnet,
        rebalance.cooldown_secs = rebalance_cooldown.as_secs(),
        rebalance.min_value_usd = min_rebalance_value_usd,
        balance_snapshot_interval_secs = telemetry_config.balance_snapshot_interval_secs,
    );

    let mut last_rebalance_at: Option<Instant> = None;
    let mut cycle_number = 0_u64;

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!(event.name = "oracle_flow_shutdown");
                break;
            }
            _ = sleep(poll_interval) => {
                cycle_number = cycle_number.saturating_add(1);
                let cycle_id = format!("{}-{}", market_id, cycle_number);
                let cycle_span = info_span!(
                    "oracle_flow.update_cycle",
                    cycle.id = %cycle_id,
                    market.id = market_id,
                    lp.authority = %authority,
                );
                match run_update_cycle(
                    &program,
                    &http_client,
                    &price_feed_url,
                    quote_threshold_bps,
                    rebalance_threshold_bps,
                    base_token_decimals,
                    quote_token_decimals,
                    optimal_quote_weight,
                    flow_reduction_factor,
                    max_flow_reduction_attempts,
                    last_rebalance_at,
                    rebalance_cooldown,
                    min_rebalance_value_usd,
                    &jupiter_config,
                    is_devnet,
                    market_id,
                    &authority,
                    liquidity_provider.clone(),
                    &cycle_id,
                ).instrument(cycle_span).await {
                    Ok(Some(rebalanced_at)) => last_rebalance_at = Some(rebalanced_at),
                    Ok(None) => {}
                    Err(error) => {
                        error!(
                            event.name = "oracle_flow_cycle_error",
                            cycle.id = %cycle_id,
                            market.id = market_id,
                            lp.authority = %authority,
                            monotonic_counter.oracle_flow_cycles_total = 1_u64,
                            ?error,
                            "update cycle failed"
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_update_cycle(
    program: &OracleProgram,
    http_client: &reqwest::Client,
    price_feed_url: &str,
    quote_threshold_bps: u64,
    rebalance_threshold_bps: u64,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    optimal_quote_weight: f64,
    flow_reduction_factor: f64,
    max_flow_reduction_attempts: usize,
    last_rebalance_at: Option<Instant>,
    rebalance_cooldown: Duration,
    min_rebalance_value_usd: f64,
    jupiter_config: &JupiterConfig,
    is_devnet: bool,
    market_id: u64,
    authority: &anchor_client::solana_sdk::pubkey::Pubkey,
    liquidity_provider: Arc<anchor_client::solana_sdk::signature::Keypair>,
    cycle_id: &str,
) -> anyhow::Result<Option<Instant>> {
    let cycle_started_at = Instant::now();
    let cycle_ts = chrono::Utc::now();
    info!(
        event.name = "oracle_flow_cycle_start",
        cycle.id = %cycle_id,
        cycle.started_at = %cycle_ts.to_rfc3339(),
        market.id = market_id,
        lp.authority = %authority,
    );

    // 1. Fetch external price
    let price_data = fetch_price(http_client, price_feed_url)
        .instrument(info_span!(
            "price.fetch",
            cycle.id = %cycle_id,
            price.feed_url = %price_feed_url,
        ))
        .await?;
    info!(
        event.name = "price_fetched",
        cycle.id = %cycle_id,
        market.id = market_id,
        price.oracle = price_data.price,
    );

    // 2. Fetch liquidity position and market state
    let (mut market_state, mut position, mut balances) =
        refresh_position_state(program, market_id, authority)
            .instrument(info_span!(
                "state.refresh",
                cycle.id = %cycle_id,
                market.id = market_id,
                lp.authority = %authority,
            ))
            .await?;

    emit_position_snapshot(
        "cycle_start",
        cycle_id,
        market_id,
        authority,
        &market_state,
        &position,
        &balances,
        base_token_decimals,
        quote_token_decimals,
        price_data.price,
        optimal_quote_weight,
    );

    // 3. Check if rebalance is needed
    let mut new_rebalance_at: Option<Instant> = None;

    let in_cooldown = last_rebalance_at
        .map(|t| t.elapsed() < rebalance_cooldown)
        .unwrap_or(false);
    let rebalance_needed = if in_cooldown {
        false
    } else {
        let rebalance_evaluate_span = info_span!(
            "rebalance.evaluate",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %authority,
            rebalance.threshold_bps = rebalance_threshold_bps,
        );
        let _rebalance_evaluate_guard = rebalance_evaluate_span.enter();
        needs_rebalance(
            &price_data,
            &balances,
            base_token_decimals,
            quote_token_decimals,
            rebalance_threshold_bps,
        )
    };

    if in_cooldown {
        let elapsed = last_rebalance_at.unwrap().elapsed();
        info!(
            event.name = "rebalance_skipped",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %authority,
            rebalance.reason = "cooldown_active",
            rebalance.cooldown_elapsed_secs = elapsed.as_secs(),
            rebalance.cooldown_required_secs = rebalance_cooldown.as_secs(),
            monotonic_counter.rebalance_skips_total = 1_u64,
        );
    } else if rebalance_needed {
        let attempt_started_at = Instant::now();
        let attempt_id = format!("{}-rebalance-{}", cycle_id, cycle_ts.timestamp_millis());
        info!(
            event.name = "rebalance_triggered",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %authority,
            rebalance.attempt_id = %attempt_id,
            rebalance.reason = "inventory_deviation",
            monotonic_counter.rebalance_attempts_total = 1_u64,
        );
        let rebalance_result = execute_rebalance(
            program,
            http_client,
            market_id,
            &market_state,
            &price_data,
            &balances,
            base_token_decimals,
            quote_token_decimals,
            position.base_flow_u64,
            position.quote_flow_u64,
            liquidity_provider.clone(),
            jupiter_config,
            flow_reduction_factor,
            max_flow_reduction_attempts,
            min_rebalance_value_usd,
            is_devnet,
            cycle_id,
            &attempt_id,
        )
        .instrument(info_span!(
            "rebalance.execute",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %authority,
            rebalance.attempt_id = %attempt_id,
        ))
        .await;

        match rebalance_result {
            Ok(RebalanceOutcome::Executed) => {
                new_rebalance_at = Some(attempt_started_at);
                match refresh_position_state(program, market_id, authority)
                    .instrument(info_span!(
                        "state.refresh",
                        cycle.id = %cycle_id,
                        market.id = market_id,
                        lp.authority = %authority,
                        rebalance.attempt_id = %attempt_id,
                    ))
                    .await
                {
                    Ok((new_market_state, new_position, new_balances)) => {
                        market_state = new_market_state;
                        position = new_position;
                        balances = new_balances;
                    }
                    Err(error) => {
                        error!(
                            event.name = "rebalance_refresh_failed",
                            cycle.id = %cycle_id,
                            market.id = market_id,
                            lp.authority = %authority,
                            rebalance.attempt_id = %attempt_id,
                            ?error,
                            "rebalance completed but refresh failed; skipping quote update"
                        );
                        return Ok(new_rebalance_at);
                    }
                }
                info!(
                    event.name = "rebalance_completed",
                    cycle.id = %cycle_id,
                    market.id = market_id,
                    lp.authority = %authority,
                    rebalance.attempt_id = %attempt_id,
                    rebalance.outcome = "executed",
                    rebalance.cooldown_secs = rebalance_cooldown.as_secs(),
                    histogram.rebalance_duration_ms = attempt_started_at.elapsed().as_millis() as f64,
                );
            }
            Ok(RebalanceOutcome::Skipped) => {
                info!(
                    event.name = "rebalance_skipped",
                    cycle.id = %cycle_id,
                    market.id = market_id,
                    lp.authority = %authority,
                    rebalance.attempt_id = %attempt_id,
                    rebalance.outcome = "skipped",
                    monotonic_counter.rebalance_skips_total = 1_u64,
                    histogram.rebalance_duration_ms = attempt_started_at.elapsed().as_millis() as f64,
                );
            }
            Err(error) => {
                new_rebalance_at = Some(attempt_started_at);
                error!(
                    event.name = "rebalance_failed",
                    cycle.id = %cycle_id,
                    market.id = market_id,
                    lp.authority = %authority,
                    rebalance.attempt_id = %attempt_id,
                    rebalance.outcome = "error",
                    rebalance.cooldown_secs = rebalance_cooldown.as_secs(),
                    histogram.rebalance_duration_ms = attempt_started_at.elapsed().as_millis() as f64,
                    ?error,
                    "rebalance failed; cooldown starts now"
                );
                match refresh_position_state(program, market_id, authority)
                    .instrument(info_span!(
                        "state.refresh",
                        cycle.id = %cycle_id,
                        market.id = market_id,
                        lp.authority = %authority,
                        rebalance.attempt_id = %attempt_id,
                    ))
                    .await
                {
                    Ok((new_market_state, new_position, new_balances)) => {
                        market_state = new_market_state;
                        position = new_position;
                        balances = new_balances;
                    }
                    Err(error) => {
                        error!(
                            event.name = "rebalance_failure_refresh_failed",
                            cycle.id = %cycle_id,
                            market.id = market_id,
                            lp.authority = %authority,
                            rebalance.attempt_id = %attempt_id,
                            ?error,
                            "refresh after rebalance failure failed; skipping quote update"
                        );
                        return Ok(new_rebalance_at);
                    }
                }
            }
        }
    } else {
        info!(
            event.name = "rebalance_skipped",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %authority,
            rebalance.reason = "within_threshold",
            monotonic_counter.rebalance_skips_total = 1_u64,
        );
    }

    // 4. Calculate optimal quote
    let optimal = {
        let quote_span = info_span!(
            "quote.compute",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %authority,
        );
        let _quote_guard = quote_span.enter();
        calculate_optimal_quote(
            &price_data,
            &position,
            &market_state,
            &balances,
            base_token_decimals,
            quote_token_decimals,
            optimal_quote_weight,
        )
    };

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
        info!(
            event.name = "flow_update_planned",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %authority,
            quote.threshold_bps = quote_threshold_bps,
            quote.current_base_flow = current_base_flow,
            quote.target_base_flow = optimal.base_flow,
            quote.current_quote_flow = current_quote_flow,
            quote.target_quote_flow = optimal.quote_flow,
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
            flow_reduction_factor,
            max_flow_reduction_attempts,
            liquidity_provider,
        )
        .instrument(info_span!(
            "twob.update_flows",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %authority,
            twob.instruction = "update_liquidity_flows",
            twob.reference_index = reference_index,
        ))
        .await?;

        info!(
            event.name = "flow_update_completed",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %authority,
            twob.instruction = "update_liquidity_flows",
            twob.reference_index = reference_index,
            quote.final_base_flow = final_base_flow,
            quote.final_quote_flow = final_quote_flow,
        );
    } else {
        info!(
            event.name = "flow_update_skipped",
            cycle.id = %cycle_id,
            market.id = market_id,
            lp.authority = %authority,
            quote.threshold_bps = quote_threshold_bps,
            quote.current_base_flow = current_base_flow,
            quote.current_quote_flow = current_quote_flow,
        );
    }

    emit_position_snapshot(
        "cycle_end",
        cycle_id,
        market_id,
        authority,
        &market_state,
        &position,
        &balances,
        base_token_decimals,
        quote_token_decimals,
        price_data.price,
        optimal_quote_weight,
    );
    info!(
        event.name = "oracle_flow_cycle_end",
        cycle.id = %cycle_id,
        market.id = market_id,
        lp.authority = %authority,
        monotonic_counter.oracle_flow_cycles_total = 1_u64,
        histogram.cycle_duration_ms = cycle_started_at.elapsed().as_millis() as f64,
    );

    Ok(new_rebalance_at)
}

async fn refresh_position_state(
    program: &OracleProgram,
    market_id: u64,
    authority: &anchor_client::solana_sdk::pubkey::Pubkey,
) -> anyhow::Result<(MarketState, LiquidityPosition, LiquidityPositionBalances)> {
    let market_state = fetch_market_state(program, market_id).await?;
    let position = fetch_liquidity_position(program, market_id, authority).await?;
    let balances = get_liquidity_position_balances(
        program,
        position,
        market_state.bookkeeping,
        market_state.market,
        market_state.current_slot,
    )
    .await;

    Ok((market_state, position, balances))
}

#[allow(clippy::too_many_arguments)]
fn emit_position_snapshot(
    stage: &str,
    cycle_id: &str,
    market_id: u64,
    authority: &anchor_client::solana_sdk::pubkey::Pubkey,
    market_state: &MarketState,
    position: &LiquidityPosition,
    balances: &LiquidityPositionBalances,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    oracle_price: f64,
    optimal_quote_weight: f64,
) {
    let base_ui = telemetry::token_amount_ui(balances.base_balance, base_token_decimals);
    let quote_ui = telemetry::token_amount_ui(balances.quote_balance, quote_token_decimals);
    let total_quote_value = base_ui.mul_add(oracle_price, quote_ui);
    let quote_weight = if total_quote_value > 0.0 {
        quote_ui / total_quote_value
    } else {
        0.0
    };
    let inventory_deviation_bps = ((quote_weight - optimal_quote_weight).abs() * 10_000.0).round();

    info!(
        event.name = "position_balance_snapshot",
        snapshot.stage = stage,
        cycle.id = %cycle_id,
        slot.current = market_state.current_slot,
        market.id = market_id,
        lp.authority = %authority,
        base.mint = %market_state.market.base_mint,
        quote.mint = %market_state.market.quote_mint,
        position.base_balance.raw = balances.base_balance,
        position.quote_balance.raw = balances.quote_balance,
        position.base_debt.raw = balances.base_debt,
        position.quote_debt.raw = balances.quote_debt,
        position.base_flow.raw = position.base_flow_u64,
        position.quote_flow.raw = position.quote_flow_u64,
        market.base_flow.raw = market_state.market.base_flow,
        market.quote_flow.raw = market_state.market.quote_flow,
        market.end_slot_interval = market_state.market.end_slot_interval,
        inventory.quote_weight = quote_weight,
        gauge.position_base_balance_raw = balances.base_balance as f64,
        gauge.position_quote_balance_raw = balances.quote_balance as f64,
        gauge.inventory_deviation_bps = inventory_deviation_bps,
    );
}

#[allow(clippy::too_many_arguments)]
async fn execute_update_flows_with_backoff(
    program: &OracleProgram,
    market_id: u64,
    base_flow: u64,
    quote_flow: u64,
    reference_index: u64,
    flow_reduction_factor: f64,
    max_flow_reduction_attempts: usize,
    signer: Arc<anchor_client::solana_sdk::signature::Keypair>,
) -> anyhow::Result<(u64, u64)> {
    let mut candidate_base_flow = base_flow.max(1);
    let mut candidate_quote_flow = quote_flow.max(1);

    for attempt in 0..max_flow_reduction_attempts {
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

        if is_blockhash_not_found(err) {
            // Transient: the blockhash hasn't propagated to all validators yet.
            // The next iteration calls signed_transaction() again, fetching a fresh one.
            warn!(
                event.name = "flow_update_simulation_retry",
                twob.instruction = "update_liquidity_flows",
                twob.reference_index = reference_index,
                update.attempt = attempt + 1,
                update.reason = "blockhash_not_found",
                "simulation returned BlockhashNotFound; retrying with fresh blockhash"
            );
            continue;
        }

        if is_liquidity_position_unhealthy(err, logs) {
            let next_base_flow = reduce_flow(candidate_base_flow, flow_reduction_factor);
            let next_quote_flow = reduce_flow(candidate_quote_flow, flow_reduction_factor);

            warn!(
                event.name = "flow_update_flow_reduced",
                twob.instruction = "update_liquidity_flows",
                twob.reference_index = reference_index,
                update.attempt = attempt + 1,
                update.reason = "liquidity_position_unhealthy",
                quote.previous_base_flow = candidate_base_flow,
                quote.next_base_flow = next_base_flow,
                quote.previous_quote_flow = candidate_quote_flow,
                quote.next_quote_flow = next_quote_flow,
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
        max_flow_reduction_attempts,
        candidate_base_flow,
        candidate_quote_flow
    )
}

fn is_blockhash_not_found(
    err: &Option<anchor_client::solana_sdk::transaction::TransactionError>,
) -> bool {
    matches!(
        err,
        Some(anchor_client::solana_sdk::transaction::TransactionError::BlockhashNotFound)
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
