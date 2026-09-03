mod config;
mod jupiter;
mod price;
mod quote;
mod quote_guard;
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
use anyhow::{Context, ensure};
use config::{Config, JupiterConfig};
use price::{PriceData, fetch_price};
use quote::{QuoteCalculationConfig, calculate_optimal_quote, should_update_quote};
use quote_guard::{validate_oracle_freshness, validate_quote};
use rebalance::{RebalanceOutcome, execute_rebalance, needs_rebalance};
use tokio::{signal, time::sleep};
use tracing::{Instrument, error, info, info_span, warn};
use twob_market_making::{
    ARRAY_LENGTH, LiquidityPositionBalances, MarketState, build_update_liquidity_flows_instruction,
    execute_update_flows, fetch_market_position_state, get_liquidity_position_balances,
    twob_anchor::{self, accounts::LiquidityPosition},
};

const LIQUIDITY_POSITION_UNHEALTHY_ERROR_CODE: u32 = 6014;
const BALANCED_QUOTE_VALUE_WEIGHT: f64 = 0.5;
type OracleProgram = anchor_client::Program<Arc<anchor_client::solana_sdk::signature::Keypair>>;

#[derive(Clone, Copy)]
struct QuoteSafetyConfig {
    max_price_deviation_bps: u64,
    max_oracle_age_secs: u64,
    max_oracle_future_skew_secs: u64,
}

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
    let quote_safety = QuoteSafetyConfig {
        max_price_deviation_bps: config.max_quote_price_deviation_bps,
        max_oracle_age_secs: config.max_oracle_age_secs,
        max_oracle_future_skew_secs: config.max_oracle_future_skew_secs,
    };
    let flow_divisor = config.flow_divisor;
    let flow_reduction_factor = config.flow_reduction_factor;
    let max_flow_reduction_attempts = config.max_flow_reduction_attempts;
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
        quote.max_price_deviation_bps = quote_safety.max_price_deviation_bps,
        oracle.max_age_secs = quote_safety.max_oracle_age_secs,
        oracle.max_future_skew_secs = quote_safety.max_oracle_future_skew_secs,
        quote.flow_divisor = flow_divisor,
        jupiter.api_key_configured = jupiter_config.api_key.is_some(),
        jupiter.dry_run = jupiter_config.dry_run,
        jupiter.swap_api_base_url = %jupiter_config.swap_api_base_url,
        jupiter.compute_unit_price_percentile = %jupiter_config.compute_unit_price_percentile,
        jupiter.max_accounts = jupiter_config.max_accounts,
        solana.devnet_mode = is_devnet,
        rebalance.min_value_usd = min_rebalance_value_usd,
        balance_snapshot_interval_secs = telemetry_config.balance_snapshot_interval_secs,
    );

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
                    quote_safety,
                    flow_divisor,
                    flow_reduction_factor,
                    max_flow_reduction_attempts,
                    min_rebalance_value_usd,
                    &jupiter_config,
                    is_devnet,
                    market_id,
                    &authority,
                    liquidity_provider.clone(),
                    &cycle_id,
                ).instrument(cycle_span).await {
                    Ok(()) => {}
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
    quote_safety: QuoteSafetyConfig,
    flow_divisor: u64,
    flow_reduction_factor: f64,
    max_flow_reduction_attempts: usize,
    min_rebalance_value_usd: f64,
    jupiter_config: &JupiterConfig,
    is_devnet: bool,
    market_id: u64,
    authority: &anchor_client::solana_sdk::pubkey::Pubkey,
    liquidity_provider: Arc<anchor_client::solana_sdk::signature::Keypair>,
    cycle_id: &str,
) -> anyhow::Result<()> {
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
    validate_oracle_freshness(
        price_data.price,
        price_data.timestamp,
        current_unix_timestamp()?,
        quote_safety.max_oracle_age_secs,
        quote_safety.max_oracle_future_skew_secs,
    )
    .context("oracle price failed freshness validation")?;
    info!(
        event.name = "price_fetched",
        cycle.id = %cycle_id,
        market.id = market_id,
        price.oracle = price_data.price,
        price.timestamp = price_data.timestamp,
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
    );

    // 3. Check if rebalance is needed
    let rebalance_needed = {
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

    if rebalance_needed {
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
            min_rebalance_value_usd,
            quote_safety.max_price_deviation_bps,
            quote_safety.max_oracle_age_secs,
            quote_safety.max_oracle_future_skew_secs,
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
                        return Ok(());
                    }
                }
                info!(
                    event.name = "rebalance_completed",
                    cycle.id = %cycle_id,
                    market.id = market_id,
                    lp.authority = %authority,
                    rebalance.attempt_id = %attempt_id,
                    rebalance.outcome = "executed",
                    histogram.rebalance_duration_ms = attempt_started_at.elapsed().as_millis() as f64,
                );

                if needs_rebalance(
                    &price_data,
                    &balances,
                    base_token_decimals,
                    quote_token_decimals,
                    rebalance_threshold_bps,
                ) {
                    error!(
                        event.name = "rebalance_postcondition_failed",
                        cycle.id = %cycle_id,
                        market.id = market_id,
                        lp.authority = %authority,
                        rebalance.attempt_id = %attempt_id,
                        "position remains outside the rebalance threshold; holding current quote"
                    );
                    return Ok(());
                }
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
                return Ok(());
            }
            Err(error) => {
                error!(
                    event.name = "rebalance_failed",
                    cycle.id = %cycle_id,
                    market.id = market_id,
                    lp.authority = %authority,
                    rebalance.attempt_id = %attempt_id,
                    rebalance.outcome = "error",
                    histogram.rebalance_duration_ms = attempt_started_at.elapsed().as_millis() as f64,
                    ?error,
                    "rebalance did not complete; holding current quote"
                );
                return Ok(());
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
            QuoteCalculationConfig {
                base_token_decimals,
                quote_token_decimals,
                weight: optimal_quote_weight,
                flow_divisor,
                max_price_deviation_bps: quote_safety.max_price_deviation_bps,
            },
        )
    };

    // 5. Get current quote from position
    let current_base_flow = position.base_flow_u64;
    let current_quote_flow = position.quote_flow_u64;
    let current_quote_safe = match validate_candidate_quote(
        current_base_flow,
        current_quote_flow,
        base_token_decimals,
        quote_token_decimals,
        &price_data,
        quote_safety,
    ) {
        Ok(()) => true,
        Err(error) => {
            error!(
                event.name = "current_quote_safety_violation",
                cycle.id = %cycle_id,
                market.id = market_id,
                lp.authority = %authority,
                quote.current_base_flow = current_base_flow,
                quote.current_quote_flow = current_quote_flow,
                ?error,
                "current quote is outside the oracle safety envelope"
            );
            false
        }
    };

    // 6. Check if update is needed
    if !current_quote_safe
        || should_update_quote(
            current_base_flow,
            current_quote_flow,
            &optimal,
            quote_threshold_bps,
        )
    {
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
            &price_data,
            base_token_decimals,
            quote_token_decimals,
            quote_safety,
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
    );
    info!(
        event.name = "oracle_flow_cycle_end",
        cycle.id = %cycle_id,
        market.id = market_id,
        lp.authority = %authority,
        monotonic_counter.oracle_flow_cycles_total = 1_u64,
        histogram.cycle_duration_ms = cycle_started_at.elapsed().as_millis() as f64,
    );

    Ok(())
}

async fn refresh_position_state(
    program: &OracleProgram,
    market_id: u64,
    authority: &anchor_client::solana_sdk::pubkey::Pubkey,
) -> anyhow::Result<(MarketState, LiquidityPosition, LiquidityPositionBalances)> {
    let (market_state, position) =
        fetch_market_position_state(program, market_id, authority).await?;
    let balances = get_liquidity_position_balances(
        program,
        position,
        market_state.bookkeeping,
        market_state.market,
        market_state.current_slot,
    )
    .await?;
    let (validation_market_state, validation_position) =
        fetch_market_position_state(program, market_id, authority).await?;
    ensure!(
        snapshot_inputs_match(
            &market_state,
            &position,
            &validation_market_state,
            &validation_position,
        ),
        "market or position changed while reconstructing balances"
    );

    Ok((market_state, position, balances))
}

fn snapshot_inputs_match(
    first_market: &MarketState,
    first_position: &LiquidityPosition,
    second_market: &MarketState,
    second_position: &LiquidityPosition,
) -> bool {
    first_market.market.base_flow == second_market.market.base_flow
        && first_market.market.quote_flow == second_market.market.quote_flow
        && first_market.market.end_slot_interval == second_market.market.end_slot_interval
        && first_market.bookkeeping.last_update_slot == second_market.bookkeeping.last_update_slot
        && first_market.bookkeeping.base_per_quote == second_market.bookkeeping.base_per_quote
        && first_market.bookkeeping.quote_per_base == second_market.bookkeeping.quote_per_base
        && first_market.bookkeeping.slots_without_trade
            == second_market.bookkeeping.slots_without_trade
        && first_position.last_update_slot == second_position.last_update_slot
        && first_position.slots_without_trade_snapshot
            == second_position.slots_without_trade_snapshot
        && first_position.base_per_quote_snapshot == second_position.base_per_quote_snapshot
        && first_position.quote_per_base_snapshot == second_position.quote_per_base_snapshot
        && first_position.base_balance == second_position.base_balance
        && first_position.quote_balance == second_position.quote_balance
        && first_position.base_debt == second_position.base_debt
        && first_position.quote_debt == second_position.quote_debt
        && first_position.base_flow_u64 == second_position.base_flow_u64
        && first_position.quote_flow_u64 == second_position.quote_flow_u64
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
) {
    let base_ui = telemetry::token_amount_ui(balances.base_balance, base_token_decimals);
    let quote_ui = telemetry::token_amount_ui(balances.quote_balance, quote_token_decimals);
    let total_quote_value = base_ui.mul_add(oracle_price, quote_ui);
    let quote_weight = if total_quote_value > 0.0 {
        quote_ui / total_quote_value
    } else {
        0.0
    };
    let inventory_deviation_bps =
        ((quote_weight - BALANCED_QUOTE_VALUE_WEIGHT).abs() * 10_000.0).round();

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
        inventory.quote_weight_target = BALANCED_QUOTE_VALUE_WEIGHT,
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
    price_data: &PriceData,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    quote_safety: QuoteSafetyConfig,
) -> anyhow::Result<(u64, u64)> {
    let original_base_flow = base_flow.max(1);
    let original_quote_flow = quote_flow.max(1);
    let mut candidate_base_flow = original_base_flow;
    let mut candidate_quote_flow = original_quote_flow;

    for attempt in 0..max_flow_reduction_attempts {
        validate_candidate_quote(
            candidate_base_flow,
            candidate_quote_flow,
            base_token_decimals,
            quote_token_decimals,
            price_data,
            quote_safety,
        )?;
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
            validate_candidate_quote(
                candidate_base_flow,
                candidate_quote_flow,
                base_token_decimals,
                quote_token_decimals,
                price_data,
                quote_safety,
            )?;
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
            let (next_base_flow, next_quote_flow) = reduce_flow_pair(
                original_base_flow,
                original_quote_flow,
                candidate_base_flow,
                candidate_quote_flow,
                flow_reduction_factor,
            );

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
                || line.contains("custom program error: 0x177e")
        })
    })
    .unwrap_or(false)
}

fn validate_candidate_quote(
    base_flow: u64,
    quote_flow: u64,
    base_token_decimals: u8,
    quote_token_decimals: u8,
    price_data: &PriceData,
    quote_safety: QuoteSafetyConfig,
) -> anyhow::Result<()> {
    validate_quote(
        base_flow,
        quote_flow,
        base_token_decimals,
        quote_token_decimals,
        price_data.price,
        price_data.timestamp,
        current_unix_timestamp()?,
        quote_safety.max_price_deviation_bps,
        quote_safety.max_oracle_age_secs,
        quote_safety.max_oracle_future_skew_secs,
    )
    .context("candidate quote failed safety validation")
}

fn current_unix_timestamp() -> anyhow::Result<u64> {
    let timestamp = chrono::Utc::now().timestamp();
    u64::try_from(timestamp).context("system clock is before the Unix epoch")
}

fn reduce_flow(flow: u64, factor: f64) -> u64 {
    if flow <= 1 {
        return flow;
    }

    let reduced = ((flow as f64) * factor).floor() as u64;
    reduced.clamp(1, flow - 1)
}

fn reduce_flow_pair(
    original_base_flow: u64,
    original_quote_flow: u64,
    current_base_flow: u64,
    current_quote_flow: u64,
    factor: f64,
) -> (u64, u64) {
    if original_base_flow >= original_quote_flow {
        let next_base_flow = reduce_flow(current_base_flow, factor);
        let next_quote_flow = scale_flow(original_quote_flow, next_base_flow, original_base_flow);
        (next_base_flow, next_quote_flow)
    } else {
        let next_quote_flow = reduce_flow(current_quote_flow, factor);
        let next_base_flow = scale_flow(original_base_flow, next_quote_flow, original_quote_flow);
        (next_base_flow, next_quote_flow)
    }
}

fn scale_flow(original_flow: u64, new_anchor_flow: u64, original_anchor_flow: u64) -> u64 {
    let scaled = u128::from(original_flow) * u128::from(new_anchor_flow)
        / u128::from(original_anchor_flow.max(1));
    u64::try_from(scaled).unwrap_or(u64::MAX).max(1)
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

    #[test]
    fn pair_reduction_scales_both_flows_from_the_original_ratio() {
        let (base, quote) =
            reduce_flow_pair(1_000_000_000, 100_000_000, 900_000_000, 90_000_000, 0.5);

        assert_eq!((base, quote), (450_000_000, 45_000_000));
    }
}
