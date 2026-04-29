use std::{env, time::Duration};

use anyhow::{Context, Result, anyhow};
use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider, Temporality},
    trace::SdkTracerProvider,
};
use tracing::warn;
use tracing_error::ErrorLayer;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{
    Layer, filter::EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

const DEFAULT_SERVICE_NAME: &str = "twob-market-maker";
const DEFAULT_BALANCE_SNAPSHOT_INTERVAL_SECS: u64 = 60;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelemetryConfig {
    pub service_name: String,
    pub stdout_json: bool,
    pub balance_snapshot_interval_secs: u64,
}

impl TelemetryConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| env::var(key).ok())
    }

    fn from_lookup<F>(lookup: F) -> Result<Self>
    where
        F: Fn(&str) -> Option<String>,
    {
        let service_name = lookup("OTEL_SERVICE_NAME")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_string());
        let stdout_json = lookup("TELEMETRY_STDOUT_JSON")
            .map(|value| parse_bool(&value))
            .transpose()?
            .unwrap_or(true);
        let balance_snapshot_interval_secs = lookup("BALANCE_SNAPSHOT_INTERVAL_SECS")
            .map(|value| {
                value.parse::<u64>().with_context(|| {
                    format!("invalid BALANCE_SNAPSHOT_INTERVAL_SECS value `{value}`")
                })
            })
            .transpose()?
            .unwrap_or(DEFAULT_BALANCE_SNAPSHOT_INTERVAL_SECS);

        Ok(Self {
            service_name,
            stdout_json,
            balance_snapshot_interval_secs,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OtlpExporterConfig {
    endpoint: Option<String>,
    headers: Vec<(String, String)>,
}

impl OtlpExporterConfig {
    fn from_env() -> Self {
        Self::from_lookup(|key| env::var(key).ok())
    }

    fn from_lookup<F>(lookup: F) -> Self
    where
        F: Fn(&str) -> Option<String>,
    {
        let endpoint = lookup("OTEL_EXPORTER_OTLP_ENDPOINT")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let headers = lookup("OTEL_EXPORTER_OTLP_HEADERS")
            .map(|value| parse_otlp_headers(&value))
            .unwrap_or_default();

        Self { endpoint, headers }
    }

    fn enabled(&self) -> bool {
        self.endpoint.is_some()
    }
}

#[derive(Clone, Debug)]
pub struct TelemetryInitConfig {
    pub service_name: String,
    pub stdout_json: bool,
    pub market_id: u64,
    pub authority: String,
    pub rpc_url: String,
    pub program_id: String,
}

#[must_use]
pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.logger_provider.take()
            && let Err(error) = provider.shutdown()
        {
            warn!(event.name = "telemetry_shutdown_error", telemetry.signal = "logs", %error);
        }
        if let Some(provider) = self.meter_provider.take()
            && let Err(error) = provider.shutdown()
        {
            warn!(event.name = "telemetry_shutdown_error", telemetry.signal = "metrics", %error);
        }
        if let Some(provider) = self.tracer_provider.take()
            && let Err(error) = provider.shutdown()
        {
            warn!(event.name = "telemetry_shutdown_error", telemetry.signal = "traces", %error);
        }
    }
}

pub fn init_telemetry(config: TelemetryInitConfig) -> Result<TelemetryGuard> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let resource = telemetry_resource(&config);
    let otlp_config = OtlpExporterConfig::from_env();

    let fmt_json_layer = fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .with_target(true)
        .boxed();
    let fmt_pretty_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .boxed();

    let stdout_layer = if config.stdout_json {
        fmt_json_layer
    } else {
        fmt_pretty_layer
    };

    let base_subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(ErrorLayer::default())
        .with(stdout_layer);

    let Some(endpoint) = otlp_config.endpoint.as_deref() else {
        base_subscriber.try_init().map_err(|error| anyhow!(error))?;
        return Ok(TelemetryGuard {
            tracer_provider: None,
            meter_provider: None,
            logger_provider: None,
        });
    };

    let headers_configured = !otlp_config.headers.is_empty();

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .build()
        .context("failed to build OTLP trace exporter")?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();
    let tracer = tracer_provider.tracer(config.service_name.clone());

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_temporality(Temporality::default())
        .build()
        .context("failed to build OTLP metric exporter")?;
    let metric_reader = PeriodicReader::builder(metric_exporter)
        .with_interval(Duration::from_secs(30))
        .build();
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(metric_reader)
        .build();
    global::set_meter_provider(meter_provider.clone());

    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .build()
        .context("failed to build OTLP log exporter")?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(log_exporter)
        .build();

    let telemetry_log_layer = OpenTelemetryTracingBridge::new(&logger_provider);
    let telemetry_trace_layer = OpenTelemetryLayer::new(tracer);
    let telemetry_metric_layer = MetricsLayer::new(meter_provider.clone());

    base_subscriber
        .with(telemetry_log_layer)
        .with(telemetry_trace_layer)
        .with(telemetry_metric_layer)
        .try_init()
        .map_err(|error| anyhow!(error))?;

    tracing::info!(
        event.name = "telemetry_initialized",
        otel.endpoint = %endpoint,
        otel.headers_configured = headers_configured,
        otel.exporter_enabled = otlp_config.enabled(),
        otel.protocol = "http/protobuf",
        service.name = %config.service_name,
        market.id = config.market_id,
        lp.authority = %config.authority,
    );

    Ok(TelemetryGuard {
        tracer_provider: Some(tracer_provider),
        meter_provider: Some(meter_provider),
        logger_provider: Some(logger_provider),
    })
}

pub fn balance_delta(after: u64, before: u64) -> i128 {
    i128::from(after) - i128::from(before)
}

pub fn external_wallet_input_estimated(input_consumed: u64, available_budget: u64) -> u64 {
    input_consumed.saturating_sub(available_budget)
}

pub fn token_amount_ui(raw_amount: u64, decimals: u8) -> f64 {
    let scale = 10_u64.checked_pow(u32::from(decimals)).unwrap_or(1) as f64;
    raw_amount as f64 / scale
}

pub fn parse_otlp_headers(headers: &str) -> Vec<(String, String)> {
    headers
        .split(',')
        .filter_map(|entry| {
            let trimmed = entry.trim();
            if trimmed.is_empty() {
                return None;
            }
            let (key, value) = trimmed.split_once('=')?;
            let key = key.trim();
            if key.is_empty() {
                return None;
            }
            Some((key.to_string(), value.trim().to_string()))
        })
        .collect()
}

fn telemetry_resource(config: &TelemetryInitConfig) -> Resource {
    Resource::builder()
        .with_service_name(config.service_name.clone())
        .with_attributes([
            KeyValue::new("service.namespace", "twob"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("deployment.environment.name", deployment_environment()),
            KeyValue::new("bot.role", "oracle-flow"),
            KeyValue::new("solana.cluster", solana_cluster(&config.rpc_url)),
            KeyValue::new("market.id", config.market_id.to_string()),
            KeyValue::new("twob.program_id", config.program_id.clone()),
            KeyValue::new("lp.authority", config.authority.clone()),
        ])
        .build()
}

fn deployment_environment() -> String {
    env::var("DEPLOYMENT_ENVIRONMENT_NAME")
        .or_else(|_| env::var("DEPLOYMENT_ENVIRONMENT"))
        .or_else(|_| env::var("RAILWAY_ENVIRONMENT_NAME"))
        .or_else(|_| {
            env::var("OTEL_RESOURCE_ATTRIBUTES")
                .ok()
                .and_then(|attributes| {
                    resource_attribute_value(&attributes, "deployment.environment.name")
                })
                .ok_or(env::VarError::NotPresent)
        })
        .unwrap_or_else(|_| "unknown".to_string())
}

fn resource_attribute_value(attributes: &str, target_key: &str) -> Option<String> {
    attributes.split(',').find_map(|entry| {
        let (key, value) = entry.trim().split_once('=')?;
        if key.trim() == target_key {
            Some(value.trim().to_string())
        } else {
            None
        }
    })
}

fn solana_cluster(rpc_url: &str) -> &'static str {
    let lower = rpc_url.to_ascii_lowercase();
    if lower.contains("devnet") {
        "devnet"
    } else if lower.contains("testnet") {
        "testnet"
    } else if lower.contains("mainnet") {
        "mainnet-beta"
    } else if lower.contains("localhost") || lower.contains("127.0.0.1") {
        "localnet"
    } else {
        "custom"
    }
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "no" | "n" | "off" => Ok(false),
        other => Err(anyhow!("invalid boolean value `{other}`")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn parses_telemetry_config_defaults() {
        let config = TelemetryConfig::from_lookup(|_| None).unwrap();

        assert_eq!(config.service_name, DEFAULT_SERVICE_NAME);
        assert!(config.stdout_json);
        assert_eq!(
            config.balance_snapshot_interval_secs,
            DEFAULT_BALANCE_SNAPSHOT_INTERVAL_SECS
        );
    }

    #[test]
    fn parses_telemetry_config_from_lookup() {
        let env = HashMap::from([
            ("OTEL_SERVICE_NAME", "custom-service"),
            ("TELEMETRY_STDOUT_JSON", "false"),
            ("BALANCE_SNAPSHOT_INTERVAL_SECS", "15"),
        ]);
        let config =
            TelemetryConfig::from_lookup(|key| env.get(key).map(|value| value.to_string()))
                .unwrap();

        assert_eq!(config.service_name, "custom-service");
        assert!(!config.stdout_json);
        assert_eq!(config.balance_snapshot_interval_secs, 15);
    }

    #[test]
    fn parses_otlp_headers_without_exposing_values() {
        let headers = parse_otlp_headers("authorization=secret, x-team = market-making ");

        assert_eq!(
            headers,
            vec![
                ("authorization".to_string(), "secret".to_string()),
                ("x-team".to_string(), "market-making".to_string())
            ]
        );
    }

    #[test]
    fn treats_otlp_exporter_as_disabled_without_endpoint() {
        let exporter = OtlpExporterConfig::from_lookup(|_| None);

        assert!(!exporter.enabled());
        assert_eq!(exporter.endpoint, None);
        assert!(exporter.headers.is_empty());
    }

    #[test]
    fn parses_otlp_exporter_endpoint_and_headers() {
        let env = HashMap::from([
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                " https://collector.example:4318 ",
            ),
            ("OTEL_EXPORTER_OTLP_HEADERS", "authorization=secret"),
        ]);
        let exporter =
            OtlpExporterConfig::from_lookup(|key| env.get(key).map(|value| value.to_string()));

        assert!(exporter.enabled());
        assert_eq!(
            exporter.endpoint,
            Some("https://collector.example:4318".to_string())
        );
        assert_eq!(
            exporter.headers,
            vec![("authorization".to_string(), "secret".to_string())]
        );
    }

    #[test]
    fn reads_deployment_environment_from_resource_attributes() {
        assert_eq!(
            resource_attribute_value(
                "service.namespace=twob,deployment.environment.name=prod,bot.role=oracle-flow",
                "deployment.environment.name",
            ),
            Some("prod".to_string())
        );
    }

    #[test]
    fn computes_balance_delta() {
        assert_eq!(balance_delta(110, 100), 10);
        assert_eq!(balance_delta(90, 100), -10);
    }

    #[test]
    fn estimates_external_wallet_input() {
        assert_eq!(external_wallet_input_estimated(150, 100), 50);
        assert_eq!(external_wallet_input_estimated(80, 100), 0);
    }

    #[test]
    fn formats_token_amount_ui() {
        assert_eq!(token_amount_ui(1_000_000, 6), 1.0);
        assert_eq!(token_amount_ui(1_000_000_000, 9), 1.0);
    }
}
