use std::env;

use anchor_client::{Cluster, solana_sdk::signature::Keypair};

use crate::telemetry::TelemetryConfig;

#[derive(Clone, Debug)]
pub struct JupiterConfig {
    pub api_key: Option<String>,
    pub swap_api_base_url: String,
    pub max_slippage_bps: u64,
    pub max_price_impact_bps: u64,
    pub compute_unit_price_percentile: String,
    pub fallback_compute_unit_price_micro_lamports: u64,
    pub max_accounts: u64,
    pub swap_mode: Option<String>,
    pub dry_run: bool,
}

pub struct Config {
    pub keypair: Keypair,
    pub rpc_url: String,
    pub ws_url: String,
    pub market_id: u64,
    pub price_feed_url: String,
    pub base_token_decimals: u8,
    pub quote_token_decimals: u8,
    pub optimal_quote_weight: f64,
    pub poll_interval_secs: u64,
    pub rebalance_threshold_bps: u64,
    pub quote_threshold_bps: u64,
    pub max_quote_price_deviation_bps: u64,
    pub max_oracle_age_secs: u64,
    pub max_oracle_future_skew_secs: u64,
    pub flow_divisor: u64,
    pub flow_reduction_factor: f64,
    pub max_flow_reduction_attempts: usize,
    pub min_rebalance_value_usd: f64,
    pub jupiter: JupiterConfig,
    pub telemetry: TelemetryConfig,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let keypair_bytes: Vec<u8> = serde_json::from_str(
            &env::var("ORACLE_FLOW_KEYPAIR")
                .map_err(|_| anyhow::anyhow!("KEYPAIR env var not set"))?,
        )?;
        let keypair = Keypair::try_from(keypair_bytes.as_slice())
            .map_err(|e| anyhow::anyhow!("Invalid keypair: {}", e))?;

        let rpc_url = env::var("RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());

        let ws_url = env::var("WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:8900".to_string());

        let market_id = env::var("MARKET_ID")
            .unwrap_or_else(|_| "1".to_string())
            .parse::<u64>()?;

        let price_feed_url = env::var("PRICE_FEED_URL").unwrap_or_else(|_| {
            let base_url = env::var("PRICE_FEED_BASE_URL")
                .unwrap_or_else(|_| "http://localhost:8080/api/v1/price".to_string());
            let base_token = env::var("BASE_TOKEN").unwrap_or_else(|_| "SOL".to_string());
            let quote_token = env::var("QUOTE_TOKEN").unwrap_or_else(|_| "USDC".to_string());

            format!(
                "{}/{}/{}",
                base_url.trim_end_matches('/'),
                base_token.trim(),
                quote_token.trim(),
            )
        });

        let base_token_decimals = env::var("BASE_TOKEN_DECIMALS")
            .unwrap_or_else(|_| "9".to_string())
            .parse::<u8>()?;

        let quote_token_decimals = env::var("QUOTE_TOKEN_DECIMALS")
            .unwrap_or_else(|_| "6".to_string())
            .parse::<u8>()?;

        let optimal_quote_weight = env::var("OPTIMAL_QUOTE_WEIGHT")
            .unwrap_or_else(|_| "0.1".to_string())
            .parse::<f64>()?;

        let poll_interval_secs = env::var("POLL_INTERVAL_SECS")
            .unwrap_or_else(|_| "1".to_string())
            .parse::<u64>()?;

        let rebalance_threshold_bps = env::var("REBALANCE_THRESHOLD_BPS")
            .unwrap_or_else(|_| "100".to_string())
            .parse::<u64>()?;

        let quote_threshold_bps = env::var("QUOTE_THRESHOLD_BPS")
            .unwrap_or_else(|_| "50".to_string())
            .parse::<u64>()?;

        let max_quote_price_deviation_bps = parse_bps(
            "MAX_QUOTE_PRICE_DEVIATION_BPS",
            env::var("MAX_QUOTE_PRICE_DEVIATION_BPS").ok(),
            100,
        )?;

        let max_oracle_age_secs = parse_positive_u64(
            "MAX_ORACLE_AGE_SECS",
            env::var("MAX_ORACLE_AGE_SECS").ok(),
            30,
        )?;

        let max_oracle_future_skew_secs = env::var("MAX_ORACLE_FUTURE_SKEW_SECS")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<u64>()?;

        let flow_divisor = parse_flow_divisor(env::var("FLOW_DIVISOR").ok())?;

        let flow_reduction_factor = parse_flow_reduction_factor(
            env::var("FLOW_REDUCTION_FACTOR")
                .unwrap_or_else(|_| "0.99".to_string())
                .parse::<f64>()?,
        )?;

        let max_flow_reduction_attempts = parse_positive_usize(
            "MAX_FLOW_REDUCTION_ATTEMPTS",
            env::var("MAX_FLOW_REDUCTION_ATTEMPTS")
                .unwrap_or_else(|_| "200".to_string())
                .parse::<usize>()?,
        )?;

        let min_rebalance_value_usd = env::var("MIN_REBALANCE_VALUE_USD")
            .unwrap_or_else(|_| "1.0".to_string())
            .parse::<f64>()?;

        let telemetry = TelemetryConfig::from_env()?;

        let jupiter = JupiterConfig {
            api_key: env::var("JUPITER_API_KEY")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            swap_api_base_url: env::var("JUPITER_SWAP_API_BASE_URL")
                .or_else(|_| env::var("JUPITER_API_BASE_URL"))
                .unwrap_or_else(|_| "https://api.jup.ag/swap/v2".to_string()),
            max_slippage_bps: env::var("JUPITER_MAX_SLIPPAGE_BPS")
                .unwrap_or_else(|_| "50".to_string())
                .parse::<u64>()?,
            max_price_impact_bps: env::var("JUPITER_MAX_PRICE_IMPACT_BPS")
                .unwrap_or_else(|_| "50".to_string())
                .parse::<u64>()?,
            compute_unit_price_percentile: env::var("JUPITER_COMPUTE_UNIT_PRICE_PERCENTILE")
                .unwrap_or_else(|_| "high".to_string()),
            fallback_compute_unit_price_micro_lamports: env::var(
                "JUPITER_FALLBACK_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS",
            )
            .unwrap_or_else(|_| "25000".to_string())
            .parse::<u64>()?,
            max_accounts: env::var("JUPITER_MAX_ACCOUNTS")
                .unwrap_or_else(|_| "48".to_string())
                .parse::<u64>()?,
            swap_mode: env::var("JUPITER_SWAP_MODE")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            dry_run: env::var("JUPITER_DRY_RUN")
                .unwrap_or_else(|_| "false".to_string())
                .parse::<bool>()?,
        };

        Ok(Self {
            keypair,
            rpc_url,
            ws_url,
            market_id,
            price_feed_url,
            base_token_decimals,
            quote_token_decimals,
            optimal_quote_weight,
            poll_interval_secs,
            rebalance_threshold_bps,
            quote_threshold_bps,
            max_quote_price_deviation_bps,
            max_oracle_age_secs,
            max_oracle_future_skew_secs,
            flow_divisor,
            flow_reduction_factor,
            max_flow_reduction_attempts,
            min_rebalance_value_usd,
            jupiter,
            telemetry,
        })
    }

    pub fn cluster(&self) -> Cluster {
        Cluster::Custom(self.rpc_url.clone(), self.ws_url.clone())
    }
}

fn parse_flow_divisor(raw: Option<String>) -> anyhow::Result<u64> {
    parse_positive_u64("FLOW_DIVISOR", raw, 5)
}

fn parse_positive_u64(name: &str, raw: Option<String>, default: u64) -> anyhow::Result<u64> {
    let value = raw.unwrap_or_else(|| default.to_string()).parse::<u64>()?;
    if value == 0 {
        anyhow::bail!("{name} must be greater than 0");
    }

    Ok(value)
}

fn parse_bps(name: &str, raw: Option<String>, default: u64) -> anyhow::Result<u64> {
    let value = raw.unwrap_or_else(|| default.to_string()).parse::<u64>()?;
    if value > 10_000 {
        anyhow::bail!("{name} must not exceed 10000");
    }

    Ok(value)
}

fn parse_flow_reduction_factor(value: f64) -> anyhow::Result<f64> {
    if !value.is_finite() || value <= 0.0 || value >= 1.0 {
        anyhow::bail!("FLOW_REDUCTION_FACTOR must be finite and between 0 and 1")
    }

    Ok(value)
}

fn parse_positive_usize(name: &str, value: usize) -> anyhow::Result<usize> {
    if value == 0 {
        anyhow::bail!("{name} must be greater than 0")
    }

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_divisor_defaults_to_five() {
        assert_eq!(parse_flow_divisor(None).unwrap(), 5);
    }

    #[test]
    fn flow_divisor_accepts_env_override() {
        assert_eq!(parse_flow_divisor(Some("8".to_string())).unwrap(), 8);
    }

    #[test]
    fn flow_divisor_rejects_zero() {
        assert!(parse_flow_divisor(Some("0".to_string())).is_err());
    }

    #[test]
    fn quote_price_deviation_rejects_more_than_one_hundred_percent() {
        assert!(parse_bps("TEST_BPS", Some("10001".to_string()), 100).is_err());
    }

    #[test]
    fn positive_values_reject_zero() {
        assert!(parse_positive_u64("TEST_VALUE", Some("0".to_string()), 1).is_err());
    }

    #[test]
    fn flow_reduction_factor_must_reduce() {
        for invalid in [f64::NAN, f64::INFINITY, 0.0, 1.0, 1.1] {
            assert!(parse_flow_reduction_factor(invalid).is_err());
        }
        assert_eq!(parse_flow_reduction_factor(0.95).unwrap(), 0.95);
    }

    #[test]
    fn reduction_attempts_must_be_positive() {
        assert!(parse_positive_usize("TEST_ATTEMPTS", 0).is_err());
    }
}
