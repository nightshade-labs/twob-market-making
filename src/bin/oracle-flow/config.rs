use std::env;

use anchor_client::{Cluster, solana_sdk::signature::Keypair};

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
            .unwrap_or_else(|_| "5".to_string())
            .parse::<u64>()?;

        let rebalance_threshold_bps = env::var("REBALANCE_THRESHOLD_BPS")
            .unwrap_or_else(|_| "100".to_string())
            .parse::<u64>()?;

        let quote_threshold_bps = env::var("QUOTE_THRESHOLD_BPS")
            .unwrap_or_else(|_| "50".to_string())
            .parse::<u64>()?;

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
        })
    }

    pub fn cluster(&self) -> Cluster {
        Cluster::Custom(self.rpc_url.clone(), self.ws_url.clone())
    }
}
