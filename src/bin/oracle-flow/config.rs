use std::env;

use anchor_client::Cluster;

pub struct Config {
    pub keypair_path: String,
    pub rpc_url: String,
    pub ws_url: String,
    pub market_id: u64,
    pub price_feed_url: String,
    pub poll_interval_secs: u64,
    pub quote_threshold_bps: u64,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let keypair_path = env::var("KEYPAIR_PATH")
            .unwrap_or_else(|_| "Users/thgehr/.config/solana/id.json".to_string());

        let rpc_url = env::var("RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());

        let ws_url = env::var("WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:8900".to_string());

        let market_id = env::var("MARKET_ID")
            .unwrap_or_else(|_| "1".to_string())
            .parse::<u64>()?;

        let price_feed_url = env::var("PRICE_FEED_URL")
            .unwrap_or_else(|_| "http://localhost:8080/price".to_string());

        let poll_interval_secs = env::var("POLL_INTERVAL_SECS")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<u64>()?;

        let quote_threshold_bps = env::var("QUOTE_THRESHOLD_BPS")
            .unwrap_or_else(|_| "50".to_string())
            .parse::<u64>()?;

        Ok(Self {
            keypair_path,
            rpc_url,
            ws_url,
            market_id,
            price_feed_url,
            poll_interval_secs,
            quote_threshold_bps,
        })
    }

    pub fn cluster(&self) -> Cluster {
        Cluster::Custom(self.rpc_url.clone(), self.ws_url.clone())
    }
}
