use std::env;

use anchor_client::{Cluster, solana_sdk::signature::Keypair};

pub struct Config {
    pub keypair: Keypair,
    pub rpc_url: String,
    pub ws_url: String,
    pub market_id: u64,
    pub flow_divisor: u64,
}

pub struct DelayConfig {
    pub critical_threshold: u128,
    pub safe_threshold: u128,
    pub critical_delay_ms: u128,
    pub normal_delay_ms: u128,
    pub delay_scale_factor: u128,
    pub max_additional_slots: u128,
}

impl Default for DelayConfig {
    fn default() -> Self {
        Self {
            critical_threshold: 25,
            safe_threshold: 10_000,
            critical_delay_ms: 100,
            normal_delay_ms: 2_000,
            delay_scale_factor: 400,
            max_additional_slots: 1_000,
        }
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let keypair_bytes: Vec<u8> = serde_json::from_str(
            &env::var("INVENTORY_FLOW_KEYPAIR")
                .map_err(|_| anyhow::anyhow!("KEYPAIR env var not set"))?,
        )?;
        let keypair = Keypair::try_from(keypair_bytes.as_slice())
            .map_err(|e| anyhow::anyhow!("Invalid keypair: {}", e))?;

        let rpc_url = env::var("RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());

        let ws_url = env::var("WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:8900".to_string());

        let market_id = env::var("MARKET_ID")
            .unwrap_or_else(|_| "1".to_string())
            .parse::<u64>()?;

        let flow_divisor = env::var("FLOW_DIVISOR")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<u64>()?;

        Ok(Self {
            keypair,
            rpc_url,
            ws_url,
            market_id,
            flow_divisor,
        })
    }

    pub fn cluster(&self) -> Cluster {
        Cluster::Custom(self.rpc_url.clone(), self.ws_url.clone())
    }
}
