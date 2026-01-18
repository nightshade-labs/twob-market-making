use std::env;

use anchor_client::Cluster;

pub struct Config {
    pub keypair_path: String,
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

fn expand_tilde(path: &str) -> String {
    if path.starts_with("~/")
        && let Some(home) = env::var_os("HOME")
    {
        return format!("{}{}", home.to_string_lossy(), &path[1..]);
    }
    path.to_string()
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let keypair_path =
            env::var("KEYPAIR_PATH").unwrap_or_else(|_| expand_tilde("~/.config/solana/id.json"));

        let rpc_url = env::var("RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());

        let ws_url = env::var("WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:8900".to_string());

        let market_id = env::var("MARKET_ID")
            .unwrap_or_else(|_| "1".to_string())
            .parse::<u64>()?;

        let flow_divisor = env::var("FLOW_DIVISOR")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<u64>()?;

        Ok(Self {
            keypair_path,
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
