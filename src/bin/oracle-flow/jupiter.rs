use std::{collections::HashMap, sync::Arc, time::Instant};

use anchor_client::solana_sdk::{
    address_lookup_table::AddressLookupTableAccount,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use anyhow::{Context, ensure};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::config::JupiterConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapDirection {
    BaseToQuote,
    QuoteToBase,
}

impl SwapDirection {
    pub fn label(self) -> &'static str {
        match self {
            Self::BaseToQuote => "base->quote",
            Self::QuoteToBase => "quote->base",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BuiltSwap {
    pub input_amount: u64,
    pub expected_output: u64,
    pub minimum_output: u64,
    pub slippage_bps: u64,
    pub price_impact_bps: Option<f64>,
    pub route_labels: Vec<String>,
    pub compute_budget_instructions: Vec<Instruction>,
    pub setup_instructions: Vec<Instruction>,
    pub swap_instruction: Instruction,
    pub cleanup_instruction: Option<Instruction>,
    pub other_instructions: Vec<Instruction>,
    pub tip_instruction: Option<Instruction>,
    pub address_lookup_tables: Vec<AddressLookupTableAccount>,
}

#[derive(Debug)]
pub struct JupiterSwapClient<'a> {
    http_client: &'a reqwest::Client,
    config: &'a JupiterConfig,
}

impl<'a> JupiterSwapClient<'a> {
    pub fn new(http_client: &'a reqwest::Client, config: &'a JupiterConfig) -> Self {
        Self {
            http_client,
            config,
        }
    }

    pub async fn build_exact_in(
        &self,
        liquidity_provider: Arc<Keypair>,
        input_mint: Pubkey,
        output_mint: Pubkey,
        destination_token_account: Pubkey,
        amount: u64,
    ) -> anyhow::Result<BuiltSwap> {
        info!(
            event.name = "jupiter_build_requested",
            jupiter.input_mint = %input_mint,
            jupiter.output_mint = %output_mint,
            jupiter.destination_token_account = %destination_token_account,
            rebalance.swap_input_requested.raw = amount,
            jupiter.api_key_configured = self
                .config
                .api_key
                .as_deref()
                .map(|key| !key.trim().is_empty())
                .unwrap_or(false),
            jupiter.compute_unit_price_percentile = %self.config.compute_unit_price_percentile,
            jupiter.max_accounts = self.config.max_accounts,
        );

        let api_key = self
            .config
            .api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .context("JUPITER_API_KEY is required for oracle-flow rebalancing")?;

        let build = self
            .fetch_build(
                api_key,
                &BuildQuery {
                    input_mint: input_mint.to_string(),
                    output_mint: output_mint.to_string(),
                    amount: amount.to_string(),
                    taker: liquidity_provider.pubkey().to_string(),
                    payer: Some(liquidity_provider.pubkey().to_string()),
                    destination_token_account: Some(destination_token_account.to_string()),
                    slippage_bps: self.config.max_slippage_bps,
                    max_accounts: self.config.max_accounts,
                    wrap_and_unwrap_sol: false,
                    compute_unit_price_percentile: self
                        .config
                        .compute_unit_price_percentile
                        .clone(),
                    mode: self.config.swap_mode.clone(),
                },
            )
            .await?;

        build.validate(input_mint, output_mint, amount, self.config)?;
        let built_swap = build.into_built_swap()?;

        info!(
            event.name = "jupiter_build_received",
            jupiter.input_mint = %input_mint,
            jupiter.output_mint = %output_mint,
            rebalance.swap_input_requested.raw = built_swap.input_amount,
            jupiter.expected_output.raw = built_swap.expected_output,
            jupiter.minimum_output.raw = built_swap.minimum_output,
            jupiter.slippage_bps = built_swap.slippage_bps,
            jupiter.price_impact_bps = ?built_swap.price_impact_bps,
            jupiter.route_labels = ?built_swap.route_labels,
            jupiter.lookup_table_count = built_swap.address_lookup_tables.len(),
            monotonic_counter.jupiter_orders_total = 1_u64,
        );

        Ok(built_swap)
    }

    async fn fetch_build(
        &self,
        api_key: &str,
        query: &BuildQuery,
    ) -> anyhow::Result<BuildResponse> {
        let url = format!(
            "{}/build",
            self.config.swap_api_base_url.trim_end_matches('/')
        );
        let started_at = Instant::now();
        let response = self
            .http_client
            .get(&url)
            .header("x-api-key", api_key)
            .query(query)
            .send()
            .await
            .context("Failed to request Jupiter swap build")?;
        let status = response.status();
        info!(
            event.name = "jupiter_build_http_response",
            http.status_code = status.as_u16(),
            histogram.jupiter_order_latency_ms = started_at.elapsed().as_millis() as f64,
        );

        parse_json_response(response, "Jupiter swap build").await
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BuildQuery {
    input_mint: String,
    output_mint: String,
    amount: String,
    taker: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    payer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination_token_account: Option<String>,
    slippage_bps: u64,
    max_accounts: u64,
    wrap_and_unwrap_sol: bool,
    compute_unit_price_percentile: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BuildResponse {
    input_mint: String,
    output_mint: String,
    in_amount: String,
    out_amount: String,
    other_amount_threshold: String,
    slippage_bps: u64,
    #[serde(default)]
    price_impact_pct: Option<NumericString>,
    #[serde(default)]
    route_plan: Vec<RoutePlanEntry>,
    #[serde(default)]
    compute_budget_instructions: Vec<ApiInstruction>,
    #[serde(default)]
    setup_instructions: Vec<ApiInstruction>,
    swap_instruction: ApiInstruction,
    cleanup_instruction: Option<ApiInstruction>,
    #[serde(default)]
    other_instructions: Vec<ApiInstruction>,
    tip_instruction: Option<ApiInstruction>,
    #[serde(default)]
    addresses_by_lookup_table_address: Option<HashMap<String, Vec<String>>>,
}

impl BuildResponse {
    fn validate(
        &self,
        input_mint: Pubkey,
        output_mint: Pubkey,
        amount: u64,
        config: &JupiterConfig,
    ) -> anyhow::Result<()> {
        ensure!(
            self.input_mint == input_mint.to_string(),
            "Jupiter build input mint mismatch: expected={} got={}",
            input_mint,
            self.input_mint
        );
        ensure!(
            self.output_mint == output_mint.to_string(),
            "Jupiter build output mint mismatch: expected={} got={}",
            output_mint,
            self.output_mint
        );
        ensure!(
            self.in_amount
                .parse::<u64>()
                .context("Invalid Jupiter inAmount")?
                == amount,
            "Jupiter build amount mismatch: expected={} got={}",
            amount,
            self.in_amount
        );
        ensure!(
            self.slippage_bps <= config.max_slippage_bps,
            "Jupiter slippage too high: {} > {}",
            self.slippage_bps,
            config.max_slippage_bps
        );

        if let Some(price_impact_bps) = self.price_impact_bps() {
            ensure!(
                price_impact_bps <= config.max_price_impact_bps as f64,
                "Jupiter price impact too high: {} bps > {} bps",
                price_impact_bps,
                config.max_price_impact_bps
            );
        }

        Ok(())
    }

    fn into_built_swap(self) -> anyhow::Result<BuiltSwap> {
        let input_amount = self
            .in_amount
            .parse::<u64>()
            .context("Invalid Jupiter inAmount")?;
        let expected_output = self
            .out_amount
            .parse::<u64>()
            .context("Invalid Jupiter outAmount")?;
        let minimum_output = self
            .other_amount_threshold
            .parse::<u64>()
            .context("Invalid Jupiter otherAmountThreshold")?;

        ensure!(
            minimum_output > 0,
            "Jupiter minimum output rounded to zero for atomic add-liquidity"
        );

        Ok(BuiltSwap {
            input_amount,
            expected_output,
            minimum_output,
            slippage_bps: self.slippage_bps,
            price_impact_bps: self.price_impact_bps(),
            route_labels: self.route_labels(),
            compute_budget_instructions: decode_instructions(self.compute_budget_instructions)?,
            setup_instructions: decode_instructions(self.setup_instructions)?,
            swap_instruction: self.swap_instruction.try_into_instruction()?,
            cleanup_instruction: self
                .cleanup_instruction
                .map(ApiInstruction::try_into_instruction)
                .transpose()?,
            other_instructions: decode_instructions(self.other_instructions)?,
            tip_instruction: self
                .tip_instruction
                .map(ApiInstruction::try_into_instruction)
                .transpose()?,
            address_lookup_tables: decode_lookup_tables(self.addresses_by_lookup_table_address)?,
        })
    }

    fn price_impact_bps(&self) -> Option<f64> {
        self.price_impact_pct
            .as_ref()
            .map(|impact| impact.value() * 10_000.0)
    }

    fn route_labels(&self) -> Vec<String> {
        self.route_plan
            .iter()
            .filter_map(|entry| entry.swap_info.label.clone())
            .collect()
    }
}

fn decode_instructions(instructions: Vec<ApiInstruction>) -> anyhow::Result<Vec<Instruction>> {
    instructions
        .into_iter()
        .map(ApiInstruction::try_into_instruction)
        .collect()
}

fn decode_lookup_tables(
    addresses_by_lookup_table_address: Option<HashMap<String, Vec<String>>>,
) -> anyhow::Result<Vec<AddressLookupTableAccount>> {
    addresses_by_lookup_table_address
        .unwrap_or_default()
        .into_iter()
        .map(|(key, addresses)| {
            Ok(AddressLookupTableAccount {
                key: key
                    .parse::<Pubkey>()
                    .with_context(|| format!("Invalid Jupiter lookup table address {key}"))?,
                addresses: addresses
                    .into_iter()
                    .map(|address| {
                        address.parse::<Pubkey>().with_context(|| {
                            format!("Invalid Jupiter lookup table entry {address}")
                        })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RoutePlanEntry {
    swap_info: SwapInfo,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SwapInfo {
    label: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum NumericString {
    String(String),
    Number(f64),
}

impl NumericString {
    fn value(&self) -> f64 {
        match self {
            Self::String(value) => value.parse::<f64>().unwrap_or(0.0),
            Self::Number(value) => *value,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiInstruction {
    program_id: String,
    accounts: Vec<ApiAccountMeta>,
    data: String,
}

impl ApiInstruction {
    fn try_into_instruction(self) -> anyhow::Result<Instruction> {
        Ok(Instruction {
            program_id: self.program_id.parse::<Pubkey>().with_context(|| {
                format!("Invalid Jupiter instruction program {}", self.program_id)
            })?,
            accounts: self
                .accounts
                .into_iter()
                .map(ApiAccountMeta::try_into_account_meta)
                .collect::<anyhow::Result<Vec<_>>>()?,
            data: BASE64_STANDARD
                .decode(self.data.as_bytes())
                .context("Invalid Jupiter instruction data")?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiAccountMeta {
    pubkey: String,
    is_signer: bool,
    is_writable: bool,
}

impl ApiAccountMeta {
    fn try_into_account_meta(self) -> anyhow::Result<AccountMeta> {
        let pubkey = self
            .pubkey
            .parse::<Pubkey>()
            .with_context(|| format!("Invalid Jupiter account meta pubkey {}", self.pubkey))?;
        Ok(if self.is_writable {
            AccountMeta::new(pubkey, self.is_signer)
        } else {
            AccountMeta::new_readonly(pubkey, self.is_signer)
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiErrorResponse {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    error_message: Option<String>,
}

impl ApiErrorResponse {
    fn message(&self) -> Option<String> {
        self.message
            .clone()
            .or_else(|| self.error_message.clone())
            .or_else(|| self.error.clone())
    }
}

async fn parse_json_response<T>(response: reqwest::Response, label: &str) -> anyhow::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("Failed to read {} response body", label))?;

    if !status.is_success() {
        let api_error = serde_json::from_str::<ApiErrorResponse>(&body).unwrap_or_default();
        anyhow::bail!(
            "{} request failed with status {}: {}",
            label,
            status,
            api_error.message().unwrap_or_else(|| body.clone())
        );
    }

    serde_json::from_str(&body).with_context(|| format!("Failed to decode {} response", label))
}

pub fn has_compute_unit_price(instructions: &[Instruction]) -> bool {
    instructions.iter().any(|instruction| {
        instruction.program_id == anchor_client::solana_sdk::compute_budget::id()
            && instruction.data.first() == Some(&3)
    })
}

pub fn without_compute_unit_limit(instructions: &[Instruction]) -> Vec<Instruction> {
    instructions
        .iter()
        .filter(|instruction| {
            instruction.program_id != anchor_client::solana_sdk::compute_budget::id()
                || instruction.data.first() != Some(&2)
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_api_instruction() {
        let api_instruction = ApiInstruction {
            program_id: "11111111111111111111111111111111".to_string(),
            accounts: vec![ApiAccountMeta {
                pubkey: "11111111111111111111111111111111".to_string(),
                is_signer: true,
                is_writable: false,
            }],
            data: BASE64_STANDARD.encode([1_u8, 2, 3]),
        };

        let instruction = api_instruction.try_into_instruction().unwrap();

        assert_eq!(instruction.program_id, Pubkey::default());
        assert_eq!(instruction.accounts.len(), 1);
        assert!(instruction.accounts[0].is_signer);
        assert!(!instruction.accounts[0].is_writable);
        assert_eq!(instruction.data, vec![1, 2, 3]);
    }

    #[test]
    fn detects_compute_unit_price_instruction() {
        let price_ix =
            anchor_client::solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(
                25_000,
            );

        assert!(has_compute_unit_price(&[price_ix]));
    }

    #[test]
    fn removes_compute_unit_limit_instruction() {
        let limit_ix =
            anchor_client::solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(
                300_000,
            );
        let price_ix =
            anchor_client::solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(
                25_000,
            );

        let filtered = without_compute_unit_limit(&[limit_ix, price_ix.clone()]);

        assert_eq!(filtered, vec![price_ix]);
    }
}
