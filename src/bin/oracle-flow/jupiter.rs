use std::{sync::Arc, time::Duration};

use anchor_client::solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::VersionedTransaction,
};
use anyhow::{Context, ensure};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwapExecution {
    pub input_consumed: u64,
    pub output_received: u64,
    pub signature: Option<String>,
}

#[derive(Debug)]
pub struct JupiterUltraClient<'a> {
    http_client: &'a reqwest::Client,
    config: &'a JupiterConfig,
}

impl<'a> JupiterUltraClient<'a> {
    pub fn new(http_client: &'a reqwest::Client, config: &'a JupiterConfig) -> Self {
        Self {
            http_client,
            config,
        }
    }

    pub async fn swap_exact_in(
        &self,
        liquidity_provider: Arc<Keypair>,
        input_mint: Pubkey,
        output_mint: Pubkey,
        amount: u64,
    ) -> anyhow::Result<SwapExecution> {
        println!(
            "[jupiter] swap_exact_in: {} -> {} amount={} dry_run={} api_key_set={}",
            input_mint,
            output_mint,
            amount,
            self.config.dry_run,
            self.config
                .api_key
                .as_deref()
                .map(|k| !k.trim().is_empty())
                .unwrap_or(false),
        );

        let api_key = self
            .config
            .api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .context("JUPITER_API_KEY is required for oracle-flow rebalancing")?;

        // Jupiter's RPC may lag behind our confirmed withdrawal by a few seconds.
        // Retry on error-code 1 (Insufficient funds) up to 5 times with increasing delay.
        const MAX_ORDER_RETRIES: u32 = 5;
        let order = 'order: {
            let mut last_err = anyhow::anyhow!("no attempts made");
            for attempt in 0..MAX_ORDER_RETRIES {
                if attempt > 0 {
                    let delay = Duration::from_secs(2u64.pow(attempt));
                    eprintln!(
                        "[jupiter] order error on attempt {} — waiting {}s before retry",
                        attempt,
                        delay.as_secs(),
                    );
                    tokio::time::sleep(delay).await;
                }

                let order = self
                    .fetch_order(
                        api_key,
                        &OrderQuery {
                            input_mint: input_mint.to_string(),
                            output_mint: output_mint.to_string(),
                            amount: amount.to_string(),
                            taker: if self.config.dry_run {
                                None
                            } else {
                                Some(liquidity_provider.pubkey().to_string())
                            },
                        },
                    )
                    .await;

                match order {
                    Ok(o) if o.error_code == Some(1) => {
                        last_err = anyhow::anyhow!(
                            "Jupiter order returned error 1: {} (attempt {})",
                            o.error_message.as_deref().unwrap_or("Insufficient funds"),
                            attempt + 1,
                        );
                    }
                    Ok(o) => break 'order o,
                    Err(e) => return Err(e),
                }
            }
            return Err(last_err);
        };

        order.validate(input_mint, output_mint, amount, self.config)?;

        if self.config.dry_run {
            let preview = order
                .dry_run_execution(amount)
                .context("Failed to derive Jupiter dry-run swap result from order response")?;
            println!(
                "Jupiter dry run preview: input_consumed={} output_received={} signature=None (execute skipped)",
                preview.input_consumed, preview.output_received
            );
            return Ok(preview);
        }

        let request_id = order
            .request_id
            .clone()
            .context("Jupiter order response missing requestId")?;
        let transaction = order
            .transaction
            .clone()
            .filter(|value| !value.trim().is_empty())
            .context("Jupiter order response missing transaction payload")?;

        let signed_transaction = sign_transaction(&transaction, liquidity_provider)?;
        let execute_response = self
            .execute_order(api_key, &request_id, &signed_transaction)
            .await?;
        execute_response.validate()?;

        let input_consumed = execute_response
            .input_amount_result
            .as_deref()
            .or(execute_response.total_input_amount.as_deref())
            .context("Jupiter execute response missing input amount result")?
            .parse::<u64>()
            .context("Failed to parse Jupiter input amount result")?;
        let output_received = execute_response
            .output_amount_result
            .as_deref()
            .or(execute_response.out_amount.as_deref())
            .context("Jupiter execute response missing output amount result")?
            .parse::<u64>()
            .context("Failed to parse Jupiter output amount result")?;

        ensure!(
            input_consumed <= amount,
            "Jupiter consumed more input than requested: requested={} consumed={}",
            amount,
            input_consumed
        );

        Ok(SwapExecution {
            input_consumed,
            output_received,
            signature: execute_response.signature,
        })
    }

    async fn fetch_order(
        &self,
        api_key: &str,
        query: &OrderQuery,
    ) -> anyhow::Result<OrderResponse> {
        let url = format!(
            "{}/order",
            self.config.ultra_api_base_url.trim_end_matches('/')
        );
        let response = self
            .http_client
            .get(&url)
            .header("x-api-key", api_key)
            .query(query)
            .send()
            .await
            .context("Failed to request Jupiter Ultra order")?;

        parse_json_response(response, "Jupiter Ultra order").await
    }

    async fn execute_order(
        &self,
        api_key: &str,
        request_id: &str,
        signed_transaction: &str,
    ) -> anyhow::Result<ExecuteResponse> {
        let url = format!(
            "{}/execute",
            self.config.ultra_api_base_url.trim_end_matches('/')
        );
        let response = self
            .http_client
            .post(&url)
            .header("x-api-key", api_key)
            .json(&ExecuteRequest {
                signed_transaction,
                request_id,
            })
            .send()
            .await
            .context("Failed to execute Jupiter Ultra order")?;

        parse_json_response(response, "Jupiter Ultra execute").await
    }
}

fn sign_transaction(
    transaction_base64: &str,
    liquidity_provider: Arc<Keypair>,
) -> anyhow::Result<String> {
    let transaction_bytes = BASE64_STANDARD
        .decode(transaction_base64)
        .context("Failed to decode Jupiter transaction payload")?;
    let mut transaction: VersionedTransaction = bincode::deserialize(&transaction_bytes)
        .context("Failed to deserialize Jupiter transaction")?;

    // Jupiter Ultra returns a partially-signed transaction: Jupiter holds co-signer slot(s).
    // try_new() would replace all signatures, destroying theirs. Instead, find our pubkey's
    // position in the static account keys and insert our signature at that slot only.
    let our_pubkey = liquidity_provider.pubkey();
    let signer_index = transaction
        .message
        .static_account_keys()
        .iter()
        .position(|key| key == &our_pubkey)
        .with_context(|| {
            format!(
                "Our pubkey {} not found in Jupiter transaction account keys",
                our_pubkey
            )
        })?;

    let message_bytes = transaction.message.serialize();
    transaction.signatures[signer_index] = liquidity_provider.sign_message(&message_bytes);

    let signed_bytes = bincode::serialize(&transaction)
        .context("Failed to serialize signed Jupiter transaction")?;
    Ok(BASE64_STANDARD.encode(signed_bytes))
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

    serde_json::from_str(&body).with_context(|| format!("Failed to parse {} response JSON", label))
}

#[derive(Debug, Serialize)]
struct OrderQuery {
    #[serde(rename = "inputMint")]
    input_mint: String,
    #[serde(rename = "outputMint")]
    output_mint: String,
    amount: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    taker: Option<String>,
}

#[derive(Debug, Serialize)]
struct ExecuteRequest<'a> {
    #[serde(rename = "signedTransaction")]
    signed_transaction: &'a str,
    #[serde(rename = "requestId")]
    request_id: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderResponse {
    request_id: Option<String>,
    transaction: Option<String>,
    input_mint: String,
    output_mint: String,
    in_amount: Option<String>,
    out_amount: Option<String>,
    slippage_bps: Option<u64>,
    price_impact: Option<f64>,
    router: Option<String>,
    error_code: Option<u64>,
    error_message: Option<String>,
}

impl OrderResponse {
    fn validate(
        &self,
        expected_input_mint: Pubkey,
        expected_output_mint: Pubkey,
        expected_amount: u64,
        config: &JupiterConfig,
    ) -> anyhow::Result<()> {
        ensure!(
            self.input_mint == expected_input_mint.to_string(),
            "Jupiter order input mint mismatch: expected={} actual={}",
            expected_input_mint,
            self.input_mint
        );
        ensure!(
            self.output_mint == expected_output_mint.to_string(),
            "Jupiter order output mint mismatch: expected={} actual={}",
            expected_output_mint,
            self.output_mint
        );

        if let Some(error_code) = self.error_code {
            anyhow::bail!(
                "Jupiter order returned error {}: {}",
                error_code,
                self.error_message
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        }

        if let Some(in_amount) = &self.in_amount {
            let parsed_in_amount = in_amount
                .parse::<u64>()
                .context("Failed to parse Jupiter order input amount")?;
            ensure!(
                parsed_in_amount == expected_amount,
                "Jupiter order input amount mismatch: expected={} actual={}",
                expected_amount,
                parsed_in_amount
            );
        }

        let slippage_bps = self.slippage_bps.unwrap_or_default();
        ensure!(
            slippage_bps <= config.max_slippage_bps,
            "Jupiter order slippage {} bps exceeds configured max {} bps",
            slippage_bps,
            config.max_slippage_bps
        );

        let price_impact_bps = self
            .price_impact
            .map(|value| value.abs() * 100.0)
            .unwrap_or_default();
        ensure!(
            price_impact_bps <= config.max_price_impact_bps as f64,
            "Jupiter order price impact {:.2} bps exceeds configured max {} bps",
            price_impact_bps,
            config.max_price_impact_bps
        );

        println!(
            "Jupiter order accepted: router={:?} slippage_bps={} price_impact_bps={:.2} expected_out={:?}",
            self.router, slippage_bps, price_impact_bps, self.out_amount
        );

        Ok(())
    }

    fn dry_run_execution(&self, requested_amount: u64) -> anyhow::Result<SwapExecution> {
        let input_consumed = self
            .in_amount
            .as_deref()
            .map(|value| {
                value
                    .parse::<u64>()
                    .context("Failed to parse Jupiter dry-run input amount")
            })
            .transpose()?
            .unwrap_or(requested_amount);
        let output_received = self
            .out_amount
            .as_deref()
            .context("Jupiter dry-run response missing outAmount")?
            .parse::<u64>()
            .context("Failed to parse Jupiter dry-run output amount")?;

        ensure!(
            input_consumed <= requested_amount,
            "Jupiter dry-run input amount exceeds requested amount: requested={} actual={}",
            requested_amount,
            input_consumed
        );

        Ok(SwapExecution {
            input_consumed,
            output_received,
            signature: None,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecuteResponse {
    status: Option<String>,
    code: Option<u64>,
    signature: Option<String>,
    error: Option<String>,
    input_amount_result: Option<String>,
    output_amount_result: Option<String>,
    total_input_amount: Option<String>,
    out_amount: Option<String>,
}

impl ExecuteResponse {
    fn validate(&self) -> anyhow::Result<()> {
        if let Some(code) = self.code {
            ensure!(
                code == 0,
                "Jupiter execute returned error code {}: {}",
                code,
                self.error
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        }

        if let Some(status) = &self.status {
            ensure!(
                status.eq_ignore_ascii_case("success"),
                "Jupiter execute returned non-success status {}: {}",
                status,
                self.error
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        }

        if let Some(error) = &self.error {
            ensure!(
                error.trim().is_empty(),
                "Jupiter execute returned error: {}",
                error
            );
        }

        Ok(())
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiErrorResponse {
    error: Option<String>,
    message: Option<String>,
    error_message: Option<String>,
    details: Option<String>,
}

impl ApiErrorResponse {
    fn message(self) -> Option<String> {
        self.error
            .or(self.message)
            .or(self.error_message)
            .or(self.details)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_price_impact_percent_to_bps() {
        let config = JupiterConfig {
            api_key: Some("test".to_string()),
            ultra_api_base_url: "https://api.jup.ag/ultra/v1".to_string(),
            max_slippage_bps: 50,
            max_price_impact_bps: 50,
            dry_run: true,
        };
        let order = OrderResponse {
            request_id: Some("req".to_string()),
            transaction: Some("tx".to_string()),
            input_mint: Pubkey::new_unique().to_string(),
            output_mint: Pubkey::new_unique().to_string(),
            in_amount: Some("100".to_string()),
            out_amount: Some("99".to_string()),
            slippage_bps: Some(20),
            price_impact: Some(0.49),
            router: Some("aggregator".to_string()),
            error_code: None,
            error_message: None,
        };

        assert!(
            order
                .validate(
                    order.input_mint.parse().unwrap(),
                    order.output_mint.parse().unwrap(),
                    100,
                    &config,
                )
                .is_ok()
        );
    }

    #[test]
    fn derives_dry_run_execution_from_order_quote() {
        let order = OrderResponse {
            request_id: Some("req".to_string()),
            transaction: None,
            input_mint: Pubkey::new_unique().to_string(),
            output_mint: Pubkey::new_unique().to_string(),
            in_amount: Some("4000000".to_string()),
            out_amount: Some("43210".to_string()),
            slippage_bps: Some(20),
            price_impact: Some(0.1),
            router: Some("aggregator".to_string()),
            error_code: None,
            error_message: None,
        };

        let execution = order.dry_run_execution(4_000_000).unwrap();
        assert_eq!(execution.input_consumed, 4_000_000);
        assert_eq!(execution.output_received, 43_210);
        assert_eq!(execution.signature, None);
    }
}
