use anyhow::{Context, anyhow};
use chrono::DateTime;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct PriceData {
    pub price: f64,
    pub timestamp: u64,
}

#[derive(Deserialize)]
struct PriceResponse {
    price: Value,
    #[serde(default)]
    timestamp: Option<Value>,
}

pub async fn fetch_price(client: &reqwest::Client, url: &str) -> anyhow::Result<PriceData> {
    println!("Fetching price feed from {}", url);
    let response: PriceResponse = client.get(url).send().await?.json().await?;

    let price = parse_price(&response.price)?;
    let timestamp = parse_timestamp(response.timestamp.as_ref()).unwrap_or_else(|err| {
        eprintln!(
            "Failed to parse price-feed timestamp ({err}). Falling back to current UNIX time."
        );
        unix_now()
    });

    Ok(PriceData { price, timestamp })
}

fn parse_price(raw: &Value) -> anyhow::Result<f64> {
    match raw {
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| anyhow!("price number cannot be represented as f64")),
        Value::String(s) => s
            .parse::<f64>()
            .with_context(|| format!("invalid price string: {s}")),
        _ => Err(anyhow!(
            "invalid price type: expected string or number, got {}",
            json_type(raw)
        )),
    }
}

fn parse_timestamp(raw: Option<&Value>) -> anyhow::Result<u64> {
    let Some(value) = raw else {
        return Ok(unix_now());
    };

    match value {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| anyhow!("timestamp number must be a non-negative integer")),
        Value::String(s) => {
            if let Ok(unix_secs) = s.parse::<u64>() {
                return Ok(unix_secs);
            }

            let parsed = DateTime::parse_from_rfc3339(s)
                .with_context(|| format!("timestamp is not RFC3339 or unix-seconds: {s}"))?;
            let unix_secs = parsed.timestamp();
            if unix_secs < 0 {
                return Err(anyhow!("timestamp must be non-negative, got {unix_secs}"));
            }
            Ok(unix_secs as u64)
        }
        _ => Err(anyhow!(
            "invalid timestamp type: expected string or number, got {}",
            json_type(value)
        )),
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_jupiter_style_payload() {
        let payload = json!({
            "fallback_used": false,
            "pair": "SOL/USDC",
            "price": "84.0181294070247",
            "source": "jupiter",
            "timestamp": "2026-02-16T14:58:01.990650Z"
        });

        let response: PriceResponse =
            serde_json::from_value(payload).expect("payload should deserialize");
        let price = parse_price(&response.price).expect("price should parse");
        let timestamp =
            parse_timestamp(response.timestamp.as_ref()).expect("timestamp should parse");

        assert!((price - 84.0181294070247).abs() < 1e-9);
        assert_eq!(timestamp, 1_771_253_881);
    }

    #[test]
    fn parses_numeric_payload() {
        let payload = json!({
            "price": 42.5,
            "timestamp": 1771255481
        });

        let response: PriceResponse =
            serde_json::from_value(payload).expect("payload should deserialize");
        let price = parse_price(&response.price).expect("price should parse");
        let timestamp =
            parse_timestamp(response.timestamp.as_ref()).expect("timestamp should parse");

        assert_eq!(price, 42.5);
        assert_eq!(timestamp, 1_771_255_481);
    }
}
