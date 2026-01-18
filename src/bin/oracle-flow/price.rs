use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct PriceData {
    pub price: f64,
    pub timestamp: u64,
}

#[derive(Deserialize)]
struct PriceResponse {
    price: f64,
    #[serde(default)]
    timestamp: Option<u64>,
}

pub async fn fetch_price(client: &reqwest::Client, url: &str) -> anyhow::Result<PriceData> {
    let response: PriceResponse = client.get(url).send().await?.json().await?;

    let timestamp = response.timestamp.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    });

    Ok(PriceData {
        price: response.price,
        timestamp,
    })
}
