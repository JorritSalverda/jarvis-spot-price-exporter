mod bigquery_client;
mod spot_price_client;
mod types;

use crate::types::SpotPrice;
use bigquery_client::{BigqueryClient, BigqueryClientConfig};
use spot_price_client::SpotPriceClient;
use std::env;
use uuid::Uuid;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let source = env::var("SOURCE")?;

    let bigquery_client_config = BigqueryClientConfig::from_env().await?;
    let bigquery_client = BigqueryClient::new(bigquery_client_config);
    bigquery_client.init_table().await?;

    let spot_price_client = SpotPriceClient::from_env()?;
    let spot_price_response = spot_price_client.get_spot_prices()?;

    for spot_price in spot_price_response.data.market_prices_electricity {
        let spot_price = SpotPrice {
            id: Some(Uuid::new_v4().to_string()),
            source: Some(source.clone()),
            ..spot_price
        };

        bigquery_client
            .insert_spot_price(&spot_price)
            .await
            .expect("Failed to insert spot_price into bigquery table");
    }

    Ok(())
}
