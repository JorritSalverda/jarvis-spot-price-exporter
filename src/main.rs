mod bigquery_client;
mod spot_price_client;
mod types;

use crate::types::SpotPrice;
use bigquery_client::BigqueryClient;
use chrono::Utc;
use spot_price_client::SpotPriceClient;
use std::env;
use std::error::Error;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use uuid::Uuid;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let source = env::var("SOURCE")?;

    let bigquery_client = BigqueryClient::from_env().await?;
    bigquery_client.init_table().await?;

    let spot_price_client = SpotPriceClient::from_env()?;
    let spot_price_response = Retry::spawn(
        ExponentialBackoff::from_millis(100).map(jitter).take(3),
        || spot_price_client.get_spot_prices(Utc::now()),
    )
    .await?;

    for spot_price in spot_price_response.data.market_prices_electricity {
        let spot_price = SpotPrice {
            id: Some(Uuid::new_v4().to_string()),
            source: Some(source.clone()),
            ..spot_price
        };

        Retry::spawn(
            ExponentialBackoff::from_millis(100).map(jitter).take(3),
            || bigquery_client.insert_spot_price(&spot_price),
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use log::info;

    #[tokio::test]
    #[ignore]
    async fn get_historic_prices() -> Result<(), Box<dyn Error>> {
        let source = env::var("SOURCE")?;

        let bigquery_client = BigqueryClient::from_env().await?;
        let spot_price_client = SpotPriceClient::from_env()?;

        bigquery_client.init_table().await?;

        let mut start_date: DateTime<Utc> = Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 0);

        while start_date < Utc::now() {
            info!("Retrieving spot prices for {}...", start_date);
            let spot_price_response = Retry::spawn(
                ExponentialBackoff::from_millis(100).map(jitter).take(3),
                || spot_price_client.get_spot_prices(start_date),
            )
            .await?;

            info!("Storing spot prices for {}...", start_date);
            for spot_price in spot_price_response.data.market_prices_electricity {
                let spot_price = SpotPrice {
                    id: Some(Uuid::new_v4().to_string()),
                    source: Some(source.clone()),
                    ..spot_price
                };

                info!("Spot price from {}", spot_price.from);
                Retry::spawn(
                    ExponentialBackoff::from_millis(100).map(jitter).take(3),
                    || bigquery_client.insert_spot_price(&spot_price),
                )
                .await?;
            }

            start_date = start_date + Duration::days(1)
        }

        Ok(())
    }
}
