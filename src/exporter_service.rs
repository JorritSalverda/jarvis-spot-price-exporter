use crate::bigquery_client::BigqueryClient;
use crate::spot_price_client::SpotPriceClient;
use crate::state_client::StateClient;
use crate::types::*;
use chrono::{DateTime, Utc};
use log::info;
use std::env;
use std::error::Error;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use uuid::Uuid;

pub struct ExporterServiceConfig {
    bigquery_client: BigqueryClient,
    spot_price_client: SpotPriceClient,
    state_client: StateClient,
    source: String,
}

impl ExporterServiceConfig {
    pub fn new(
        bigquery_client: BigqueryClient,
        spot_price_client: SpotPriceClient,
        state_client: StateClient,
        source: &str,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            bigquery_client,
            spot_price_client,
            state_client,
            source: source.to_string(),
        })
    }

    pub fn from_env(
        bigquery_client: BigqueryClient,
        spot_price_client: SpotPriceClient,
        state_client: StateClient,
    ) -> Result<Self, Box<dyn Error>> {
        let source = env::var("SOURCE")?;

        Self::new(bigquery_client, spot_price_client, state_client, &source)
    }
}

pub struct ExporterService {
    config: ExporterServiceConfig,
}

impl ExporterService {
    pub fn new(config: ExporterServiceConfig) -> Self {
        Self { config }
    }

    pub fn from_env(
        bigquery_client: BigqueryClient,
        spot_price_client: SpotPriceClient,
        state_client: StateClient,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self::new(ExporterServiceConfig::from_env(
            bigquery_client,
            spot_price_client,
            state_client,
        )?))
    }

    pub async fn run(&self, start_date: DateTime<Utc>) -> Result<(), Box<dyn Error>> {
        self.config.bigquery_client.init_table().await?;

        info!("Reading previous state...");
        let state = self.config.state_client.read_state()?;

        info!("Retrieving spot prices for {}...", start_date);
        let spot_price_response = Retry::spawn(
            ExponentialBackoff::from_millis(100).map(jitter).take(3),
            || self.config.spot_price_client.get_spot_prices(start_date),
        )
        .await?;

        info!("Storing retrieved spot prices for {}...", start_date);
        let mut future_spot_prices: Vec<SpotPrice> = vec![];
        let mut last_from: Option<DateTime<Utc>> = None;
        for spot_price in &spot_price_response.data.market_prices_electricity {
            let spot_price = SpotPrice {
                id: Some(Uuid::new_v4().to_string()),
                source: Some(self.config.source.clone()),
                ..spot_price.clone()
            };

            if spot_price.till > Utc::now() {
                future_spot_prices.push(spot_price.clone());
            }

            info!("{:?}", spot_price);
            let mut write_spot_price = spot_price.till < Utc::now();
            if let Some(st) = &state {
                write_spot_price = write_spot_price && spot_price.from > st.last_from;
            }

            if write_spot_price {
                Retry::spawn(
                    ExponentialBackoff::from_millis(100).map(jitter).take(3),
                    || self.config.bigquery_client.insert_spot_price(&spot_price),
                )
                .await?;
                last_from = Some(spot_price.from);
            } else {
                info!("Skipping writing to BigQuery, already present")
            }
        }

        if last_from.is_some() {
            info!("Writing new state...");
            let new_state = State {
                future_spot_prices,
                last_from: last_from.unwrap(),
            };

            self.config.state_client.store_state(&new_state).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration, TimeZone, Utc};

    #[tokio::test]
    #[ignore]
    async fn get_historic_prices() -> Result<(), Box<dyn Error>> {
        let bigquery_client = BigqueryClient::from_env().await?;
        let spot_price_client = SpotPriceClient::from_env()?;
        let state_client = StateClient::from_env().await?;

        let exporter_service =
            ExporterService::from_env(bigquery_client, spot_price_client, state_client)?;

        let mut start_date: DateTime<Utc> = Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 0);

        while start_date < Utc::now() {
            exporter_service.run(start_date).await?;
            start_date = start_date + Duration::days(1)
        }

        Ok(())
    }
}
