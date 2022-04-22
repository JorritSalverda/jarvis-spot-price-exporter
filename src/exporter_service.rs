use crate::bigquery_client::BigqueryClient;
use crate::spot_price_client::SpotPriceClient;
use crate::state_client::StateClient;
use crate::types::*;
use chrono::prelude::*;
use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use log::{info, warn};
use std::cmp::Ordering;
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
    predictions_available_from_hour: u32,
    local_time_zone: String,
}

impl ExporterServiceConfig {
    pub fn new(
        bigquery_client: BigqueryClient,
        spot_price_client: SpotPriceClient,
        state_client: StateClient,
        source: &str,
        predictions_available_from_hour: u32,
        local_time_zone: &str,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            bigquery_client,
            spot_price_client,
            state_client,
            source: source.to_string(),
            predictions_available_from_hour,
            local_time_zone: local_time_zone.to_string(),
        })
    }

    pub fn from_env(
        bigquery_client: BigqueryClient,
        spot_price_client: SpotPriceClient,
        state_client: StateClient,
    ) -> Result<Self, Box<dyn Error>> {
        let source = env::var("SOURCE")?;
        let predications_available_from_hour: u32 = env::var("PREDICTIONS_AVAILABLE_FROM_HOUR")
            .unwrap_or_else(|_| "13".to_string())
            .parse()
            .unwrap_or(13);

        let local_time_zone =
            env::var("LOCAL_TIME_ZONE").unwrap_or_else(|_| "Europe/Amsterdam".to_string());

        Self::new(
            bigquery_client,
            spot_price_client,
            state_client,
            &source,
            predications_available_from_hour,
            &local_time_zone,
        )
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
        let now: DateTime<Utc> = Utc::now();

        info!("Initalizing BigQuery table...");
        self.config.bigquery_client.init_table().await?;

        info!("Reading previous state...");
        let state = self.config.state_client.read_state()?;

        info!("Retrieving spot prices for {}...", start_date);
        let spot_price_response = Retry::spawn(
            ExponentialBackoff::from_millis(100).map(jitter).take(3),
            || self.config.spot_price_client.get_spot_prices(start_date),
        )
        .await?;

        let local_time_zone = self.config.local_time_zone.parse::<Tz>()?;
        let mut retrieved_spot_prices = correct_timezone_errors(
            local_time_zone,
            spot_price_response.data.market_prices_electricity,
        );
        info!("Retrieved {} spot prices", retrieved_spot_prices.len());
        if start_date.date() == now.date()
            && now.hour() > self.config.predictions_available_from_hour
        {
            let tomorrow = start_date + Duration::days(1);
            info!("Retrieving spot price predictions for {}...", tomorrow);
            match Retry::spawn(
                ExponentialBackoff::from_millis(100).map(jitter).take(3),
                || self.config.spot_price_client.get_spot_prices(tomorrow),
            )
            .await
            {
                Ok(prices) => {
                    info!(
                        "Retrieved {} spot price predictions",
                        prices.data.market_prices_electricity.len()
                    );
                    if !prices.data.market_prices_electricity.is_empty() {
                        retrieved_spot_prices.append(&mut correct_timezone_errors(local_time_zone, prices.data.market_prices_electricity));
                    } else {
                        warn!("No predictions for tomorrow yet, will try again next run");
                    }
                }
                Err(_) => {
                    warn!("No predictions for tomorrow yet, will try again next run");
                }
            };
        }

        info!("Storing retrieved spot prices for {}...", start_date);
        let mut future_spot_prices: Vec<SpotPrice> = vec![];
        let mut last_from: Option<DateTime<Utc>> = None;
        for spot_price in &retrieved_spot_prices {
            let spot_price = SpotPrice {
                id: Some(Uuid::new_v4().to_string()),
                source: Some(self.config.source.clone()),
                ..spot_price.clone()
            };

            info!("{:?}", spot_price);
            let is_prediction = spot_price.till > now;
            let mut write_spot_price = !is_prediction;
            if is_prediction {
                future_spot_prices.push(spot_price.clone());
            } else if let Some(st) = &state {
                write_spot_price = write_spot_price && spot_price.from > st.last_from;
            }

            if write_spot_price {
                Retry::spawn(
                    ExponentialBackoff::from_millis(100).map(jitter).take(3),
                    || self.config.bigquery_client.insert_spot_price(&spot_price),
                )
                .await?;
                last_from = Some(spot_price.from);
            } else if is_prediction {
                info!("Skipping writing to BigQuery, it's a prediction");
            } else {
                info!("Skipping writing to BigQuery, already present");
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

fn correct_timezone_errors(local_time_zone: Tz, spot_prices: Vec<SpotPrice>) -> Vec<SpotPrice> {
    let start_time_hour_shift = spot_prices
        .first()
        .unwrap()
        .from
        .with_timezone(&local_time_zone)
        .hour();
    let end_time_hour_shift = spot_prices
        .last()
        .unwrap()
        .till
        .with_timezone(&local_time_zone)
        .hour();

    if start_time_hour_shift > 0 && end_time_hour_shift == 0 {
        info!("Correcting errors in spot price times, transitioning to winter time");
        // transition to winter time
        spot_prices
            .into_iter()
            .enumerate()
            .map(|(index, spot_price)| match index.cmp(&2) {
                Ordering::Less => shift_to_correct_utc(spot_price, start_time_hour_shift),
                Ordering::Equal => {
                    let mut spot_price = spot_price;

                    spot_price.from =
                        spot_price.from - Duration::hours(start_time_hour_shift.into());
                    spot_price
                }
                Ordering::Greater => spot_price,
            })
            .collect()
    } else if start_time_hour_shift == 0 && end_time_hour_shift > 0 {
        info!("Correcting errors in spot price times, transitioning to summer time");
        // transition to summer time
        spot_prices
            .into_iter()
            .enumerate()
            .map(|(index, spot_price)| {
                if index < 2 {
                    spot_price
                } else {
                    shift_to_correct_utc(spot_price, end_time_hour_shift)
                }
            })
            .collect()
    } else if start_time_hour_shift > 0 && end_time_hour_shift > 0 {
        info!("Correcting errors in spot price times, during summer time");
        spot_prices
            .into_iter()
            .map(|spot_price| shift_to_correct_utc(spot_price, start_time_hour_shift))
            .collect()
    } else {
        info!("No timezone errors in spot price times, moving on");
        spot_prices
    }
}

fn shift_to_correct_utc(spot_price: SpotPrice, num_hours_to_shift: u32) -> SpotPrice {
    let mut spot_price = spot_price;

    spot_price.from = spot_price.from - Duration::hours(num_hours_to_shift.into());
    spot_price.till = spot_price.till - Duration::hours(num_hours_to_shift.into());

    spot_price
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration, TimeZone, Utc};

    #[test]
    fn correct_timezone_errors_returns_correct_times_for_switch_to_summer_time(
    ) -> Result<(), Box<dyn Error>> {
        let response_body = r#"{"data":{"marketPricesElectricity":[
          {"till":"2022-03-27T00:00:00.000Z","from":"2022-03-26T23:00:00.000Z","marketPrice":0.235,"marketPriceTax":0.04935,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},
          {"till":"2022-03-27T01:00:00.000Z","from":"2022-03-27T00:00:00.000Z","marketPrice":0.222,"marketPriceTax":0.0466053,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},
          {"till":"2022-03-27T03:00:00.000Z","from":"2022-03-27T02:00:00.000Z","marketPrice":0.214,"marketPriceTax":0.044944200000000004,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T04:00:00.000Z","from":"2022-03-27T03:00:00.000Z","marketPrice":0.212,"marketPriceTax":0.04452,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T05:00:00.000Z","from":"2022-03-27T04:00:00.000Z","marketPrice":0.211,"marketPriceTax":0.0443289,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T06:00:00.000Z","from":"2022-03-27T05:00:00.000Z","marketPrice":0.21,"marketPriceTax":0.0441651,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T07:00:00.000Z","from":"2022-03-27T06:00:00.000Z","marketPrice":0.214,"marketPriceTax":0.0449463,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T08:00:00.000Z","from":"2022-03-27T07:00:00.000Z","marketPrice":0.214,"marketPriceTax":0.044956800000000005,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T09:00:00.000Z","from":"2022-03-27T08:00:00.000Z","marketPrice":0.214,"marketPriceTax":0.04495469999999999,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T10:00:00.000Z","from":"2022-03-27T09:00:00.000Z","marketPrice":0.205,"marketPriceTax":0.0430542,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T11:00:00.000Z","from":"2022-03-27T10:00:00.000Z","marketPrice":0.192,"marketPriceTax":0.0403977,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T12:00:00.000Z","from":"2022-03-27T11:00:00.000Z","marketPrice":0.143,"marketPriceTax":0.0299397,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T13:00:00.000Z","from":"2022-03-27T12:00:00.000Z","marketPrice":0.09,"marketPriceTax":0.018879000000000003,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T14:00:00.000Z","from":"2022-03-27T13:00:00.000Z","marketPrice":0.078,"marketPriceTax":0.0163737,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T15:00:00.000Z","from":"2022-03-27T14:00:00.000Z","marketPrice":0.09,"marketPriceTax":0.0189126,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T16:00:00.000Z","from":"2022-03-27T15:00:00.000Z","marketPrice":0.142,"marketPriceTax":0.0297339,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T17:00:00.000Z","from":"2022-03-27T16:00:00.000Z","marketPrice":0.195,"marketPriceTax":0.0409143,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T18:00:00.000Z","from":"2022-03-27T17:00:00.000Z","marketPrice":0.22,"marketPriceTax":0.0462,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T19:00:00.000Z","from":"2022-03-27T18:00:00.000Z","marketPrice":0.25,"marketPriceTax":0.0525,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T20:00:00.000Z","from":"2022-03-27T19:00:00.000Z","marketPrice":0.276,"marketPriceTax":0.0579075,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T21:00:00.000Z","from":"2022-03-27T20:00:00.000Z","marketPrice":0.26,"marketPriceTax":0.054621,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T22:00:00.000Z","from":"2022-03-27T21:00:00.000Z","marketPrice":0.243,"marketPriceTax":0.0510951,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-03-27T23:00:00.000Z","from":"2022-03-27T22:00:00.000Z","marketPrice":0.219,"marketPriceTax":0.04599,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081}],"marketPricesGas":[{"from":"2022-03-26T23:00:00.000Z","till":"2022-03-27T00:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T00:00:00.000Z","till":"2022-03-27T01:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T01:00:00.000Z","till":"2022-03-27T02:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T02:00:00.000Z","till":"2022-03-27T03:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T03:00:00.000Z","till":"2022-03-27T04:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T04:00:00.000Z","till":"2022-03-27T05:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T05:00:00.000Z","till":"2022-03-27T06:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T06:00:00.000Z","till":"2022-03-27T07:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T07:00:00.000Z","till":"2022-03-27T08:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T08:00:00.000Z","till":"2022-03-27T09:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T09:00:00.000Z","till":"2022-03-27T10:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T10:00:00.000Z","till":"2022-03-27T11:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T11:00:00.000Z","till":"2022-03-27T12:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T12:00:00.000Z","till":"2022-03-27T13:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T13:00:00.000Z","till":"2022-03-27T14:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T14:00:00.000Z","till":"2022-03-27T15:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T15:00:00.000Z","till":"2022-03-27T16:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T16:00:00.000Z","till":"2022-03-27T17:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T17:00:00.000Z","till":"2022-03-27T18:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T18:00:00.000Z","till":"2022-03-27T19:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T19:00:00.000Z","till":"2022-03-27T20:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T20:00:00.000Z","till":"2022-03-27T21:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-03-27T21:00:00.000Z","till":"2022-03-27T22:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},
          {"from":"2022-03-27T22:00:00.000Z","till":"2022-03-27T23:00:00.000Z","marketPrice":0.914,"marketPriceTax":0.192,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544}]}}"#;
        let spot_price_response = serde_json::from_str::<SpotPriceResponse>(&response_body)?;
        let local_time_zone = "Europe/Amsterdam".parse::<Tz>()?;

        // act
        let corrected_spot_prices = correct_timezone_errors(
            local_time_zone,
            spot_price_response.data.market_prices_electricity,
        );

        assert_eq!(corrected_spot_prices.len(), 23);

        assert_eq!(
            corrected_spot_prices[0].from,
            Utc.ymd(2022, 3, 26).and_hms(23, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[0].till,
            Utc.ymd(2022, 3, 27).and_hms(0, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[1].from,
            Utc.ymd(2022, 3, 27).and_hms(0, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[1].till,
            Utc.ymd(2022, 3, 27).and_hms(1, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[2].from,
            Utc.ymd(2022, 3, 27).and_hms(1, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[2].till,
            Utc.ymd(2022, 3, 27).and_hms(2, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[22].from,
            Utc.ymd(2022, 3, 27).and_hms(21, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[22].till,
            Utc.ymd(2022, 3, 27).and_hms(22, 0, 0)
        );

        Ok(())
    }

    #[test]
    fn correct_timezone_errors_returns_as_is_for_switch_to_summer_time_without_errors_in_source(
    ) -> Result<(), Box<dyn Error>> {
        let response_body = r#"{ "data": { "marketPricesElectricity": [ { "till": "2022-03-27T00:00:00.000Z", "from": "2022-03-26T23:00:00.000Z", "marketPrice": 0.235, "marketPriceTax": 0.04935, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T01:00:00.000Z", "from": "2022-03-27T00:00:00.000Z", "marketPrice": 0.222, "marketPriceTax": 0.0466053, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T02:00:00.000Z", "from": "2022-03-27T01:00:00.000Z", "marketPrice": 0.214, "marketPriceTax": 0.044944200000000004, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T03:00:00.000Z", "from": "2022-03-27T02:00:00.000Z", "marketPrice": 0.212, "marketPriceTax": 0.04452, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T04:00:00.000Z", "from": "2022-03-27T03:00:00.000Z", "marketPrice": 0.211, "marketPriceTax": 0.0443289, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T05:00:00.000Z", "from": "2022-03-27T04:00:00.000Z", "marketPrice": 0.21, "marketPriceTax": 0.0441651, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T06:00:00.000Z", "from": "2022-03-27T05:00:00.000Z", "marketPrice": 0.214, "marketPriceTax": 0.0449463, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T07:00:00.000Z", "from": "2022-03-27T06:00:00.000Z", "marketPrice": 0.214, "marketPriceTax": 0.044956800000000005, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T08:00:00.000Z", "from": "2022-03-27T07:00:00.000Z", "marketPrice": 0.214, "marketPriceTax": 0.04495469999999999, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T09:00:00.000Z", "from": "2022-03-27T08:00:00.000Z", "marketPrice": 0.205, "marketPriceTax": 0.0430542, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T10:00:00.000Z", "from": "2022-03-27T09:00:00.000Z", "marketPrice": 0.192, "marketPriceTax": 0.0403977, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T11:00:00.000Z", "from": "2022-03-27T10:00:00.000Z", "marketPrice": 0.143, "marketPriceTax": 0.0299397, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T12:00:00.000Z", "from": "2022-03-27T11:00:00.000Z", "marketPrice": 0.09, "marketPriceTax": 0.018879000000000003, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T13:00:00.000Z", "from": "2022-03-27T12:00:00.000Z", "marketPrice": 0.078, "marketPriceTax": 0.0163737, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T14:00:00.000Z", "from": "2022-03-27T13:00:00.000Z", "marketPrice": 0.09, "marketPriceTax": 0.0189126, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T15:00:00.000Z", "from": "2022-03-27T14:00:00.000Z", "marketPrice": 0.142, "marketPriceTax": 0.0297339, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T16:00:00.000Z", "from": "2022-03-27T15:00:00.000Z", "marketPrice": 0.195, "marketPriceTax": 0.0409143, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T17:00:00.000Z", "from": "2022-03-27T16:00:00.000Z", "marketPrice": 0.22, "marketPriceTax": 0.0462, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T18:00:00.000Z", "from": "2022-03-27T17:00:00.000Z", "marketPrice": 0.25, "marketPriceTax": 0.0525, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T19:00:00.000Z", "from": "2022-03-27T18:00:00.000Z", "marketPrice": 0.276, "marketPriceTax": 0.0579075, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T20:00:00.000Z", "from": "2022-03-27T19:00:00.000Z", "marketPrice": 0.26, "marketPriceTax": 0.054621, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T21:00:00.000Z", "from": "2022-03-27T20:00:00.000Z", "marketPrice": 0.243, "marketPriceTax": 0.0510951, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-03-27T22:00:00.000Z", "from": "2022-03-27T21:00:00.000Z", "marketPrice": 0.219, "marketPriceTax": 0.04599, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 } ], "marketPricesGas": [ { "from": "2022-03-26T23:00:00.000Z", "till": "2022-03-27T00:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T00:00:00.000Z", "till": "2022-03-27T01:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T01:00:00.000Z", "till": "2022-03-27T02:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T02:00:00.000Z", "till": "2022-03-27T03:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T03:00:00.000Z", "till": "2022-03-27T04:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T04:00:00.000Z", "till": "2022-03-27T05:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T05:00:00.000Z", "till": "2022-03-27T06:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T06:00:00.000Z", "till": "2022-03-27T07:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T07:00:00.000Z", "till": "2022-03-27T08:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T08:00:00.000Z", "till": "2022-03-27T09:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T09:00:00.000Z", "till": "2022-03-27T10:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T10:00:00.000Z", "till": "2022-03-27T11:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T11:00:00.000Z", "till": "2022-03-27T12:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T12:00:00.000Z", "till": "2022-03-27T13:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T13:00:00.000Z", "till": "2022-03-27T14:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T14:00:00.000Z", "till": "2022-03-27T15:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T15:00:00.000Z", "till": "2022-03-27T16:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T16:00:00.000Z", "till": "2022-03-27T17:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T17:00:00.000Z", "till": "2022-03-27T18:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T18:00:00.000Z", "till": "2022-03-27T19:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T19:00:00.000Z", "till": "2022-03-27T20:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T20:00:00.000Z", "till": "2022-03-27T21:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T21:00:00.000Z", "till": "2022-03-27T22:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-03-27T22:00:00.000Z", "till": "2022-03-27T23:00:00.000Z", "marketPrice": 0.914, "marketPriceTax": 0.192, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 } ] }}"#;
        let spot_price_response = serde_json::from_str::<SpotPriceResponse>(&response_body)?;
        let local_time_zone = "Europe/Amsterdam".parse::<Tz>()?;

        // act
        let corrected_spot_prices = correct_timezone_errors(
            local_time_zone,
            spot_price_response.data.market_prices_electricity,
        );

        assert_eq!(corrected_spot_prices.len(), 23);

        assert_eq!(
            corrected_spot_prices[0].from,
            Utc.ymd(2022, 3, 26).and_hms(23, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[0].till,
            Utc.ymd(2022, 3, 27).and_hms(0, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[1].from,
            Utc.ymd(2022, 3, 27).and_hms(0, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[1].till,
            Utc.ymd(2022, 3, 27).and_hms(1, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[2].from,
            Utc.ymd(2022, 3, 27).and_hms(1, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[2].till,
            Utc.ymd(2022, 3, 27).and_hms(2, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[22].from,
            Utc.ymd(2022, 3, 27).and_hms(21, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[22].till,
            Utc.ymd(2022, 3, 27).and_hms(22, 0, 0)
        );

        Ok(())
    }

    #[test]
    fn correct_timezone_errors_returns_correct_times_for_switch_to_winter_time(
    ) -> Result<(), Box<dyn Error>> {
        let response_body = r#"{"data":{"marketPricesElectricity":[
          {"till":"2021-10-31T00:00:00.000Z","from":"2021-10-30T23:00:00.000Z","marketPrice":0.13,"marketPriceTax":0.0272202,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},
          {"till":"2021-10-31T01:00:00.000Z","from":"2021-10-31T00:00:00.000Z","marketPrice":0.114,"marketPriceTax":0.0239694,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},
          {"till":"2021-10-31T01:00:00.000Z","from":"2021-10-31T01:00:00.000Z","marketPrice":0.08,"marketPriceTax":0.0168084,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},
          {"till":"2021-10-31T02:00:00.000Z","from":"2021-10-31T01:00:00.000Z","marketPrice":0.08,"marketPriceTax":0.0168084,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T03:00:00.000Z","from":"2021-10-31T02:00:00.000Z","marketPrice":0.057,"marketPriceTax":0.0119931,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T04:00:00.000Z","from":"2021-10-31T03:00:00.000Z","marketPrice":0.059,"marketPriceTax":0.0123165,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T05:00:00.000Z","from":"2021-10-31T04:00:00.000Z","marketPrice":0.054,"marketPriceTax":0.0112581,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T06:00:00.000Z","from":"2021-10-31T05:00:00.000Z","marketPrice":0.055,"marketPriceTax":0.011493299999999998,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T07:00:00.000Z","from":"2021-10-31T06:00:00.000Z","marketPrice":0.06,"marketPriceTax":0.0126504,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T08:00:00.000Z","from":"2021-10-31T07:00:00.000Z","marketPrice":0.061,"marketPriceTax":0.0128289,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T09:00:00.000Z","from":"2021-10-31T08:00:00.000Z","marketPrice":0.072,"marketPriceTax":0.0150591,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T10:00:00.000Z","from":"2021-10-31T09:00:00.000Z","marketPrice":0.065,"marketPriceTax":0.013647899999999998,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T11:00:00.000Z","from":"2021-10-31T10:00:00.000Z","marketPrice":0.065,"marketPriceTax":0.0136311,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T12:00:00.000Z","from":"2021-10-31T11:00:00.000Z","marketPrice":0.062,"marketPriceTax":0.0129486,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T13:00:00.000Z","from":"2021-10-31T12:00:00.000Z","marketPrice":0.056,"marketPriceTax":0.0117852,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T14:00:00.000Z","from":"2021-10-31T13:00:00.000Z","marketPrice":0.056,"marketPriceTax":0.0117915,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T15:00:00.000Z","from":"2021-10-31T14:00:00.000Z","marketPrice":0.063,"marketPriceTax":0.013251,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T16:00:00.000Z","from":"2021-10-31T15:00:00.000Z","marketPrice":0.07,"marketPriceTax":0.0146832,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T17:00:00.000Z","from":"2021-10-31T16:00:00.000Z","marketPrice":0.082,"marketPriceTax":0.0171675,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T18:00:00.000Z","from":"2021-10-31T17:00:00.000Z","marketPrice":0.108,"marketPriceTax":0.022579199999999997,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T19:00:00.000Z","from":"2021-10-31T18:00:00.000Z","marketPrice":0.12,"marketPriceTax":0.025179,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T20:00:00.000Z","from":"2021-10-31T19:00:00.000Z","marketPrice":0.1,"marketPriceTax":0.021,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T21:00:00.000Z","from":"2021-10-31T20:00:00.000Z","marketPrice":0.058,"marketPriceTax":0.0121695,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T22:00:00.000Z","from":"2021-10-31T21:00:00.000Z","marketPrice":0.08,"marketPriceTax":0.016779,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15},{"till":"2021-10-31T23:00:00.000Z","from":"2021-10-31T22:00:00.000Z","marketPrice":0.059,"marketPriceTax":0.012461400000000001,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.15}],"marketPricesGas":[{"from":"2021-10-30T23:00:00.000Z","till":"2021-10-31T00:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T00:00:00.000Z","till":"2021-10-31T01:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T01:00:00.000Z","till":"2021-10-31T02:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T02:00:00.000Z","till":"2021-10-31T03:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T03:00:00.000Z","till":"2021-10-31T04:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T04:00:00.000Z","till":"2021-10-31T05:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T05:00:00.000Z","till":"2021-10-31T06:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T06:00:00.000Z","till":"2021-10-31T07:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T07:00:00.000Z","till":"2021-10-31T08:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T08:00:00.000Z","till":"2021-10-31T09:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T09:00:00.000Z","till":"2021-10-31T10:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T10:00:00.000Z","till":"2021-10-31T11:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T11:00:00.000Z","till":"2021-10-31T12:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T12:00:00.000Z","till":"2021-10-31T13:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T13:00:00.000Z","till":"2021-10-31T14:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T14:00:00.000Z","till":"2021-10-31T15:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T15:00:00.000Z","till":"2021-10-31T16:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T16:00:00.000Z","till":"2021-10-31T17:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T17:00:00.000Z","till":"2021-10-31T18:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T18:00:00.000Z","till":"2021-10-31T19:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T19:00:00.000Z","till":"2021-10-31T20:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T20:00:00.000Z","till":"2021-10-31T21:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},{"from":"2021-10-31T21:00:00.000Z","till":"2021-10-31T22:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525},
          {"from":"2021-10-31T22:00:00.000Z","till":"2021-10-31T23:00:00.000Z","marketPrice":0.605,"marketPriceTax":0.127,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.525}]}}"#;
        let spot_price_response = serde_json::from_str::<SpotPriceResponse>(&response_body)?;
        let local_time_zone = "Europe/Amsterdam".parse::<Tz>()?;

        // act
        let corrected_spot_prices = correct_timezone_errors(
            local_time_zone,
            spot_price_response.data.market_prices_electricity,
        );

        assert_eq!(corrected_spot_prices.len(), 25);
        assert_eq!(
            corrected_spot_prices[0].from,
            Utc.ymd(2021, 10, 30).and_hms(22, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[0].till,
            Utc.ymd(2021, 10, 30).and_hms(23, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[1].from,
            Utc.ymd(2021, 10, 30).and_hms(23, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[1].till,
            Utc.ymd(2021, 10, 31).and_hms(0, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[2].from,
            Utc.ymd(2021, 10, 31).and_hms(0, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[2].till,
            Utc.ymd(2021, 10, 31).and_hms(1, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[24].from,
            Utc.ymd(2021, 10, 31).and_hms(22, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[24].till,
            Utc.ymd(2021, 10, 31).and_hms(23, 0, 0)
        );

        Ok(())
    }

    #[test]
    fn correct_timezone_errors_returns_as_is_for_switch_to_winter_time_when_source_has_no_errors(
    ) -> Result<(), Box<dyn Error>> {
        let response_body = r#"{ "data": { "marketPricesElectricity": [ { "till": "2021-10-30T23:00:00.000Z", "from": "2021-10-30T22:00:00.000Z", "marketPrice": 0.13, "marketPriceTax": 0.0272202, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T00:00:00.000Z", "from": "2021-10-30T23:00:00.000Z", "marketPrice": 0.114, "marketPriceTax": 0.0239694, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T01:00:00.000Z", "from": "2021-10-31T00:00:00.000Z", "marketPrice": 0.08, "marketPriceTax": 0.0168084, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T02:00:00.000Z", "from": "2021-10-31T01:00:00.000Z", "marketPrice": 0.08, "marketPriceTax": 0.0168084, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T03:00:00.000Z", "from": "2021-10-31T02:00:00.000Z", "marketPrice": 0.057, "marketPriceTax": 0.0119931, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T04:00:00.000Z", "from": "2021-10-31T03:00:00.000Z", "marketPrice": 0.059, "marketPriceTax": 0.0123165, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T05:00:00.000Z", "from": "2021-10-31T04:00:00.000Z", "marketPrice": 0.054, "marketPriceTax": 0.0112581, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T06:00:00.000Z", "from": "2021-10-31T05:00:00.000Z", "marketPrice": 0.055, "marketPriceTax": 0.011493299999999998, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T07:00:00.000Z", "from": "2021-10-31T06:00:00.000Z", "marketPrice": 0.06, "marketPriceTax": 0.0126504, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T08:00:00.000Z", "from": "2021-10-31T07:00:00.000Z", "marketPrice": 0.061, "marketPriceTax": 0.0128289, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T09:00:00.000Z", "from": "2021-10-31T08:00:00.000Z", "marketPrice": 0.072, "marketPriceTax": 0.0150591, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T10:00:00.000Z", "from": "2021-10-31T09:00:00.000Z", "marketPrice": 0.065, "marketPriceTax": 0.013647899999999998, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T11:00:00.000Z", "from": "2021-10-31T10:00:00.000Z", "marketPrice": 0.065, "marketPriceTax": 0.0136311, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T12:00:00.000Z", "from": "2021-10-31T11:00:00.000Z", "marketPrice": 0.062, "marketPriceTax": 0.0129486, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T13:00:00.000Z", "from": "2021-10-31T12:00:00.000Z", "marketPrice": 0.056, "marketPriceTax": 0.0117852, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T14:00:00.000Z", "from": "2021-10-31T13:00:00.000Z", "marketPrice": 0.056, "marketPriceTax": 0.0117915, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T15:00:00.000Z", "from": "2021-10-31T14:00:00.000Z", "marketPrice": 0.063, "marketPriceTax": 0.013251, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T16:00:00.000Z", "from": "2021-10-31T15:00:00.000Z", "marketPrice": 0.07, "marketPriceTax": 0.0146832, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T17:00:00.000Z", "from": "2021-10-31T16:00:00.000Z", "marketPrice": 0.082, "marketPriceTax": 0.0171675, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T18:00:00.000Z", "from": "2021-10-31T17:00:00.000Z", "marketPrice": 0.108, "marketPriceTax": 0.022579199999999997, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T19:00:00.000Z", "from": "2021-10-31T18:00:00.000Z", "marketPrice": 0.12, "marketPriceTax": 0.025179, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T20:00:00.000Z", "from": "2021-10-31T19:00:00.000Z", "marketPrice": 0.1, "marketPriceTax": 0.021, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T21:00:00.000Z", "from": "2021-10-31T20:00:00.000Z", "marketPrice": 0.058, "marketPriceTax": 0.0121695, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T22:00:00.000Z", "from": "2021-10-31T21:00:00.000Z", "marketPrice": 0.08, "marketPriceTax": 0.016779, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 }, { "till": "2021-10-31T23:00:00.000Z", "from": "2021-10-31T22:00:00.000Z", "marketPrice": 0.059, "marketPriceTax": 0.012461400000000001, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.15 } ], "marketPricesGas": [ { "from": "2021-10-30T23:00:00.000Z", "till": "2021-10-31T00:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T00:00:00.000Z", "till": "2021-10-31T01:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T01:00:00.000Z", "till": "2021-10-31T02:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T02:00:00.000Z", "till": "2021-10-31T03:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T03:00:00.000Z", "till": "2021-10-31T04:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T04:00:00.000Z", "till": "2021-10-31T05:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T05:00:00.000Z", "till": "2021-10-31T06:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T06:00:00.000Z", "till": "2021-10-31T07:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T07:00:00.000Z", "till": "2021-10-31T08:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T08:00:00.000Z", "till": "2021-10-31T09:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T09:00:00.000Z", "till": "2021-10-31T10:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T10:00:00.000Z", "till": "2021-10-31T11:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T11:00:00.000Z", "till": "2021-10-31T12:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T12:00:00.000Z", "till": "2021-10-31T13:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T13:00:00.000Z", "till": "2021-10-31T14:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T14:00:00.000Z", "till": "2021-10-31T15:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T15:00:00.000Z", "till": "2021-10-31T16:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T16:00:00.000Z", "till": "2021-10-31T17:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T17:00:00.000Z", "till": "2021-10-31T18:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T18:00:00.000Z", "till": "2021-10-31T19:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T19:00:00.000Z", "till": "2021-10-31T20:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T20:00:00.000Z", "till": "2021-10-31T21:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T21:00:00.000Z", "till": "2021-10-31T22:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 }, { "from": "2021-10-31T22:00:00.000Z", "till": "2021-10-31T23:00:00.000Z", "marketPrice": 0.605, "marketPriceTax": 0.127, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.525 } ] }}"#;
        let spot_price_response = serde_json::from_str::<SpotPriceResponse>(&response_body)?;
        let local_time_zone = "Europe/Amsterdam".parse::<Tz>()?;

        // act
        let corrected_spot_prices = correct_timezone_errors(
            local_time_zone,
            spot_price_response.data.market_prices_electricity,
        );

        assert_eq!(corrected_spot_prices.len(), 25);
        assert_eq!(
            corrected_spot_prices[0].from,
            Utc.ymd(2021, 10, 30).and_hms(22, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[0].till,
            Utc.ymd(2021, 10, 30).and_hms(23, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[1].from,
            Utc.ymd(2021, 10, 30).and_hms(23, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[1].till,
            Utc.ymd(2021, 10, 31).and_hms(0, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[2].from,
            Utc.ymd(2021, 10, 31).and_hms(0, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[2].till,
            Utc.ymd(2021, 10, 31).and_hms(1, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[24].from,
            Utc.ymd(2021, 10, 31).and_hms(22, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[24].till,
            Utc.ymd(2021, 10, 31).and_hms(23, 0, 0)
        );

        Ok(())
    }

    #[test]
    fn correct_timezone_errors_returns_winter_time_as_is() -> Result<(), Box<dyn Error>> {
        let response_body = r#"{"data":{"marketPricesElectricity":[{"till":"2022-01-01T00:00:00.000Z","from":"2021-12-31T23:00:00.000Z","marketPrice":0.125,"marketPriceTax":0.026187,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T01:00:00.000Z","from":"2022-01-01T00:00:00.000Z","marketPrice":0.125,"marketPriceTax":0.026187,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T02:00:00.000Z","from":"2022-01-01T01:00:00.000Z","marketPrice":0.134,"marketPriceTax":0.02814,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T03:00:00.000Z","from":"2022-01-01T02:00:00.000Z","marketPrice":0.059,"marketPriceTax":0.012348,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T04:00:00.000Z","from":"2022-01-01T03:00:00.000Z","marketPrice":0.038,"marketPriceTax":0.0079107,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T05:00:00.000Z","from":"2022-01-01T04:00:00.000Z","marketPrice":0.04,"marketPriceTax":0.008337,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T06:00:00.000Z","from":"2022-01-01T05:00:00.000Z","marketPrice":0.041,"marketPriceTax":0.0085239,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T07:00:00.000Z","from":"2022-01-01T06:00:00.000Z","marketPrice":0.043,"marketPriceTax":0.0090846,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T08:00:00.000Z","from":"2022-01-01T07:00:00.000Z","marketPrice":0.05,"marketPriceTax":0.0104286,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T09:00:00.000Z","from":"2022-01-01T08:00:00.000Z","marketPrice":0.07,"marketPriceTax":0.0147105,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T10:00:00.000Z","from":"2022-01-01T09:00:00.000Z","marketPrice":0.077,"marketPriceTax":0.016125900000000002,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T11:00:00.000Z","from":"2022-01-01T10:00:00.000Z","marketPrice":0.084,"marketPriceTax":0.017661,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T12:00:00.000Z","from":"2022-01-01T11:00:00.000Z","marketPrice":0.095,"marketPriceTax":0.019895399999999997,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T13:00:00.000Z","from":"2022-01-01T12:00:00.000Z","marketPrice":0.097,"marketPriceTax":0.020328,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T14:00:00.000Z","from":"2022-01-01T13:00:00.000Z","marketPrice":0.097,"marketPriceTax":0.0204057,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T15:00:00.000Z","from":"2022-01-01T14:00:00.000Z","marketPrice":0.101,"marketPriceTax":0.0212667,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T16:00:00.000Z","from":"2022-01-01T15:00:00.000Z","marketPrice":0.127,"marketPriceTax":0.026586,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T17:00:00.000Z","from":"2022-01-01T16:00:00.000Z","marketPrice":0.15,"marketPriceTax":0.0314937,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T18:00:00.000Z","from":"2022-01-01T17:00:00.000Z","marketPrice":0.146,"marketPriceTax":0.030729300000000005,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T19:00:00.000Z","from":"2022-01-01T18:00:00.000Z","marketPrice":0.14,"marketPriceTax":0.0294588,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T20:00:00.000Z","from":"2022-01-01T19:00:00.000Z","marketPrice":0.122,"marketPriceTax":0.0255948,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T21:00:00.000Z","from":"2022-01-01T20:00:00.000Z","marketPrice":0.103,"marketPriceTax":0.021548099999999997,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T22:00:00.000Z","from":"2022-01-01T21:00:00.000Z","marketPrice":0.097,"marketPriceTax":0.020466599999999998,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-01-01T23:00:00.000Z","from":"2022-01-01T22:00:00.000Z","marketPrice":0.085,"marketPriceTax":0.0178836,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081}],"marketPricesGas":[{"from":"2021-12-31T23:00:00.000Z","till":"2022-01-01T00:00:00.000Z","marketPrice":0.767,"marketPriceTax":0.161,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T00:00:00.000Z","till":"2022-01-01T01:00:00.000Z","marketPrice":0.767,"marketPriceTax":0.161,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T01:00:00.000Z","till":"2022-01-01T02:00:00.000Z","marketPrice":0.767,"marketPriceTax":0.161,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T02:00:00.000Z","till":"2022-01-01T03:00:00.000Z","marketPrice":0.767,"marketPriceTax":0.161,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T03:00:00.000Z","till":"2022-01-01T04:00:00.000Z","marketPrice":0.767,"marketPriceTax":0.161,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T04:00:00.000Z","till":"2022-01-01T05:00:00.000Z","marketPrice":0.767,"marketPriceTax":0.161,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T05:00:00.000Z","till":"2022-01-01T06:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T06:00:00.000Z","till":"2022-01-01T07:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T07:00:00.000Z","till":"2022-01-01T08:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T08:00:00.000Z","till":"2022-01-01T09:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T09:00:00.000Z","till":"2022-01-01T10:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T10:00:00.000Z","till":"2022-01-01T11:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T11:00:00.000Z","till":"2022-01-01T12:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T12:00:00.000Z","till":"2022-01-01T13:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T13:00:00.000Z","till":"2022-01-01T14:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T14:00:00.000Z","till":"2022-01-01T15:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T15:00:00.000Z","till":"2022-01-01T16:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T16:00:00.000Z","till":"2022-01-01T17:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T17:00:00.000Z","till":"2022-01-01T18:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T18:00:00.000Z","till":"2022-01-01T19:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T19:00:00.000Z","till":"2022-01-01T20:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T20:00:00.000Z","till":"2022-01-01T21:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T21:00:00.000Z","till":"2022-01-01T22:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-01-01T22:00:00.000Z","till":"2022-01-01T23:00:00.000Z","marketPrice":0.694,"marketPriceTax":0.146,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544}]}}"#;
        let spot_price_response = serde_json::from_str::<SpotPriceResponse>(&response_body)?;
        let local_time_zone = "Europe/Amsterdam".parse::<Tz>()?;

        // act
        let corrected_spot_prices = correct_timezone_errors(
            local_time_zone,
            spot_price_response.data.market_prices_electricity,
        );

        assert_eq!(corrected_spot_prices.len(), 24);
        assert_eq!(
            corrected_spot_prices[0].from,
            Utc.ymd(2021, 12, 31).and_hms(23, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[0].till,
            Utc.ymd(2022, 1, 1).and_hms(0, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[23].from,
            Utc.ymd(2022, 1, 1).and_hms(22, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[23].till,
            Utc.ymd(2022, 1, 1).and_hms(23, 0, 0)
        );

        Ok(())
    }

    #[test]
    fn correct_timezone_errors_returns_summer_time_shifted_one_hour() -> Result<(), Box<dyn Error>>
    {
        let response_body = r#"{"data":{"marketPricesElectricity":[{"till":"2022-04-01T00:00:00.000Z","from":"2022-03-31T23:00:00.000Z","marketPrice":0.173,"marketPriceTax":0.0363846,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T01:00:00.000Z","from":"2022-04-01T00:00:00.000Z","marketPrice":0.152,"marketPriceTax":0.031983000000000004,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T02:00:00.000Z","from":"2022-04-01T01:00:00.000Z","marketPrice":0.16,"marketPriceTax":0.033579000000000005,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T03:00:00.000Z","from":"2022-04-01T02:00:00.000Z","marketPrice":0.151,"marketPriceTax":0.03171,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T04:00:00.000Z","from":"2022-04-01T03:00:00.000Z","marketPrice":0.15,"marketPriceTax":0.0315,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T05:00:00.000Z","from":"2022-04-01T04:00:00.000Z","marketPrice":0.17,"marketPriceTax":0.0357,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T06:00:00.000Z","from":"2022-04-01T05:00:00.000Z","marketPrice":0.181,"marketPriceTax":0.0380961,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T07:00:00.000Z","from":"2022-04-01T06:00:00.000Z","marketPrice":0.207,"marketPriceTax":0.0433965,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T08:00:00.000Z","from":"2022-04-01T07:00:00.000Z","marketPrice":0.286,"marketPriceTax":0.0599844,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T09:00:00.000Z","from":"2022-04-01T08:00:00.000Z","marketPrice":0.25,"marketPriceTax":0.052479,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T10:00:00.000Z","from":"2022-04-01T09:00:00.000Z","marketPrice":0.204,"marketPriceTax":0.04284,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T11:00:00.000Z","from":"2022-04-01T10:00:00.000Z","marketPrice":0.181,"marketPriceTax":0.0380394,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T12:00:00.000Z","from":"2022-04-01T11:00:00.000Z","marketPrice":0.172,"marketPriceTax":0.0361074,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T13:00:00.000Z","from":"2022-04-01T12:00:00.000Z","marketPrice":0.16,"marketPriceTax":0.0335811,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T14:00:00.000Z","from":"2022-04-01T13:00:00.000Z","marketPrice":0.147,"marketPriceTax":0.030890999999999995,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T15:00:00.000Z","from":"2022-04-01T14:00:00.000Z","marketPrice":0.141,"marketPriceTax":0.0295722,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T16:00:00.000Z","from":"2022-04-01T15:00:00.000Z","marketPrice":0.15,"marketPriceTax":0.0315,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T17:00:00.000Z","from":"2022-04-01T16:00:00.000Z","marketPrice":0.184,"marketPriceTax":0.03859800000000001,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T18:00:00.000Z","from":"2022-04-01T17:00:00.000Z","marketPrice":0.204,"marketPriceTax":0.0429177,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T19:00:00.000Z","from":"2022-04-01T18:00:00.000Z","marketPrice":0.246,"marketPriceTax":0.05166,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T20:00:00.000Z","from":"2022-04-01T19:00:00.000Z","marketPrice":0.3,"marketPriceTax":0.062979,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T21:00:00.000Z","from":"2022-04-01T20:00:00.000Z","marketPrice":0.266,"marketPriceTax":0.05586,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T22:00:00.000Z","from":"2022-04-01T21:00:00.000Z","marketPrice":0.29,"marketPriceTax":0.0609,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081},{"till":"2022-04-01T23:00:00.000Z","from":"2022-04-01T22:00:00.000Z","marketPrice":0.3,"marketPriceTax":0.0629958,"sourcingMarkupPrice":0.017,"energyTaxPrice":0.081}],"marketPricesGas":[{"from":"2022-03-31T23:00:00.000Z","till":"2022-04-01T00:00:00.000Z","marketPrice":1.138,"marketPriceTax":0.239,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T00:00:00.000Z","till":"2022-04-01T01:00:00.000Z","marketPrice":1.138,"marketPriceTax":0.239,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T01:00:00.000Z","till":"2022-04-01T02:00:00.000Z","marketPrice":1.138,"marketPriceTax":0.239,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T02:00:00.000Z","till":"2022-04-01T03:00:00.000Z","marketPrice":1.138,"marketPriceTax":0.239,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T03:00:00.000Z","till":"2022-04-01T04:00:00.000Z","marketPrice":1.138,"marketPriceTax":0.239,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T04:00:00.000Z","till":"2022-04-01T05:00:00.000Z","marketPrice":1.138,"marketPriceTax":0.239,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T05:00:00.000Z","till":"2022-04-01T06:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T06:00:00.000Z","till":"2022-04-01T07:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T07:00:00.000Z","till":"2022-04-01T08:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T08:00:00.000Z","till":"2022-04-01T09:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T09:00:00.000Z","till":"2022-04-01T10:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T10:00:00.000Z","till":"2022-04-01T11:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T11:00:00.000Z","till":"2022-04-01T12:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T12:00:00.000Z","till":"2022-04-01T13:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T13:00:00.000Z","till":"2022-04-01T14:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T14:00:00.000Z","till":"2022-04-01T15:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T15:00:00.000Z","till":"2022-04-01T16:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T16:00:00.000Z","till":"2022-04-01T17:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T17:00:00.000Z","till":"2022-04-01T18:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T18:00:00.000Z","till":"2022-04-01T19:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T19:00:00.000Z","till":"2022-04-01T20:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T20:00:00.000Z","till":"2022-04-01T21:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T21:00:00.000Z","till":"2022-04-01T22:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544},{"from":"2022-04-01T22:00:00.000Z","till":"2022-04-01T23:00:00.000Z","marketPrice":1.22,"marketPriceTax":0.256,"sourcingMarkupPrice":0.097,"energyTaxPrice":0.544}]}}"#;
        let spot_price_response = serde_json::from_str::<SpotPriceResponse>(&response_body)?;
        let local_time_zone = "Europe/Amsterdam".parse::<Tz>()?;

        // act
        let corrected_spot_prices = correct_timezone_errors(
            local_time_zone,
            spot_price_response.data.market_prices_electricity,
        );

        assert_eq!(corrected_spot_prices.len(), 24);
        assert_eq!(
            corrected_spot_prices[0].from,
            Utc.ymd(2022, 3, 31).and_hms(22, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[0].till,
            Utc.ymd(2022, 3, 31).and_hms(23, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[23].from,
            Utc.ymd(2022, 4, 1).and_hms(21, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[23].till,
            Utc.ymd(2022, 4, 1).and_hms(22, 0, 0)
        );

        Ok(())
    }

    #[test]
    fn correct_timezone_errors_returns_summer_time_as_is_when_source_has_no_errors(
    ) -> Result<(), Box<dyn Error>> {
        let response_body = r#"{ "data": { "marketPricesElectricity": [ { "till": "2022-03-31T23:00:00.000Z", "from": "2022-03-31T22:00:00.000Z", "marketPrice": 0.173, "marketPriceTax": 0.0363846, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T00:00:00.000Z", "from": "2022-03-31T23:00:00.000Z", "marketPrice": 0.152, "marketPriceTax": 0.031983000000000004, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T01:00:00.000Z", "from": "2022-04-01T00:00:00.000Z", "marketPrice": 0.16, "marketPriceTax": 0.033579000000000005, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T02:00:00.000Z", "from": "2022-04-01T01:00:00.000Z", "marketPrice": 0.151, "marketPriceTax": 0.03171, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T03:00:00.000Z", "from": "2022-04-01T02:00:00.000Z", "marketPrice": 0.15, "marketPriceTax": 0.0315, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T04:00:00.000Z", "from": "2022-04-01T03:00:00.000Z", "marketPrice": 0.17, "marketPriceTax": 0.0357, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T05:00:00.000Z", "from": "2022-04-01T04:00:00.000Z", "marketPrice": 0.181, "marketPriceTax": 0.0380961, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T06:00:00.000Z", "from": "2022-04-01T05:00:00.000Z", "marketPrice": 0.207, "marketPriceTax": 0.0433965, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T07:00:00.000Z", "from": "2022-04-01T06:00:00.000Z", "marketPrice": 0.286, "marketPriceTax": 0.0599844, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T08:00:00.000Z", "from": "2022-04-01T07:00:00.000Z", "marketPrice": 0.25, "marketPriceTax": 0.052479, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T09:00:00.000Z", "from": "2022-04-01T08:00:00.000Z", "marketPrice": 0.204, "marketPriceTax": 0.04284, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T10:00:00.000Z", "from": "2022-04-01T09:00:00.000Z", "marketPrice": 0.181, "marketPriceTax": 0.0380394, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T11:00:00.000Z", "from": "2022-04-01T10:00:00.000Z", "marketPrice": 0.172, "marketPriceTax": 0.0361074, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T12:00:00.000Z", "from": "2022-04-01T11:00:00.000Z", "marketPrice": 0.16, "marketPriceTax": 0.0335811, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T13:00:00.000Z", "from": "2022-04-01T12:00:00.000Z", "marketPrice": 0.147, "marketPriceTax": 0.030890999999999995, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T14:00:00.000Z", "from": "2022-04-01T13:00:00.000Z", "marketPrice": 0.141, "marketPriceTax": 0.0295722, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T15:00:00.000Z", "from": "2022-04-01T14:00:00.000Z", "marketPrice": 0.15, "marketPriceTax": 0.0315, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T16:00:00.000Z", "from": "2022-04-01T15:00:00.000Z", "marketPrice": 0.184, "marketPriceTax": 0.03859800000000001, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T17:00:00.000Z", "from": "2022-04-01T16:00:00.000Z", "marketPrice": 0.204, "marketPriceTax": 0.0429177, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T18:00:00.000Z", "from": "2022-04-01T17:00:00.000Z", "marketPrice": 0.246, "marketPriceTax": 0.05166, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T19:00:00.000Z", "from": "2022-04-01T18:00:00.000Z", "marketPrice": 0.3, "marketPriceTax": 0.062979, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T20:00:00.000Z", "from": "2022-04-01T19:00:00.000Z", "marketPrice": 0.266, "marketPriceTax": 0.05586, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T21:00:00.000Z", "from": "2022-04-01T20:00:00.000Z", "marketPrice": 0.29, "marketPriceTax": 0.0609, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 }, { "till": "2022-04-01T22:00:00.000Z", "from": "2022-04-01T21:00:00.000Z", "marketPrice": 0.3, "marketPriceTax": 0.0629958, "sourcingMarkupPrice": 0.017, "energyTaxPrice": 0.081 } ], "marketPricesGas": [ { "from": "2022-03-31T23:00:00.000Z", "till": "2022-04-01T00:00:00.000Z", "marketPrice": 1.138, "marketPriceTax": 0.239, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T00:00:00.000Z", "till": "2022-04-01T01:00:00.000Z", "marketPrice": 1.138, "marketPriceTax": 0.239, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T01:00:00.000Z", "till": "2022-04-01T02:00:00.000Z", "marketPrice": 1.138, "marketPriceTax": 0.239, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T02:00:00.000Z", "till": "2022-04-01T03:00:00.000Z", "marketPrice": 1.138, "marketPriceTax": 0.239, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T03:00:00.000Z", "till": "2022-04-01T04:00:00.000Z", "marketPrice": 1.138, "marketPriceTax": 0.239, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T04:00:00.000Z", "till": "2022-04-01T05:00:00.000Z", "marketPrice": 1.138, "marketPriceTax": 0.239, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T05:00:00.000Z", "till": "2022-04-01T06:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T06:00:00.000Z", "till": "2022-04-01T07:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T07:00:00.000Z", "till": "2022-04-01T08:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T08:00:00.000Z", "till": "2022-04-01T09:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T09:00:00.000Z", "till": "2022-04-01T10:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T10:00:00.000Z", "till": "2022-04-01T11:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T11:00:00.000Z", "till": "2022-04-01T12:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T12:00:00.000Z", "till": "2022-04-01T13:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T13:00:00.000Z", "till": "2022-04-01T14:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T14:00:00.000Z", "till": "2022-04-01T15:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T15:00:00.000Z", "till": "2022-04-01T16:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T16:00:00.000Z", "till": "2022-04-01T17:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T17:00:00.000Z", "till": "2022-04-01T18:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T18:00:00.000Z", "till": "2022-04-01T19:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T19:00:00.000Z", "till": "2022-04-01T20:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T20:00:00.000Z", "till": "2022-04-01T21:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T21:00:00.000Z", "till": "2022-04-01T22:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 }, { "from": "2022-04-01T22:00:00.000Z", "till": "2022-04-01T23:00:00.000Z", "marketPrice": 1.22, "marketPriceTax": 0.256, "sourcingMarkupPrice": 0.097, "energyTaxPrice": 0.544 } ] } }"#;
        let spot_price_response = serde_json::from_str::<SpotPriceResponse>(&response_body)?;
        let local_time_zone = "Europe/Amsterdam".parse::<Tz>()?;

        // act
        let corrected_spot_prices = correct_timezone_errors(
            local_time_zone,
            spot_price_response.data.market_prices_electricity,
        );

        assert_eq!(corrected_spot_prices.len(), 24);
        assert_eq!(
            corrected_spot_prices[0].from,
            Utc.ymd(2022, 3, 31).and_hms(22, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[0].till,
            Utc.ymd(2022, 3, 31).and_hms(23, 0, 0)
        );

        assert_eq!(
            corrected_spot_prices[23].from,
            Utc.ymd(2022, 4, 1).and_hms(21, 0, 0)
        );
        assert_eq!(
            corrected_spot_prices[23].till,
            Utc.ymd(2022, 4, 1).and_hms(22, 0, 0)
        );

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn get_historic_prices() -> Result<(), Box<dyn Error>> {
        let bigquery_client = BigqueryClient::from_env().await?;
        let spot_price_client = SpotPriceClient::from_env()?;
        let state_client = StateClient::from_env().await?;

        let exporter_service =
            ExporterService::from_env(bigquery_client, spot_price_client, state_client)?;

        let mut start_date: DateTime<Utc> = Utc.ymd(2020, 12, 1).and_hms_nano(0, 0, 0, 0);
        let end_date: DateTime<Utc> = Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 0);

        while start_date < end_date {
            exporter_service.run(start_date).await?;
            start_date = start_date + Duration::days(1)
        }

        Ok(())
    }
}
