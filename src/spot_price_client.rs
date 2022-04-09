use crate::types::{SpotPriceRequest,SpotPriceRequestVariables,SpotPriceResponse};
use chrono::{Duration, Utc};
use log::debug;
use std::env;
use std::error::Error;

pub struct SpotPriceClientConfig {
    url: String,
    query: String,
}

impl SpotPriceClientConfig {
    pub fn new(url: &str, query: &str) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            url: url.to_string(),
            query: query.to_string(),
        })
    }

    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        let url = env::var("URL")?;
        let query = env::var("QUERY")?;

        Self::new(&url, &query)
    }
}

pub struct SpotPriceClient {
    config: SpotPriceClientConfig,
}

impl SpotPriceClient {
    pub fn new(config: SpotPriceClientConfig) -> Self {
        Self { config }
    }

    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        Ok(Self::new(SpotPriceClientConfig::from_env()?))
    }

    pub fn get_spot_prices(&self) -> Result<SpotPriceResponse, Box<dyn Error>> {

        let today = Utc::now();
        let tomorrow = today  + Duration::days(1);
     
        let request_body = SpotPriceRequest{
          query: self.config.query.clone(),
          variables:           SpotPriceRequestVariables{
            start_date: today.format("%Y-%m-%d").to_string(),
            end_date: tomorrow.format("%Y-%m-%d").to_string(),
          },
          operation_name:"MarketPrices".to_string(),
        };

        let request_body = serde_json::to_string(&request_body)?;
        debug!("request body:\n{}", request_body);

        let response = reqwest::blocking::Client::new()
            .post(&self.config.url)
            .body(request_body)
            .send()?;

        let status_code = response.status();
        debug!("response status: {}", status_code);

        let response_body = response.text()?;
        debug!("response body:\n{}", response_body);

        if !status_code.is_success() {
            return Err(Box::<dyn Error>::from(format!(
                "Status code {} indicates failure",
                status_code
            )));
        }

        Ok(serde_json::from_str::<SpotPriceResponse>(&response_body)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn get_spot_prices() {
        let spot_price_client = SpotPriceClient::new(
            SpotPriceClientConfig::from_env()
                .expect("Failed creating SpotPriceClientConfig"),
        );

        // act
        let spot_price_response = spot_price_client
            .get_spot_prices()
            .expect("Failed retrieving spot prices");

        assert_eq!(spot_price_response.data.market_prices_electricity.len(), 24);
    }
}
