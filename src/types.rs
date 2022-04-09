use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpotPriceRequest {
    pub query: String,
    pub variables: SpotPriceRequestVariables,
    pub operation_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpotPriceRequestVariables {
    pub start_date: String,
    pub end_date: String,
}


#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpotPriceResponse {
    pub data: SpotPriceData,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpotPriceData {
    pub market_prices_electricity: Vec<SpotPrice>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpotPrice {
    pub id: Option<String>,
    pub source: Option<String>,
    pub from: DateTime<Utc>,
    pub till: DateTime<Utc>,
    pub market_price: f64,
    pub market_price_tax: f64,
    pub sourcing_markup_price: f64,
    pub energy_tax_price: f64,
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    env_logger::init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn deserialize_spot_price_response() {
        let spot_price_predictions_content = fs::read_to_string("spot_price_predictions.json")
            .expect("Failed reading spot_price_predictions.json");

        let spot_price_response: SpotPriceResponse =
            serde_json::from_str(&spot_price_predictions_content).expect("Failed parsing json");

        assert_eq!(spot_price_response.data.market_prices_electricity.len(), 24);
    }
}
