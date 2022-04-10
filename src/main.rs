mod bigquery_client;
mod exporter_service;
mod spot_price_client;
mod state_client;
mod types;

use bigquery_client::BigqueryClient;
use chrono::Utc;
use exporter_service::ExporterService;
use spot_price_client::SpotPriceClient;
use state_client::StateClient;
use std::error::Error;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let bigquery_client = BigqueryClient::from_env().await?;
    let spot_price_client = SpotPriceClient::from_env()?;
    let state_client = StateClient::from_env().await?;

    let exporter_service =
        ExporterService::from_env(bigquery_client, spot_price_client, state_client)?;

    exporter_service.run(Utc::now()).await
}
