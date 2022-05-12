use crate::types::SpotPrice;
use gcp_bigquery_client::model::table::Table;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use gcp_bigquery_client::model::table_schema::TableSchema;
use gcp_bigquery_client::model::time_partitioning::TimePartitioning;
use log::info;
use std::env;
use std::error::Error;
use std::{thread, time};

pub struct BigqueryClientConfig {
    project_id: String,
    dataset: String,
    table: String,
    enable: bool,
    init: bool,
    client: gcp_bigquery_client::Client,
}

impl BigqueryClientConfig {
    pub async fn new(
        project_id: &str,
        dataset: &str,
        table: &str,
        google_application_credentials: &str,
        enable: bool,
        init: bool,
    ) -> Result<Self, Box<dyn Error>> {
        let client = gcp_bigquery_client::Client::from_service_account_key_file(
            google_application_credentials,
        )
        .await;

        Ok(Self {
            project_id: project_id.to_string(),
            dataset: dataset.to_string(),
            table: table.to_string(),
            enable,
            init,
            client,
        })
    }

    pub async fn from_env() -> Result<Self, Box<dyn Error>> {
        let google_application_credentials = env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .unwrap_or_else(|_| String::from("/secrets/keyfile.json"));
        let project_id = env::var("BQ_PROJECT_ID")?;
        let dataset = env::var("BQ_DATASET")?;
        let table = env::var("BQ_TABLE")?;
        let enable: bool = env::var("BQ_ENABLE")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);
        let init: bool = env::var("BQ_INIT")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);

        Self::new(
            &project_id,
            &dataset,
            &table,
            &google_application_credentials,
            enable,
            init,
        )
        .await
    }
}

pub struct BigqueryClient {
    config: BigqueryClientConfig,
}

impl BigqueryClient {
    pub fn new(config: BigqueryClientConfig) -> Self {
        Self { config }
    }

    pub async fn from_env() -> Result<Self, Box<dyn Error>> {
        Ok(Self::new(BigqueryClientConfig::from_env().await?))
    }

    pub async fn check_if_table_exists(&self) -> bool {
        if !self.config.enable {
            return false;
        }

        return self
            .config
            .client
            .table()
            .get(
                &self.config.project_id,
                &self.config.dataset,
                &self.config.table,
                None,
            )
            .await
            .is_ok();
    }

    pub async fn create_table(&self, wait_ready: bool) -> Result<(), Box<dyn Error>> {
        if !self.config.enable {
            return Ok(());
        }

        let dataset = &self
            .config
            .client
            .dataset()
            .get(&self.config.project_id, &self.config.dataset)
            .await?;

        dataset
            .create_table(
                &self.config.client,
                Table::from_dataset(
                    dataset,
                    &self.config.table,
                    TableSchema::new(vec![
                        TableFieldSchema::string("id"),
                        TableFieldSchema::string("source"),
                        TableFieldSchema::timestamp("from"),
                        TableFieldSchema::timestamp("till"),
                        TableFieldSchema::float("marketPrice"),
                        TableFieldSchema::float("marketPriceTax"),
                        TableFieldSchema::float("sourcingMarkupPrice"),
                        TableFieldSchema::float("energyTaxPrice"),
                    ]),
                )
                .time_partitioning(TimePartitioning::per_day().field("from")),
            )
            .await?;

        if wait_ready {
            loop {
                if self.check_if_table_exists().await {
                    break;
                }

                thread::sleep(time::Duration::from_secs(1));
            }
        }

        info!("Created bigquery table {}", &self.config.table);

        Ok(())
    }

    pub async fn update_table_schema(&self) -> Result<(), Box<dyn Error>> {
        if !self.config.enable {
            return Ok(());
        }

        if !self.config.enable {
            return Ok(());
        }

        let dataset = &self
            .config
            .client
            .dataset()
            .get(&self.config.project_id, &self.config.dataset)
            .await?;

        self.config
            .client
            .table()
            .update(
                &self.config.project_id,
                &self.config.dataset,
                &self.config.table,
                Table::from_dataset(
                    dataset,
                    &self.config.table,
                    TableSchema::new(vec![
                        TableFieldSchema::string("id"),
                        TableFieldSchema::string("source"),
                        TableFieldSchema::timestamp("from"),
                        TableFieldSchema::timestamp("till"),
                        TableFieldSchema::float("marketPrice"),
                        TableFieldSchema::float("marketPriceTax"),
                        TableFieldSchema::float("sourcingMarkupPrice"),
                        TableFieldSchema::float("energyTaxPrice"),
                    ]),
                )
                .time_partitioning(TimePartitioning::per_day().field("from")),
            )
            .await?;

        info!("Updated schema for bigquery table {}", &self.config.table);

        Ok(())
    }

    pub async fn insert_spot_price(&self, spot_price: &SpotPrice) -> Result<(), Box<dyn Error>> {
        if !self.config.enable {
            return Ok(());
        }

        let mut insert_request = TableDataInsertAllRequest::new();
        insert_request.add_row(None, spot_price)?;

        self.config
            .client
            .tabledata()
            .insert_all(
                &self.config.project_id,
                &self.config.dataset,
                &self.config.table,
                insert_request,
            )
            .await?;

        info!(
            "Inserted spot price {:#?} into bigquery table {}",
            &spot_price, &self.config.table
        );

        Ok(())
    }

    pub async fn init_table(&self) -> Result<(), Box<dyn Error>> {
        if !self.config.enable || !self.config.init {
            return Ok(());
        }

        if !self.check_if_table_exists().await {
            self.create_table(true).await?
        } else {
            self.update_table_schema().await?
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn create_table() -> Result<(), Box<dyn Error>> {
        let bigquery_client = BigqueryClient::from_env().await?;

        // act
        bigquery_client.init_table().await?;

        Ok(())
    }
}
