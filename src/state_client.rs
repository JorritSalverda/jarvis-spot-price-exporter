use crate::types::*;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    api::{Api, PostParams},
    Client,
};
use log::info;
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

pub struct StateClientConfig {
    kube_client: Option<kube::Client>,
    state_file_path: String,
    state_file_configmap_name: String,
    current_namespace: String,
    enable: bool,
}

impl StateClientConfig {
    pub fn new(
        kube_client: Option<kube::Client>,
        state_file_path: &str,
        state_file_configmap_name: &str,
        current_namespace: &str,
        enable: bool,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            kube_client,
            state_file_path: state_file_path.into(),
            state_file_configmap_name: state_file_configmap_name.into(),
            current_namespace: current_namespace.into(),
            enable,
        })
    }

    pub async fn from_env() -> Result<Self, Box<dyn Error>> {
        let enable: bool = env::var("STATE_ENABLE")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .unwrap_or(false);

        let state_file_path =
            env::var("STATE_FILE_PATH").unwrap_or_else(|_| "/configs/state.yaml".to_string());
        let state_file_configmap_name = env::var("STATE_FILE_CONFIG_MAP_NAME")
            .unwrap_or_else(|_| "jarvis-spot-price-exporter".to_string());

        if enable {
            let kube_client: kube::Client = Client::try_default().await?;
            let current_namespace =
                fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")?;

            Self::new(
                Some(kube_client),
                &state_file_path,
                &state_file_configmap_name,
                &current_namespace,
                enable,
            )
        } else {
            Self::new(
                None,
                &state_file_path,
                &state_file_configmap_name,
                "",
                enable,
            )
        }
    }
}

pub struct StateClient {
    config: StateClientConfig,
}

impl StateClient {
    pub fn new(config: StateClientConfig) -> StateClient {
        StateClient { config }
    }

    pub async fn from_env() -> Result<Self, Box<dyn Error>> {
        Ok(Self::new(StateClientConfig::from_env().await?))
    }

    pub fn read_state(&self) -> Result<Option<State>, Box<dyn std::error::Error>> {
        if !self.config.enable {
            return Ok(None);
        }

        let state_file_contents = match fs::read_to_string(&self.config.state_file_path) {
            Ok(c) => c,
            Err(_) => return Ok(Option::None),
        };

        let last_state: Option<State> = match serde_yaml::from_str(&state_file_contents) {
            Ok(lm) => Some(lm),
            Err(_) => return Ok(Option::None),
        };

        info!("Read state file at {}", &self.config.state_file_path);

        Ok(last_state)
    }

    async fn get_state_configmap(&self) -> Result<ConfigMap, Box<dyn std::error::Error>> {
        let configmaps_api: Api<ConfigMap> = Api::namespaced(
            self.config.kube_client.as_ref().unwrap().clone(),
            &self.config.current_namespace,
        );

        let config_map = configmaps_api
            .get(&self.config.state_file_configmap_name)
            .await?;

        Ok(config_map)
    }

    async fn update_state_configmap(
        &self,
        config_map: &ConfigMap,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let configmaps_api: Api<ConfigMap> = Api::namespaced(
            self.config.kube_client.as_ref().unwrap().clone(),
            &self.config.current_namespace,
        );

        configmaps_api
            .replace(
                &self.config.state_file_configmap_name,
                &PostParams::default(),
                config_map,
            )
            .await?;

        Ok(())
    }

    pub async fn store_state(&self, state: &State) -> Result<(), Box<dyn std::error::Error>> {
        if !self.config.enable {
            return Ok(());
        }

        // retrieve configmap
        let mut config_map = self.get_state_configmap().await?;

        // marshal state to yaml
        let yaml_data = match serde_yaml::to_string(state) {
            Ok(yd) => yd,
            Err(e) => return Err(Box::new(e)),
        };

        // extract filename from config file path
        let state_file_path = Path::new(&self.config.state_file_path);
        let state_file_name = match state_file_path.file_name() {
            Some(filename) => match filename.to_str() {
                Some(filename) => String::from(filename),
                None => return Err(Box::<dyn Error>::from("No filename found in path")),
            },
            None => return Err(Box::<dyn Error>::from("No filename found in path")),
        };

        // update data in configmap
        let mut data: std::collections::BTreeMap<String, String> = match config_map.data {
            Some(d) => d,
            None => BTreeMap::new(),
        };
        data.insert(state_file_name, yaml_data);
        config_map.data = Some(data);

        // update configmap to have state available when the application runs the next time and for other applications
        self.update_state_configmap(&config_map).await?;

        info!(
            "Stored last state in configmap {}",
            &self.config.state_file_configmap_name
        );

        Ok(())
    }
}
