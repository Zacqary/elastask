use std::fs;
use yaml_rust::{Yaml, YamlLoader};

const DEFAULT_CONFIG_PATH: &str = "elastask.yaml";

const DEFAULT_USERNAME: &str = "elastic";
const DEFAULT_INDICES_USERNAME: &str = "system_indices_superuser";
const DEFAULT_PASSWORD: &str = "changeme";
const DEFAULT_ELASTICSEARCH_PATH: &str = "http://localhost:9200";

const DEFAULT_KIBANA_CAPACITY: isize = 10;
const DEFAULT_POLLING_INTERVAL: u64 = 3000;

const DEFAULT_KIBANA_HOST: &str = "http://localhost:5601";

pub struct Config {
    pub elasticsearch_path: String,
    pub username: String,
    pub indices_username: String,
    pub password: String,
    pub kibana_capacity: isize,
    pub polling_interval: u64,
    pub kibana_hosts: Vec<String>,
}

pub fn read_config() -> Config {
    let path = DEFAULT_CONFIG_PATH;

    let file_contents = fs::read_to_string(path).expect("Unable to read config file");

    let loaded_config =
        YamlLoader::load_from_str(&file_contents).expect("Unable to parse YAML file");

    if loaded_config.len() == 0 {
        return Config {
            elasticsearch_path: DEFAULT_ELASTICSEARCH_PATH.to_string(),
            username: DEFAULT_USERNAME.to_string(),
            indices_username: DEFAULT_INDICES_USERNAME.to_string(),
            password: DEFAULT_PASSWORD.to_string(),
            kibana_capacity: DEFAULT_KIBANA_CAPACITY,
            polling_interval: DEFAULT_POLLING_INTERVAL,
            kibana_hosts: vec![DEFAULT_KIBANA_HOST.to_string()],
        };
    }

    let config = &loaded_config[0];

    Config {
        elasticsearch_path: match &config["elasticsearch.host"] {
            Yaml::String(v) => v.to_string(),
            _ => DEFAULT_ELASTICSEARCH_PATH.to_string(),
        },
        username: match &config["elasticsearch.username"] {
            Yaml::String(v) => v.to_string(),
            _ => DEFAULT_USERNAME.to_string(),
        },
        indices_username: match &config["elasticsearch.indices_username"] {
            Yaml::String(v) => v.to_string(),
            _ => DEFAULT_INDICES_USERNAME.to_string(),
        },
        password: match &config["elasticsearch.password"] {
            Yaml::String(v) => v.to_string(),
            _ => DEFAULT_PASSWORD.to_string(),
        },
        kibana_capacity: match &config["kibana_capacity"] {
            Yaml::Integer(v) => *v as isize,
            _ => DEFAULT_KIBANA_CAPACITY,
        },
        polling_interval: match &config["polling_interval"] {
            Yaml::Integer(v) => *v as u64,
            _ => DEFAULT_POLLING_INTERVAL,
        },
        kibana_hosts: match &config["kibana.hosts"] {
            Yaml::Array(v) => {
                let mut result: Vec<String> = vec![];
                for item in v {
                    result.push(item.as_str().unwrap().to_string());
                }
                result
            }
            _ => vec![DEFAULT_KIBANA_HOST.to_string()],
        },
    }
}
