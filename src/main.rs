extern crate yaml_rust;
use chrono::{DateTime, TimeZone, Utc};
use std::collections::HashMap;
use std::time::Duration;
use url::Url;

mod config;
mod kibana;
mod task;
use crate::config::{read_config, Config};
use crate::kibana::{choose_free_kibana_node, KibanaNode};
use crate::task::{Task, TaskOperation};

const TASK_MANAGER_INDEX: &str = ".kibana_task_manager/";
const SEARCH_API: &str = "_search/";
const UPDATE_API: &str = "_update/";

#[tokio::main]
async fn main() {
    let config = read_config();
    let kibana_paths = &config.kibana_hosts;
    let mut kibanas: Vec<KibanaNode> = vec![];
    for i in 0..kibana_paths.len() {
        let path = &kibana_paths[i];
        kibanas.push(KibanaNode::new(path));
    }

    loop {
        let poll_result = poll_tasks(&config).await.unwrap();
        let tasks = poll_result.0;
        let owners = poll_result.1;
        claim_tasks(&config, tasks, &kibanas, &owners).await;

        std::thread::sleep(Duration::from_millis(config.polling_interval));
    }
}

fn build_tm_url(config: &Config, api: &str) -> Result<Url, Box<dyn std::error::Error>> {
    let url = Url::parse(&config.elasticsearch_path)?
        .join(TASK_MANAGER_INDEX)?
        .join(api)?;
    return Ok(url);
}
fn get_update_url_from_id(config: &Config, id: &str) -> String {
    String::from(
        urlencoding::decode(
            &build_tm_url(config, UPDATE_API)
                .unwrap()
                .join(&urlencoding::encode(&id))
                .unwrap()
                .to_string(),
        )
        .unwrap(),
    )
}

async fn poll_tasks(
    config: &Config,
) -> Result<(Vec<(Task, TaskOperation)>, HashMap<String, isize>), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();

    let mut get_url = build_tm_url(&config, SEARCH_API).unwrap();
    get_url.set_query(Some("size=5000"));
    println!("Polling {}", get_url.to_string());

    let resp = client
        .get(get_url.to_string())
        .basic_auth(&config.username, Some(&config.password))
        .header("Content-Type", "application/json")
        .send()
        .await?
        .text()
        .await?;
    let data = json::parse(&resp).unwrap();
    let mut tasks: Vec<(Task, TaskOperation)> = Vec::new();
    let mut owners: HashMap<String, isize> = HashMap::new();

    for i in 0..data["hits"]["hits"].len() {
        let hit = &data["hits"]["hits"][i];
        let task = Task::parse(hit);

        match task.get_owner() {
            Some(owner) => match owners.get(&owner) {
                Some(&prev_capacity) => {
                    owners.insert(owner.to_string(), prev_capacity + 1);
                }
                None => {
                    owners.insert(owner.to_string(), 0);
                }
            },
            None => {}
        }

        match task.ready_to() {
            Some(operation) => tasks.push((task, operation)),
            None => {}
        }
    }

    return Ok((tasks, owners));
}

async fn claim_tasks(
    config: &Config,
    tasks: Vec<(Task, TaskOperation)>,
    kibanas: &Vec<KibanaNode>,
    owners: &HashMap<String, isize>,
) {
    let client = reqwest::Client::new();
    let mut current_owners = owners.clone();

    for i in 0..tasks.len() {
        let kibana_node;
        match choose_free_kibana_node(&kibanas, &current_owners) {
            Some(node) => kibana_node = node,
            None => continue,
        }

        let task = tasks[i].0.clone();
        let operation = &tasks[i].1;
        let id = String::from(&task.get_id());
        let now: DateTime<Utc> = Utc::now();

        let mut new_attempts = task.attempts;
        match operation {
            TaskOperation::Run => {}
            TaskOperation::Retry => new_attempts += 1,
            TaskOperation::Fail => {
                fail_task(config, id);
                continue;
            }
        }
        let claim_body = json::stringify(json::object! {
            doc: {
                task: {
                    scheduledAt: now.to_rfc3339(),
                    retryAt: Utc.timestamp_millis(now.timestamp_millis() + 30000).to_rfc3339(),
                    attempts: new_attempts,
                    status: "claiming",
                    ownerId: kibana_node.id.to_string()
                }
            }
        });
        let url = get_update_url_from_id(config, &id);

        let claim_request = client
            .post(&url)
            .basic_auth(&config.indices_username, Some(&config.password))
            .header("Content-Type", "application/json")
            .body(claim_body);
        let run_request = client
            .post(&url)
            .basic_auth(&config.indices_username, Some(&config.password))
            .header("Content-Type", "application/json");
        let kbn_request = kibana_node.assign(task);
        match current_owners.get(&kibana_node.id) {
            Some(&owned) => current_owners.insert(kibana_node.id.to_string(), owned + 1),
            None => current_owners.insert(kibana_node.id.to_string(), 1),
        };
        println!(
            "{} capacity is now {}",
            kibana_node.path,
            config.kibana_capacity - current_owners.get(&kibana_node.id).unwrap()
        );

        tokio::spawn(async move {
            match claim_request.send().await {
                Ok(_) => {}
                Err(_) => panic!("Failed to claim task {}", &id),
            }
            println!("Claimed {}", &id);
            let run_body = json::object! {
                doc: {
                    task: {
                        startedAt:  Utc::now().to_rfc3339(),
                        status: "running"
                    }
                }
            };
            kbn_request();
            let run_resp = run_request
                .body(json::stringify(run_body))
                .send()
                .await
                .unwrap()
                .text()
                .await;
            match run_resp {
                Ok(_) => {
                    println!("Ran {}", &id)
                }
                Err(e) => {
                    panic!("Failed to run {}, {}", &id, e)
                }
            }
        });
    }
}

fn fail_task(config: &Config, id: String) {
    let client = reqwest::Client::new();
    let url = get_update_url_from_id(config, &id);
    let fail_request = client
        .post(&url)
        .basic_auth(&config.indices_username, Some(&config.password))
        .header("Content-Type", "application/json")
        .body(json::stringify(json::object! {
            doc: {
                task: {
                    status: "failed"
                }
            }
        }));
    tokio::spawn(async move {
        match fail_request.send().await {
            Ok(_) => {}
            Err(e) => panic!("Failed to set task {} as failed: {:?}", &id, e),
        }
    });
}
