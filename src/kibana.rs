use uuid::Uuid;
use url::{Url};
use std::collections::HashMap;

use crate::task::{Task};
use crate::config::{read_config};

const KIBANA_RUN_NOW_API: &str = "api/task_manager/_run_now";

#[derive(Clone)]
pub struct KibanaNode {
  pub id: String,
  pub path: String,
}
impl KibanaNode {
  pub fn new(path: &str) -> KibanaNode {
      KibanaNode {
          path: path.to_string(),
          id: Uuid::new_v4().to_string(),
      }
  }

  pub fn assign(&self, task: Task) -> impl FnOnce() {
      run_kibana_task(self.path.to_string(), task)
  }

}

fn run_kibana_task(node_path: String, task: Task) -> impl FnOnce() {
  let client = reqwest::Client::new();
  let config = read_config();

  let url = String::from(urlencoding::decode(
      &Url::parse(&node_path).unwrap().join(KIBANA_RUN_NOW_API).unwrap().to_string()
  ).unwrap());
  let body = task.stringify();
  let task_id = task.get_id();
  let kbn_request = client.post(&url)
      .basic_auth(config.username, Some(config.password))
      .header("Content-Type", "application/json")
      .header("kbn-xsrf", "aaaaa")
      .body(body);
  move || {
    tokio::spawn(async move {
        let res = kbn_request.send().await;
        match res{
            Ok(_) => { println!("Sent {} to Kibana node {}", task.get_id(), &node_path) },
            Err(e) => { println!("Error sending {} to Kibana: {}", task_id, e) }
        }
    });
  }
}



pub fn choose_free_kibana_node(kibanas: &Vec<KibanaNode>, owners: &HashMap<String, isize>) -> Option<KibanaNode> {
    let config = read_config();

    let mut highest_capacity_node_index: Option<usize> = None;
    let mut highest_found_capacity: isize = 0;
    for i in 0..kibanas.len() {
        let k = &kibanas[i];
        let id = k.id.clone();
        let capacity: isize = || -> isize {
            match owners.get(&id) {
                Some(owned) => config.kibana_capacity - *owned as isize,
                None => config.kibana_capacity
            }
        }();
        if capacity > highest_found_capacity {
            highest_found_capacity = capacity;
            highest_capacity_node_index = Some(i);
        }
    }

    match highest_capacity_node_index {
        Some(i) => Some(kibanas[i].clone()),
        None => None
    }
}