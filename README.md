# elastask - a task manager daemon written in Rust

## Overview

This service is meant to replace Kibana's "pull" model of task scheduling, replacing it with a "push" model. It implements most of the high-level implementation details described in the [task manager readme](https://github.com/elastic/kibana/tree/main/x-pack/plugins/task_manager#implementation-details), with some tweaks:

- Every `{poll_interval}` milliseconds, check the `{index}` for any tasks that need to be run:
  - `runAt` is past
  - `attempts` is less than the configured threshold
- Attempt to claim the task by using optimistic concurrency to set:
  - status to `running`
  - `startedAt` to now
  - `retryAt` to next time task should retry if it times out and is still in `running` status
- Determine which Kibana nodes have capacity to accept new tasks:
  - Count how many tasks are still `running` and have not passed their `retryAt` time
  - Group them by `ownerId`, and only assign a new task to a Kibana `ownerId` with less than 10 `running` tasks
- Push the task to Kibana using a `_run_now` API
- If the task passes its `retryAt` without switching back to `idle`, increment the `attempts` count and reschedule it

When Kibana receives a task through `_run_now`:

- Execute the task through the `ephemeralRunNow` function
- If the task succeeds:
  - If it is recurring, store the result of the run in the index, and reschedule
  - If it is not recurring, remove it from the index

This creates a cyclical pattern of:

```
    ↱ →  Elasticsearch  → ↴
    ↑                     ↓
 Kibana   ←   ←   ←    Elastask
```

## Usage

### Setting up Kibana

- Check out or cherry-pick `7bf01ac` from this [Kibana feature branch](https://github.com/Zacqary/kibana/tree/external-task-manager).
- In `kibana.dev.yml`, set:
  ```yaml
  xpack.task_manager.ephemeral_tasks.enabled: true
  xpack.task_manager.unsafe.disable_task_claiming: true
  ```
- Run as many Kibana nodes from this branch as you want to test with

### Setting up Elastask

- Install Rustup from [rustup.rs](https://rustup.rs/)
- In this repo, run `cargo build`
- Configure `elastask.yaml` as needed. By default, it will connect to a single kibana running at `localhost:5601`, so be sure to add as many nodes as you'll be spinning up

### Running Elastask

- Start Elasticsearch and all your Kibana instances
- Run `cargo run`
