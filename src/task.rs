use chrono::{DateTime, FixedOffset, Utc};
use json::JsonValue;

const MAX_ATTEMPTS: u32 = 3;

#[derive(Debug)]
pub enum TaskOperation {
    Run,
    Retry,
    Fail,
}

#[derive(Clone)]
struct Schedule {
    interval: String,
}

impl Schedule {
    fn parse(sched: &JsonValue) -> Schedule {
        Schedule {
            interval: sched["interval"].to_string(),
        }
    }
}

#[derive(Clone)]
pub struct Task {
    id: String,
    pub attempts: u32,
    retry_at: Option<DateTime<FixedOffset>>,
    run_at: Option<DateTime<FixedOffset>>,
    started_at: Option<DateTime<FixedOffset>>,
    scheduled_at: Option<DateTime<FixedOffset>>,
    params: String,
    owner_id: String,
    schedule: Schedule,
    task_type: String,
    scope: String,
    traceparent: String,
    state: String,
    status: String,
}

impl Task {
    pub fn parse(hit: &JsonValue) -> Task {
        let id = &hit["_id"];
        let source = &hit["_source"]["task"];

        fn json_to_datetime(source: &json::JsonValue, key: &str) -> Option<DateTime<FixedOffset>> {
            let result = DateTime::parse_from_rfc3339(&source[key].to_string());
            match result {
                Ok(date) => Some(date),
                Err(_) => None,
            }
        }

        let mut scope: Vec<String> = vec![];
        for i in 0..source["scope"].len() {
            scope.push(source["scope"][i].to_string());
        }

        Task {
            id: id.to_string(),
            retry_at: json_to_datetime(source, "retryAt"),
            run_at: json_to_datetime(source, "runAt"),
            started_at: json_to_datetime(source, "startedAt"),
            scheduled_at: json_to_datetime(source, "scheduledAT"),
            params: source["params"].to_string(),
            owner_id: source["ownerId"].to_string(),
            schedule: Schedule::parse(&source["schedule"]),
            task_type: source["taskType"].to_string(),
            scope: source["scope"].to_string(),
            traceparent: source["traceparent"].to_string(),
            state: source["state"].to_string(),
            attempts: source["attempts"].to_string().parse::<u32>().unwrap(),
            status: source["status"].to_string(),
        }
    }

    pub fn stringify(&self) -> String {
        fn stringify_datetime_option(opt: Option<DateTime<FixedOffset>>) -> String {
            match opt {
                Some(date) => date.to_rfc3339(),
                None => "null".to_string(),
            }
        }

        json::stringify(json::object! {
            id: self.id.clone(),
            retryAt: stringify_datetime_option(self.retry_at),
            runAt: stringify_datetime_option(self.run_at),
            startedAt: stringify_datetime_option(self.started_at),
            scheduledAt: stringify_datetime_option(self.scheduled_at),
            params: json::parse(&self.params).unwrap(),
            state: json::parse(&self.state).unwrap(),
            scope: json::parse(&self.scope).unwrap(),
            ownerId: self.owner_id.clone(),
            taskType: self.task_type.clone(),
            traceparent: self.traceparent.clone(),
            attempts: self.attempts.clone(),
            status: self.status.clone(),
            schedule: json::object!{
                interval: self.schedule.interval.clone()
            }
        })
    }

    pub fn get_id(&self) -> String {
        self.id.clone()
    }

    pub fn get_owner(&self) -> Option<String> {
        if self.is_unclaimed() || self.is_failed() {
            return None;
        }
        return Some(self.owner_id.clone());
    }

    pub fn ready_to(&self) -> Option<TaskOperation> {
        if self.ready_to_run_now() {
            return Some(TaskOperation::Run);
        }
        return self.ready_to_retry_or_fail();
    }

    fn is_unclaimed(&self) -> bool {
        self.status == "idle"
    }

    fn is_failed(&self) -> bool {
        self.status == "failed"
    }

    fn has_maxxed_attempts(&self) -> bool {
        self.attempts >= MAX_ATTEMPTS
    }

    fn ready_to_run_now(&self) -> bool {
        let now = Utc::now();
        let run_at_ts = match self.run_at {
            Some(date) => date.timestamp(),
            None => 0,
        };
        self.is_unclaimed() && run_at_ts <= now.timestamp()
    }

    fn ready_to_retry_or_fail(&self) -> Option<TaskOperation> {
        if self.is_unclaimed() {
            return None;
        }
        let now = Utc::now();
        let retry_at_ts = match self.retry_at {
            Some(date) => date.timestamp(),
            None => 0,
        };
        if retry_at_ts > now.timestamp() {
            return None;
        }

        if self.has_maxxed_attempts() {
            return Some(TaskOperation::Fail);
        }

        return Some(TaskOperation::Retry);
    }
}
