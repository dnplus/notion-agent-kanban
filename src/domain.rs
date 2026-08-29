use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    #[default]
    Backlog,
    Triage,
    Scheduled,
    Ready,
    Running,
    Review,
    Blocked,
    Done,
    Cancel,
    Archived,
}

impl TaskStatus {
    pub fn is_dispatchable(self, now: DateTime<Utc>, scheduled_at: Option<DateTime<Utc>>) -> bool {
        match self {
            Self::Triage | Self::Ready => true,
            Self::Scheduled => scheduled_at.is_some_and(|at| at <= now),
            _ => false,
        }
    }

    pub fn is_human_status(self) -> bool {
        matches!(
            self,
            Self::Backlog
                | Self::Triage
                | Self::Scheduled
                | Self::Ready
                | Self::Cancel
                | Self::Archived
        )
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Backlog => "backlog",
            Self::Triage => "triage",
            Self::Scheduled => "scheduled",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Review => "review",
            Self::Blocked => "blocked",
            Self::Done => "done",
            Self::Cancel => "cancel",
            Self::Archived => "archived",
        };
        f.write_str(value)
    }
}

impl FromStr for TaskStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "backlog" => Ok(Self::Backlog),
            "triage" => Ok(Self::Triage),
            "scheduled" => Ok(Self::Scheduled),
            "ready" => Ok(Self::Ready),
            "running" => Ok(Self::Running),
            "review" => Ok(Self::Review),
            "blocked" => Ok(Self::Blocked),
            "done" => Ok(Self::Done),
            "cancel" | "cancelled" | "canceled" => Ok(Self::Cancel),
            "archived" => Ok(Self::Archived),
            other => Err(format!("unknown task status: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionMode {
    Triage,
    Execute,
}

impl fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Triage => "triage",
            Self::Execute => "execute",
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct Task {
    pub id: String,
    pub name: String,
    pub status: TaskStatus,
    pub project_id: Option<String>,
    pub agent: Option<String>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub due: Option<DateTime<Utc>>,
    pub execution_id: Option<String>,
    pub result: Option<String>,
    pub body: Option<String>,
    pub last_edited_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub path: Option<String>,
    pub default_agent: Option<String>,
    pub active: bool,
    pub last_activity: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkContract {
    pub task_id: String,
    pub execution_id: String,
    pub mode: ExecutionMode,
    pub title: String,
    pub body: String,
    pub project_name: String,
    pub project_path: String,
    pub due: Option<DateTime<Utc>>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub agent_kind: String,
    pub report_command: String,
}

impl WorkContract {
    pub fn prompt(&self) -> String {
        let report = match self.mode {
            ExecutionMode::Triage => {
                "完成需求整理後，必須執行 kbctl report review --summary \"...\" 或 kbctl report blocked --reason \"...\"。"
            }
            ExecutionMode::Execute => {
                "完成工作後，必須執行 kbctl report done --summary \"...\"；無法完成時使用 kbctl report blocked --reason \"...\"；需要人工檢視時使用 kbctl report review --summary \"...\"。"
            }
        };
        format!(
            "你正在處理 kbctl work contract。\nTask ID: {}\nExecution ID: {}\nMode: {}\nTitle: {}\nProject: {}\nWorking directory: {}\nDue: {}\nScheduled at: {}\n\n需求正文：\n{}\n\n{}\n不要自行把 Notion Task 標成 done；business status 由 kbctl 驗證後寫回。",
            self.task_id,
            self.execution_id,
            self.mode,
            self.title,
            self.project_name,
            self.project_path,
            self.due
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "none".to_string()),
            self.scheduled_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "none".to_string()),
            self.body,
            report,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub id: String,
    pub task_id: String,
    pub agent_kind: String,
    pub mode: ExecutionMode,
    pub started_at: DateTime<Utc>,
    pub runtime_id: Option<String>,
    #[serde(default = "default_execution_attempt")]
    pub attempt: u32,
    #[serde(default)]
    pub retry_at: Option<DateTime<Utc>>,
}

impl Execution {
    pub fn new(
        task_id: impl Into<String>,
        agent_kind: impl Into<String>,
        mode: ExecutionMode,
    ) -> Self {
        Self::new_with_attempt(task_id, agent_kind, mode, 1)
    }

    pub fn new_with_attempt(
        task_id: impl Into<String>,
        agent_kind: impl Into<String>,
        mode: ExecutionMode,
        attempt: u32,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            task_id: task_id.into(),
            agent_kind: agent_kind.into(),
            mode,
            started_at: Utc::now(),
            runtime_id: None,
            attempt: attempt.max(1),
            retry_at: None,
        }
    }
}

fn default_execution_attempt() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub execution_id: String,
    pub status: TaskStatus,
    pub summary: Option<String>,
    pub reason: Option<String>,
    pub result_file: Option<String>,
    pub reported_at: DateTime<Utc>,
}

impl Report {
    pub fn validate(&self, mode: ExecutionMode) -> Result<(), String> {
        match self.status {
            TaskStatus::Done if mode == ExecutionMode::Triage => {
                Err("triage execution must report review or blocked".to_string())
            }
            TaskStatus::Done | TaskStatus::Review => {
                if self
                    .summary
                    .as_deref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    Err("done/review report requires a non-empty summary".to_string())
                } else {
                    Ok(())
                }
            }
            TaskStatus::Blocked => {
                if self
                    .reason
                    .as_deref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    Err("blocked report requires a non-empty reason".to_string())
                } else {
                    Ok(())
                }
            }
            _ => Err("reports may only use done, blocked, or review".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SchemaSnapshot {
    pub database_id: String,
    pub data_source_id: Option<String>,
    pub properties: serde_json::Value,
    pub fingerprint: String,
}
