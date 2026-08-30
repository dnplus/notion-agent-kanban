use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    str::FromStr,
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionRole {
    #[default]
    Standalone,
    Supervisor,
    Worker,
    Reviewer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkMode {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PlanState {
    #[default]
    Planning,
    Executing,
    AwaitingApproval,
    Reviewing,
    AwaitingMerge,
    Blocked,
    Done,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkItemState {
    #[default]
    Pending,
    Running,
    Submitted,
    Reviewing,
    Rework,
    Accepted,
    Integrating,
    Merged,
    Blocked,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanStep {
    pub id: String,
    pub title: String,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub profile: String,
    pub risk: RiskLevel,
    pub mode: WorkMode,
    #[serde(default)]
    pub write_scope: Vec<String>,
    #[serde(default)]
    pub acceptance: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanDag {
    pub parent_task_id: String,
    pub version: u32,
    pub summary: String,
    pub steps: Vec<PlanStep>,
}

impl PlanDag {
    pub fn validate(&self, max_steps: usize) -> Result<(), String> {
        if self.parent_task_id.trim().is_empty() {
            return Err("plan parent_task_id is required".to_string());
        }
        if self.version == 0 {
            return Err("plan version must be greater than zero".to_string());
        }
        if self.summary.trim().is_empty() {
            return Err("plan summary is required".to_string());
        }
        if self.steps.is_empty() {
            return Err("plan requires at least one step".to_string());
        }
        if self.steps.len() > max_steps.max(1) {
            return Err(format!("plan has more than {} steps", max_steps.max(1)));
        }
        let mut ids = BTreeSet::new();
        for step in &self.steps {
            if step.id.trim().is_empty() || !ids.insert(step.id.as_str()) {
                return Err(format!("plan step id is empty or duplicated: {}", step.id));
            }
            if step.title.trim().is_empty() || step.objective.trim().is_empty() {
                return Err(format!(
                    "plan step {} requires title and objective",
                    step.id
                ));
            }
            if step.profile.trim().is_empty() {
                return Err(format!("plan step {} requires a profile", step.id));
            }
            if step.mode == WorkMode::Read && !step.write_scope.is_empty() {
                return Err(format!(
                    "read-only step {} cannot declare write_scope",
                    step.id
                ));
            }
            if step.mode == WorkMode::Write && step.write_scope.is_empty() {
                return Err(format!("write step {} requires write_scope", step.id));
            }
            if step.acceptance.iter().any(|value| value.trim().is_empty()) {
                return Err(format!(
                    "plan step {} has an empty acceptance item",
                    step.id
                ));
            }
        }
        let steps = self
            .steps
            .iter()
            .map(|step| (step.id.as_str(), step))
            .collect::<HashMap<_, _>>();
        for step in &self.steps {
            for dependency in &step.depends_on {
                if dependency == &step.id || !steps.contains_key(dependency.as_str()) {
                    return Err(format!(
                        "plan step {} has invalid dependency {}",
                        step.id, dependency
                    ));
                }
            }
        }
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        for step in &self.steps {
            visit_step(step.id.as_str(), &steps, &mut visiting, &mut visited)?;
        }
        Ok(())
    }
}

fn visit_step<'a>(
    id: &'a str,
    steps: &HashMap<&'a str, &'a PlanStep>,
    visiting: &mut BTreeSet<&'a str>,
    visited: &mut BTreeSet<&'a str>,
) -> Result<(), String> {
    if visited.contains(id) {
        return Ok(());
    }
    if !visiting.insert(id) {
        return Err(format!("plan contains a dependency cycle at {id}"));
    }
    for dependency in &steps[id].depends_on {
        visit_step(dependency, steps, visiting, visited)?;
    }
    visiting.remove(id);
    visited.insert(id);
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrchestrationRun {
    pub parent_task_id: String,
    pub plan_version: u32,
    pub state: PlanState,
    pub supervisor_execution_id: Option<String>,
    pub approved_plan_version: Option<u32>,
    pub base_commit: Option<String>,
    pub base_branch: Option<String>,
    pub integration_branch: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkItem {
    pub id: String,
    pub parent_task_id: String,
    pub plan_version: u32,
    pub step: PlanStep,
    pub state: WorkItemState,
    pub attempt: u32,
    pub execution_id: Option<String>,
    pub branch: Option<String>,
    pub checkout_path: Option<String>,
    pub summary: Option<String>,
    #[serde(default)]
    pub head_commit: Option<String>,
    #[serde(default)]
    pub review_round: u32,
}

impl WorkItem {
    pub fn from_step(parent_task_id: &str, plan_version: u32, step: PlanStep) -> Self {
        Self {
            id: format!("{}:{}:{}", parent_task_id, plan_version, step.id),
            parent_task_id: parent_task_id.to_string(),
            plan_version,
            step,
            state: WorkItemState::Pending,
            attempt: 0,
            execution_id: None,
            branch: None,
            checkout_path: None,
            summary: None,
            head_commit: None,
            review_round: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompletionEnvelope {
    pub work_item_id: String,
    pub summary: String,
    pub head_commit: Option<String>,
    #[serde(default)]
    pub artifacts: Vec<String>,
    #[serde(default)]
    pub known_issues: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReviewDecisionKind {
    Accept,
    Rework,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReviewFinding {
    pub severity: String,
    pub problem: String,
    pub required_change: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReviewDecision {
    pub target_id: String,
    pub decision: ReviewDecisionKind,
    pub summary: String,
    #[serde(default)]
    pub review_round: u32,
    #[serde(default)]
    pub findings: Vec<ReviewFinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubmissionEnvelope {
    Plan { plan: PlanDag },
    Completion { completion: CompletionEnvelope },
    Review { review: ReviewDecision },
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
    pub profile_name: String,
    pub role: ExecutionRole,
    pub model: Option<String>,
    pub reasoning: Option<String>,
    pub agent: Option<String>,
    pub read_only: bool,
    pub plan_version: Option<u32>,
    pub work_item_id: Option<String>,
    pub submission_path: String,
    pub report_command: String,
}

impl WorkContract {
    pub fn prompt(&self) -> String {
        let report = match self.role {
            ExecutionRole::Supervisor => {
                "這是新一輪 triage，不是 review。無論正文是否包含舊 execution、舊回報或歷史結論，本輪只能回傳 Plan envelope，禁止回傳 Review envelope。若需求不足，Plan 仍須建立一個 low-risk read step，用來整理缺漏與產生可供人工補充的問題。Plan 格式為 {\"type\":\"plan\",\"plan\":{\"parent_task_id\":\"...\",\"version\":1,\"summary\":\"...\",\"steps\":[{\"id\":\"step-1\",\"title\":\"...\",\"objective\":\"...\",\"depends_on\":[],\"profile\":\"fast_worker\",\"risk\":\"low\",\"mode\":\"read\",\"write_scope\":[],\"acceptance\":[\"...\"]}]}}。不要直接啟動 Worker、修改專案或改 Notion 狀態。"
            }
            ExecutionRole::Reviewer => {
                "這是明確指定 target 的 review 回合，本輪只能回傳 Review envelope，禁止回傳 Plan envelope。Review 格式為 {\"type\":\"review\",\"review\":{\"target_id\":\"...\",\"decision\":\"accept\",\"summary\":\"...\",\"review_round\":1,\"findings\":[]}}。不要直接啟動 Worker、修改專案或改 Notion 狀態。"
            }
            ExecutionRole::Worker => {
                "完成工作後回傳 Completion envelope，格式為 {\"type\":\"completion\",\"completion\":{\"work_item_id\":\"...\",\"summary\":\"...\",\"head_commit\":\"...或null\",\"artifacts\":[],\"known_issues\":[]}}。寫入工作必須先 commit。不要自行合併或修改 Notion 狀態。"
            }
            ExecutionRole::Standalone => match self.mode {
                ExecutionMode::Triage => {
                    "完成需求整理後，必須執行 kbctl report review --summary \"...\" 或 kbctl report blocked --reason \"...\"。"
                }
                ExecutionMode::Execute => {
                    "完成工作後，必須執行 kbctl report done --summary \"...\"；無法完成時使用 kbctl report blocked --reason \"...\"；需要人工檢視時使用 kbctl report review --summary \"...\"。"
                }
            },
        };
        format!(
            "你正在處理 kbctl work contract。\nTask ID: {}\nExecution ID: {}\nRole: {:?}\nPlan version: {}\nWork item: {}\nMode: {}\nTitle: {}\nProject: {}\nWorking directory: {}\nDue: {}\nScheduled at: {}\n\n需求正文：\n{}\n\n{}\n{}\n不要自行把 Notion Task 標成 done；business status 由 kbctl 驗證後寫回。",
            self.task_id,
            self.execution_id,
            self.role,
            self.plan_version
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
            self.work_item_id.as_deref().unwrap_or("none"),
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
            if self.role == ExecutionRole::Standalone {
                String::new()
            } else {
                crate::orchestration::runtime_envelope_instruction()
            },
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub id: String,
    pub task_id: String,
    pub agent_kind: String,
    pub mode: ExecutionMode,
    #[serde(default)]
    pub role: ExecutionRole,
    #[serde(default)]
    pub parent_task_id: Option<String>,
    #[serde(default)]
    pub work_item_id: Option<String>,
    #[serde(default)]
    pub plan_version: Option<u32>,
    #[serde(default)]
    pub checkout_path: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub submission_path: Option<String>,
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
            role: ExecutionRole::Standalone,
            parent_task_id: None,
            work_item_id: None,
            plan_version: None,
            checkout_path: None,
            branch: None,
            submission_path: None,
            started_at: Utc::now(),
            runtime_id: None,
            attempt: attempt.max(1),
            retry_at: None,
        }
    }
}

#[cfg(test)]
mod orchestration_tests {
    use super::*;

    fn step(id: &str, dependencies: &[&str]) -> PlanStep {
        PlanStep {
            id: id.to_string(),
            title: id.to_string(),
            objective: format!("do {id}"),
            depends_on: dependencies.iter().map(|value| value.to_string()).collect(),
            profile: "fast_worker".to_string(),
            risk: RiskLevel::Low,
            mode: WorkMode::Read,
            write_scope: Vec::new(),
            acceptance: vec!["result is explained".to_string()],
        }
    }

    #[test]
    fn plan_validation_accepts_a_dag() {
        let plan = PlanDag {
            parent_task_id: "parent".to_string(),
            version: 1,
            summary: "plan".to_string(),
            steps: vec![step("a", &[]), step("b", &["a"])],
        };
        assert_eq!(plan.validate(8), Ok(()));
    }

    #[test]
    fn plan_validation_rejects_cycles() {
        let plan = PlanDag {
            parent_task_id: "parent".to_string(),
            version: 1,
            summary: "plan".to_string(),
            steps: vec![step("a", &["b"]), step("b", &["a"])],
        };
        assert!(plan.validate(8).unwrap_err().contains("cycle"));
    }

    #[test]
    fn write_steps_require_scope() {
        let mut item = step("a", &[]);
        item.mode = WorkMode::Write;
        let plan = PlanDag {
            parent_task_id: "parent".to_string(),
            version: 1,
            summary: "plan".to_string(),
            steps: vec![item],
        };
        assert!(plan.validate(8).unwrap_err().contains("write_scope"));
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
