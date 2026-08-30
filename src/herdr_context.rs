use crate::{
    domain::{Execution, Task},
    error::KbctlError,
    herdr::RuntimeExecution,
    store::Store,
};
use serde_json::Value;
use std::env;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HerdrContext {
    pub workspace_id: Option<String>,
    pub tab_id: Option<String>,
    pub focused_pane_id: Option<String>,
    pub focused_pane_cwd: Option<String>,
    pub focused_pane_agent: Option<String>,
    pub task_id: Option<String>,
    pub execution_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedContext {
    pub task: Task,
    pub execution: Option<Execution>,
    pub runtime: Option<RuntimeExecution>,
}

impl HerdrContext {
    pub fn from_env() -> Result<Self, KbctlError> {
        let value = match env::var("HERDR_PLUGIN_CONTEXT_JSON") {
            Ok(raw) if raw.trim().is_empty() => None,
            Ok(raw) => Some(parse_plugin_context(&raw)?),
            Err(env::VarError::NotPresent) => None,
            Err(error) => {
                return Err(KbctlError::Runtime(format!(
                    "read HERDR_PLUGIN_CONTEXT_JSON: {error}"
                )));
            }
        };
        Ok(Self {
            workspace_id: first_value(
                value.as_ref(),
                &[&["workspace_id"], &["workspace", "workspace_id"]],
            )
            .or_else(|| env_value(&["HERDR_WORKSPACE_ID"])),
            tab_id: first_value(value.as_ref(), &[&["tab_id"], &["tab", "tab_id"]])
                .or_else(|| env_value(&["HERDR_TAB_ID", "HERDR_ACTIVE_TAB_ID"])),
            focused_pane_id: first_value(
                value.as_ref(),
                &[
                    &["focused_pane_id"],
                    &["focused_pane", "pane_id"],
                    &["pane_id"],
                ],
            )
            .or_else(|| {
                env_value(&[
                    "HERDR_PANE_ID",
                    "HERDR_ACTIVE_PANE_ID",
                    "HERDR_PLUGIN_PANE_ID",
                ])
            }),
            focused_pane_cwd: first_value(
                value.as_ref(),
                &[
                    &["focused_pane_cwd"],
                    &["focused_pane", "cwd"],
                    &["focused_pane", "foreground_cwd"],
                ],
            )
            .or_else(|| env_value(&["HERDR_ACTIVE_PANE_CWD", "HERDR_PANE_CWD"])),
            focused_pane_agent: first_value(
                value.as_ref(),
                &[
                    &["focused_pane_agent"],
                    &["focused_pane", "agent"],
                    &["focused_pane", "agent", "name"],
                ],
            )
            .or_else(|| env_value(&["HERDR_AGENT_NAME", "HERDR_PANE_AGENT"])),
            task_id: first_value(value.as_ref(), &[&["task_id"], &["kbctl_task_id"]])
                .or_else(|| env_value(&["KBCTL_CONTEXT_TASK_ID"])),
            execution_id: first_value(
                value.as_ref(),
                &[&["execution_id"], &["kbctl_execution_id"]],
            )
            .or_else(|| env_value(&["KBCTL_CONTEXT_EXECUTION_ID"])),
        })
    }

    pub fn pane_id(&self) -> Option<&str> {
        self.focused_pane_id.as_deref()
    }

    pub fn matches_runtime(&self, runtime: &RuntimeExecution) -> bool {
        let pane_matches = self.focused_pane_id.as_deref().is_none_or(|pane| {
            pane == runtime.pane_id || runtime.board_pane_id.as_deref() == Some(pane)
        });
        let agent_matches = self
            .focused_pane_agent
            .as_deref()
            .is_none_or(|agent| agent == runtime.agent_name);
        let workspace_matches = self
            .workspace_id
            .as_deref()
            .is_none_or(|workspace| workspace == runtime.workspace_id);
        let tab_matches = self
            .tab_id
            .as_deref()
            .is_none_or(|tab| tab == runtime.tab_id);
        let has_locator = self.focused_pane_id.is_some() || self.focused_pane_agent.is_some();
        let has_scope = self.workspace_id.is_some() || self.tab_id.is_some();
        (has_locator || has_scope)
            && pane_matches
            && agent_matches
            && workspace_matches
            && tab_matches
    }

    pub fn resolve(&self, store: &Store) -> Result<ResolvedContext, KbctlError> {
        self.try_resolve(store)?.ok_or_else(|| {
            KbctlError::Validation(
                "Herdr context is not associated with a cached kbctl task".to_string(),
            )
        })
    }

    pub fn try_resolve(&self, store: &Store) -> Result<Option<ResolvedContext>, KbctlError> {
        if self.execution_id.is_none()
            && self.task_id.is_none()
            && self.focused_pane_id.is_none()
            && self.focused_pane_agent.is_none()
            && self.workspace_id.is_none()
            && self.tab_id.is_none()
        {
            return Ok(None);
        }
        let tasks = store.cached_tasks()?;
        if let Some(execution_id) = self.execution_id.as_deref() {
            let execution = store.execution(execution_id)?.ok_or_else(|| {
                KbctlError::Validation(format!("execution {execution_id} is not in local state"))
            })?;
            return resolved_from_execution(&tasks, execution).map(Some);
        }
        if let Some(task_id) = self.task_id.as_deref() {
            let task = find_task(&tasks, task_id)?;
            let execution = match task.execution_id.as_deref() {
                Some(execution_id) => store
                    .execution(execution_id)?
                    .or(store.execution_for_task(&task.id)?),
                None => store.execution_for_task(&task.id)?,
            };
            let runtime = runtime_from_execution(execution.as_ref())?;
            return Ok(Some(ResolvedContext {
                task,
                execution,
                runtime,
            }));
        }
        let mut matches = Vec::new();
        for execution in store.running_executions()? {
            let Some(runtime_id) = execution.runtime_id.as_deref() else {
                continue;
            };
            let runtime = match serde_json::from_str::<RuntimeExecution>(runtime_id) {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::warn!(
                        execution_id = %execution.id,
                        error = %error,
                        "ignore corrupt Herdr runtime while resolving action context"
                    );
                    continue;
                }
            };
            if self.matches_runtime(&runtime) {
                matches.push((execution, runtime));
            }
        }
        match matches.len() {
            0 => Ok(None),
            1 => {
                let (execution, runtime) = matches.remove(0);
                resolved_from_runtime(&tasks, execution, runtime).map(Some)
            }
            _ => Err(KbctlError::Validation(format!(
                "Herdr context matches multiple active kbctl executions: {}",
                matches
                    .iter()
                    .map(|(execution, _)| execution.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }
}

fn parse_plugin_context(raw: &str) -> Result<Value, KbctlError> {
    serde_json::from_str(raw)
        .map_err(|error| KbctlError::Runtime(format!("parse HERDR_PLUGIN_CONTEXT_JSON: {error}")))
}

fn resolved_from_execution(
    tasks: &[Task],
    execution: Execution,
) -> Result<ResolvedContext, KbctlError> {
    let task = find_task(tasks, &execution.task_id)?;
    let runtime = runtime_from_execution(Some(&execution))?;
    Ok(ResolvedContext {
        task,
        execution: Some(execution),
        runtime,
    })
}

fn runtime_from_execution(
    execution: Option<&Execution>,
) -> Result<Option<RuntimeExecution>, KbctlError> {
    execution
        .and_then(|execution| execution.runtime_id.as_deref())
        .map(|runtime_id| {
            serde_json::from_str(runtime_id).map_err(|error| {
                KbctlError::State(format!("invalid cached Herdr runtime id: {error}"))
            })
        })
        .transpose()
}

fn resolved_from_runtime(
    tasks: &[Task],
    execution: Execution,
    runtime: RuntimeExecution,
) -> Result<ResolvedContext, KbctlError> {
    let task = find_task(tasks, &execution.task_id)?;
    Ok(ResolvedContext {
        task,
        execution: Some(execution),
        runtime: Some(runtime),
    })
}

fn find_task(tasks: &[Task], task_id: &str) -> Result<Task, KbctlError> {
    tasks
        .iter()
        .find(|task| task.id == task_id)
        .cloned()
        .ok_or_else(|| KbctlError::Validation(format!("task {task_id} is not in local cache")))
}

fn first_value(value: Option<&Value>, paths: &[&[&str]]) -> Option<String> {
    let value = value?;
    paths.iter().find_map(|path| nested_string(value, path))
}

fn nested_string(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().map(ToOwned::to_owned).or_else(|| {
        current
            .get("name")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    })
}

fn env_value(names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| env::var(name).ok().filter(|value| !value.trim().is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{ExecutionMode, TaskStatus},
        herdr::RuntimeExecution,
    };
    use chrono::Utc;

    fn runtime() -> RuntimeExecution {
        RuntimeExecution {
            workspace_id: "w1".to_string(),
            tab_id: "w1:t1".to_string(),
            pane_id: "w1:p1".to_string(),
            board_pane_id: Some("w1:p2".to_string()),
            agent_name: "codex-fix-abc".to_string(),
            agent_kind: "codex".to_string(),
        }
    }

    #[test]
    fn parses_nested_plugin_context() {
        let value = serde_json::json!({
            "workspace_id": "w1",
            "tab": {"tab_id": "w1:t1"},
            "focused_pane": {
                "pane_id": "w1:p1",
                "cwd": "/tmp/project",
                "agent": {"name": "codex-fix-abc"}
            }
        });
        assert_eq!(
            first_value(Some(&value), &[&["focused_pane", "pane_id"]]),
            Some("w1:p1".to_string())
        );
        assert_eq!(
            first_value(Some(&value), &[&["focused_pane", "agent"]]),
            Some("codex-fix-abc".to_string())
        );
    }

    #[test]
    fn runtime_matches_focused_pane_and_scope() {
        let context = HerdrContext {
            workspace_id: Some("w1".to_string()),
            tab_id: Some("w1:t1".to_string()),
            focused_pane_id: Some("w1:p1".to_string()),
            ..Default::default()
        };
        assert!(context.matches_runtime(&runtime()));
    }

    #[test]
    fn runtime_matches_its_board_pane() {
        let context = HerdrContext {
            focused_pane_id: Some("w1:p2".to_string()),
            ..Default::default()
        };
        assert!(context.matches_runtime(&runtime()));
    }

    #[test]
    fn runtime_does_not_match_a_different_tab() {
        let context = HerdrContext {
            workspace_id: Some("w1".to_string()),
            tab_id: Some("w1:t2".to_string()),
            focused_pane_id: Some("w1:p1".to_string()),
            ..Default::default()
        };
        assert!(!context.matches_runtime(&runtime()));
    }

    #[test]
    fn runtime_requires_every_provided_locator_to_match() {
        let context = HerdrContext {
            focused_pane_id: Some("w1:p1".to_string()),
            focused_pane_agent: Some("another-agent".to_string()),
            ..Default::default()
        };
        assert!(!context.matches_runtime(&runtime()));
    }

    #[test]
    fn malformed_plugin_context_is_rejected() {
        assert!(parse_plugin_context("{").is_err());
    }

    #[test]
    fn resolved_execution_uses_cached_task() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let task = Task {
            id: "task-1".to_string(),
            name: "Fix".to_string(),
            status: TaskStatus::Running,
            ..Default::default()
        };
        store.cache_task(&task).unwrap();
        let execution = Execution {
            id: "exec-1".to_string(),
            task_id: task.id.clone(),
            agent_kind: "codex".to_string(),
            mode: ExecutionMode::Execute,
            started_at: Utc::now(),
            runtime_id: Some(serde_json::to_string(&runtime()).unwrap()),
            attempt: 1,
            retry_at: None,
            role: Default::default(),
            parent_task_id: None,
            work_item_id: None,
            plan_version: None,
            checkout_path: None,
            branch: None,
            submission_path: None,
        };
        store.save_execution(&execution).unwrap();
        let context = HerdrContext {
            execution_id: Some(execution.id.clone()),
            ..Default::default()
        };
        let resolved = context.resolve(&store).unwrap();
        assert_eq!(resolved.task.id, task.id);
        assert_eq!(resolved.runtime.unwrap().agent_name, "codex-fix-abc");
    }

    #[test]
    fn explicit_execution_rejects_a_corrupt_runtime_id() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let task = Task {
            id: "task-1".to_string(),
            status: TaskStatus::Running,
            ..Default::default()
        };
        store.cache_task(&task).unwrap();
        let execution = Execution {
            id: "exec-1".to_string(),
            task_id: task.id,
            agent_kind: "codex".to_string(),
            mode: ExecutionMode::Execute,
            started_at: Utc::now(),
            runtime_id: Some("{".to_string()),
            attempt: 1,
            retry_at: None,
            role: Default::default(),
            parent_task_id: None,
            work_item_id: None,
            plan_version: None,
            checkout_path: None,
            branch: None,
            submission_path: None,
        };
        store.save_execution(&execution).unwrap();
        let context = HerdrContext {
            execution_id: Some(execution.id),
            ..Default::default()
        };
        let error = context.resolve(&store).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid cached Herdr runtime id")
        );
    }

    #[test]
    fn unmatched_context_refuses_to_guess_a_task() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let context = HerdrContext {
            workspace_id: Some("w1".to_string()),
            tab_id: Some("w1:t1".to_string()),
            ..Default::default()
        };
        let error = context.resolve(&store).unwrap_err();
        assert!(error.to_string().contains("not associated"));
    }

    #[test]
    fn scoped_context_requires_a_unique_running_execution() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.db")).unwrap();
        for index in 1..=2 {
            let task = Task {
                id: format!("task-{index}"),
                name: format!("Task {index}"),
                status: TaskStatus::Running,
                ..Default::default()
            };
            store.cache_task(&task).unwrap();
            let runtime = RuntimeExecution {
                pane_id: format!("w1:p{index}"),
                agent_name: format!("codex-task-{index}"),
                ..runtime()
            };
            let execution = Execution {
                id: format!("exec-{index}"),
                task_id: task.id,
                agent_kind: "codex".to_string(),
                mode: ExecutionMode::Execute,
                started_at: Utc::now(),
                runtime_id: Some(serde_json::to_string(&runtime).unwrap()),
                attempt: 1,
                retry_at: None,
                role: Default::default(),
                parent_task_id: None,
                work_item_id: None,
                plan_version: None,
                checkout_path: None,
                branch: None,
                submission_path: None,
            };
            store.save_execution(&execution).unwrap();
        }
        let context = HerdrContext {
            workspace_id: Some("w1".to_string()),
            tab_id: Some("w1:t1".to_string()),
            ..Default::default()
        };
        let error = context.resolve(&store).unwrap_err();
        assert!(error.to_string().contains("multiple active"));
    }
}
