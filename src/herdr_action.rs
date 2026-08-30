use crate::{
    config::{Config, default_state_path},
    error::KbctlError,
    herdr::{
        BoardPaneContext, board_pane_open_args, board_pane_resize_args, first_nested_string,
        run_sync_json,
    },
    herdr_context::{HerdrContext, ResolvedContext},
    store::Store,
};
use serde_json::Value;
use std::{env, path::PathBuf};

pub fn open_board() -> Result<(), KbctlError> {
    let context = HerdrContext::from_env()?;
    let store = Store::open(default_state_path())?;
    let target = context.try_resolve(&store)?;
    open_board_with_target(context, target)
}

pub fn open_task() -> Result<(), KbctlError> {
    let context = HerdrContext::from_env()?;
    let store = Store::open(default_state_path())?;
    let target = context.resolve(&store)?;
    open_board_with_target(context, Some(target))
}

fn open_board_with_target(
    context: HerdrContext,
    target: Option<ResolvedContext>,
) -> Result<(), KbctlError> {
    let config = Config::load(None)?;
    let binary = env::var("HERDR_BIN_PATH").unwrap_or_else(|_| config.herdr.binary.clone());
    let panes = run_sync_json(&binary, &["pane", "list"])?;
    let pane_list = panes
        .get("result")
        .and_then(|value| value.get("panes"))
        .and_then(Value::as_array)
        .ok_or_else(|| KbctlError::Runtime("Herdr did not return pane list".to_string()))?;
    let active_pane = context
        .pane_id()
        .map(ToOwned::to_owned)
        .or_else(|| env::var("HERDR_ACTIVE_PANE_ID").ok())
        .or_else(|| env::var("HERDR_PANE_ID").ok())
        .or_else(|| focused_pane_id(pane_list))
        .ok_or_else(|| {
            KbctlError::Runtime(
                "Herdr did not provide an active pane; run this action from Herdr".to_string(),
            )
        })?;
    let active_pane_info = pane_list
        .iter()
        .find(|pane| pane_id(pane) == Some(active_pane.as_str()));
    let active_tab = context
        .tab_id
        .clone()
        .or_else(|| env::var("HERDR_ACTIVE_TAB_ID").ok())
        .or_else(|| env::var("HERDR_TAB_ID").ok())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            active_pane_info
                .and_then(pane_tab_id)
                .map(ToOwned::to_owned)
        })
        .ok_or_else(|| KbctlError::Runtime("Herdr did not provide an active tab".to_string()))?;
    let configured_cwd = config
        .project
        .default
        .map(|binding| binding.path)
        .map(PathBuf::from)
        .filter(|path| path.is_dir())
        .map(|path| path.to_string_lossy().to_string());
    let cwd = context
        .focused_pane_cwd
        .clone()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| env::var("HERDR_ACTIVE_PANE_CWD").ok())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| active_pane_info.and_then(pane_cwd).map(ToOwned::to_owned))
        .or(configured_cwd)
        .or_else(|| {
            env::current_dir()
                .ok()
                .map(|path| path.to_string_lossy().to_string())
        })
        .ok_or_else(|| KbctlError::Runtime("resolve Herdr board cwd".to_string()))?;
    let board_panes = board_pane_ids(pane_list, &active_tab);
    let anchor_pane = board_anchor_pane(
        &active_pane,
        &active_tab,
        pane_list,
        &board_panes,
        target.as_ref(),
    );
    let board_context = target.as_ref().map(|target| BoardPaneContext {
        task_id: &target.task.id,
        execution_id: target
            .execution
            .as_ref()
            .map(|execution| execution.id.as_str()),
    });
    let args = board_pane_open_args(&anchor_pane, &cwd, board_context);
    let opened = run_sync_json(&binary, &args)?;
    let opened_pane_id = first_nested_string(
        &opened,
        &[
            &["result", "plugin_pane", "pane", "pane_id"],
            &["plugin_pane", "pane", "pane_id"],
            &["result", "pane", "pane_id"],
            &["result", "pane_id"],
        ],
    )
    .ok_or_else(|| {
        KbctlError::Runtime("Herdr did not return the kbctl board pane id".to_string())
    })?;
    for pane_id in board_panes
        .iter()
        .filter(|pane_id| pane_id.as_str() != opened_pane_id)
    {
        if let Err(error) = run_sync_json(&binary, &["pane", "close", pane_id.as_str()]) {
            tracing::warn!(pane_id, error = %error, "close replaced kbctl board pane failed");
        }
    }
    if !board_panes.contains(&anchor_pane)
        && let Err(error) = run_sync_json(&binary, &board_pane_resize_args(&anchor_pane))
    {
        tracing::warn!(anchor_pane, error = %error, "resize kbctl board pane failed");
    }
    Ok(())
}

fn focused_pane_id(panes: &[Value]) -> Option<String> {
    panes.iter().find_map(|pane| {
        pane.get("focused")
            .and_then(Value::as_bool)
            .filter(|focused| *focused)
            .and_then(|_| pane_id(pane))
            .map(ToOwned::to_owned)
    })
}

fn pane_id(pane: &Value) -> Option<&str> {
    pane.get("pane_id").and_then(Value::as_str)
}

fn pane_tab_id(pane: &Value) -> Option<&str> {
    pane.get("tab_id").and_then(Value::as_str)
}

fn pane_cwd(pane: &Value) -> Option<&str> {
    pane.get("foreground_cwd")
        .or_else(|| pane.get("cwd"))
        .and_then(Value::as_str)
}

fn is_board_pane(pane: &Value) -> bool {
    pane.get("terminal_title_stripped")
        .or_else(|| pane.get("label"))
        .and_then(Value::as_str)
        == Some("kbctl board")
}

fn board_pane_ids(panes: &[Value], tab_id: &str) -> Vec<String> {
    panes
        .iter()
        .filter(|pane| pane_tab_id(pane) == Some(tab_id) && is_board_pane(pane))
        .filter_map(pane_id)
        .map(ToOwned::to_owned)
        .collect()
}

fn board_anchor_pane(
    active_pane: &str,
    active_tab: &str,
    panes: &[Value],
    board_panes: &[String],
    target: Option<&ResolvedContext>,
) -> String {
    if !board_panes.iter().any(|pane| pane == active_pane) {
        return active_pane.to_string();
    }
    target
        .and_then(|target| target.runtime.as_ref())
        .filter(|runtime| runtime.tab_id == active_tab)
        .map(|runtime| runtime.pane_id.as_str())
        .filter(|pane| {
            panes.iter().any(|value| pane_id(value) == Some(*pane))
                && !board_panes.iter().any(|board| board == pane)
        })
        .map(ToOwned::to_owned)
        .or_else(|| {
            panes
                .iter()
                .filter(|pane| pane_tab_id(pane) == Some(active_tab) && !is_board_pane(pane))
                .find_map(pane_id)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| active_pane.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{Execution, ExecutionMode, Task, TaskStatus},
        herdr::RuntimeExecution,
    };
    use chrono::Utc;

    #[test]
    fn replacing_a_focused_board_uses_the_task_agent_as_anchor() {
        let panes = serde_json::json!([
            {"pane_id":"agent-pane","tab_id":"tab-1","label":"codex"},
            {"pane_id":"board-pane","tab_id":"tab-1","label":"kbctl board"}
        ]);
        let runtime = RuntimeExecution {
            workspace_id: "workspace-1".to_string(),
            tab_id: "tab-1".to_string(),
            pane_id: "agent-pane".to_string(),
            board_pane_id: Some("board-pane".to_string()),
            agent_name: "codex-task".to_string(),
            agent_kind: "codex".to_string(),
        };
        let target = ResolvedContext {
            task: Task {
                id: "task-1".to_string(),
                status: TaskStatus::Running,
                ..Default::default()
            },
            execution: Some(Execution {
                id: "exec-1".to_string(),
                task_id: "task-1".to_string(),
                agent_kind: "codex".to_string(),
                mode: ExecutionMode::Execute,
                started_at: Utc::now(),
                runtime_id: None,
                attempt: 1,
                retry_at: None,
            }),
            runtime: Some(runtime),
        };
        let panes = panes.as_array().unwrap();
        let boards = board_pane_ids(panes, "tab-1");
        assert_eq!(
            board_anchor_pane("board-pane", "tab-1", panes, &boards, Some(&target)),
            "agent-pane"
        );
    }

    #[test]
    fn board_panes_are_scoped_to_the_active_tab() {
        let panes = serde_json::json!([
            {"pane_id":"board-1","tab_id":"tab-1","label":"kbctl board"},
            {"pane_id":"board-2","tab_id":"tab-2","label":"kbctl board"}
        ]);
        assert_eq!(
            board_pane_ids(panes.as_array().unwrap(), "tab-1"),
            vec!["board-1"]
        );
    }
}
