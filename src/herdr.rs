use crate::{
    domain::{Execution, WorkContract},
    error::KbctlError,
    report_spool,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashSet,
    ffi::OsStr,
    path::PathBuf,
    process::{Command as SyncCommand, Stdio},
    sync::{Arc, Mutex},
};
use tokio::process::Command as TokioCommand;
use tokio::sync::broadcast;
use tokio::time::{Duration, sleep};

const HERDR_INTERRUPT_KEY: &str = "ctrl+c";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeState {
    Idle,
    Working,
    Blocked,
    Done,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct RuntimeEvent {
    pub event_id: String,
    pub pane_id: String,
    pub state: RuntimeState,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeExecution {
    pub workspace_id: String,
    pub tab_id: String,
    pub pane_id: String,
    #[serde(default)]
    pub board_pane_id: Option<String>,
    pub agent_name: String,
    #[serde(default)]
    pub agent_kind: String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BoardPaneContext<'a> {
    pub task_id: &'a str,
    pub execution_id: Option<&'a str>,
}

#[async_trait]
pub trait AgentRuntime: Send + Sync {
    fn runtime_kind(&self) -> &'static str {
        "runtime"
    }
    fn runtime_group_id(&self, runtime_id: &str) -> Result<Option<String>, KbctlError> {
        let _ = runtime_id;
        Ok(None)
    }
    async fn ensure(
        &self,
        execution: &Execution,
        contract: &WorkContract,
    ) -> Result<String, KbctlError> {
        self.start(execution, contract).await
    }
    async fn start(
        &self,
        execution: &Execution,
        contract: &WorkContract,
    ) -> Result<String, KbctlError>;
    async fn inspect(&self, runtime_id: &str) -> Result<RuntimeState, KbctlError>;
    async fn prompt(&self, runtime_id: &str, prompt: &str) -> Result<(), KbctlError> {
        let _ = (runtime_id, prompt);
        Err(KbctlError::Runtime(
            "runtime prompt is not supported".to_string(),
        ))
    }
    async fn read_recent(&self, runtime_id: &str, lines: usize) -> Result<String, KbctlError> {
        let _ = (runtime_id, lines);
        Err(KbctlError::Runtime(
            "runtime output reading is not supported".to_string(),
        ))
    }
    async fn focus(&self, runtime_id: &str) -> Result<(), KbctlError>;
    async fn cancel(&self, runtime_id: &str) -> Result<(), KbctlError>;
    fn watch(&self, runtime_id: &str) -> Result<(), KbctlError> {
        let _ = runtime_id;
        Ok(())
    }
    fn subscribe(&self) -> Option<broadcast::Receiver<RuntimeEvent>> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct HerdrRuntime {
    binary: PathBuf,
    events: broadcast::Sender<RuntimeEvent>,
    watched_panes: Arc<Mutex<HashSet<String>>>,
}

impl HerdrRuntime {
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            binary: binary.into(),
            events,
            watched_panes: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn status(&self) -> Result<Value, KbctlError> {
        run_json(&self.binary, &["api".to_string(), "snapshot".to_string()]).await
    }

    pub async fn version(&self) -> Result<String, KbctlError> {
        let output = TokioCommand::new(&self.binary)
            .arg("--version")
            .output()
            .await
            .map_err(|error| KbctlError::Runtime(format!("run Herdr version: {error}")))?;
        if !output.status.success() {
            return Err(KbctlError::Runtime(
                "Herdr version check failed".to_string(),
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    pub async fn open_integration_diff(
        &self,
        cwd: &str,
        base_branch: &str,
        integration_branch: &str,
    ) -> Result<(), KbctlError> {
        let target = std::env::var("HERDR_PANE_ID")
            .map_err(|_| KbctlError::Runtime("board is not running in a Herdr pane".to_string()))?;
        let split = self
            .command(&[
                "pane".to_string(),
                "split".to_string(),
                "--pane".to_string(),
                target,
                "--direction".to_string(),
                "down".to_string(),
                "--ratio".to_string(),
                "0.45".to_string(),
                "--cwd".to_string(),
                cwd.to_string(),
                "--focus".to_string(),
            ])
            .await?;
        let pane_id = first_nested_string(
            &split,
            &[
                &["pane_id"],
                &["result", "pane", "pane_id"],
                &["result", "pane_id"],
            ],
        )
        .ok_or_else(|| KbctlError::Runtime("Herdr did not return the diff pane id".to_string()))?;
        let range = format!("{base_branch}...{integration_branch}");
        self.command(&[
            "pane".to_string(),
            "run".to_string(),
            pane_id,
            "git".to_string(),
            "diff".to_string(),
            "--color=always".to_string(),
            range,
        ])
        .await?;
        Ok(())
    }

    async fn command(&self, args: &[String]) -> Result<Value, KbctlError> {
        run_json(&self.binary, args).await
    }

    fn workspace_ids(&self, value: &Value) -> Result<(String, String, String), KbctlError> {
        let workspace_id = first_nested_string(
            value,
            &[
                &["workspace_id"],
                &["workspace", "workspace_id"],
                &["result", "workspace", "workspace_id"],
                &["result", "root_pane", "workspace_id"],
            ],
        )
        .ok_or_else(|| KbctlError::Runtime("Herdr did not return a workspace id".to_string()))?;
        let tab_id = first_nested_string(
            value,
            &[
                &["tab_id"],
                &["tab", "tab_id"],
                &["result", "tab", "tab_id"],
                &["result", "root_pane", "tab_id"],
            ],
        )
        .ok_or_else(|| KbctlError::Runtime("Herdr did not return a tab id".to_string()))?;
        let pane_id = first_nested_string(
            value,
            &[
                &["pane_id"],
                &["root_pane", "pane_id"],
                &["result", "root_pane", "pane_id"],
            ],
        )
        .ok_or_else(|| KbctlError::Runtime("Herdr did not return a pane id".to_string()))?;
        Ok((workspace_id, tab_id, pane_id))
    }

    async fn start_agent_with_retry(&self, args: &[String]) -> Result<Value, KbctlError> {
        let mut last_error = None;
        for attempt in 0..40 {
            match self.command(args).await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < 39 && pane_is_not_ready(&error) => {
                    last_error = Some(error);
                    sleep(Duration::from_millis(250)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            KbctlError::Runtime("Herdr agent start did not complete".to_string())
        }))
    }

    async fn prompt_with_retry(&self, target: &str, text: &str) -> Result<(), KbctlError> {
        let mut last_error = None;
        for attempt in 0..3 {
            let args = prompt_command_args(target, text);
            match self.command(&args).await {
                Ok(_) => return Ok(()),
                Err(error) if attempt < 2 && prompt_is_stalled(&error) => {
                    tracing::warn!(
                        target,
                        attempt = attempt + 1,
                        error = %error,
                        "Herdr prompt did not start an agent turn; retrying"
                    );
                    last_error = Some(error);
                    sleep(Duration::from_millis(500)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            KbctlError::Runtime("Herdr prompt did not start an agent turn".to_string())
        }))
    }

    async fn open_board_pane(
        &self,
        target_pane: &str,
        cwd: &str,
        task_id: &str,
        execution_id: &str,
    ) -> Option<String> {
        match self
            .command(&board_pane_open_args(
                target_pane,
                cwd,
                Some(BoardPaneContext {
                    task_id,
                    execution_id: Some(execution_id),
                }),
            ))
            .await
        {
            Ok(value) => {
                let board_pane_id = first_nested_string(
                    &value,
                    &[
                        &["result", "plugin_pane", "pane", "pane_id"],
                        &["plugin_pane", "pane", "pane_id"],
                    ],
                );
                if board_pane_id.is_none() {
                    tracing::warn!(target_pane, "Herdr did not return the kbctl board pane id");
                }
                if let Err(error) = self.command(&board_pane_resize_args(target_pane)).await {
                    tracing::warn!(target_pane, error = %error, "resize Herdr kbctl board pane failed");
                }
                board_pane_id
            }
            Err(error) => {
                tracing::warn!(target_pane, error = %error, "open Herdr kbctl board pane failed");
                None
            }
        }
    }

    fn watch_runtime(&self, runtime: RuntimeExecution) {
        if !self
            .watched_panes
            .lock()
            .expect("watch set")
            .insert(runtime.pane_id.clone())
        {
            return;
        }
        let sender = self.events.clone();
        let pane_id = runtime.pane_id;
        tokio::spawn(async move {
            subscribe_to_pane_events(pane_id, sender).await;
        });
    }
}

#[async_trait]
impl AgentRuntime for HerdrRuntime {
    fn runtime_kind(&self) -> &'static str {
        "herdr"
    }

    fn runtime_group_id(&self, runtime_id: &str) -> Result<Option<String>, KbctlError> {
        let runtime: RuntimeExecution = serde_json::from_str(runtime_id)
            .map_err(|error| KbctlError::Runtime(format!("invalid Herdr runtime id: {error}")))?;
        Ok(Some(runtime.workspace_id))
    }

    async fn start(
        &self,
        execution: &Execution,
        contract: &WorkContract,
    ) -> Result<String, KbctlError> {
        let agent_name = display_agent_name(&contract.agent_kind, &contract.title, &execution.id);
        let tab_label = execution_tab_label(execution, contract);
        let (workspace_id, tab_id, pane_id) =
            if let Some(workspace_id) = contract.runtime_group_id.as_deref() {
                match self
                    .command(&tab_create_args(
                        workspace_id,
                        execution,
                        contract,
                        &tab_label,
                    ))
                    .await
                {
                    Ok(tab) => self.workspace_ids(&tab)?,
                    Err(error) if workspace_is_gone(&error) => {
                        self.create_workspace(execution, contract, &tab_label)
                            .await?
                    }
                    Err(error) => return Err(error),
                }
            } else {
                self.create_workspace(execution, contract, &tab_label)
                    .await?
            };

        let board_pane_id = self
            .open_board_pane(
                &pane_id,
                &contract.project_path,
                &execution.task_id,
                &execution.id,
            )
            .await;

        let mut start_args = vec![
            "agent".to_string(),
            "start".to_string(),
            agent_name.clone(),
            "--kind".to_string(),
            contract.agent_kind.clone(),
            "--pane".to_string(),
            pane_id.clone(),
        ];
        start_args.extend(agent_arguments(contract));
        let started = self.start_agent_with_retry(&start_args).await?;
        let started_name = first_nested_string(
            &started,
            &[
                &["name"],
                &["agent", "name"],
                &["result", "name"],
                &["result", "agent", "name"],
            ],
        )
        .unwrap_or(agent_name);
        start_args.clear();

        self.prompt_with_retry(&started_name, &contract.prompt())
            .await?;

        let runtime = RuntimeExecution {
            workspace_id,
            tab_id,
            pane_id,
            board_pane_id,
            agent_name: started_name,
            agent_kind: contract.agent_kind.clone(),
        };
        self.watch_runtime(runtime.clone());
        serde_json::to_string(&runtime).map_err(|error| KbctlError::Runtime(error.to_string()))
    }

    async fn inspect(&self, runtime_id: &str) -> Result<RuntimeState, KbctlError> {
        let runtime: RuntimeExecution = serde_json::from_str(runtime_id)
            .map_err(|error| KbctlError::Runtime(format!("invalid Herdr runtime id: {error}")))?;
        let process_info = self
            .command(&[
                "pane".to_string(),
                "process-info".to_string(),
                "--pane".to_string(),
                runtime.pane_id.clone(),
            ])
            .await;
        let process_info = match process_info {
            Ok(value) => value,
            Err(error) if pane_is_gone(&error) => return Ok(RuntimeState::Done),
            Err(error) => return Err(error),
        };
        if !foreground_agent_present(&process_info, &runtime.agent_kind) {
            return Ok(RuntimeState::Done);
        }
        let value = self
            .command(&["agent".to_string(), "get".to_string(), runtime.agent_name])
            .await;
        let value = match value {
            Ok(value) => value,
            Err(error) if agent_is_gone(&error) => return Ok(RuntimeState::Unknown),
            Err(error) => return Err(error),
        };
        Ok(first_nested_string(
            &value,
            &[
                &["agent_status"],
                &["status"],
                &["result", "agent_status"],
                &["result", "status"],
                &["result", "agent", "agent_status"],
                &["result", "agent", "status"],
            ],
        )
        .map(|status| match status.to_ascii_lowercase().as_str() {
            "idle" => RuntimeState::Idle,
            "working" => RuntimeState::Working,
            "blocked" => RuntimeState::Blocked,
            "done" => RuntimeState::Idle,
            _ => RuntimeState::Unknown,
        })
        .unwrap_or(RuntimeState::Unknown))
    }

    async fn prompt(&self, runtime_id: &str, prompt: &str) -> Result<(), KbctlError> {
        let runtime: RuntimeExecution = serde_json::from_str(runtime_id)
            .map_err(|error| KbctlError::Runtime(format!("invalid Herdr runtime id: {error}")))?;
        self.prompt_with_retry(&runtime.agent_name, prompt).await
    }

    async fn read_recent(&self, runtime_id: &str, lines: usize) -> Result<String, KbctlError> {
        let runtime: RuntimeExecution = serde_json::from_str(runtime_id)
            .map_err(|error| KbctlError::Runtime(format!("invalid Herdr runtime id: {error}")))?;
        run_text(
            &self.binary,
            &[
                "agent".to_string(),
                "read".to_string(),
                runtime.pane_id,
                "--source".to_string(),
                "recent-unwrapped".to_string(),
                "--lines".to_string(),
                lines.max(1).to_string(),
            ],
        )
        .await
    }

    async fn focus(&self, runtime_id: &str) -> Result<(), KbctlError> {
        let runtime: RuntimeExecution = serde_json::from_str(runtime_id)
            .map_err(|error| KbctlError::Runtime(format!("invalid Herdr runtime id: {error}")))?;
        self.command(&["agent".to_string(), "focus".to_string(), runtime.agent_name])
            .await
            .map(|_| ())
    }

    async fn cancel(&self, runtime_id: &str) -> Result<(), KbctlError> {
        let runtime: RuntimeExecution = serde_json::from_str(runtime_id)
            .map_err(|error| KbctlError::Runtime(format!("invalid Herdr runtime id: {error}")))?;
        self.command(&[
            "agent".to_string(),
            "send-keys".to_string(),
            runtime.agent_name,
            HERDR_INTERRUPT_KEY.to_string(),
        ])
        .await
        .map(|_| ())
    }

    fn watch(&self, runtime_id: &str) -> Result<(), KbctlError> {
        let runtime: RuntimeExecution = serde_json::from_str(runtime_id)
            .map_err(|error| KbctlError::Runtime(format!("invalid Herdr runtime id: {error}")))?;
        self.watch_runtime(runtime);
        Ok(())
    }

    fn subscribe(&self) -> Option<broadcast::Receiver<RuntimeEvent>> {
        Some(self.events.subscribe())
    }
}

impl HerdrRuntime {
    async fn create_workspace(
        &self,
        execution: &Execution,
        contract: &WorkContract,
        tab_label: &str,
    ) -> Result<(String, String, String), KbctlError> {
        let workspace = self
            .command(&workspace_create_args(execution, contract))
            .await?;
        let ids = self.workspace_ids(&workspace)?;
        self.command(&[
            "tab".to_string(),
            "rename".to_string(),
            ids.1.clone(),
            tab_label.to_string(),
        ])
        .await?;
        Ok(ids)
    }
}

#[cfg(unix)]
async fn subscribe_to_pane_events(pane_id: String, sender: broadcast::Sender<RuntimeEvent>) {
    use sha2::{Digest, Sha256};
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixStream,
    };
    loop {
        let socket = std::env::var_os("HERDR_SOCKET_PATH")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(|home| PathBuf::from(home).join(".config/herdr/herdr.sock"))
            });
        let Some(socket) = socket else {
            return;
        };
        match UnixStream::connect(&socket).await {
            Ok(stream) => {
                let (reader, mut writer) = stream.into_split();
                let request = serde_json::json!({
                    "id": format!("kbctl-{}", uuid::Uuid::new_v4()),
                    "method": "events.subscribe",
                    "params": {
                        "subscriptions": [
                            {"type": "pane.agent_status_changed", "pane_id": pane_id},
                            {"type": "pane.exited", "pane_id": pane_id},
                            {"type": "pane.agent_detected", "pane_id": pane_id}
                        ]
                    }
                });
                if writer
                    .write_all(format!("{request}\n").as_bytes())
                    .await
                    .is_ok()
                {
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let Ok(payload) = serde_json::from_str::<Value>(&line) else {
                            continue;
                        };
                        let Some(state) = runtime_state_from_event(&payload) else {
                            continue;
                        };
                        let event_id = format!("{:x}", Sha256::digest(line.as_bytes()));
                        let _ = sender.send(RuntimeEvent {
                            event_id,
                            pane_id: pane_id.clone(),
                            state,
                            payload,
                        });
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, path = %socket.display(), "Herdr event subscription connect failed")
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(not(unix))]
async fn subscribe_to_pane_events(_pane_id: String, _sender: broadcast::Sender<RuntimeEvent>) {}

fn runtime_state_from_event(value: &Value) -> Option<RuntimeState> {
    if let Some(status) = find_string_key(value, "agent_status") {
        return Some(match status {
            "idle" => RuntimeState::Idle,
            "working" => RuntimeState::Working,
            "blocked" => RuntimeState::Blocked,
            "done" => RuntimeState::Done,
            _ => RuntimeState::Unknown,
        });
    }
    let event_type = find_string_key(value, "type")?;
    matches!(event_type, "pane_exited" | "pane.exited").then_some(RuntimeState::Done)
}

fn find_string_key<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    match value {
        Value::Object(values) => values.get(key).and_then(Value::as_str).or_else(|| {
            values
                .values()
                .find_map(|value| find_string_key(value, key))
        }),
        Value::Array(values) => values.iter().find_map(|value| find_string_key(value, key)),
        _ => None,
    }
}

fn execution_environment_args(execution: &Execution, contract: &WorkContract) -> Vec<String> {
    [
        format!("KBCTL_EXECUTION_ID={}", execution.id),
        format!("KBCTL_TASK_ID={}", execution.task_id),
        format!(
            "{}={}",
            report_spool::REPORT_FILE_ENV,
            report_spool::path_for(std::path::Path::new(&contract.project_path), &execution.id)
                .display()
        ),
        format!("KBCTL_EXECUTION_MODE={}", execution.mode),
        format!("KBCTL_EXECUTION_ROLE={:?}", execution.role).to_ascii_lowercase(),
        "KBCTL_TRANSPORT=herdr".to_string(),
        format!(
            "{}={}",
            report_spool::SUBMISSION_FILE_ENV,
            contract.submission_path
        ),
    ]
    .into_iter()
    .flat_map(|value| ["--env".to_string(), value])
    .collect()
}

fn workspace_create_args(execution: &Execution, contract: &WorkContract) -> Vec<String> {
    let mut args = vec![
        "workspace".to_string(),
        "create".to_string(),
        "--cwd".to_string(),
        contract.project_path.clone(),
        "--label".to_string(),
        contract.project_name.clone(),
        "--no-focus".to_string(),
    ];
    args.extend(execution_environment_args(execution, contract));
    args
}

fn tab_create_args(
    workspace_id: &str,
    execution: &Execution,
    contract: &WorkContract,
    label: &str,
) -> Vec<String> {
    let mut args = vec![
        "tab".to_string(),
        "create".to_string(),
        "--workspace".to_string(),
        workspace_id.to_string(),
        "--cwd".to_string(),
        contract.project_path.clone(),
        "--label".to_string(),
        label.to_string(),
        "--no-focus".to_string(),
    ];
    args.extend(execution_environment_args(execution, contract));
    args
}

fn execution_tab_label(execution: &Execution, contract: &WorkContract) -> String {
    let role = match execution.role {
        crate::domain::ExecutionRole::Supervisor => "Supervisor",
        crate::domain::ExecutionRole::Reviewer => "Review",
        crate::domain::ExecutionRole::Worker => "Worker",
        crate::domain::ExecutionRole::Standalone => "Task",
    };
    format!("{role} · {}", contract.title)
}

fn agent_arguments(contract: &WorkContract) -> Vec<String> {
    let mut args = Vec::new();
    match contract.agent_kind.as_str() {
        "codex" => {
            args.push("--".to_string());
            if let Some(model) = &contract.model {
                args.extend(["--model".to_string(), model.clone()]);
            }
            if let Some(reasoning) = &contract.reasoning {
                args.extend([
                    "--config".to_string(),
                    format!("model_reasoning_effort=\"{reasoning}\""),
                ]);
            }
            if contract.role == crate::domain::ExecutionRole::Worker {
                args.extend(["--disable".to_string(), "multi_agent".to_string()]);
            }
            args.extend([
                "--sandbox".to_string(),
                if contract.read_only {
                    "read-only"
                } else {
                    "workspace-write"
                }
                .to_string(),
            ]);
        }
        "opencode" => {
            args.push("--".to_string());
            if let Some(model) = &contract.model {
                args.extend(["--model".to_string(), model.clone()]);
            }
            if let Some(agent) = &contract.agent {
                args.extend(["--agent".to_string(), agent.clone()]);
            }
        }
        "grok" => {
            args.push("--".to_string());
            if let Some(model) = &contract.model {
                args.extend(["--model".to_string(), model.clone()]);
            }
            if let Some(agent) = &contract.agent {
                args.extend(["--agent".to_string(), agent.clone()]);
            }
            if contract.role == crate::domain::ExecutionRole::Worker {
                args.push("--no-subagents".to_string());
            }
        }
        _ => {}
    }
    args
}

async fn run_json(binary: &PathBuf, args: &[String]) -> Result<Value, KbctlError> {
    let output = TokioCommand::new(binary)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|error| KbctlError::Runtime(format!("run {}: {error}", binary.display())))?;
    parse_output(output.status.success(), &output.stdout, &output.stderr)
}

async fn run_text(binary: &PathBuf, args: &[String]) -> Result<String, KbctlError> {
    let output = TokioCommand::new(binary)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|error| KbctlError::Runtime(format!("run {}: {error}", binary.display())))?;
    if !output.status.success() {
        return Err(KbctlError::Runtime(error_message(
            &Value::Null,
            &output.stderr,
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub(crate) fn run_sync_json<S>(binary: &str, args: &[S]) -> Result<Value, KbctlError>
where
    S: AsRef<OsStr>,
{
    let output = SyncCommand::new(binary)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| KbctlError::Runtime(format!("run {binary}: {error}")))?;
    parse_output(output.status.success(), &output.stdout, &output.stderr)
}

fn parse_output(success: bool, stdout: &[u8], stderr: &[u8]) -> Result<Value, KbctlError> {
    let text = String::from_utf8_lossy(stdout).trim().to_string();
    if let Ok(value) = serde_json::from_str::<Value>(&text) {
        if !success {
            return Err(KbctlError::Runtime(error_message(&value, stderr)));
        }
        if let Some(error) = value.get("error") {
            return Err(KbctlError::Runtime(error.to_string()));
        }
        return Ok(value);
    }
    let message = if text.is_empty() {
        String::from_utf8_lossy(stderr).trim().to_string()
    } else {
        text
    };
    if success {
        Err(KbctlError::Runtime(format!(
            "Herdr returned non-JSON output: {message}"
        )))
    } else {
        Err(KbctlError::Runtime(error_message(&Value::Null, stderr)))
    }
}

fn error_message(value: &Value, stderr: &[u8]) -> String {
    value
        .get("error")
        .and_then(|error| {
            error
                .as_str()
                .map(ToOwned::to_owned)
                .or_else(|| {
                    error
                        .get("message")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .or_else(|| {
                    error
                        .get("code")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .or_else(|| Some(error.to_string()))
        })
        .or_else(|| {
            value
                .get("message")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| {
            let text = String::from_utf8_lossy(stderr).trim().to_string();
            if text.is_empty() {
                "Herdr command failed".to_string()
            } else {
                text
            }
        })
}

fn nested_string(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().map(ToOwned::to_owned)
}

pub(crate) fn first_nested_string(value: &Value, paths: &[&[&str]]) -> Option<String> {
    paths.iter().find_map(|path| nested_string(value, path))
}

fn pane_is_not_ready(error: &KbctlError) -> bool {
    let message = error.to_string();
    message.contains("agent_pane_busy") || message.contains("not an available shell")
}

fn prompt_command_args(target: &str, text: &str) -> Vec<String> {
    vec![
        "agent".to_string(),
        "prompt".to_string(),
        target.to_string(),
        text.to_string(),
        "--wait".to_string(),
        "--until".to_string(),
        "working".to_string(),
        "--timeout".to_string(),
        "15000".to_string(),
    ]
}

pub(crate) fn board_pane_open_args(
    target_pane: &str,
    cwd: &str,
    context: Option<BoardPaneContext<'_>>,
) -> Vec<String> {
    let mut args = vec![
        "plugin".to_string(),
        "pane".to_string(),
        "open".to_string(),
        "--plugin".to_string(),
        "kbctl".to_string(),
        "--entrypoint".to_string(),
        "board".to_string(),
        "--placement".to_string(),
        "split".to_string(),
        "--target-pane".to_string(),
        target_pane.to_string(),
        "--direction".to_string(),
        "right".to_string(),
        "--cwd".to_string(),
        cwd.to_string(),
    ];
    if let Some(context) = context {
        args.push("--env".to_string());
        args.push(format!("KBCTL_CONTEXT_TASK_ID={}", context.task_id));
        if let Some(execution_id) = context.execution_id {
            args.push("--env".to_string());
            args.push(format!("KBCTL_CONTEXT_EXECUTION_ID={execution_id}"));
        }
    }
    args.push("--no-focus".to_string());
    args
}

pub(crate) fn board_pane_resize_args(target_pane: &str) -> Vec<String> {
    vec![
        "pane".to_string(),
        "resize".to_string(),
        "--pane".to_string(),
        target_pane.to_string(),
        "--direction".to_string(),
        "right".to_string(),
        "--amount".to_string(),
        "0.25".to_string(),
    ]
}

fn display_agent_name(agent_kind: &str, title: &str, execution_id: &str) -> String {
    let kind = identifier_slug(agent_kind, "agent");
    let title = identifier_slug(title, "task");
    let suffix = execution_id
        .chars()
        .filter(|value| value.is_ascii_alphanumeric())
        .map(|value| value.to_ascii_lowercase())
        .take(8)
        .collect::<String>();
    let suffix = if suffix.is_empty() {
        "run".to_string()
    } else {
        suffix
    };
    let prefix_limit = 32usize.saturating_sub(suffix.len() + 1);
    let prefix = format!("{kind}-{title}");
    let prefix = prefix
        .chars()
        .take(prefix_limit)
        .collect::<String>()
        .trim_end_matches('-')
        .to_string();
    format!("{prefix}-{suffix}")
}

fn identifier_slug(value: &str, fallback: &str) -> String {
    let mut slug = String::new();
    let mut separator = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character.to_ascii_lowercase());
            separator = false;
        } else if !slug.is_empty() {
            separator = true;
        }
        if separator && slug.len() < 32 {
            if !slug.ends_with('-') && !slug.ends_with('_') {
                slug.push('-');
            }
            separator = false;
        }
    }
    while slug.ends_with('-') || slug.ends_with('_') {
        slug.pop();
    }
    if slug.is_empty() {
        fallback.to_string()
    } else {
        slug
    }
}

fn prompt_is_stalled(error: &KbctlError) -> bool {
    error.to_string().contains("agent_prompt_stalled")
}

fn agent_is_gone(error: &KbctlError) -> bool {
    error.to_string().contains("agent_not_found")
}

fn pane_is_gone(error: &KbctlError) -> bool {
    error.to_string().contains("pane_not_found")
}

fn workspace_is_gone(error: &KbctlError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("workspace_not_found")
        || (message.contains("workspace") && message.contains("not found"))
}

fn foreground_agent_present(value: &Value, agent_kind: &str) -> bool {
    let Some(processes) = value
        .get("result")
        .and_then(|value| value.get("process_info"))
        .and_then(|value| value.get("foreground_processes"))
        .and_then(Value::as_array)
    else {
        return true;
    };
    let kind = agent_kind.trim().to_ascii_lowercase();
    processes.iter().any(|process| {
        let fields = [
            process.get("name").and_then(Value::as_str),
            process.get("argv0").and_then(Value::as_str),
            process.get("cmdline").and_then(Value::as_str),
        ];
        if kind.is_empty() {
            return fields.iter().flatten().any(|value| {
                !matches!(
                    value.to_ascii_lowercase().as_str(),
                    "zsh" | "bash" | "fish" | "sh" | "node"
                )
            });
        }
        fields
            .iter()
            .flatten()
            .any(|value| value.to_ascii_lowercase().contains(&kind))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract(runtime_group_id: Option<&str>) -> WorkContract {
        WorkContract {
            task_id: "task-1".to_string(),
            execution_id: "exec-1".to_string(),
            mode: crate::domain::ExecutionMode::Execute,
            title: "Compare clients".to_string(),
            body: "Research".to_string(),
            project_name: "kbctl".to_string(),
            project_path: "/tmp/kbctl".to_string(),
            due: None,
            scheduled_at: None,
            agent_kind: "codex".to_string(),
            profile_name: "fast_worker".to_string(),
            role: crate::domain::ExecutionRole::Worker,
            model: Some("gpt-5.6-luna".to_string()),
            reasoning: Some("high".to_string()),
            agent: None,
            read_only: true,
            plan_version: Some(1),
            work_item_id: Some("work-1".to_string()),
            submission_path: "/tmp/submission.json".to_string(),
            report_command: "kbctl report submit".to_string(),
            runtime_group_id: runtime_group_id.map(ToOwned::to_owned),
            available_worker_profiles: Vec::new(),
        }
    }

    #[test]
    fn parses_success_and_failure_output() {
        let value = parse_output(true, br#"{"result":{"type":"ok"}}"#, b"").unwrap();
        assert_eq!(value["result"]["type"], "ok");
        let error = parse_output(false, br#"{"error":"nope"}"#, b"").unwrap_err();
        assert!(error.to_string().contains("nope"));
    }

    #[test]
    fn preserves_nested_error_codes() {
        let error =
            parse_output(false, br#"{"error":{"code":"agent_prompt_stalled"}}"#, b"").unwrap_err();
        assert!(error.to_string().contains("agent_prompt_stalled"));
    }

    #[test]
    fn runtime_id_round_trips() {
        let runtime = RuntimeExecution {
            workspace_id: "w1".to_string(),
            tab_id: "t1".to_string(),
            pane_id: "p1".to_string(),
            board_pane_id: Some("p2".to_string()),
            agent_name: "kbctl-a".to_string(),
            agent_kind: "codex".to_string(),
        };
        let encoded = serde_json::to_string(&runtime).unwrap();
        let decoded: RuntimeExecution = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.agent_name, "kbctl-a");
        assert_eq!(decoded.board_pane_id.as_deref(), Some("p2"));
    }

    #[test]
    fn old_runtime_ids_remain_readable() {
        let decoded: RuntimeExecution = serde_json::from_str(
            r#"{"workspace_id":"w1","tab_id":"t1","pane_id":"p1","agent_name":"kbctl-a"}"#,
        )
        .unwrap();
        assert_eq!(decoded.board_pane_id, None);
    }

    #[test]
    fn parses_wrapped_workspace_response() {
        let value = serde_json::json!({
            "result": {
                "root_pane": {
                    "workspace_id": "w4",
                    "tab_id": "w4:t1",
                    "pane_id": "w4:p1"
                },
                "tab": {"tab_id": "w4:t1"},
                "workspace": {"workspace_id": "w4"}
            }
        });
        assert_eq!(
            HerdrRuntime::new("herdr").workspace_ids(&value).unwrap(),
            ("w4".to_string(), "w4:t1".to_string(), "w4:p1".to_string())
        );
    }

    #[test]
    fn first_execution_creates_a_project_workspace() {
        let mut execution =
            Execution::new("task-1", "codex", crate::domain::ExecutionMode::Execute);
        execution.id = "exec-1".to_string();
        execution.role = crate::domain::ExecutionRole::Worker;
        let args = workspace_create_args(&execution, &contract(None));
        assert_eq!(
            &args[..7],
            [
                "workspace",
                "create",
                "--cwd",
                "/tmp/kbctl",
                "--label",
                "kbctl",
                "--no-focus"
            ]
        );
        assert!(args.contains(&"KBCTL_EXECUTION_ID=exec-1".to_string()));
    }

    #[test]
    fn later_execution_creates_a_labeled_tab_in_the_project_workspace() {
        let mut execution =
            Execution::new("task-1", "codex", crate::domain::ExecutionMode::Execute);
        execution.id = "exec-1".to_string();
        execution.role = crate::domain::ExecutionRole::Worker;
        let value = contract(Some("w1"));
        let args = tab_create_args(
            value.runtime_group_id.as_deref().unwrap(),
            &execution,
            &value,
            &execution_tab_label(&execution, &value),
        );
        assert_eq!(
            &args[..9],
            [
                "tab",
                "create",
                "--workspace",
                "w1",
                "--cwd",
                "/tmp/kbctl",
                "--label",
                "Worker · Compare clients",
                "--no-focus"
            ]
        );
    }

    #[test]
    fn codex_worker_cannot_spawn_hidden_subagents() {
        let args = agent_arguments(&contract(Some("w1")));
        assert!(
            args.windows(2)
                .any(|values| values == ["--disable", "multi_agent"])
        );
    }

    #[test]
    fn parses_wrapped_agent_response() {
        let value = serde_json::json!({
            "result": {
                "agent": {"name": "kbctl-agent", "agent_status": "working"}
            }
        });
        assert_eq!(
            first_nested_string(
                &value,
                &[
                    &["name"],
                    &["agent", "name"],
                    &["result", "name"],
                    &["result", "agent", "name"]
                ]
            )
            .as_deref(),
            Some("kbctl-agent")
        );
        assert_eq!(
            first_nested_string(
                &value,
                &[
                    &["agent_status"],
                    &["status"],
                    &["result", "agent_status"],
                    &["result", "status"],
                    &["result", "agent", "agent_status"]
                ]
            )
            .as_deref(),
            Some("working")
        );
    }

    #[test]
    fn retries_only_when_herdr_pane_is_not_ready() {
        assert!(pane_is_not_ready(&KbctlError::Runtime(
            "{\"error\":{\"code\":\"agent_pane_busy\"}}".to_string()
        )));
        assert!(!pane_is_not_ready(&KbctlError::Runtime(
            "agent kind is unavailable".to_string()
        )));
    }

    #[test]
    fn prompt_command_waits_for_working_state() {
        let args = prompt_command_args("agent-1", "contract body");
        assert_eq!(
            args,
            vec![
                "agent",
                "prompt",
                "agent-1",
                "contract body",
                "--wait",
                "--until",
                "working",
                "--timeout",
                "15000"
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
        );
    }

    #[test]
    fn opens_a_narrow_board_pane_in_the_agent_workspace() {
        assert_eq!(
            board_pane_open_args(
                "wA:p1",
                "/tmp/project",
                Some(BoardPaneContext {
                    task_id: "task-1",
                    execution_id: Some("exec-1")
                })
            ),
            vec![
                "plugin",
                "pane",
                "open",
                "--plugin",
                "kbctl",
                "--entrypoint",
                "board",
                "--placement",
                "split",
                "--target-pane",
                "wA:p1",
                "--direction",
                "right",
                "--cwd",
                "/tmp/project",
                "--env",
                "KBCTL_CONTEXT_TASK_ID=task-1",
                "--env",
                "KBCTL_CONTEXT_EXECUTION_ID=exec-1",
                "--no-focus"
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
        );
        assert_eq!(
            board_pane_resize_args("wA:p1"),
            vec![
                "pane",
                "resize",
                "--pane",
                "wA:p1",
                "--direction",
                "right",
                "--amount",
                "0.25"
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
        );
    }

    #[test]
    fn agent_name_uses_provider_and_task_title() {
        assert_eq!(
            display_agent_name("codex", "測試回ok", "120bdfac-c808"),
            "codex-ok-120bdfac"
        );
        assert_eq!(
            display_agent_name("claude", "  long\n title ", "abcdef12"),
            "claude-long-title-abcdef12"
        );
        assert_eq!(
            display_agent_name("grok", "測試回ok", "120bdfac-c808"),
            "grok-ok-120bdfac"
        );
        let name = display_agent_name("codex", "這是一個很長的中文任務名稱", "abcdef123456");
        assert!(name.starts_with("codex-task-"));
        assert!(name.len() <= 32);
        assert!(name.chars().all(|character| character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || character == '-'
            || character == '_'));
    }

    #[test]
    fn prompt_stall_is_retryable() {
        assert!(prompt_is_stalled(&KbctlError::Runtime(
            "{\"error\":{\"code\":\"agent_prompt_stalled\"}}".to_string()
        )));
        assert!(!prompt_is_stalled(&KbctlError::Runtime(
            "agent kind is unavailable".to_string()
        )));
    }

    #[test]
    fn uses_herdr_interrupt_key_syntax() {
        assert_eq!(HERDR_INTERRUPT_KEY, "ctrl+c");
    }

    #[test]
    fn treats_missing_agent_as_gone() {
        assert!(agent_is_gone(&KbctlError::Runtime(
            "{\"error\":{\"code\":\"agent_not_found\"}}".to_string()
        )));
        assert!(!agent_is_gone(&KbctlError::Runtime(
            "agent is blocked".to_string()
        )));
    }

    #[test]
    fn recognizes_live_and_exited_agent_processes() {
        let live = serde_json::json!({
            "result": {"process_info": {"foreground_processes": [{"name": "codex"}]}}
        });
        let shell = serde_json::json!({
            "result": {"process_info": {"foreground_processes": [{"name": "zsh"}]}}
        });
        let grok = serde_json::json!({
            "result": {"process_info": {"foreground_processes": [{"name": "grok"}]}}
        });
        assert!(foreground_agent_present(&live, "codex"));
        assert!(!foreground_agent_present(&shell, "codex"));
        assert!(foreground_agent_present(&grok, "grok"));
        assert!(!foreground_agent_present(&shell, "grok"));
    }
}
