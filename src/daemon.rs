use crate::{
    config::{Config, LocalProjectBinding},
    domain::{Execution, ExecutionMode, Task, TaskStatus, WorkContract},
    error::KbctlError,
    herdr::{AgentRuntime, RuntimeState},
    notion::{KanbanProvider, ProjectUpdate, TaskUpdate},
    report_spool,
    store::{PendingReport, Store},
};
use chrono::{Duration as ChronoDuration, Utc};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::Arc,
};
use tokio::time::{self, Duration, MissedTickBehavior};

pub struct Daemon {
    config: Config,
    provider: Arc<dyn KanbanProvider>,
    runtime: Arc<dyn AgentRuntime>,
    store: Store,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CycleSummary {
    pub tasks_seen: usize,
    pub dispatched: usize,
    pub reconciled: usize,
    pub reports_applied: usize,
}

impl Daemon {
    pub fn new(
        config: Config,
        provider: Arc<dyn KanbanProvider>,
        runtime: Arc<dyn AgentRuntime>,
        store: Store,
    ) -> Self {
        Self {
            config,
            provider,
            runtime,
            store,
        }
    }

    pub async fn run(&self) -> Result<(), KbctlError> {
        let _lock = self.store.acquire_daemon_lock()?;
        let interval_seconds = self.config.daemon.poll_interval_seconds.max(1);
        let mut interval = time::interval(Duration::from_secs(interval_seconds));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.run_once().await {
                        Ok(summary) => {
                            tracing::info!(
                                tasks_seen = summary.tasks_seen,
                                dispatched = summary.dispatched,
                                reconciled = summary.reconciled,
                                reports_applied = summary.reports_applied,
                                "daemon sync cycle"
                            );
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "daemon cycle failed");
                        }
                    }
                }
                signal = tokio::signal::ctrl_c() => {
                    signal.map_err(|error| KbctlError::Runtime(format!("listen for shutdown: {error}")))?;
                    return Ok(());
                }
            }
        }
    }

    pub async fn run_once(&self) -> Result<CycleSummary, KbctlError> {
        let reconciled_tasks = self.reconcile_running().await?;
        let mut summary = CycleSummary {
            reports_applied: self.flush_pending_reports().await?,
            ..Default::default()
        };
        summary.reconciled = reconciled_tasks.len();
        if let Err(error) = self.provider.schema_is_current().await {
            tracing::warn!(error = %error, "schema check blocks new dispatch");
            return Err(error);
        }
        let tasks = self.provider.list_tasks().await?;
        self.store.cache_tasks(&tasks)?;
        summary.tasks_seen = tasks.len();
        let task_projects = tasks
            .iter()
            .map(|task| {
                (
                    task.id.clone(),
                    task.project_id
                        .clone()
                        .unwrap_or_else(|| "__implicit__".to_string()),
                )
            })
            .collect::<HashMap<_, _>>();
        let running_executions = self.store.running_executions()?;
        let mut running_projects = running_executions
            .iter()
            .filter_map(|execution| task_projects.get(&execution.task_id).cloned())
            .collect::<HashSet<_>>();
        let running_count = running_executions.len();
        if running_count >= self.config.daemon.max_concurrency.max(1) {
            return Ok(summary);
        }
        for task in tasks {
            if running_count + summary.dispatched >= self.config.daemon.max_concurrency.max(1) {
                break;
            }
            if !task.status.is_dispatchable(Utc::now(), task.scheduled_at) {
                continue;
            }
            if reconciled_tasks.contains(&task.id) {
                continue;
            }
            if task.execution_id.is_some() || self.store.execution_for_task(&task.id)?.is_some() {
                continue;
            }
            if let Some(retry) = self.store.retry_for_task(&task.id)?
                && retry.retry_at.is_some_and(|retry_at| retry_at > Utc::now())
            {
                continue;
            }
            let project_key = task
                .project_id
                .clone()
                .unwrap_or_else(|| "__implicit__".to_string());
            if running_projects.contains(&project_key) {
                continue;
            }
            match self.dispatch(&task).await {
                Ok(()) => {
                    running_projects.insert(project_key);
                    summary.dispatched += 1;
                }
                Err(error) => {
                    tracing::warn!(task_id = %task.id, error = %error, "task dispatch failed");
                }
            }
        }
        Ok(summary)
    }

    pub async fn flush_reports_once(&self) -> Result<usize, KbctlError> {
        self.flush_pending_reports().await
    }

    async fn dispatch(&self, task: &Task) -> Result<(), KbctlError> {
        let task = if task.body.is_some() {
            task.clone()
        } else {
            let detailed = self.provider.get_task(&task.id).await?;
            if !detailed
                .status
                .is_dispatchable(Utc::now(), detailed.scheduled_at)
            {
                return Err(KbctlError::Validation(format!(
                    "task {} is no longer dispatchable",
                    detailed.id
                )));
            }
            detailed
        };
        let binding = self.binding_for(&task)?;
        validate_task(&task, &binding)?;
        let mode = if task.status == TaskStatus::Triage {
            ExecutionMode::Triage
        } else {
            ExecutionMode::Execute
        };
        let agent_kind = task
            .agent
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .or(Some(binding.default_agent.as_str()))
            .unwrap_or("codex")
            .to_string();
        let attempt = self.store.next_execution_attempt(&task.id)?;
        let execution = Execution::new_with_attempt(&task.id, &agent_kind, mode, attempt);
        self.store.save_execution(&execution)?;
        if let Err(error) = self
            .provider
            .update_task(TaskUpdate {
                id: task.id.clone(),
                status: Some(TaskStatus::Running),
                execution_id: Some(execution.id.clone()),
                ..Default::default()
            })
            .await
        {
            self.store
                .mark_execution_state(&execution.id, "dispatch_failed")?;
            return Err(error);
        }
        let mut running_task = task.clone();
        running_task.status = TaskStatus::Running;
        running_task.execution_id = Some(execution.id.clone());
        self.store.cache_task(&running_task)?;
        let contract = WorkContract {
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            mode,
            title: task.name.clone(),
            body: task.body.clone().unwrap_or_default(),
            project_name: binding.name.clone(),
            project_path: binding.path.clone(),
            due: task.due,
            scheduled_at: task.scheduled_at,
            agent_kind,
            report_command: format!(
                "kbctl report <done|blocked|review> --execution {}",
                execution.id
            ),
        };
        let runtime_id = match self.runtime.start(&execution, &contract).await {
            Ok(value) => value,
            Err(error) => {
                self.store
                    .mark_execution_state(&execution.id, "runtime_failed")?;
                let reason = format!("Herdr could not start the agent: {error}");
                let _ = self
                    .provider
                    .update_task(TaskUpdate {
                        id: task.id.clone(),
                        status: Some(TaskStatus::Blocked),
                        clear_execution_id: true,
                        result: Some(reason.clone()),
                        ..Default::default()
                    })
                    .await;
                let mut blocked_task = task.clone();
                blocked_task.status = TaskStatus::Blocked;
                blocked_task.execution_id = None;
                blocked_task.result = Some(reason);
                self.store.cache_task(&blocked_task)?;
                return Err(error);
            }
        };
        self.store.set_runtime_id(&execution.id, &runtime_id)?;
        Ok(())
    }

    async fn reconcile_running(&self) -> Result<HashSet<String>, KbctlError> {
        let mut reconciled_tasks = HashSet::new();
        for execution in self.store.running_executions()? {
            let task = self.provider.get_task(&execution.task_id).await?;
            if matches!(task.status, TaskStatus::Cancel | TaskStatus::Archived) {
                if let Some(runtime_id) = execution.runtime_id.as_deref()
                    && let Err(error) = self.runtime.cancel(runtime_id).await
                {
                    tracing::warn!(execution_id = %execution.id, error = %error, "cancel Herdr execution failed");
                }
                self.provider
                    .update_task(TaskUpdate {
                        id: task.id.clone(),
                        clear_execution_id: true,
                        ..Default::default()
                    })
                    .await?;
                let mut cancelled_task = task;
                cancelled_task.execution_id = None;
                self.store.cache_task(&cancelled_task)?;
                self.store
                    .mark_execution_state(&execution.id, "cancelled")?;
                reconciled_tasks.insert(execution.task_id);
                continue;
            }
            if self.store.report(&execution.id)?.is_some() {
                continue;
            }
            match self.ingest_spooled_report(&task, &execution) {
                Ok(true) => {
                    reconciled_tasks.insert(execution.task_id);
                    continue;
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(
                        execution_id = %execution.id,
                        error = %error,
                        "agent report spool could not be ingested"
                    );
                }
            }
            let state = match execution.runtime_id.as_deref() {
                Some(runtime_id) => match self.runtime.inspect(runtime_id).await {
                    Ok(state) => state,
                    Err(error) => {
                        tracing::warn!(execution_id = %execution.id, error = %error, "inspect Herdr execution failed");
                        continue;
                    }
                },
                None => RuntimeState::Done,
            };
            if !matches!(state, RuntimeState::Done) {
                continue;
            }
            if execution.attempt < self.config.daemon.max_attempts.max(1) {
                let retry_at = Utc::now()
                    + ChronoDuration::seconds(self.config.daemon.retry_delay_seconds as i64);
                let message = format!(
                    "Agent ended without a valid kbctl report; retrying attempt {} at {}",
                    execution.attempt.saturating_add(1),
                    retry_at.to_rfc3339()
                );
                self.provider
                    .update_task(TaskUpdate {
                        id: task.id.clone(),
                        status: Some(TaskStatus::Ready),
                        clear_execution_id: true,
                        result: Some(message.clone()),
                        ..Default::default()
                    })
                    .await?;
                let mut retry_task = task;
                retry_task.status = TaskStatus::Ready;
                retry_task.execution_id = None;
                retry_task.result = Some(message);
                self.store.cache_task(&retry_task)?;
                self.store.mark_execution_retry(&execution.id, retry_at)?;
                reconciled_tasks.insert(execution.task_id);
            } else {
                let message = format!(
                    "Agent ended without a valid kbctl report after {} attempts",
                    execution.attempt
                );
                self.provider
                    .update_task(TaskUpdate {
                        id: task.id.clone(),
                        status: Some(TaskStatus::Review),
                        clear_execution_id: true,
                        result: Some(message.clone()),
                        ..Default::default()
                    })
                    .await?;
                let mut review_task = task;
                review_task.status = TaskStatus::Review;
                review_task.execution_id = None;
                review_task.result = Some(message);
                self.store.cache_task(&review_task)?;
                self.store
                    .mark_execution_state(&execution.id, "ended_without_report")?;
                reconciled_tasks.insert(execution.task_id);
            }
        }
        Ok(reconciled_tasks)
    }

    fn ingest_spooled_report(
        &self,
        task: &Task,
        execution: &Execution,
    ) -> Result<bool, KbctlError> {
        let binding = self.binding_for(task)?;
        let path = report_spool::path_for(Path::new(&binding.path), &execution.id);
        if !path.is_file() {
            return Ok(false);
        }
        let spooled = report_spool::read(&path)?;
        if spooled.task_id != task.id {
            return Err(KbctlError::Validation(format!(
                "report spool task {} does not match execution task {}",
                spooled.task_id, task.id
            )));
        }
        if spooled.report.execution_id != execution.id {
            return Err(KbctlError::Validation(format!(
                "report spool execution {} does not match {}",
                spooled.report.execution_id, execution.id
            )));
        }
        spooled
            .report
            .validate(execution.mode)
            .map_err(KbctlError::Validation)?;
        self.store
            .record_report(&task.id, &spooled.report, &spooled.result_text)?;
        fs::remove_file(&path).map_err(|error| {
            KbctlError::State(format!("remove report spool {}: {error}", path.display()))
        })?;
        Ok(true)
    }

    async fn flush_pending_reports(&self) -> Result<usize, KbctlError> {
        let mut applied = 0;
        for pending in self.store.pending_reports()? {
            match self.apply_report(&pending).await {
                Ok(()) => {
                    self.store.mark_report_applied(&pending.execution_id)?;
                    applied += 1;
                }
                Err(error) => {
                    tracing::warn!(execution_id = %pending.execution_id, error = %error, "report remains in outbox");
                    if let Err(state_error) = self
                        .store
                        .mark_report_failed(&pending.execution_id, &error.to_string())
                    {
                        tracing::warn!(execution_id = %pending.execution_id, error = %state_error, "record report retry failure failed");
                    }
                }
            }
        }
        Ok(applied)
    }

    async fn apply_report(&self, pending: &PendingReport) -> Result<(), KbctlError> {
        let task = self.provider.get_task(&pending.task_id).await?;
        let execution = self
            .store
            .execution(&pending.execution_id)?
            .ok_or_else(|| {
                KbctlError::State(format!("execution {} was not found", pending.execution_id))
            })?;
        pending
            .report
            .validate(execution.mode)
            .map_err(KbctlError::Validation)?;
        let result_summary = pending
            .report
            .summary
            .as_deref()
            .or(pending.report.reason.as_deref())
            .unwrap_or(&pending.result_text)
            .to_string();
        let already_updated = task.execution_id.is_none()
            && task.status == pending.report.status
            && task.result.as_deref() == Some(result_summary.as_str());
        if already_updated {
            return Ok(());
        }
        if task.execution_id.as_deref() != Some(pending.execution_id.as_str()) {
            return Err(KbctlError::Validation(format!(
                "task {} is not currently owned by execution {}",
                task.id, pending.execution_id
            )));
        }
        self.provider
            .append_result(&task.id, &pending.result_text)
            .await?;
        self.provider
            .update_task(TaskUpdate {
                id: task.id.clone(),
                status: Some(pending.report.status),
                clear_execution_id: true,
                result: Some(result_summary.clone()),
                ..Default::default()
            })
            .await?;
        if let Some(project_id) = task.project_id.clone() {
            self.provider
                .update_project(ProjectUpdate {
                    id: project_id,
                    last_activity: Some(Utc::now()),
                    ..Default::default()
                })
                .await?;
        }
        let mut completed_task = task;
        completed_task.status = pending.report.status;
        completed_task.execution_id = None;
        completed_task.result = Some(result_summary);
        self.store.cache_task(&completed_task)?;
        Ok(())
    }

    fn binding_for(&self, task: &Task) -> Result<LocalProjectBinding, KbctlError> {
        self.config
            .project_binding(task.project_id.as_deref())
            .cloned()
            .ok_or_else(|| {
                KbctlError::Validation(format!("task {} has no local Project binding", task.id))
            })
    }
}

fn validate_task(task: &Task, binding: &LocalProjectBinding) -> Result<(), KbctlError> {
    if task.name.trim().is_empty() {
        return Err(KbctlError::Validation(format!(
            "task {} has no Name",
            task.id
        )));
    }
    if task.due.is_none() {
        return Err(KbctlError::Validation(format!(
            "task {} has no Due date",
            task.id
        )));
    }
    if !binding.active {
        return Err(KbctlError::Validation(format!(
            "Project {} is inactive",
            binding.name
        )));
    }
    let path = Path::new(&binding.path);
    if !path.is_dir() {
        return Err(KbctlError::Validation(format!(
            "Project path is not a directory: {}",
            binding.path
        )));
    }
    Ok(())
}

impl std::fmt::Debug for Daemon {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Daemon")
            .field("config", &self.config)
            .field("store", &self.store.path())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{DaemonConfig, HerdrConfig, LocalProjectBinding, NotionConfig, ProjectConfig},
        domain::{Report, Task},
        herdr::RuntimeState,
        notion::ProjectUpdate,
    };
    use async_trait::async_trait;
    use chrono::{Duration as ChronoDuration, Utc};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct FakeProvider {
        tasks: Arc<Mutex<Vec<Task>>>,
        appended: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl KanbanProvider for FakeProvider {
        async fn discover_schema(
            &self,
            _target: crate::notion::DatabaseTarget,
        ) -> Result<crate::domain::SchemaSnapshot, KbctlError> {
            Ok(crate::domain::SchemaSnapshot::default())
        }

        async fn list_tasks(&self) -> Result<Vec<Task>, KbctlError> {
            Ok(self.tasks.lock().unwrap().clone())
        }

        async fn get_task(&self, id: &str) -> Result<Task, KbctlError> {
            self.tasks
                .lock()
                .unwrap()
                .iter()
                .find(|task| task.id == id)
                .cloned()
                .ok_or_else(|| KbctlError::Notion("task not found".to_string()))
        }

        async fn update_task(&self, update: TaskUpdate) -> Result<(), KbctlError> {
            let mut tasks = self.tasks.lock().unwrap();
            let task = tasks
                .iter_mut()
                .find(|task| task.id == update.id)
                .ok_or_else(|| KbctlError::Notion("task not found".to_string()))?;
            if let Some(status) = update.status {
                task.status = status;
            }
            if let Some(execution_id) = update.execution_id {
                task.execution_id = Some(execution_id);
            }
            if update.clear_execution_id {
                task.execution_id = None;
            }
            if let Some(result) = update.result {
                task.result = Some(result);
            }
            Ok(())
        }

        async fn append_result(&self, _id: &str, result: &str) -> Result<(), KbctlError> {
            self.appended.lock().unwrap().push(result.to_string());
            Ok(())
        }

        async fn update_project(&self, _update: ProjectUpdate) -> Result<(), KbctlError> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct AppendOnceProvider {
        inner: FakeProvider,
        fail_append_once: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl KanbanProvider for AppendOnceProvider {
        async fn discover_schema(
            &self,
            target: crate::notion::DatabaseTarget,
        ) -> Result<crate::domain::SchemaSnapshot, KbctlError> {
            self.inner.discover_schema(target).await
        }

        async fn list_tasks(&self) -> Result<Vec<Task>, KbctlError> {
            self.inner.list_tasks().await
        }

        async fn get_task(&self, id: &str) -> Result<Task, KbctlError> {
            self.inner.get_task(id).await
        }

        async fn update_task(&self, update: TaskUpdate) -> Result<(), KbctlError> {
            self.inner.update_task(update).await
        }

        async fn append_result(&self, id: &str, result: &str) -> Result<(), KbctlError> {
            let should_fail = {
                let mut fail = self.fail_append_once.lock().unwrap();
                if *fail {
                    *fail = false;
                    true
                } else {
                    false
                }
            };
            if should_fail {
                return Err(KbctlError::Notion("simulated append failure".to_string()));
            }
            self.inner.append_result(id, result).await
        }

        async fn update_project(&self, update: ProjectUpdate) -> Result<(), KbctlError> {
            self.inner.update_project(update).await
        }
    }

    #[derive(Clone)]
    struct FakeRuntime {
        state: Arc<Mutex<RuntimeState>>,
    }

    #[derive(Clone)]
    struct ContractRuntime {
        state: Arc<Mutex<RuntimeState>>,
        body: Arc<Mutex<Option<String>>>,
    }

    #[async_trait]
    impl AgentRuntime for FakeRuntime {
        async fn start(
            &self,
            _execution: &Execution,
            _contract: &WorkContract,
        ) -> Result<String, KbctlError> {
            *self.state.lock().unwrap() = RuntimeState::Working;
            Ok("fake-runtime".to_string())
        }

        async fn inspect(&self, _runtime_id: &str) -> Result<RuntimeState, KbctlError> {
            Ok(*self.state.lock().unwrap())
        }
        async fn focus(&self, _runtime_id: &str) -> Result<(), KbctlError> {
            Ok(())
        }
        async fn cancel(&self, _runtime_id: &str) -> Result<(), KbctlError> {
            *self.state.lock().unwrap() = RuntimeState::Done;
            Ok(())
        }
    }

    #[async_trait]
    impl AgentRuntime for ContractRuntime {
        async fn start(
            &self,
            _execution: &Execution,
            contract: &WorkContract,
        ) -> Result<String, KbctlError> {
            *self.body.lock().unwrap() = Some(contract.body.clone());
            *self.state.lock().unwrap() = RuntimeState::Working;
            Ok("contract-runtime".to_string())
        }

        async fn inspect(&self, _runtime_id: &str) -> Result<RuntimeState, KbctlError> {
            Ok(*self.state.lock().unwrap())
        }

        async fn focus(&self, _runtime_id: &str) -> Result<(), KbctlError> {
            Ok(())
        }

        async fn cancel(&self, _runtime_id: &str) -> Result<(), KbctlError> {
            *self.state.lock().unwrap() = RuntimeState::Done;
            Ok(())
        }
    }

    #[derive(Clone)]
    struct BodyProvider {
        inner: FakeProvider,
        body: String,
    }

    #[async_trait]
    impl KanbanProvider for BodyProvider {
        async fn discover_schema(
            &self,
            target: crate::notion::DatabaseTarget,
        ) -> Result<crate::domain::SchemaSnapshot, KbctlError> {
            self.inner.discover_schema(target).await
        }

        async fn list_tasks(&self) -> Result<Vec<Task>, KbctlError> {
            self.inner.list_tasks().await
        }

        async fn get_task(&self, id: &str) -> Result<Task, KbctlError> {
            let mut task = self.inner.get_task(id).await?;
            task.body = Some(self.body.clone());
            Ok(task)
        }

        async fn update_task(&self, update: TaskUpdate) -> Result<(), KbctlError> {
            self.inner.update_task(update).await
        }

        async fn append_result(&self, id: &str, result: &str) -> Result<(), KbctlError> {
            self.inner.append_result(id, result).await
        }

        async fn update_project(&self, update: ProjectUpdate) -> Result<(), KbctlError> {
            self.inner.update_project(update).await
        }
    }

    fn config(path: &str) -> Config {
        Config {
            notion: NotionConfig::default(),
            mapping: Default::default(),
            project: ProjectConfig {
                default: Some(LocalProjectBinding {
                    id: "__implicit__".to_string(),
                    name: "Test".to_string(),
                    path: path.to_string(),
                    default_agent: "codex".to_string(),
                    active: true,
                }),
                bindings: Default::default(),
            },
            daemon: DaemonConfig {
                poll_interval_seconds: 15,
                max_concurrency: 1,
                max_attempts: 3,
                retry_delay_seconds: 15,
            },
            herdr: HerdrConfig::default(),
        }
    }

    fn task(status: TaskStatus, due: Option<chrono::DateTime<Utc>>) -> Task {
        Task {
            id: "task-1".to_string(),
            name: "Test task".to_string(),
            status,
            due,
            scheduled_at: Some(Utc::now() - ChronoDuration::minutes(1)),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn ready_report_done_writes_back_once() {
        let directory = tempfile::tempdir().unwrap();
        let provider = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![task(
                TaskStatus::Ready,
                Some(Utc::now() + ChronoDuration::hours(1)),
            )])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let runtime = FakeRuntime {
            state: Arc::new(Mutex::new(RuntimeState::Working)),
        };
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider.clone()),
            Arc::new(runtime),
            store.clone(),
        );
        let summary = daemon.run_once().await.unwrap();
        assert_eq!(summary.dispatched, 1);
        let running = provider.tasks.lock().unwrap()[0].clone();
        let execution_id = running.execution_id.clone().unwrap();
        let report = Report {
            execution_id: execution_id.clone(),
            status: TaskStatus::Done,
            summary: Some("finished".to_string()),
            reason: None,
            result_file: None,
            reported_at: Utc::now(),
        };
        assert!(
            store
                .record_report("task-1", &report, "kbctl-execution:".to_string().as_str())
                .unwrap()
        );
        let applied = daemon.flush_reports_once().await.unwrap();
        assert_eq!(applied, 1);
        let finished = provider.tasks.lock().unwrap()[0].clone();
        assert_eq!(finished.status, TaskStatus::Done);
        assert!(finished.execution_id.is_none());
        assert_eq!(finished.result.as_deref(), Some("finished"));
        assert_eq!(provider.appended.lock().unwrap().len(), 1);
        assert_eq!(daemon.flush_reports_once().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn dispatch_loads_full_task_body_before_starting_agent() {
        let directory = tempfile::tempdir().unwrap();
        let inner = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![task(
                TaskStatus::Ready,
                Some(Utc::now() + ChronoDuration::hours(1)),
            )])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let provider = BodyProvider {
            inner,
            body: "page body from Notion".to_string(),
        };
        let body = Arc::new(Mutex::new(None));
        let runtime = ContractRuntime {
            state: Arc::new(Mutex::new(RuntimeState::Working)),
            body: body.clone(),
        };
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider),
            Arc::new(runtime),
            Store::open(directory.path().join("state.db")).unwrap(),
        );

        assert_eq!(daemon.run_once().await.unwrap().dispatched, 1);
        assert_eq!(
            body.lock().unwrap().as_deref(),
            Some("page body from Notion")
        );
    }

    #[tokio::test]
    async fn ingests_agent_report_spool_without_global_state_access() {
        let directory = tempfile::tempdir().unwrap();
        let provider = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![task(
                TaskStatus::Ready,
                Some(Utc::now() + ChronoDuration::hours(1)),
            )])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let runtime = FakeRuntime {
            state: Arc::new(Mutex::new(RuntimeState::Working)),
        };
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider.clone()),
            Arc::new(runtime),
            store.clone(),
        );

        assert_eq!(daemon.run_once().await.unwrap().dispatched, 1);
        let execution_id = provider.tasks.lock().unwrap()[0]
            .execution_id
            .clone()
            .unwrap();
        let report = report_spool::AgentReport {
            task_id: "task-1".to_string(),
            report: Report {
                execution_id: execution_id.clone(),
                status: TaskStatus::Done,
                summary: Some("finished".to_string()),
                reason: None,
                result_file: None,
                reported_at: Utc::now(),
            },
            result_text: "finished".to_string(),
        };
        let report_path = report_spool::path_for(directory.path(), &execution_id);
        report_spool::write(&report_path, &report).unwrap();

        let summary = daemon.run_once().await.unwrap();
        assert_eq!(summary.reports_applied, 1);
        assert!(!report_path.exists());
        let completed = provider.tasks.lock().unwrap()[0].clone();
        assert_eq!(completed.status, TaskStatus::Done);
        assert!(completed.execution_id.is_none());
        assert!(store.report(&execution_id).unwrap().unwrap().2);
    }

    #[tokio::test]
    async fn report_writeback_retries_after_partial_append_failure() {
        let directory = tempfile::tempdir().unwrap();
        let inner = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![task(
                TaskStatus::Ready,
                Some(Utc::now() + ChronoDuration::hours(1)),
            )])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let provider = AppendOnceProvider {
            inner: inner.clone(),
            fail_append_once: Arc::new(Mutex::new(true)),
        };
        let runtime = FakeRuntime {
            state: Arc::new(Mutex::new(RuntimeState::Working)),
        };
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider),
            Arc::new(runtime),
            store.clone(),
        );
        assert_eq!(daemon.run_once().await.unwrap().dispatched, 1);
        let execution_id = inner.tasks.lock().unwrap()[0].execution_id.clone().unwrap();
        let report = Report {
            execution_id: execution_id.clone(),
            status: TaskStatus::Done,
            summary: Some("finished".to_string()),
            reason: None,
            result_file: None,
            reported_at: Utc::now(),
        };
        assert!(store.record_report("task-1", &report, "result").unwrap());
        assert_eq!(daemon.flush_reports_once().await.unwrap(), 0);
        assert_eq!(inner.tasks.lock().unwrap()[0].status, TaskStatus::Running);
        assert_eq!(daemon.flush_reports_once().await.unwrap(), 1);
        assert_eq!(inner.tasks.lock().unwrap()[0].status, TaskStatus::Done);
        assert_eq!(inner.appended.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn already_written_notion_report_is_not_appended_again() {
        let directory = tempfile::tempdir().unwrap();
        let execution = Execution::new("task-1", "codex", ExecutionMode::Execute);
        let mut done_task = task(
            TaskStatus::Done,
            Some(Utc::now() + ChronoDuration::hours(1)),
        );
        done_task.result = Some("finished".to_string());
        let provider = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![done_task])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let store = Store::open(directory.path().join("state.db")).unwrap();
        store.save_execution(&execution).unwrap();
        let report = Report {
            execution_id: execution.id.clone(),
            status: TaskStatus::Done,
            summary: Some("finished".to_string()),
            reason: None,
            result_file: None,
            reported_at: Utc::now(),
        };
        assert!(store.record_report("task-1", &report, "result").unwrap());
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider.clone()),
            Arc::new(FakeRuntime {
                state: Arc::new(Mutex::new(RuntimeState::Done)),
            }),
            store,
        );
        assert_eq!(daemon.flush_reports_once().await.unwrap(), 1);
        assert!(provider.appended.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn exited_agent_is_retried_after_backoff() {
        let directory = tempfile::tempdir().unwrap();
        let execution = Execution::new("task-1", "codex", ExecutionMode::Execute);
        let mut running_task = task(
            TaskStatus::Running,
            Some(Utc::now() + ChronoDuration::hours(1)),
        );
        running_task.execution_id = Some(execution.id.clone());
        let provider = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![running_task])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let runtime_state = Arc::new(Mutex::new(RuntimeState::Done));
        let runtime = FakeRuntime {
            state: runtime_state.clone(),
        };
        let store = Store::open(directory.path().join("state.db")).unwrap();
        store.save_execution(&execution).unwrap();
        store.set_runtime_id(&execution.id, "fake-runtime").unwrap();
        let mut daemon_config = config(directory.path().to_str().unwrap());
        daemon_config.daemon.retry_delay_seconds = 0;
        let daemon = Daemon::new(
            daemon_config,
            Arc::new(provider.clone()),
            Arc::new(runtime),
            store.clone(),
        );

        assert_eq!(daemon.run_once().await.unwrap().dispatched, 0);
        let retry_task = provider.tasks.lock().unwrap()[0].clone();
        assert_eq!(retry_task.status, TaskStatus::Ready);
        assert!(retry_task.execution_id.is_none());
        assert!(store.retry_for_task("task-1").unwrap().is_some());
        assert_eq!(store.execution(&execution.id).unwrap().unwrap().attempt, 1);

        assert_eq!(daemon.run_once().await.unwrap().dispatched, 1);
        let retried_task = provider.tasks.lock().unwrap()[0].clone();
        assert_eq!(retried_task.status, TaskStatus::Running);
        let retried_execution = store.execution_for_task("task-1").unwrap().unwrap();
        assert_eq!(retried_execution.attempt, 2);
        assert_eq!(*runtime_state.lock().unwrap(), RuntimeState::Working);
    }

    #[tokio::test]
    async fn cancelled_task_exit_is_not_retried() {
        let directory = tempfile::tempdir().unwrap();
        let execution = Execution::new("task-1", "codex", ExecutionMode::Execute);
        let mut cancelled_task = task(
            TaskStatus::Cancel,
            Some(Utc::now() + ChronoDuration::hours(1)),
        );
        cancelled_task.execution_id = Some(execution.id.clone());
        let provider = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![cancelled_task])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let runtime = FakeRuntime {
            state: Arc::new(Mutex::new(RuntimeState::Working)),
        };
        let store = Store::open(directory.path().join("state.db")).unwrap();
        store.save_execution(&execution).unwrap();
        store.set_runtime_id(&execution.id, "fake-runtime").unwrap();
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider.clone()),
            Arc::new(runtime),
            store.clone(),
        );

        let summary = daemon.run_once().await.unwrap();
        assert_eq!(summary.dispatched, 0);
        assert_eq!(summary.reconciled, 1);
        let task = provider.tasks.lock().unwrap()[0].clone();
        assert_eq!(task.status, TaskStatus::Cancel);
        assert!(task.execution_id.is_none());
        assert!(store.retry_for_task("task-1").unwrap().is_none());
        assert_eq!(
            store.execution(&execution.id).unwrap().unwrap().runtime_id,
            Some("fake-runtime".to_string())
        );
    }

    #[tokio::test]
    async fn orphaned_execution_without_runtime_is_retried() {
        let directory = tempfile::tempdir().unwrap();
        let execution = Execution::new("task-1", "codex", ExecutionMode::Execute);
        let mut running_task = task(
            TaskStatus::Running,
            Some(Utc::now() + ChronoDuration::hours(1)),
        );
        running_task.execution_id = Some(execution.id.clone());
        let provider = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![running_task])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let runtime = FakeRuntime {
            state: Arc::new(Mutex::new(RuntimeState::Working)),
        };
        let store = Store::open(directory.path().join("state.db")).unwrap();
        store.save_execution(&execution).unwrap();
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider.clone()),
            Arc::new(runtime),
            store.clone(),
        );

        let summary = daemon.run_once().await.unwrap();
        assert_eq!(summary.dispatched, 0);
        assert_eq!(summary.reconciled, 1);
        assert_eq!(provider.tasks.lock().unwrap()[0].status, TaskStatus::Ready);
        assert!(store.retry_for_task("task-1").unwrap().is_some());
    }

    #[tokio::test]
    async fn exhausted_attempts_enter_review_without_retry() {
        let directory = tempfile::tempdir().unwrap();
        let execution = Execution::new_with_attempt("task-1", "codex", ExecutionMode::Execute, 3);
        let mut running_task = task(
            TaskStatus::Running,
            Some(Utc::now() + ChronoDuration::hours(1)),
        );
        running_task.execution_id = Some(execution.id.clone());
        let provider = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![running_task])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let runtime = FakeRuntime {
            state: Arc::new(Mutex::new(RuntimeState::Done)),
        };
        let store = Store::open(directory.path().join("state.db")).unwrap();
        store.save_execution(&execution).unwrap();
        store.set_runtime_id(&execution.id, "fake-runtime").unwrap();
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider.clone()),
            Arc::new(runtime),
            store.clone(),
        );

        let summary = daemon.run_once().await.unwrap();
        assert_eq!(summary.dispatched, 0);
        assert_eq!(summary.reconciled, 1);
        let task = provider.tasks.lock().unwrap()[0].clone();
        assert_eq!(task.status, TaskStatus::Review);
        assert!(task.execution_id.is_none());
        assert!(store.retry_for_task("task-1").unwrap().is_none());
        assert_eq!(store.execution(&execution.id).unwrap().unwrap().attempt, 3);
    }

    #[tokio::test]
    async fn missing_due_date_does_not_dispatch() {
        let directory = tempfile::tempdir().unwrap();
        let provider = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![task(TaskStatus::Ready, None)])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let runtime = FakeRuntime {
            state: Arc::new(Mutex::new(RuntimeState::Working)),
        };
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider.clone()),
            Arc::new(runtime),
            Store::open(directory.path().join("state.db")).unwrap(),
        );
        assert_eq!(daemon.run_once().await.unwrap().dispatched, 0);
        assert!(provider.tasks.lock().unwrap()[0].execution_id.is_none());
    }

    #[tokio::test]
    async fn scheduled_work_waits_until_due() {
        let directory = tempfile::tempdir().unwrap();
        let provider = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![Task {
                scheduled_at: Some(Utc::now() + ChronoDuration::hours(1)),
                ..task(
                    TaskStatus::Scheduled,
                    Some(Utc::now() + ChronoDuration::hours(2)),
                )
            }])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let runtime = FakeRuntime {
            state: Arc::new(Mutex::new(RuntimeState::Working)),
        };
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider.clone()),
            Arc::new(runtime),
            Store::open(directory.path().join("state.db")).unwrap(),
        );
        assert_eq!(daemon.run_once().await.unwrap().dispatched, 0);
    }
}
