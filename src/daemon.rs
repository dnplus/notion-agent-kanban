use crate::{
    config::{Config, LocalProjectBinding},
    domain::{
        Execution, ExecutionMode, ExecutionRole, OrchestrationRun, PlanState, SubmissionEnvelope,
        Task, TaskStatus, WorkContract, WorkItem, WorkItemState, WorkMode,
    },
    error::KbctlError,
    git_workspace,
    herdr::{AgentRuntime, RuntimeState},
    notion::{KanbanProvider, ProjectUpdate, TaskUpdate},
    orchestration, report_spool,
    store::{PendingReport, Store},
};
use chrono::{Duration as ChronoDuration, Utc};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
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
        for execution in self.store.running_executions()? {
            if let Some(runtime_id) = execution.runtime_id.as_deref() {
                self.runtime.watch(runtime_id)?;
            }
        }
        let mut runtime_events = self.runtime.subscribe();
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
                event = receive_runtime_event(&mut runtime_events) => {
                    if let Some(event) = event
                        && self.store.record_runtime_event("herdr", &event.event_id, &event.payload)?
                    {
                        tracing::info!(pane_id = %event.pane_id, state = ?event.state, "Herdr runtime event triggered reconciliation");
                        if let Err(error) = self.run_once().await {
                            tracing::warn!(error = %error, "event-driven daemon cycle failed");
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
        for task in &tasks {
            if self.store.orchestration_run(&task.id)?.is_some() {
                match self.advance_orchestration(task).await {
                    Ok(dispatched) => summary.dispatched += dispatched,
                    Err(error) => {
                        tracing::warn!(task_id = %task.id, error = %error, "orchestration advance failed")
                    }
                }
            }
        }
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
            if self.store.orchestration_run(&task.id)?.is_some() {
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
        let profile_name = if mode == ExecutionMode::Triage {
            self.config.orchestration.supervisor_profile.clone()
        } else {
            task.agent
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(binding.default_agent.as_str())
                .to_string()
        };
        let profile = self.config.profile(&profile_name).ok_or_else(|| {
            KbctlError::Validation(format!("unknown agent profile: {profile_name}"))
        })?;
        if mode == ExecutionMode::Triage && profile.role != ExecutionRole::Supervisor {
            return Err(KbctlError::Validation(format!(
                "triage profile {profile_name} is not a supervisor"
            )));
        }
        let agent_kind = profile.kind.clone();
        let attempt = self.store.next_execution_attempt(&task.id)?;
        let mut execution = Execution::new_with_attempt(&task.id, &agent_kind, mode, attempt);
        if mode == ExecutionMode::Triage {
            execution.role = ExecutionRole::Supervisor;
            execution.parent_task_id = Some(task.id.clone());
            execution.plan_version = Some(match self.store.orchestration_run(&task.id)? {
                Some(run)
                    if run.state == PlanState::Planning
                        && self.store.plan(&task.id, run.plan_version)?.is_none() =>
                {
                    run.plan_version
                }
                Some(run) => run.plan_version.saturating_add(1),
                None => 1,
            });
            self.store.save_orchestration_run(&OrchestrationRun {
                parent_task_id: task.id.clone(),
                plan_version: execution.plan_version.unwrap_or(1),
                state: PlanState::Planning,
                supervisor_execution_id: Some(execution.id.clone()),
                approved_plan_version: None,
                base_commit: None,
                base_branch: None,
                integration_branch: None,
                updated_at: Utc::now(),
            })?;
        }
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
        let contract_path = binding.path.clone();
        let submission_path =
            report_spool::submission_path_for(Path::new(&contract_path), &execution.id);
        execution.checkout_path = Some(contract_path.clone());
        execution.submission_path = Some(submission_path.display().to_string());
        if let Some(parent) = submission_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                KbctlError::State(format!(
                    "create submission spool directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        self.store.save_execution(&execution)?;
        let contract = WorkContract {
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            mode,
            title: task.name.clone(),
            body: task.body.clone().unwrap_or_default(),
            project_name: binding.name.clone(),
            project_path: contract_path,
            due: task.due,
            scheduled_at: task.scheduled_at,
            agent_kind,
            profile_name,
            role: execution.role,
            model: profile.model,
            reasoning: profile.reasoning,
            agent: profile.agent,
            read_only: false,
            plan_version: execution.plan_version,
            work_item_id: None,
            submission_path: submission_path.display().to_string(),
            report_command: format!(
                "kbctl report <done|blocked|review> --execution {}",
                execution.id
            ),
            runtime_group_id: self
                .store
                .runtime_group(&binding.id, self.runtime.runtime_kind())?,
            available_worker_profiles: if execution.role == ExecutionRole::Supervisor {
                self.config.worker_profiles()
            } else {
                Vec::new()
            },
        };
        let runtime_id = match self.runtime.ensure(&execution, &contract).await {
            Ok(value) => value,
            Err(error) => {
                self.store
                    .mark_execution_state(&execution.id, "runtime_failed")?;
                if execution.role == ExecutionRole::Supervisor
                    && let Some(mut run) = self.store.orchestration_run(&task.id)?
                {
                    run.supervisor_execution_id = None;
                    run.state = PlanState::Planning;
                    run.updated_at = Utc::now();
                    self.store.save_orchestration_run(&run)?;
                }
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
        self.remember_runtime_group(&binding.id, &runtime_id)?;
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
            if execution.role != ExecutionRole::Standalone {
                match self.ingest_spooled_submission(&execution) {
                    Ok(true) => {
                        reconciled_tasks.insert(execution.task_id.clone());
                        continue;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(execution_id = %execution.id, error = %error, "agent submission spool could not be ingested")
                    }
                }
                if self.store.execution_state(&execution.id)?.as_deref() != Some("running") {
                    continue;
                }
            } else {
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
            if !matches!(
                state,
                RuntimeState::Done | RuntimeState::Idle | RuntimeState::Unknown
            ) {
                continue;
            }
            if execution.role != ExecutionRole::Standalone {
                self.retry_or_block_orchestration(&task, &execution).await?;
                reconciled_tasks.insert(execution.task_id);
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

    fn ingest_spooled_submission(&self, execution: &Execution) -> Result<bool, KbctlError> {
        let task = self
            .store
            .cached_tasks()?
            .into_iter()
            .find(|task| task.id == execution.task_id)
            .ok_or_else(|| {
                KbctlError::State(format!("task {} is not cached", execution.task_id))
            })?;
        let binding = self.binding_for(&task)?;
        let path = execution
            .submission_path
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let root = execution
                    .checkout_path
                    .as_deref()
                    .unwrap_or(binding.path.as_str());
                report_spool::submission_path_for(Path::new(root), &execution.id)
            });
        if !path.is_file() {
            return Ok(false);
        }
        let spooled = report_spool::read_submission(&path)?;
        if spooled.execution_id != execution.id {
            return Err(KbctlError::Validation(format!(
                "submission execution {} does not match {}",
                spooled.execution_id, execution.id
            )));
        }
        orchestration::apply_submission(
            &self.config,
            &self.store,
            &execution.id,
            &spooled.envelope,
        )?;
        fs::remove_file(&path).map_err(|error| {
            KbctlError::State(format!(
                "remove submission spool {}: {error}",
                path.display()
            ))
        })?;
        self.store
            .mark_execution_state(&execution.id, "submitted")?;
        Ok(true)
    }

    async fn retry_or_block_orchestration(
        &self,
        task: &Task,
        execution: &Execution,
    ) -> Result<(), KbctlError> {
        if execution.attempt < self.config.daemon.max_attempts.max(1) {
            if let Some(work_item_id) = execution.work_item_id.as_deref() {
                if let Some(mut item) = self.store.work_item(work_item_id)? {
                    item.state = WorkItemState::Pending;
                    item.execution_id = None;
                    self.store.save_work_item(&item)?;
                }
            } else if let Some(mut run) = self.store.orchestration_run(&task.id)? {
                run.state = PlanState::Planning;
                run.supervisor_execution_id = None;
                run.updated_at = Utc::now();
                self.store.save_orchestration_run(&run)?;
                self.provider
                    .update_task(TaskUpdate {
                        id: task.id.clone(),
                        status: Some(TaskStatus::Triage),
                        clear_execution_id: true,
                        result: Some(
                            "supervisor exited without a valid submission; retrying".to_string(),
                        ),
                        ..Default::default()
                    })
                    .await?;
            }
            let retry_at =
                Utc::now() + ChronoDuration::seconds(self.config.daemon.retry_delay_seconds as i64);
            self.store.mark_execution_retry(&execution.id, retry_at)?;
            return Ok(());
        }
        if let Some(work_item_id) = execution.work_item_id.as_deref()
            && let Some(mut item) = self.store.work_item(work_item_id)?
        {
            item.state = WorkItemState::Failed;
            item.summary = Some("agent exited without a valid submission".to_string());
            self.store.save_work_item(&item)?;
        }
        if let Some(mut run) = self.store.orchestration_run(&task.id)? {
            run.state = PlanState::Blocked;
            run.updated_at = Utc::now();
            self.store.save_orchestration_run(&run)?;
        }
        self.provider
            .update_task(TaskUpdate {
                id: task.id.clone(),
                status: Some(TaskStatus::Review),
                clear_execution_id: true,
                result: Some(
                    "orchestration agent exhausted retries without a valid submission".to_string(),
                ),
                ..Default::default()
            })
            .await?;
        self.store
            .mark_execution_state(&execution.id, "ended_without_submission")?;
        Ok(())
    }

    async fn advance_orchestration(&self, task: &Task) -> Result<usize, KbctlError> {
        let Some(mut run) = self.store.orchestration_run(&task.id)? else {
            return Ok(0);
        };
        if task.status == TaskStatus::Done {
            return Ok(0);
        }
        if matches!(task.status, TaskStatus::Cancel | TaskStatus::Archived) {
            run.state = PlanState::Cancelled;
            run.updated_at = Utc::now();
            self.store.save_orchestration_run(&run)?;
            for mut item in self.store.work_items(&task.id, run.plan_version)? {
                if !matches!(item.state, WorkItemState::Merged | WorkItemState::Cancelled) {
                    item.state = WorkItemState::Cancelled;
                    self.store.save_work_item(&item)?;
                }
            }
            return Ok(0);
        }
        if run.state == PlanState::Planning && run.supervisor_execution_id.is_none() {
            if task.status == TaskStatus::Triage {
                self.dispatch(task).await?;
                return Ok(1);
            }
            return Ok(0);
        }
        if run.state == PlanState::AwaitingApproval && task.status == TaskStatus::Ready {
            run.approved_plan_version = Some(run.plan_version);
            run.state = PlanState::Executing;
            run.updated_at = Utc::now();
            self.store.save_orchestration_run(&run)?;
            self.provider
                .update_task(TaskUpdate {
                    id: task.id.clone(),
                    status: Some(TaskStatus::Running),
                    result: Some(format!("approved plan v{}", run.plan_version)),
                    ..Default::default()
                })
                .await?;
        }
        let Some(plan) = self.store.plan(&task.id, run.plan_version)? else {
            return Ok(0);
        };
        if run.state == PlanState::AwaitingApproval
            && !matches!(task.status, TaskStatus::Review | TaskStatus::Ready)
        {
            self.provider
                .append_result(
                    &task.id,
                    &format!("Plan v{}\n{}", plan.version, plan.summary),
                )
                .await?;
            self.provider
                .update_task(TaskUpdate {
                    id: task.id.clone(),
                    status: Some(TaskStatus::Review),
                    clear_execution_id: true,
                    result: Some(format!("plan v{} awaits approval", plan.version)),
                    ..Default::default()
                })
                .await?;
        }
        let mut items = self.store.work_items(&task.id, run.plan_version)?;
        for item in items
            .iter_mut()
            .filter(|item| item.state == WorkItemState::Accepted)
        {
            if let Err(error) = self.integrate_work_item(&mut run, item).await {
                item.state = WorkItemState::Blocked;
                item.summary = Some(error.to_string());
                run.state = PlanState::Blocked;
                run.updated_at = Utc::now();
                self.store.save_orchestration_run(&run)?;
            }
            self.store.save_work_item(item)?;
        }
        for item in items
            .iter_mut()
            .filter(|item| item.state == WorkItemState::Rework)
        {
            if item.review_round > self.config.orchestration.max_rework {
                item.state = WorkItemState::Blocked;
                run.state = PlanState::Blocked;
            } else {
                item.state = WorkItemState::Pending;
                item.execution_id = None;
                item.head_commit = None;
            }
            self.store.save_work_item(item)?;
        }
        for item in items
            .iter_mut()
            .filter(|item| item.state == WorkItemState::Submitted)
        {
            item.state = WorkItemState::Reviewing;
            self.store.save_work_item(item)?;
            if let Err(error) = self.request_work_item_review(&run, item).await {
                if let Some(execution_id) = run.supervisor_execution_id.as_deref() {
                    self.store
                        .mark_execution_state(execution_id, "prompt_failed")?;
                }
                item.state = WorkItemState::Blocked;
                item.summary = Some(error.to_string());
                run.state = PlanState::Blocked;
                run.updated_at = Utc::now();
                self.store.save_work_item(item)?;
                self.store.save_orchestration_run(&run)?;
                self.provider
                    .update_task(TaskUpdate {
                        id: task.id.clone(),
                        status: Some(TaskStatus::Blocked),
                        clear_execution_id: true,
                        result: Some(error.to_string()),
                        ..Default::default()
                    })
                    .await?;
                return Ok(0);
            }
        }
        items = self.store.work_items(&task.id, run.plan_version)?;
        if run.state == PlanState::Done {
            let result = match run.supervisor_execution_id.as_deref() {
                Some(execution_id) => match self.store.submission(execution_id)? {
                    Some(SubmissionEnvelope::Review { review }) if review.target_id == task.id => {
                        review.summary
                    }
                    _ => format!("plan v{} accepted", run.plan_version),
                },
                None => format!("plan v{} accepted", run.plan_version),
            };
            self.provider
                .append_result(
                    &task.id,
                    &format!(
                        "kbctl-orchestration:{}:v{}\n{}",
                        task.id, run.plan_version, result
                    ),
                )
                .await?;
            if let Some(project_id) = task.project_id.clone() {
                self.provider
                    .update_project(ProjectUpdate {
                        id: project_id,
                        last_activity: Some(run.updated_at),
                        ..Default::default()
                    })
                    .await?;
            }
            self.provider
                .update_task(TaskUpdate {
                    id: task.id.clone(),
                    status: Some(TaskStatus::Done),
                    clear_execution_id: true,
                    result: Some(result),
                    ..Default::default()
                })
                .await?;
            return Ok(0);
        }
        if run.state == PlanState::AwaitingMerge {
            self.provider
                .update_task(TaskUpdate {
                    id: task.id.clone(),
                    status: Some(TaskStatus::Review),
                    clear_execution_id: true,
                    result: Some(format!(
                        "integration branch {} awaits human merge",
                        run.integration_branch.as_deref().unwrap_or("missing")
                    )),
                    ..Default::default()
                })
                .await?;
            return Ok(0);
        }
        if !items.is_empty() && items.iter().all(|item| item.state == WorkItemState::Merged) {
            if run.state != PlanState::Reviewing
                && !matches!(run.state, PlanState::AwaitingMerge | PlanState::Done)
            {
                if let Err(error) = self.prepare_final_review(&run, task).await {
                    run.state = PlanState::Blocked;
                    run.updated_at = Utc::now();
                    self.store.save_orchestration_run(&run)?;
                    self.provider
                        .update_task(TaskUpdate {
                            id: task.id.clone(),
                            status: Some(TaskStatus::Blocked),
                            clear_execution_id: true,
                            result: Some(error.to_string()),
                            ..Default::default()
                        })
                        .await?;
                    return Ok(0);
                }
                run.state = PlanState::Reviewing;
                run.updated_at = Utc::now();
                self.store.save_orchestration_run(&run)?;
                if let Err(error) = self.request_final_review(&run, &items).await {
                    if let Some(execution_id) = run.supervisor_execution_id.as_deref() {
                        self.store
                            .mark_execution_state(execution_id, "prompt_failed")?;
                    }
                    run.state = PlanState::Blocked;
                    run.updated_at = Utc::now();
                    self.store.save_orchestration_run(&run)?;
                    self.provider
                        .update_task(TaskUpdate {
                            id: task.id.clone(),
                            status: Some(TaskStatus::Blocked),
                            clear_execution_id: true,
                            result: Some(error.to_string()),
                            ..Default::default()
                        })
                        .await?;
                }
            }
            return Ok(0);
        }
        if !matches!(
            run.state,
            PlanState::Executing | PlanState::AwaitingApproval
        ) {
            return Ok(0);
        }
        let approved = run.approved_plan_version == Some(run.plan_version);
        let active = items
            .iter()
            .filter(|item| {
                matches!(
                    item.state,
                    WorkItemState::Running | WorkItemState::Submitted | WorkItemState::Reviewing
                )
            })
            .count();
        let global_active = self.store.running_executions()?.len();
        let global_capacity = self
            .config
            .daemon
            .max_concurrency
            .max(1)
            .saturating_sub(global_active);
        let capacity = self
            .config
            .orchestration
            .max_workers_per_plan
            .saturating_sub(active)
            .min(global_capacity);
        let runnable = orchestration::runnable_items(&items, approved, capacity);
        let mut dispatched = 0;
        for id in runnable {
            let mut item = self
                .store
                .work_item(&id)?
                .ok_or_else(|| KbctlError::State(format!("runnable work item {id} disappeared")))?;
            self.dispatch_work_item(task, &plan.summary, &mut run, &mut item)
                .await?;
            self.store.save_work_item(&item)?;
            dispatched += 1;
        }
        self.store.save_orchestration_run(&run)?;
        Ok(dispatched)
    }

    async fn dispatch_work_item(
        &self,
        task: &Task,
        plan_summary: &str,
        run: &mut OrchestrationRun,
        item: &mut WorkItem,
    ) -> Result<(), KbctlError> {
        let binding = self.binding_for(task)?;
        let profile = self.config.profile(&item.step.profile).ok_or_else(|| {
            KbctlError::Validation(format!("unknown profile: {}", item.step.profile))
        })?;
        let mut checkout_path = binding.path.clone();
        let mut branch = None;
        if item.step.mode == WorkMode::Write {
            let snapshot = git_workspace::inspect(Path::new(&binding.path)).await?;
            if !snapshot.clean {
                return Err(KbctlError::Validation(
                    "write orchestration requires a clean Git repository".to_string(),
                ));
            }
            if run.base_commit.is_none() {
                run.base_commit = Some(snapshot.head.clone());
                run.base_branch = Some(snapshot.branch.clone());
                run.integration_branch = Some(git_workspace::integration_branch(
                    &task.id,
                    run.plan_version,
                ));
            }
            let root = worktree_root(&task.id, run.plan_version);
            let integration_path = root.join("integration");
            git_workspace::create_worktree(
                &snapshot.root,
                &integration_path,
                run.integration_branch.as_deref().unwrap(),
                run.base_commit.as_deref().unwrap(),
            )
            .await?;
            let worker_branch =
                git_workspace::worker_branch(&task.id, run.plan_version, &item.step.id);
            let worker_path = root.join(&item.step.id);
            git_workspace::create_worktree(
                &snapshot.root,
                &worker_path,
                &worker_branch,
                run.integration_branch.as_deref().unwrap(),
            )
            .await?;
            checkout_path = worker_path.display().to_string();
            branch = Some(worker_branch);
        }
        item.attempt = item.attempt.saturating_add(1);
        let mut execution = Execution::new_with_attempt(
            &task.id,
            &profile.kind,
            ExecutionMode::Execute,
            item.attempt,
        );
        execution.role = ExecutionRole::Worker;
        execution.parent_task_id = Some(task.id.clone());
        execution.work_item_id = Some(item.id.clone());
        execution.plan_version = Some(run.plan_version);
        execution.checkout_path = Some(checkout_path.clone());
        execution.branch = branch.clone();
        let submission_path =
            report_spool::submission_path_for(Path::new(&checkout_path), &execution.id);
        if let Some(parent) = submission_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                KbctlError::State(format!(
                    "create submission spool directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        execution.submission_path = Some(submission_path.display().to_string());
        self.store.save_execution(&execution)?;
        let body = format!(
            "Parent plan: {plan_summary}\nStep: {}\nObjective: {}\nAcceptance:\n- {}\nWrite scope:\n- {}",
            item.step.title,
            item.step.objective,
            item.step.acceptance.join("\n- "),
            item.step.write_scope.join("\n- ")
        );
        let contract = WorkContract {
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            mode: ExecutionMode::Execute,
            title: item.step.title.clone(),
            body,
            project_name: binding.name.clone(),
            project_path: checkout_path,
            due: task.due,
            scheduled_at: task.scheduled_at,
            agent_kind: profile.kind,
            profile_name: item.step.profile.clone(),
            role: ExecutionRole::Worker,
            model: profile.model,
            reasoning: profile.reasoning,
            agent: profile.agent,
            read_only: false,
            plan_version: Some(run.plan_version),
            work_item_id: Some(item.id.clone()),
            submission_path: submission_path.display().to_string(),
            report_command: format!(
                "kbctl report submit --execution {} --manifest <file>",
                execution.id
            ),
            runtime_group_id: self
                .store
                .runtime_group(&binding.id, self.runtime.runtime_kind())?,
            available_worker_profiles: Vec::new(),
        };
        let runtime_id = self.runtime.ensure(&execution, &contract).await?;
        self.store.set_runtime_id(&execution.id, &runtime_id)?;
        self.remember_runtime_group(&binding.id, &runtime_id)?;
        item.state = WorkItemState::Running;
        item.execution_id = Some(execution.id);
        item.branch = branch;
        item.checkout_path = execution.checkout_path;
        Ok(())
    }

    async fn request_work_item_review(
        &self,
        run: &OrchestrationRun,
        item: &WorkItem,
    ) -> Result<(), KbctlError> {
        let execution_id = run.supervisor_execution_id.as_deref().ok_or_else(|| {
            KbctlError::State("orchestration run has no supervisor execution".to_string())
        })?;
        let execution = self
            .store
            .execution(execution_id)?
            .ok_or_else(|| KbctlError::State("supervisor execution was not found".to_string()))?;
        let runtime_id = execution
            .runtime_id
            .as_deref()
            .ok_or_else(|| KbctlError::State("supervisor runtime was not started".to_string()))?;
        let prompt = format!(
            "Review work item {}. Summary: {}. Head commit: {}. Return a Review envelope targeting {}, review_round {}. Use accept, rework, or blocked.\n{}",
            item.step.title,
            item.summary.as_deref().unwrap_or("missing"),
            item.head_commit.as_deref().unwrap_or("none"),
            item.id,
            item.review_round.saturating_add(1),
            orchestration::submission_instruction(execution_id)
        );
        self.store.mark_execution_state(execution_id, "running")?;
        self.runtime.prompt(runtime_id, &prompt).await?;
        Ok(())
    }

    async fn request_final_review(
        &self,
        run: &OrchestrationRun,
        items: &[WorkItem],
    ) -> Result<(), KbctlError> {
        let execution_id = run.supervisor_execution_id.as_deref().ok_or_else(|| {
            KbctlError::State("orchestration run has no supervisor execution".to_string())
        })?;
        let execution = self
            .store
            .execution(execution_id)?
            .ok_or_else(|| KbctlError::State("supervisor execution was not found".to_string()))?;
        let runtime_id = execution
            .runtime_id
            .as_deref()
            .ok_or_else(|| KbctlError::State("supervisor runtime was not started".to_string()))?;
        let summaries = items
            .iter()
            .map(|item| {
                format!(
                    "{}: {}",
                    item.step.id,
                    item.summary.as_deref().unwrap_or("missing")
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        self.store.mark_execution_state(execution_id, "running")?;
        self.runtime
            .prompt(
                runtime_id,
                &format!(
                    "Perform final Parent review for {}. Integrated work:\n{}\nReturn a Review envelope targeting {}.\n{}",
                    run.parent_task_id,
                    summaries,
                    run.parent_task_id,
                    orchestration::submission_instruction(execution_id)
                ),
            )
            .await?;
        Ok(())
    }

    async fn prepare_final_review(
        &self,
        run: &OrchestrationRun,
        task: &Task,
    ) -> Result<(), KbctlError> {
        let Some(base_branch) = run.base_branch.as_deref() else {
            return Ok(());
        };
        let integration_path =
            worktree_root(&run.parent_task_id, run.plan_version).join("integration");
        git_workspace::merge(&integration_path, base_branch).await?;
        let binding = self.binding_for(task)?;
        git_workspace::run_checks(
            &integration_path,
            &binding.checks,
            binding.check_timeout_seconds,
        )
        .await
    }

    async fn integrate_work_item(
        &self,
        run: &mut OrchestrationRun,
        item: &mut WorkItem,
    ) -> Result<(), KbctlError> {
        if item.step.mode == WorkMode::Read {
            item.state = WorkItemState::Merged;
            return Ok(());
        }
        let head = item.head_commit.as_deref().ok_or_else(|| {
            KbctlError::Validation(format!("work item {} has no head commit", item.id))
        })?;
        let base = run
            .base_commit
            .as_deref()
            .ok_or_else(|| KbctlError::State("orchestration run has no base commit".to_string()))?;
        let checkout = item
            .checkout_path
            .as_deref()
            .ok_or_else(|| KbctlError::State(format!("work item {} has no checkout", item.id)))?;
        let worker_snapshot = git_workspace::inspect(Path::new(checkout)).await?;
        if worker_snapshot.head != head {
            return Err(KbctlError::Validation(format!(
                "submitted head {head} does not match worker branch head {}",
                worker_snapshot.head
            )));
        }
        let files = git_workspace::changed_files(Path::new(checkout), base, head).await?;
        git_workspace::validate_write_scope(&files, &item.step.write_scope)?;
        let integration_path =
            worktree_root(&run.parent_task_id, run.plan_version).join("integration");
        let branch = item
            .branch
            .as_deref()
            .ok_or_else(|| KbctlError::State(format!("work item {} has no branch", item.id)))?;
        git_workspace::merge(&integration_path, branch).await?;
        let task = self.provider.get_task(&run.parent_task_id).await?;
        let binding = self.binding_for(&task)?;
        git_workspace::run_checks(
            &integration_path,
            &binding.checks,
            binding.check_timeout_seconds,
        )
        .await?;
        item.state = WorkItemState::Merged;
        Ok(())
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
        if let Some(project_id) = task.project_id.clone() {
            self.provider
                .update_project(ProjectUpdate {
                    id: project_id,
                    last_activity: Some(Utc::now()),
                    ..Default::default()
                })
                .await?;
        }
        self.provider
            .update_task(TaskUpdate {
                id: task.id.clone(),
                status: Some(pending.report.status),
                clear_execution_id: true,
                result: Some(result_summary.clone()),
                ..Default::default()
            })
            .await?;
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

    fn remember_runtime_group(&self, project_id: &str, runtime_id: &str) -> Result<(), KbctlError> {
        if let Some(group_id) = self.runtime.runtime_group_id(runtime_id)? {
            self.store
                .save_runtime_group(project_id, self.runtime.runtime_kind(), &group_id)?;
        }
        Ok(())
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

fn worktree_root(parent_task_id: &str, plan_version: u32) -> PathBuf {
    let safe = parent_task_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    crate::config::default_state_path()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("worktrees")
        .join(safe)
        .join(format!("v{plan_version}"))
}

async fn receive_runtime_event(
    receiver: &mut Option<tokio::sync::broadcast::Receiver<crate::herdr::RuntimeEvent>>,
) -> Option<crate::herdr::RuntimeEvent> {
    match receiver {
        Some(receiver) => loop {
            match receiver.recv().await {
                Ok(event) => return Some(event),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        },
        None => std::future::pending().await,
    }
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
        domain::{
            OrchestrationRun, PlanDag, PlanState, Report, ReviewDecision, ReviewDecisionKind,
            SubmissionEnvelope, Task,
        },
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

    #[derive(Clone)]
    struct ProjectRecordingProvider {
        inner: FakeProvider,
        updates: Arc<Mutex<Vec<ProjectUpdate>>>,
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

    #[async_trait]
    impl KanbanProvider for ProjectRecordingProvider {
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
            self.inner.append_result(id, result).await
        }

        async fn update_project(&self, update: ProjectUpdate) -> Result<(), KbctlError> {
            self.updates.lock().unwrap().push(update);
            Ok(())
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
                    checks: Vec::new(),
                    check_timeout_seconds: 900,
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
            orchestration: Default::default(),
            profiles: Default::default(),
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
    async fn final_parent_review_summary_is_written_to_notion_result() {
        let directory = tempfile::tempdir().unwrap();
        let mut parent = task(
            TaskStatus::Running,
            Some(Utc::now() + ChronoDuration::hours(1)),
        );
        parent.project_id = Some("project-1".to_string());
        let inner = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![parent])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let updates = Arc::new(Mutex::new(Vec::new()));
        let provider = ProjectRecordingProvider {
            inner: inner.clone(),
            updates: updates.clone(),
        };
        let runtime = FakeRuntime {
            state: Arc::new(Mutex::new(RuntimeState::Done)),
        };
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let mut supervisor = Execution::new("task-1", "codex", ExecutionMode::Triage);
        supervisor.role = ExecutionRole::Supervisor;
        supervisor.parent_task_id = Some("task-1".to_string());
        supervisor.plan_version = Some(1);
        store.save_execution(&supervisor).unwrap();
        store
            .mark_execution_state(&supervisor.id, "submitted")
            .unwrap();
        store
            .save_plan(&PlanDag {
                parent_task_id: "task-1".to_string(),
                version: 1,
                summary: "plan".to_string(),
                steps: Vec::new(),
            })
            .unwrap();
        store
            .save_orchestration_run(&OrchestrationRun {
                parent_task_id: "task-1".to_string(),
                plan_version: 1,
                state: PlanState::Done,
                supervisor_execution_id: Some(supervisor.id.clone()),
                approved_plan_version: None,
                base_commit: None,
                base_branch: None,
                integration_branch: None,
                updated_at: Utc::now(),
            })
            .unwrap();
        store
            .record_submission(
                &supervisor.id,
                &SubmissionEnvelope::Review {
                    review: ReviewDecision {
                        target_id: "task-1".to_string(),
                        decision: ReviewDecisionKind::Accept,
                        summary: "integrated final result".to_string(),
                        review_round: 0,
                        findings: Vec::new(),
                    },
                },
            )
            .unwrap();
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider.clone()),
            Arc::new(runtime),
            store,
        );

        daemon.run_once().await.unwrap();

        let task = inner.tasks.lock().unwrap()[0].clone();
        assert_eq!(task.status, TaskStatus::Done);
        assert_eq!(task.result.as_deref(), Some("integrated final result"));
        assert_eq!(inner.appended.lock().unwrap().len(), 1);
        assert!(inner.appended.lock().unwrap()[0].starts_with("kbctl-orchestration:task-1:v1\n"));
        assert_eq!(updates.lock().unwrap().len(), 1);
        assert_eq!(updates.lock().unwrap()[0].id, "project-1");
    }

    #[tokio::test]
    async fn completed_parent_is_not_reopened_by_stale_orchestration() {
        let directory = tempfile::tempdir().unwrap();
        let provider = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![task(
                TaskStatus::Done,
                Some(Utc::now() + ChronoDuration::hours(1)),
            )])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let step = crate::domain::PlanStep {
            id: "step-1".to_string(),
            title: "Research".to_string(),
            objective: "Inspect the project".to_string(),
            depends_on: Vec::new(),
            profile: "fast_worker".to_string(),
            risk: crate::domain::RiskLevel::Low,
            mode: WorkMode::Read,
            write_scope: Vec::new(),
            acceptance: vec!["findings".to_string()],
        };
        store
            .save_plan(&PlanDag {
                parent_task_id: "task-1".to_string(),
                version: 1,
                summary: "stale plan".to_string(),
                steps: vec![step.clone()],
            })
            .unwrap();
        store
            .save_work_item(&WorkItem {
                id: "task-1:1:step-1".to_string(),
                parent_task_id: "task-1".to_string(),
                plan_version: 1,
                step,
                state: WorkItemState::Merged,
                attempt: 1,
                execution_id: None,
                branch: None,
                checkout_path: None,
                summary: Some("finished".to_string()),
                head_commit: None,
                review_round: 1,
            })
            .unwrap();
        store
            .save_orchestration_run(&OrchestrationRun {
                parent_task_id: "task-1".to_string(),
                plan_version: 1,
                state: PlanState::Executing,
                supervisor_execution_id: None,
                approved_plan_version: Some(1),
                base_commit: None,
                base_branch: None,
                integration_branch: None,
                updated_at: Utc::now(),
            })
            .unwrap();
        let daemon = Daemon::new(
            config(directory.path().to_str().unwrap()),
            Arc::new(provider.clone()),
            Arc::new(FakeRuntime {
                state: Arc::new(Mutex::new(RuntimeState::Done)),
            }),
            store.clone(),
        );

        daemon.run_once().await.unwrap();

        assert_eq!(provider.tasks.lock().unwrap()[0].status, TaskStatus::Done);
        assert_eq!(
            store.orchestration_run("task-1").unwrap().unwrap().state,
            PlanState::Executing
        );
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
    async fn ingests_orchestration_submission_from_project_spool() {
        let directory = tempfile::tempdir().unwrap();
        let provider = FakeProvider {
            tasks: Arc::new(Mutex::new(vec![task(
                TaskStatus::Triage,
                Some(Utc::now() + ChronoDuration::hours(1)),
            )])),
            appended: Arc::new(Mutex::new(Vec::new())),
        };
        let state = Arc::new(Mutex::new(RuntimeState::Working));
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let mut daemon_config = config(directory.path().to_str().unwrap());
        daemon_config.ensure_default_profiles();
        let daemon = Daemon::new(
            daemon_config,
            Arc::new(provider),
            Arc::new(FakeRuntime { state }),
            store.clone(),
        );

        assert_eq!(daemon.run_once().await.unwrap().dispatched, 1);
        let run = store.orchestration_run("task-1").unwrap().unwrap();
        let supervisor = run.supervisor_execution_id.unwrap();
        let execution = store.execution(&supervisor).unwrap().unwrap();
        let path = PathBuf::from(execution.submission_path.unwrap());
        report_spool::write_submission(
            &path,
            &report_spool::AgentSubmission {
                execution_id: supervisor.clone(),
                envelope: SubmissionEnvelope::Plan {
                    plan: PlanDag {
                        parent_task_id: "task-1".to_string(),
                        version: 1,
                        summary: "two reads".to_string(),
                        steps: vec![crate::domain::PlanStep {
                            id: "read-1".to_string(),
                            title: "Read".to_string(),
                            objective: "Research".to_string(),
                            depends_on: Vec::new(),
                            profile: "fast_worker".to_string(),
                            risk: crate::domain::RiskLevel::Low,
                            mode: WorkMode::Read,
                            write_scope: Vec::new(),
                            acceptance: vec!["sources".to_string()],
                        }],
                    },
                },
            },
        )
        .unwrap();

        assert_eq!(daemon.run_once().await.unwrap().reconciled, 1);
        assert!(!path.exists());
        assert_eq!(
            store.latest_plan("task-1").unwrap().unwrap().summary,
            "two reads"
        );
        assert_eq!(
            store.execution_state(&supervisor).unwrap().as_deref(),
            Some("submitted")
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
