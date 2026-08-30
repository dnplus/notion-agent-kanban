use crate::{
    config::Config,
    domain::{
        ExecutionRole, OrchestrationRun, PlanDag, PlanState, ReviewDecisionKind, RiskLevel,
        SubmissionEnvelope, WorkItem, WorkItemState, WorkMode,
    },
    error::KbctlError,
    store::Store,
};
use chrono::Utc;
use std::collections::{BTreeSet, HashMap};

pub fn validate_plan(config: &Config, plan: &PlanDag) -> Result<(), KbctlError> {
    plan.validate(config.orchestration.max_steps)
        .map_err(KbctlError::Validation)?;
    for step in &plan.steps {
        let profile = config.profile(&step.profile).ok_or_else(|| {
            KbctlError::Validation(format!(
                "unknown profile for step {}: {}",
                step.id, step.profile
            ))
        })?;
        if profile.role != crate::domain::ExecutionRole::Worker {
            return Err(KbctlError::Validation(format!(
                "step {} profile {} is not a worker",
                step.id, step.profile
            )));
        }
    }
    validate_parallel_write_scopes(plan)
}

pub fn runnable_items(items: &[WorkItem], approved: bool, capacity: usize) -> Vec<String> {
    let states = items
        .iter()
        .map(|item| (item.step.id.as_str(), item.state))
        .collect::<HashMap<_, _>>();
    items
        .iter()
        .filter(|item| item.state == WorkItemState::Pending)
        .filter(|item| approved || item.step.risk == RiskLevel::Low)
        .filter(|item| {
            item.step.depends_on.iter().all(|dependency| {
                states
                    .get(dependency.as_str())
                    .is_some_and(|state| *state == WorkItemState::Merged)
            })
        })
        .take(capacity)
        .map(|item| item.id.clone())
        .collect()
}

pub fn requires_human_approval(plan: &PlanDag) -> bool {
    plan.steps.iter().any(|step| step.risk != RiskLevel::Low)
}

pub fn apply_submission(
    config: &Config,
    store: &Store,
    execution_id: &str,
    envelope: &SubmissionEnvelope,
) -> Result<(), KbctlError> {
    let submission_key = Store::submission_key(envelope);
    if let Some(existing) = store.submission_by_key(execution_id, &submission_key)? {
        if existing == *envelope {
            return Ok(());
        }
        return Err(KbctlError::Validation(format!(
            "submission {submission_key} was already recorded with different content"
        )));
    }
    let execution = store
        .execution(execution_id)?
        .ok_or_else(|| KbctlError::Validation(format!("execution {execution_id} was not found")))?;
    match (execution.role, envelope) {
        (
            ExecutionRole::Supervisor | ExecutionRole::Reviewer,
            SubmissionEnvelope::Plan { plan },
        ) => {
            let parent = execution
                .parent_task_id
                .as_deref()
                .unwrap_or(&execution.task_id);
            if plan.parent_task_id != parent {
                return Err(KbctlError::Validation(
                    "plan parent does not match execution".to_string(),
                ));
            }
            validate_plan(config, plan)?;
            let previous = store.orchestration_run(parent)?;
            if previous
                .as_ref()
                .is_some_and(|run| plan.version < run.plan_version)
            {
                return Err(KbctlError::Validation(
                    "plan version cannot move backwards".to_string(),
                ));
            }
            if store.plan(parent, plan.version)?.is_some() {
                let existing = store.plan(parent, plan.version)?.unwrap();
                if existing != *plan {
                    return Err(KbctlError::Validation(
                        "plan version already exists with different content".to_string(),
                    ));
                }
                return Ok(());
            }
            store.save_plan(plan)?;
            let mut run = previous.unwrap_or(OrchestrationRun {
                parent_task_id: parent.to_string(),
                plan_version: plan.version,
                state: PlanState::Planning,
                supervisor_execution_id: Some(execution_id.to_string()),
                approved_plan_version: None,
                base_commit: None,
                base_branch: None,
                integration_branch: None,
                updated_at: Utc::now(),
            });
            run.plan_version = plan.version;
            run.approved_plan_version = None;
            run.state = if requires_human_approval(plan) {
                PlanState::AwaitingApproval
            } else {
                PlanState::Executing
            };
            run.updated_at = Utc::now();
            store.save_orchestration_run(&run)?;
        }
        (ExecutionRole::Worker, SubmissionEnvelope::Completion { completion }) => {
            let expected = execution.work_item_id.as_deref().ok_or_else(|| {
                KbctlError::Validation("worker execution has no work_item_id".to_string())
            })?;
            if completion.work_item_id != expected || completion.summary.trim().is_empty() {
                return Err(KbctlError::Validation(
                    "completion does not match execution or has no summary".to_string(),
                ));
            }
            let mut item = store.work_item(expected)?.ok_or_else(|| {
                KbctlError::Validation(format!("work item {expected} was not found"))
            })?;
            item.state = WorkItemState::Submitted;
            item.summary = Some(completion.summary.clone());
            item.head_commit = completion.head_commit.clone();
            store.save_work_item(&item)?;
        }
        (
            ExecutionRole::Supervisor | ExecutionRole::Reviewer,
            SubmissionEnvelope::Review { review },
        ) => {
            if review.summary.trim().is_empty() {
                return Err(KbctlError::Validation(
                    "review summary is required".to_string(),
                ));
            }
            if review.decision == ReviewDecisionKind::Rework
                && review.review_round > config.orchestration.max_rework
            {
                return Err(KbctlError::Validation(
                    "review exceeds max_rework".to_string(),
                ));
            }
            if let Some(mut item) = store.work_item(&review.target_id)? {
                item.review_round = review.review_round;
                item.state = match review.decision {
                    ReviewDecisionKind::Accept => WorkItemState::Accepted,
                    ReviewDecisionKind::Rework => WorkItemState::Rework,
                    ReviewDecisionKind::Blocked => WorkItemState::Blocked,
                };
                item.summary = Some(review.summary.clone());
                store.save_work_item(&item)?;
            } else if let Some(mut run) = store.orchestration_run(&review.target_id)? {
                run.state = match review.decision {
                    ReviewDecisionKind::Accept => {
                        let items = store.work_items(&run.parent_task_id, run.plan_version)?;
                        if items.iter().any(|item| item.step.mode == WorkMode::Write) {
                            PlanState::AwaitingMerge
                        } else {
                            PlanState::Done
                        }
                    }
                    ReviewDecisionKind::Rework | ReviewDecisionKind::Blocked => PlanState::Blocked,
                };
                run.updated_at = Utc::now();
                store.save_orchestration_run(&run)?;
            } else {
                return Err(KbctlError::Validation(
                    "review target was not found".to_string(),
                ));
            }
        }
        _ => {
            return Err(KbctlError::Validation(
                "submission type is not allowed for execution role".to_string(),
            ));
        }
    }
    store.record_submission(execution_id, envelope)?;
    Ok(())
}

fn validate_parallel_write_scopes(plan: &PlanDag) -> Result<(), KbctlError> {
    let ancestors = plan
        .steps
        .iter()
        .map(|step| {
            let mut values = BTreeSet::new();
            collect_dependencies(&step.id, plan, &mut values);
            (step.id.as_str(), values)
        })
        .collect::<HashMap<_, _>>();
    for (index, left) in plan.steps.iter().enumerate() {
        if left.mode != WorkMode::Write {
            continue;
        }
        for right in plan.steps.iter().skip(index + 1) {
            if right.mode != WorkMode::Write {
                continue;
            }
            let ordered = ancestors[&left.id.as_str()].contains(right.id.as_str())
                || ancestors[&right.id.as_str()].contains(left.id.as_str());
            if !ordered && scopes_overlap(&left.write_scope, &right.write_scope) {
                return Err(KbctlError::Validation(format!(
                    "parallel write scopes overlap: {} and {}",
                    left.id, right.id
                )));
            }
        }
    }
    Ok(())
}

fn collect_dependencies<'a>(id: &str, plan: &'a PlanDag, values: &mut BTreeSet<&'a str>) {
    let Some(step) = plan.steps.iter().find(|step| step.id == id) else {
        return;
    };
    for dependency in &step.depends_on {
        if values.insert(dependency) {
            collect_dependencies(dependency, plan, values);
        }
    }
}

fn scopes_overlap(left: &[String], right: &[String]) -> bool {
    left.iter().any(|left| {
        right.iter().any(|right| {
            let left = static_prefix(left);
            let right = static_prefix(right);
            left.starts_with(right) || right.starts_with(left)
        })
    })
}

fn static_prefix(pattern: &str) -> &str {
    pattern
        .find(['*', '?', '[', '{'])
        .map(|index| &pattern[..index])
        .unwrap_or(pattern)
        .trim_end_matches('/')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{PlanStep, RiskLevel};

    fn step(id: &str, risk: RiskLevel, mode: WorkMode, scope: &[&str]) -> PlanStep {
        PlanStep {
            id: id.to_string(),
            title: id.to_string(),
            objective: id.to_string(),
            depends_on: Vec::new(),
            profile: "fast_worker".to_string(),
            risk,
            mode,
            write_scope: scope.iter().map(|value| value.to_string()).collect(),
            acceptance: vec!["passes".to_string()],
        }
    }

    #[test]
    fn overlapping_parallel_writes_are_rejected() {
        let plan = PlanDag {
            parent_task_id: "task".to_string(),
            version: 1,
            summary: "plan".to_string(),
            steps: vec![
                step("a", RiskLevel::Low, WorkMode::Write, &["src/**"]),
                step("b", RiskLevel::Low, WorkMode::Write, &["src/lib.rs"]),
            ],
        };
        assert!(validate_plan(&Config::default(), &plan).is_err());
    }

    #[test]
    fn medium_work_waits_for_approval() {
        let plan_step = step("a", RiskLevel::Medium, WorkMode::Read, &[]);
        let item = WorkItem::from_step("task", 1, plan_step);
        assert!(runnable_items(std::slice::from_ref(&item), false, 3).is_empty());
        assert_eq!(runnable_items(&[item], true, 3).len(), 1);
    }

    #[test]
    fn plan_submission_is_persisted_and_replay_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let mut execution =
            crate::domain::Execution::new("task", "codex", crate::domain::ExecutionMode::Triage);
        execution.role = ExecutionRole::Supervisor;
        execution.parent_task_id = Some("task".to_string());
        store.save_execution(&execution).unwrap();
        let plan = PlanDag {
            parent_task_id: "task".to_string(),
            version: 1,
            summary: "read plan".to_string(),
            steps: vec![step("a", RiskLevel::Low, WorkMode::Read, &[])],
        };
        let envelope = SubmissionEnvelope::Plan { plan };
        apply_submission(&Config::default(), &store, &execution.id, &envelope).unwrap();
        apply_submission(&Config::default(), &store, &execution.id, &envelope).unwrap();
        assert_eq!(store.latest_plan("task").unwrap().unwrap().version, 1);
        assert_eq!(store.work_items("task", 1).unwrap().len(), 1);
        assert_eq!(
            store.orchestration_run("task").unwrap().unwrap().state,
            PlanState::Executing
        );
    }

    #[test]
    fn conflicting_submission_replay_is_rejected_before_state_changes() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let mut execution =
            crate::domain::Execution::new("task", "codex", crate::domain::ExecutionMode::Triage);
        execution.role = ExecutionRole::Supervisor;
        execution.parent_task_id = Some("task".to_string());
        store.save_execution(&execution).unwrap();
        let first = SubmissionEnvelope::Plan {
            plan: PlanDag {
                parent_task_id: "task".to_string(),
                version: 1,
                summary: "first".to_string(),
                steps: vec![step("a", RiskLevel::Low, WorkMode::Read, &[])],
            },
        };
        apply_submission(&Config::default(), &store, &execution.id, &first).unwrap();
        let mut conflicting = first.clone();
        let SubmissionEnvelope::Plan { plan } = &mut conflicting else {
            unreachable!()
        };
        plan.summary = "conflicting".to_string();
        assert!(apply_submission(&Config::default(), &store, &execution.id, &conflicting).is_err());
        assert_eq!(store.latest_plan("task").unwrap().unwrap().summary, "first");
    }
}
