use crate::{
    domain::{Report, SubmissionEnvelope},
    error::KbctlError,
};
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

pub const REPORT_FILE_ENV: &str = "KBCTL_REPORT_FILE";
pub const SUBMISSION_FILE_ENV: &str = "KBCTL_SUBMISSION_FILE";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentReport {
    pub task_id: String,
    pub report: Report,
    pub result_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSubmission {
    pub execution_id: String,
    pub envelope: SubmissionEnvelope,
}

pub fn configured_path() -> Option<PathBuf> {
    env::var_os(REPORT_FILE_ENV)
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

pub fn configured_submission_path() -> Option<PathBuf> {
    env::var_os(SUBMISSION_FILE_ENV)
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

pub fn path_for(project_path: &Path, execution_id: &str) -> PathBuf {
    project_path
        .join(".kbctl")
        .join("reports")
        .join(format!("{execution_id}.json"))
}

pub fn submission_path_for(project_path: &Path, execution_id: &str) -> PathBuf {
    project_path
        .join(".kbctl")
        .join("submissions")
        .join(format!("{execution_id}.json"))
}

pub fn write(path: &Path, report: &AgentReport) -> Result<(), KbctlError> {
    let parent = path.parent().ok_or_else(|| {
        KbctlError::State(format!("report path has no parent: {}", path.display()))
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| KbctlError::State(format!("create report spool directory: {error}")))?;
    let encoded = serde_json::to_vec(report)
        .map_err(|error| KbctlError::State(format!("encode report spool: {error}")))?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, encoded)
        .map_err(|error| KbctlError::State(format!("write report spool: {error}")))?;
    fs::rename(&temporary, path)
        .map_err(|error| KbctlError::State(format!("commit report spool: {error}")))?;
    Ok(())
}

pub fn read(path: &Path) -> Result<AgentReport, KbctlError> {
    let encoded = fs::read(path).map_err(|error| {
        KbctlError::State(format!("read report spool {}: {error}", path.display()))
    })?;
    serde_json::from_slice(&encoded).map_err(|error| {
        KbctlError::State(format!("decode report spool {}: {error}", path.display()))
    })
}

pub fn write_submission(path: &Path, submission: &AgentSubmission) -> Result<(), KbctlError> {
    write_json(path, submission, "submission")
}

pub fn read_submission(path: &Path) -> Result<AgentSubmission, KbctlError> {
    let encoded = fs::read(path).map_err(|error| {
        KbctlError::State(format!("read submission spool {}: {error}", path.display()))
    })?;
    serde_json::from_slice(&encoded).map_err(|error| {
        KbctlError::State(format!(
            "decode submission spool {}: {error}",
            path.display()
        ))
    })
}

fn write_json<T: Serialize>(path: &Path, value: &T, kind: &str) -> Result<(), KbctlError> {
    let parent = path.parent().ok_or_else(|| {
        KbctlError::State(format!("{kind} path has no parent: {}", path.display()))
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| KbctlError::State(format!("create {kind} spool directory: {error}")))?;
    let encoded = serde_json::to_vec(value)
        .map_err(|error| KbctlError::State(format!("encode {kind} spool: {error}")))?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, encoded)
        .map_err(|error| KbctlError::State(format!("write {kind} spool: {error}")))?;
    fs::rename(&temporary, path)
        .map_err(|error| KbctlError::State(format!("commit {kind} spool: {error}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::TaskStatus;
    use chrono::Utc;

    #[test]
    fn report_spool_round_trips_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let path = path_for(directory.path(), "execution-1");
        let report = AgentReport {
            task_id: "task-1".to_string(),
            report: Report {
                execution_id: "execution-1".to_string(),
                status: TaskStatus::Done,
                summary: Some("finished".to_string()),
                reason: None,
                result_file: None,
                reported_at: Utc::now(),
            },
            result_text: "finished".to_string(),
        };

        write(&path, &report).unwrap();
        assert_eq!(read(&path).unwrap().task_id, "task-1");
        assert!(!path.with_extension("json.tmp").exists());
    }

    #[test]
    fn submission_spool_round_trips_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let path = submission_path_for(directory.path(), "execution-1");
        let submission = AgentSubmission {
            execution_id: "execution-1".to_string(),
            envelope: SubmissionEnvelope::Completion {
                completion: crate::domain::CompletionEnvelope {
                    work_item_id: "work-1".to_string(),
                    summary: "done".to_string(),
                    head_commit: None,
                    artifacts: Vec::new(),
                    known_issues: Vec::new(),
                },
            },
        };
        write_submission(&path, &submission).unwrap();
        assert_eq!(read_submission(&path).unwrap().execution_id, "execution-1");
    }
}
