use crate::{
    domain::{Execution, Report, Task},
    error::KbctlError,
};
use chrono::{DateTime, Utc};
use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};

#[derive(Clone)]
pub struct Store {
    path: PathBuf,
}

pub struct DaemonLock {
    file: File,
}

impl Drop for DaemonLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[derive(Debug, Clone)]
pub struct PendingReport {
    pub execution_id: String,
    pub task_id: String,
    pub report: Report,
    pub result_text: String,
}

impl Store {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, KbctlError> {
        let store = Self { path: path.into() };
        store.initialize()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn acquire_daemon_lock(&self) -> Result<DaemonLock, KbctlError> {
        let lock_path = self.path.with_extension("lock");
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| KbctlError::State(format!("create lock directory: {error}")))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|error| KbctlError::State(format!("open daemon lock: {error}")))?;
        file.try_lock_exclusive().map_err(|_| {
            KbctlError::State("another kbctl daemon is already running".to_string())
        })?;
        Ok(DaemonLock { file })
    }

    pub fn save_execution(&self, execution: &Execution) -> Result<(), KbctlError> {
        let connection = self.connection()?;
        let encoded = serde_json::to_string(execution)
            .map_err(|error| KbctlError::State(error.to_string()))?;
        connection
            .execute(
                "INSERT INTO executions (id, task_id, execution_json, runtime_id, state) VALUES (?1, ?2, ?3, ?4, 'running') ON CONFLICT(id) DO UPDATE SET task_id=excluded.task_id, execution_json=excluded.execution_json, runtime_id=excluded.runtime_id",
                params![execution.id, execution.task_id, encoded, execution.runtime_id],
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        Ok(())
    }

    pub fn execution(&self, execution_id: &str) -> Result<Option<Execution>, KbctlError> {
        let connection = self.connection()?;
        let encoded = connection
            .query_row(
                "SELECT execution_json FROM executions WHERE id = ?1",
                params![execution_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| KbctlError::State(error.to_string()))?;
        encoded
            .map(|value| {
                serde_json::from_str(&value).map_err(|error| KbctlError::State(error.to_string()))
            })
            .transpose()
    }

    pub fn execution_for_task(&self, task_id: &str) -> Result<Option<Execution>, KbctlError> {
        let connection = self.connection()?;
        let encoded = connection
            .query_row(
                "SELECT execution_json FROM executions WHERE task_id = ?1 AND state = 'running' ORDER BY rowid DESC LIMIT 1",
                params![task_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| KbctlError::State(error.to_string()))?;
        encoded
            .map(|value| {
                serde_json::from_str(&value).map_err(|error| KbctlError::State(error.to_string()))
            })
            .transpose()
    }

    pub fn set_runtime_id(&self, execution_id: &str, runtime_id: &str) -> Result<(), KbctlError> {
        let connection = self.connection()?;
        let encoded = connection
            .query_row(
                "SELECT execution_json FROM executions WHERE id = ?1",
                params![execution_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| KbctlError::State(error.to_string()))?
            .ok_or_else(|| KbctlError::State(format!("execution {execution_id} was not found")))?;
        let mut value: serde_json::Value =
            serde_json::from_str(&encoded).map_err(|error| KbctlError::State(error.to_string()))?;
        value["runtime_id"] = serde_json::Value::String(runtime_id.to_string());
        let updated =
            serde_json::to_string(&value).map_err(|error| KbctlError::State(error.to_string()))?;
        connection
            .execute(
                "UPDATE executions SET runtime_id = ?2, execution_json = ?3 WHERE id = ?1",
                params![execution_id, runtime_id, updated],
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        Ok(())
    }

    pub fn next_execution_attempt(&self, task_id: &str) -> Result<u32, KbctlError> {
        let connection = self.connection()?;
        let encoded = connection
            .query_row(
                "SELECT execution_json FROM executions WHERE task_id = ?1 ORDER BY rowid DESC LIMIT 1",
                params![task_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| KbctlError::State(error.to_string()))?;
        let Some(encoded) = encoded else {
            return Ok(1);
        };
        let execution: Execution =
            serde_json::from_str(&encoded).map_err(|error| KbctlError::State(error.to_string()))?;
        Ok(execution.attempt.saturating_add(1).max(1))
    }

    pub fn retry_for_task(&self, task_id: &str) -> Result<Option<Execution>, KbctlError> {
        let connection = self.connection()?;
        let encoded = connection
            .query_row(
                "SELECT execution_json FROM executions WHERE task_id = ?1 AND state = 'retry_wait' ORDER BY rowid DESC LIMIT 1",
                params![task_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| KbctlError::State(error.to_string()))?;
        encoded
            .map(|value| {
                serde_json::from_str(&value).map_err(|error| KbctlError::State(error.to_string()))
            })
            .transpose()
    }

    pub fn mark_execution_retry(
        &self,
        execution_id: &str,
        retry_at: DateTime<Utc>,
    ) -> Result<(), KbctlError> {
        let connection = self.connection()?;
        let encoded = connection
            .query_row(
                "SELECT execution_json FROM executions WHERE id = ?1",
                params![execution_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| KbctlError::State(error.to_string()))?
            .ok_or_else(|| KbctlError::State(format!("execution {execution_id} was not found")))?;
        let mut execution: Execution =
            serde_json::from_str(&encoded).map_err(|error| KbctlError::State(error.to_string()))?;
        execution.retry_at = Some(retry_at);
        let updated = serde_json::to_string(&execution)
            .map_err(|error| KbctlError::State(error.to_string()))?;
        connection
            .execute(
                "UPDATE executions SET execution_json = ?2, state = 'retry_wait' WHERE id = ?1",
                params![execution_id, updated],
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        Ok(())
    }

    pub fn cache_tasks(&self, tasks: &[Task]) -> Result<(), KbctlError> {
        let connection = self.connection()?;
        let transaction = connection
            .unchecked_transaction()
            .map_err(|error| KbctlError::State(error.to_string()))?;
        transaction
            .execute("DELETE FROM task_cache", [])
            .map_err(|error| KbctlError::State(error.to_string()))?;
        let updated_at = Utc::now().to_rfc3339();
        for task in tasks {
            let encoded = serde_json::to_string(task)
                .map_err(|error| KbctlError::State(error.to_string()))?;
            transaction
                .execute(
                    "INSERT INTO task_cache (id, task_json, updated_at) VALUES (?1, ?2, ?3)",
                    params![task.id, encoded, updated_at],
                )
                .map_err(|error| KbctlError::State(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| KbctlError::State(error.to_string()))?;
        Ok(())
    }

    pub fn cache_task(&self, task: &Task) -> Result<(), KbctlError> {
        let connection = self.connection()?;
        let encoded =
            serde_json::to_string(task).map_err(|error| KbctlError::State(error.to_string()))?;
        connection
            .execute(
                "INSERT INTO task_cache (id, task_json, updated_at) VALUES (?1, ?2, ?3) ON CONFLICT(id) DO UPDATE SET task_json = excluded.task_json, updated_at = excluded.updated_at",
                params![task.id, encoded, Utc::now().to_rfc3339()],
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        Ok(())
    }

    pub fn cached_tasks(&self) -> Result<Vec<Task>, KbctlError> {
        let connection = self.connection()?;
        let mut statement = connection
            .prepare("SELECT task_json FROM task_cache ORDER BY rowid")
            .map_err(|error| KbctlError::State(error.to_string()))?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| KbctlError::State(error.to_string()))?;
        rows.map(|row| {
            let encoded = row.map_err(|error| KbctlError::State(error.to_string()))?;
            serde_json::from_str(&encoded).map_err(|error| KbctlError::State(error.to_string()))
        })
        .collect()
    }

    pub fn running_executions(&self) -> Result<Vec<Execution>, KbctlError> {
        let connection = self.connection()?;
        let mut statement = connection
            .prepare("SELECT execution_json FROM executions WHERE state = 'running' ORDER BY rowid")
            .map_err(|error| KbctlError::State(error.to_string()))?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| KbctlError::State(error.to_string()))?;
        rows.map(|row| {
            let encoded = row.map_err(|error| KbctlError::State(error.to_string()))?;
            serde_json::from_str(&encoded).map_err(|error| KbctlError::State(error.to_string()))
        })
        .collect()
    }

    pub fn record_report(
        &self,
        task_id: &str,
        report: &Report,
        result_text: &str,
    ) -> Result<bool, KbctlError> {
        let connection = self.connection()?;
        let encoded =
            serde_json::to_string(report).map_err(|error| KbctlError::State(error.to_string()))?;
        let transaction = connection
            .unchecked_transaction()
            .map_err(|error| KbctlError::State(error.to_string()))?;
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO reports (execution_id, task_id, report_json, result_text, applied) VALUES (?1, ?2, ?3, ?4, 0)",
                params![report.execution_id, task_id, encoded, result_text],
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO outbox (execution_id, task_id, report_json, result_text) VALUES (?1, ?2, ?3, ?4)",
                params![report.execution_id, task_id, encoded, result_text],
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| KbctlError::State(error.to_string()))?;
        Ok(inserted == 1)
    }

    pub fn report(&self, execution_id: &str) -> Result<Option<(String, Report, bool)>, KbctlError> {
        let connection = self.connection()?;
        let value = connection
            .query_row(
                "SELECT task_id, report_json, applied FROM reports WHERE execution_id = ?1",
                params![execution_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)? == 1,
                    ))
                },
            )
            .optional()
            .map_err(|error| KbctlError::State(error.to_string()))?;
        value
            .map(|(task_id, encoded, applied)| {
                let report = serde_json::from_str(&encoded)
                    .map_err(|error| KbctlError::State(error.to_string()))?;
                Ok((task_id, report, applied))
            })
            .transpose()
    }

    pub fn pending_reports(&self) -> Result<Vec<PendingReport>, KbctlError> {
        let connection = self.connection()?;
        let mut statement = connection
            .prepare("SELECT execution_id, task_id, report_json, result_text FROM outbox WHERE applied = 0 ORDER BY created_at")
            .map_err(|error| KbctlError::State(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|error| KbctlError::State(error.to_string()))?;
        let mut pending = Vec::new();
        for row in rows {
            let (execution_id, task_id, report_json, result_text) =
                row.map_err(|error| KbctlError::State(error.to_string()))?;
            let report = serde_json::from_str(&report_json)
                .map_err(|error| KbctlError::State(error.to_string()))?;
            pending.push(PendingReport {
                execution_id,
                task_id,
                report,
                result_text,
            });
        }
        Ok(pending)
    }

    pub fn mark_report_applied(&self, execution_id: &str) -> Result<(), KbctlError> {
        let connection = self.connection()?;
        connection
            .execute(
                "UPDATE reports SET applied = 1 WHERE execution_id = ?1",
                params![execution_id],
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        connection
            .execute(
                "UPDATE outbox SET applied = 1 WHERE execution_id = ?1",
                params![execution_id],
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        connection
            .execute(
                "UPDATE executions SET state = 'reported' WHERE id = ?1",
                params![execution_id],
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        Ok(())
    }

    pub fn mark_report_failed(&self, execution_id: &str, error: &str) -> Result<(), KbctlError> {
        let connection = self.connection()?;
        connection
            .execute(
                "UPDATE outbox SET attempts = attempts + 1, last_error = ?2 WHERE execution_id = ?1 AND applied = 0",
                params![execution_id, error],
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        Ok(())
    }

    pub fn mark_execution_state(&self, execution_id: &str, state: &str) -> Result<(), KbctlError> {
        let connection = self.connection()?;
        connection
            .execute(
                "UPDATE executions SET state = ?2 WHERE id = ?1",
                params![execution_id, state],
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        Ok(())
    }

    fn initialize(&self) -> Result<(), KbctlError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                KbctlError::State(format!("create {}: {error}", parent.display()))
            })?;
        }
        let connection = self.connection()?;
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 CREATE TABLE IF NOT EXISTS executions (
                     id TEXT PRIMARY KEY,
                     task_id TEXT NOT NULL,
                     execution_json TEXT NOT NULL,
                     runtime_id TEXT,
                     state TEXT NOT NULL,
                     created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                 );
                 CREATE TABLE IF NOT EXISTS reports (
                     execution_id TEXT PRIMARY KEY,
                     task_id TEXT NOT NULL,
                     report_json TEXT NOT NULL,
                     result_text TEXT NOT NULL,
                     applied INTEGER NOT NULL DEFAULT 0,
                     created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                 );
                 CREATE TABLE IF NOT EXISTS outbox (
                     execution_id TEXT PRIMARY KEY,
                     task_id TEXT NOT NULL,
                     report_json TEXT NOT NULL,
                     result_text TEXT NOT NULL,
                     applied INTEGER NOT NULL DEFAULT 0,
                     attempts INTEGER NOT NULL DEFAULT 0,
                     last_error TEXT,
                     created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                 );
                 CREATE TABLE IF NOT EXISTS task_cache (
                     id TEXT PRIMARY KEY,
                     task_json TEXT NOT NULL,
                     updated_at TEXT NOT NULL
                 );",
            )
            .map_err(|error| KbctlError::State(error.to_string()))?;
        Ok(())
    }

    fn connection(&self) -> Result<Connection, KbctlError> {
        Connection::open(&self.path)
            .map_err(|error| KbctlError::State(format!("open {}: {error}", self.path.display())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ExecutionMode, Task, TaskStatus};
    use chrono::Utc;

    #[test]
    fn report_recording_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let execution = Execution::new("task-1", "codex", ExecutionMode::Execute);
        store.save_execution(&execution).unwrap();
        let report = Report {
            execution_id: execution.id.clone(),
            status: TaskStatus::Done,
            summary: Some("finished".to_string()),
            reason: None,
            result_file: None,
            reported_at: Utc::now(),
        };
        assert!(store.record_report("task-1", &report, "finished").unwrap());
        assert!(!store.record_report("task-1", &report, "finished").unwrap());
        assert_eq!(store.pending_reports().unwrap().len(), 1);
    }

    #[test]
    fn report_retry_failure_is_persisted() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let execution = Execution::new("task-1", "codex", ExecutionMode::Execute);
        store.save_execution(&execution).unwrap();
        let report = Report {
            execution_id: execution.id.clone(),
            status: TaskStatus::Done,
            summary: Some("finished".to_string()),
            reason: None,
            result_file: None,
            reported_at: Utc::now(),
        };
        store.record_report("task-1", &report, "finished").unwrap();
        store
            .mark_report_failed(&execution.id, "temporary failure")
            .unwrap();
        let connection = store.connection().unwrap();
        let (attempts, error): (i64, String) = connection
            .query_row(
                "SELECT attempts, last_error FROM outbox WHERE execution_id = ?1",
                params![execution.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(attempts, 1);
        assert_eq!(error, "temporary failure");
    }

    #[test]
    fn task_cache_replaces_and_reads_canonical_tasks() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let first = Task {
            id: "task-1".to_string(),
            name: "First".to_string(),
            status: TaskStatus::Ready,
            ..Default::default()
        };
        store.cache_tasks(std::slice::from_ref(&first)).unwrap();
        let cached = store.cached_tasks().unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].id, first.id);
        assert_eq!(cached[0].status, first.status);
        let second = Task {
            id: "task-2".to_string(),
            name: "Second".to_string(),
            status: TaskStatus::Done,
            ..Default::default()
        };
        store.cache_tasks(std::slice::from_ref(&second)).unwrap();
        let cached = store.cached_tasks().unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].id, second.id);
        assert_eq!(cached[0].status, second.status);
    }

    #[test]
    fn execution_for_task_returns_only_the_current_running_attempt() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("state.db")).unwrap();
        let old = Execution::new_with_attempt("task-1", "codex", ExecutionMode::Execute, 1);
        store.save_execution(&old).unwrap();
        store.mark_execution_state(&old.id, "reported").unwrap();
        let current = Execution::new_with_attempt("task-1", "codex", ExecutionMode::Execute, 2);
        store.save_execution(&current).unwrap();
        assert_eq!(
            store.execution_for_task("task-1").unwrap().unwrap().id,
            current.id
        );
    }
}
