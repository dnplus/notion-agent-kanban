use crate::error::KbctlError;
use globset::{Glob, GlobSetBuilder};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{process::Command, time::timeout};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitSnapshot {
    pub root: PathBuf,
    pub branch: String,
    pub head: String,
    pub clean: bool,
}

pub async fn inspect(path: &Path) -> Result<GitSnapshot, KbctlError> {
    let root = git_output(path, &["rev-parse", "--show-toplevel"]).await?;
    let branch = git_output(path, &["symbolic-ref", "--short", "HEAD"])
        .await
        .map_err(|_| KbctlError::Validation("Git repository is detached".to_string()))?;
    let head = git_output(path, &["rev-parse", "HEAD"]).await?;
    let status = git_output(path, &["status", "--porcelain=v1"]).await?;
    Ok(GitSnapshot {
        root: PathBuf::from(root),
        branch,
        head,
        clean: status.trim().is_empty(),
    })
}

pub async fn create_worktree(
    repository: &Path,
    destination: &Path,
    branch: &str,
    base: &str,
) -> Result<(), KbctlError> {
    if destination.exists() {
        let existing = inspect(destination).await?;
        if existing.branch != branch {
            return Err(KbctlError::Validation(format!(
                "worktree {} is on {}, expected {branch}",
                destination.display(),
                existing.branch
            )));
        }
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| KbctlError::State(format!("create worktree directory: {error}")))?;
    }
    git_status(
        repository,
        &[
            "worktree",
            "add",
            "-b",
            branch,
            &destination.display().to_string(),
            base,
        ],
    )
    .await
}

pub async fn changed_files(
    repository: &Path,
    base: &str,
    head: &str,
) -> Result<Vec<String>, KbctlError> {
    let range = format!("{base}...{head}");
    let output = git_output(repository, &["diff", "--name-only", &range]).await?;
    Ok(output
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

pub fn validate_write_scope(files: &[String], patterns: &[String]) -> Result<(), KbctlError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern).map_err(|error| {
            KbctlError::Validation(format!("invalid write scope {pattern}: {error}"))
        })?);
    }
    let set = builder
        .build()
        .map_err(|error| KbctlError::Validation(error.to_string()))?;
    let outside = files
        .iter()
        .filter(|path| !set.is_match(path))
        .cloned()
        .collect::<Vec<_>>();
    if outside.is_empty() {
        Ok(())
    } else {
        Err(KbctlError::Validation(format!(
            "files outside write scope: {}",
            outside.join(", ")
        )))
    }
}

pub async fn merge(repository: &Path, branch: &str) -> Result<(), KbctlError> {
    git_status(repository, &["merge", "--no-ff", "--no-edit", branch]).await
}

pub async fn is_ancestor(
    repository: &Path,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, KbctlError> {
    let status = Command::new("git")
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .current_dir(repository)
        .status()
        .await
        .map_err(|error| KbctlError::State(format!("run git merge-base: {error}")))?;
    match status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(KbctlError::State("git merge-base failed".to_string())),
    }
}

pub async fn run_checks(
    path: &Path,
    checks: &[String],
    timeout_seconds: u64,
) -> Result<(), KbctlError> {
    for check in checks {
        let mut command = Command::new("sh");
        command
            .args(["-lc", check])
            .current_dir(path)
            .stdin(Stdio::null());
        let status = timeout(
            Duration::from_secs(timeout_seconds.max(1)),
            command.status(),
        )
        .await
        .map_err(|_| KbctlError::Validation(format!("check timed out: {check}")))?
        .map_err(|error| KbctlError::State(format!("run check {check}: {error}")))?;
        if !status.success() {
            return Err(KbctlError::Validation(format!("check failed: {check}")));
        }
    }
    Ok(())
}

pub fn integration_branch(parent_task_id: &str, plan_version: u32) -> String {
    let sanitized = sanitize_branch_component(parent_task_id);
    format!("kbctl/{sanitized}/v{plan_version}")
}

pub fn worker_branch(parent_task_id: &str, plan_version: u32, step_id: &str) -> String {
    let parent = sanitize_branch_component(parent_task_id);
    let step = sanitize_branch_component(step_id);
    format!("kbctl-worker/{parent}/v{plan_version}-{step}")
}

fn sanitize_branch_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

async fn git_output(path: &Path, args: &[&str]) -> Result<String, KbctlError> {
    let output = Command::new("git")
        .args(args)
        .current_dir(path)
        .output()
        .await
        .map_err(|error| KbctlError::State(format!("run git {}: {error}", args.join(" "))))?;
    if !output.status.success() {
        return Err(KbctlError::Validation(
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn git_status(path: &Path, args: &[&str]) -> Result<(), KbctlError> {
    git_output(path, args).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_scope_rejects_unlisted_paths() {
        validate_write_scope(&["src/lib.rs".to_string()], &["src/**".to_string()]).unwrap();
        assert!(
            validate_write_scope(&["Cargo.toml".to_string()], &["src/**".to_string()]).is_err()
        );
    }

    #[test]
    fn branch_names_are_stable_and_safe() {
        assert_eq!(integration_branch("task/one", 2), "kbctl/task-one/v2");
    }

    #[tokio::test]
    async fn worker_branch_merges_without_touching_the_user_checkout() {
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repo");
        std::fs::create_dir(&repository).unwrap();
        git_status(&repository, &["init", "-b", "main"])
            .await
            .unwrap();
        git_status(&repository, &["config", "user.email", "kbctl@example.test"])
            .await
            .unwrap();
        git_status(&repository, &["config", "user.name", "kbctl test"])
            .await
            .unwrap();
        std::fs::create_dir(repository.join("src")).unwrap();
        std::fs::write(repository.join("src/lib.rs"), "base\n").unwrap();
        git_status(&repository, &["add", "src/lib.rs"])
            .await
            .unwrap();
        git_status(&repository, &["commit", "-m", "base"])
            .await
            .unwrap();
        let base = inspect(&repository).await.unwrap();
        let integration = directory.path().join("integration");
        let worker = directory.path().join("worker");
        create_worktree(&repository, &integration, "kbctl/task/v1", &base.head)
            .await
            .unwrap();
        create_worktree(
            &repository,
            &worker,
            "kbctl-worker/task/v1-step",
            "kbctl/task/v1",
        )
        .await
        .unwrap();
        std::fs::write(worker.join("src/lib.rs"), "changed\n").unwrap();
        git_status(&worker, &["add", "src/lib.rs"]).await.unwrap();
        git_status(&worker, &["commit", "-m", "worker"])
            .await
            .unwrap();
        let worker_head = inspect(&worker).await.unwrap().head;
        let files = changed_files(&worker, &base.head, &worker_head)
            .await
            .unwrap();
        validate_write_scope(&files, &["src/**".to_string()]).unwrap();
        merge(&integration, "kbctl-worker/task/v1-step")
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(repository.join("src/lib.rs")).unwrap(),
            "base\n"
        );
        assert_eq!(inspect(&repository).await.unwrap().branch, "main");
        assert_eq!(
            std::fs::read_to_string(integration.join("src/lib.rs")).unwrap(),
            "changed\n"
        );
    }
}
