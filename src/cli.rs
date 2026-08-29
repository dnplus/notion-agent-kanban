use crate::{
    config::{Config, LocalProjectBinding},
    daemon::Daemon,
    domain::{ExecutionMode, Report, TaskStatus},
    error::KbctlError,
    herdr::{AgentRuntime, HerdrRuntime},
    install,
    notion::{KanbanProvider, NotionProvider, TaskUpdate},
    report_spool,
    store::Store,
    tui,
};
use anyhow::Result;
use chrono::Utc;
use clap::{Args, Parser, Subcommand};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Parser)]
#[command(
    name = "kbctl",
    version,
    about = "Notion-backed Kanban control for local agents"
)]
pub struct Cli {
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init(InitArgs),
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    Board,
    Project {
        #[command(subcommand)]
        command: ProjectCommand,
    },
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    Report {
        #[command(subcommand)]
        command: ReportCommand,
    },
    Doctor,
    Install(InstallArgs),
    #[command(name = "_herdr-open-board", hide = true)]
    HerdrOpenBoard,
}

#[derive(Debug, Args)]
struct InitArgs {
    #[arg(long, value_name = "PAGE_ID_OR_URL", env = "NOTION_PARENT_PAGE_ID")]
    parent: Option<String>,
    #[arg(long, value_name = "DIRECTORY")]
    project_path: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    Run,
}

#[derive(Debug, Subcommand)]
enum ProjectCommand {
    Bind {
        project: String,
        directory: PathBuf,
        #[arg(long, default_value = "codex")]
        agent: String,
    },
}

#[derive(Debug, Subcommand)]
enum TaskCommand {
    Move { task: String, status: TaskStatus },
}

#[derive(Debug, Subcommand)]
enum ReportCommand {
    Done(ReportArgs),
    Blocked(ReportArgs),
    Review(ReportArgs),
}

#[derive(Debug, Args)]
struct ReportArgs {
    #[arg(long, env = "KBCTL_EXECUTION_ID")]
    execution: Option<String>,
    #[arg(long)]
    summary: Option<String>,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long, value_name = "PATH")]
    result_file: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct InstallArgs {
    #[arg(long)]
    grok: bool,
    #[arg(long)]
    herdr: bool,
    #[arg(long)]
    codex: bool,
}

pub async fn run() -> Result<()> {
    dotenvy::dotenv().ok();
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kbctl=info")
        .try_init();
    let cli = Cli::parse();
    let config_path = cli.config.as_deref();
    let config = if report_spool::configured_path().is_some()
        && matches!(&cli.command, Command::Report { .. })
    {
        Config::default()
    } else {
        Config::load(config_path)?
    };
    match cli.command {
        Command::Init(args) => init(config, config_path, args).await?,
        Command::Daemon {
            command: DaemonCommand::Run,
        } => run_daemon(config).await?,
        Command::Board => run_board(config).await?,
        Command::Project {
            command:
                ProjectCommand::Bind {
                    project,
                    directory,
                    agent,
                },
        } => {
            bind_project(config, config_path, project, directory, agent)?;
        }
        Command::Task {
            command: TaskCommand::Move { task, status },
        } => move_task(config, task, status).await?,
        Command::Report { command } => report(config, command).await?,
        Command::Doctor => doctor(config).await?,
        Command::Install(args) => install::run(config, args.grok, args.codex, args.herdr)?,
        Command::HerdrOpenBoard => install::open_herdr_board()?,
    }
    Ok(())
}

async fn init(mut config: Config, config_path: Option<&Path>, args: InitArgs) -> Result<()> {
    if config.notion.tasks_database_id.is_some() {
        println!(
            "kbctl 已初始化。資料庫之後搬到哪個 workspace 都不必重跑 init，只要目前的 Notion token 仍能存取同一組 database。若要重建，先從設定檔移除 database ID。"
        );
        return Ok(());
    }
    let provider = NotionProvider::new(config.clone())?;
    let parent_id = args
        .parent
        .or(config.notion.parent_page_id.clone())
        .map(|value| normalize_notion_id(&value))
        .transpose()?;
    let result = provider
        .initialize_workspace(parent_id.as_deref(), &mut config)
        .await?;
    if let Some(path) = args.project_path
        && let Some(binding) = config.project.default.as_mut()
    {
        binding.path = canonical_directory(&path)?;
    }
    let path = config.save(config_path)?;
    println!("已建立 Tasks database：{}", result.tasks_database_id);
    println!("已建立 Projects database：{}", result.projects_database_id);
    println!("預設 Project：{}", result.default_project_id);
    if let Some(view_id) = result.board_view_id {
        println!("已建立 Board view：{}", view_id);
    }
    println!("設定已保存至 {}", path.display());
    Ok(())
}

fn bind_project(
    config: Config,
    config_path: Option<&Path>,
    project: String,
    directory: PathBuf,
    agent: String,
) -> Result<()> {
    let mut config = config;
    let path = canonical_directory(&directory)?;
    let id = normalize_binding_id(&project);
    let name = if id == "__implicit__" {
        "Default Project".to_string()
    } else {
        project.clone()
    };
    let binding = LocalProjectBinding {
        id: id.clone(),
        name,
        path,
        default_agent: agent,
        active: true,
    };
    if id == "__implicit__" || project.eq_ignore_ascii_case("default") {
        config.project.default = Some(binding);
    } else {
        config.project.bindings.insert(id, binding);
    }
    let path = config.save(config_path)?;
    println!("Project binding 已保存至 {}", path.display());
    Ok(())
}

async fn run_daemon(config: Config) -> Result<()> {
    let provider = Arc::new(NotionProvider::new(config.clone())?);
    let runtime = Arc::new(HerdrRuntime::new(config.herdr.binary.clone()));
    let store = Store::open(crate::config::default_state_path())?;
    Daemon::new(config, provider, runtime, store).run().await?;
    Ok(())
}

async fn run_board(config: Config) -> Result<()> {
    tui::run(config).await?;
    Ok(())
}

async fn move_task(config: Config, task_id: String, status: TaskStatus) -> Result<()> {
    if !status.is_human_status() {
        return Err(KbctlError::Validation(
            "task move 只能使用 backlog、triage、scheduled、ready、cancel 或 archived".to_string(),
        )
        .into());
    }
    let provider = NotionProvider::new(config.clone())?;
    let task_id = normalize_notion_id(&task_id)?;
    let task = provider.get_task(&task_id).await?;
    if status == TaskStatus::Cancel
        && let Some(execution_id) = task.execution_id.as_deref()
    {
        let store = Store::open(crate::config::default_state_path())?;
        if let Some(execution) = store.execution(execution_id)?
            && let Some(runtime_id) = execution.runtime_id.as_deref()
        {
            let runtime = HerdrRuntime::new(config.herdr.binary.clone());
            let _ = runtime.cancel(runtime_id).await;
        }
    }
    provider
        .update_task(TaskUpdate {
            id: task.id.clone(),
            status: Some(status),
            clear_execution_id: status == TaskStatus::Cancel,
            ..Default::default()
        })
        .await?;
    let store = Store::open(crate::config::default_state_path())?;
    let mut cached_task = task;
    cached_task.status = status;
    if status == TaskStatus::Cancel {
        cached_task.execution_id = None;
    }
    store.cache_task(&cached_task)?;
    println!("Task 已更新為 {status}");
    Ok(())
}

async fn report(config: Config, command: ReportCommand) -> Result<()> {
    let (status, args) = match command {
        ReportCommand::Done(args) => (TaskStatus::Done, args),
        ReportCommand::Blocked(args) => (TaskStatus::Blocked, args),
        ReportCommand::Review(args) => (TaskStatus::Review, args),
    };
    let execution_id = args
        .execution
        .or_else(|| std::env::var("KBCTL_EXECUTION_ID").ok())
        .ok_or_else(|| {
            KbctlError::Validation("必須提供 --execution 或 KBCTL_EXECUTION_ID".to_string())
        })?;
    let result_file = args
        .result_file
        .map(|path| {
            std::fs::read_to_string(&path).map_err(|error| {
                KbctlError::Validation(format!("讀取 result file {}: {error}", path.display()))
            })
        })
        .transpose()?;
    let summary = args.summary.filter(|value| !value.trim().is_empty());
    let reason = args.reason.filter(|value| !value.trim().is_empty());
    let report = Report {
        execution_id: execution_id.clone(),
        status,
        summary: summary.clone(),
        reason: reason.clone(),
        result_file: result_file.as_ref().map(|_| "provided".to_string()),
        reported_at: Utc::now(),
    };
    let result_text = render_report(&report, result_file.as_deref());
    if let Some(path) = report_spool::configured_path() {
        let task_id = std::env::var("KBCTL_TASK_ID").map_err(|_| {
            KbctlError::Validation("KBCTL_TASK_ID is required for an agent report".to_string())
        })?;
        let mode = match std::env::var("KBCTL_EXECUTION_MODE").ok().as_deref() {
            Some("triage") => ExecutionMode::Triage,
            Some("execute") => ExecutionMode::Execute,
            Some(value) => {
                return Err(KbctlError::Validation(format!(
                    "invalid KBCTL_EXECUTION_MODE: {value}"
                ))
                .into());
            }
            None => {
                return Err(KbctlError::Validation(
                    "KBCTL_EXECUTION_MODE is required for an agent report".to_string(),
                )
                .into());
            }
        };
        report.validate(mode).map_err(KbctlError::Validation)?;
        report_spool::write(
            &path,
            &report_spool::AgentReport {
                task_id,
                report,
                result_text,
            },
        )?;
        println!("report 已寫入本機回報檔，daemon 會同步回 Notion。");
        return Ok(());
    }
    let store = Store::open(crate::config::default_state_path())?;
    let execution = store
        .execution(&execution_id)?
        .ok_or_else(|| KbctlError::Validation(format!("找不到 execution {execution_id}")))?;
    report
        .validate(execution.mode)
        .map_err(KbctlError::Validation)?;
    let inserted = store.record_report(&execution.task_id, &report, &result_text)?;
    if !inserted {
        println!("execution {execution_id} 的 report 已存在，保留冪等結果。");
    }
    let provider = Arc::new(NotionProvider::new(config.clone())?);
    let runtime = Arc::new(HerdrRuntime::new(config.herdr.binary.clone()));
    let daemon = Daemon::new(config, provider, runtime, store);
    let applied = daemon.flush_reports_once().await?;
    if applied == 0 {
        println!("report 已寫入本機 outbox，Notion 暫時未完成同步；daemon 會重試。");
    } else {
        println!("report 已驗證並同步回 Notion。");
    }
    Ok(())
}

async fn doctor(config: Config) -> Result<()> {
    let provider = match NotionProvider::new(config.clone()) {
        Ok(provider) => provider,
        Err(error) => {
            println!("FAIL Notion credential: {error}");
            return Ok(());
        }
    };
    match provider.verify().await {
        Ok(value) => {
            let workspace = value
                .get("bot")
                .or_else(|| value.get("user"))
                .and_then(|value| value.get("name"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("connected");
            println!("PASS Notion API ({workspace})");
        }
        Err(error) => println!("FAIL Notion API: {error}"),
    }
    if config.notion.tasks_data_source_id.is_none() {
        println!("WARN Tasks database 尚未設定；請先執行 kbctl init");
    }
    if let Err(error) = provider.schema_is_current().await {
        println!("FAIL schema: {error}");
    } else {
        println!("PASS schema");
    }
    for binding in config
        .project
        .bindings
        .values()
        .chain(config.project.default.iter())
    {
        if Path::new(&binding.path).is_dir() {
            println!("PASS project {}: {}", binding.name, binding.path);
        } else {
            println!("FAIL project {} path: {}", binding.name, binding.path);
        }
    }
    let runtime = HerdrRuntime::new(config.herdr.binary);
    match runtime.status().await {
        Ok(_) => println!("PASS Herdr"),
        Err(error) => println!("FAIL Herdr: {error}"),
    }
    Ok(())
}

fn render_report(report: &Report, result_file: Option<&str>) -> String {
    let mut lines = vec![
        format!("kbctl-execution:{}", report.execution_id),
        format!("status: {}", report.status),
    ];
    if let Some(summary) = report.summary.as_deref() {
        lines.push(format!("summary: {summary}"));
    }
    if let Some(reason) = report.reason.as_deref() {
        lines.push(format!("reason: {reason}"));
    }
    if let Some(result_file) = result_file.filter(|value| !value.trim().is_empty()) {
        lines.push(format!("result:\n{result_file}"));
    }
    lines.join("\n")
}

fn canonical_directory(path: &Path) -> Result<String, KbctlError> {
    let path = std::fs::canonicalize(path).map_err(|error| {
        KbctlError::Validation(format!("project directory {}: {error}", path.display()))
    })?;
    if !path.is_dir() {
        return Err(KbctlError::Validation(format!(
            "not a directory: {}",
            path.display()
        )));
    }
    Ok(path.display().to_string())
}

fn normalize_binding_id(value: &str) -> String {
    if value.eq_ignore_ascii_case("default") || value.eq_ignore_ascii_case("implicit") {
        "__implicit__".to_string()
    } else {
        value.to_string()
    }
}

fn normalize_notion_id(value: &str) -> Result<String, KbctlError> {
    let trimmed = value.trim().trim_end_matches('/');
    let candidate = trimmed
        .rsplit('/')
        .next()
        .unwrap_or(trimmed)
        .split('?')
        .next()
        .unwrap_or(trimmed)
        .split('#')
        .next()
        .unwrap_or(trimmed);
    let compact = candidate.replace('-', "");
    if compact.len() == 32
        && compact
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return Ok(compact);
    }
    if !trimmed.is_empty() {
        Ok(trimmed.to_string())
    } else {
        Err(KbctlError::Validation("Notion ID 不可為空".to_string()))
    }
}
