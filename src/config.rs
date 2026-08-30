use crate::{domain::ExecutionRole, error::KbctlError};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub notion: NotionConfig,
    pub mapping: MappingConfig,
    pub project: ProjectConfig,
    pub daemon: DaemonConfig,
    pub herdr: HerdrConfig,
    pub orchestration: OrchestrationConfig,
    pub profiles: BTreeMap<String, AgentProfile>,
}

impl Default for Config {
    fn default() -> Self {
        let mut config = Self {
            notion: NotionConfig::default(),
            mapping: MappingConfig::default(),
            project: ProjectConfig::default(),
            daemon: DaemonConfig::default(),
            herdr: HerdrConfig::default(),
            orchestration: OrchestrationConfig::default(),
            profiles: BTreeMap::new(),
        };
        config.ensure_default_profiles();
        config
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct NotionConfig {
    pub token: Option<String>,
    pub parent_page_id: Option<String>,
    pub workspace_id: Option<String>,
    pub workspace_name: Option<String>,
    pub tasks_database_id: Option<String>,
    pub tasks_data_source_id: Option<String>,
    pub projects_database_id: Option<String>,
    pub projects_data_source_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct MappingConfig {
    pub tasks: PropertyMapping,
    pub projects: PropertyMapping,
    pub status_options: BTreeMap<String, String>,
    pub schema_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PropertyMapping {
    pub name: Option<String>,
    pub status: Option<String>,
    pub project: Option<String>,
    pub agent: Option<String>,
    pub scheduled_at: Option<String>,
    pub due: Option<String>,
    pub execution_id: Option<String>,
    pub result: Option<String>,
    pub path: Option<String>,
    pub default_agent: Option<String>,
    pub active: Option<String>,
    pub last_activity: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProjectConfig {
    pub default: Option<LocalProjectBinding>,
    pub bindings: BTreeMap<String, LocalProjectBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalProjectBinding {
    pub id: String,
    pub name: String,
    pub path: String,
    pub default_agent: String,
    pub active: bool,
    #[serde(default)]
    pub checks: Vec<String>,
    #[serde(default = "default_check_timeout_seconds")]
    pub check_timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OrchestrationConfig {
    pub supervisor_profile: String,
    pub max_steps: usize,
    pub max_workers_per_plan: usize,
    pub max_rework: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentProfile {
    pub kind: String,
    pub model: Option<String>,
    pub reasoning: Option<String>,
    pub agent: Option<String>,
    pub role: ExecutionRole,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DaemonConfig {
    pub poll_interval_seconds: u64,
    pub max_concurrency: usize,
    pub max_attempts: u32,
    pub retry_delay_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HerdrConfig {
    pub binary: String,
    pub plugin_directory: Option<String>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            poll_interval_seconds: 15,
            max_concurrency: 1,
            max_attempts: 3,
            retry_delay_seconds: 15,
        }
    }
}

impl Default for HerdrConfig {
    fn default() -> Self {
        Self {
            binary: "herdr".to_string(),
            plugin_directory: None,
        }
    }
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            supervisor_profile: "supervisor".to_string(),
            max_steps: 8,
            max_workers_per_plan: 3,
            max_rework: 2,
        }
    }
}

impl Default for AgentProfile {
    fn default() -> Self {
        Self {
            kind: "codex".to_string(),
            model: None,
            reasoning: None,
            agent: None,
            role: ExecutionRole::Worker,
        }
    }
}

fn default_check_timeout_seconds() -> u64 {
    900
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self, KbctlError> {
        let path = path.map(PathBuf::from).unwrap_or_else(default_config_path);
        if !path.exists() {
            return Ok(Self::default());
        }
        let contents = fs::read_to_string(&path)
            .map_err(|error| KbctlError::Config(format!("read {}: {error}", path.display())))?;
        let mut config: Self = toml::from_str(&contents)
            .map_err(|error| KbctlError::Config(format!("parse {}: {error}", path.display())))?;
        config.ensure_default_profiles();
        Ok(config)
    }

    pub fn save(&self, path: Option<&Path>) -> Result<PathBuf, KbctlError> {
        let path = path.map(PathBuf::from).unwrap_or_else(default_config_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                KbctlError::Config(format!("create {}: {error}", parent.display()))
            })?;
        }
        let rendered = toml::to_string_pretty(self)
            .map_err(|error| KbctlError::Config(format!("serialize config: {error}")))?;
        let temporary = path.with_extension("toml.tmp");
        fs::write(&temporary, rendered).map_err(|error| {
            KbctlError::Config(format!("write {}: {error}", temporary.display()))
        })?;
        fs::rename(&temporary, &path)
            .map_err(|error| KbctlError::Config(format!("replace {}: {error}", path.display())))?;
        set_private_permissions(&path)?;
        Ok(path)
    }

    pub fn token(&self) -> Result<String, KbctlError> {
        env::var("NOTION_API_TOKEN")
            .ok()
            .or_else(|| self.notion.token.clone())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                KbctlError::Config(
                    "Notion PAT is missing; set NOTION_API_TOKEN or notion.token in config.toml"
                        .to_string(),
                )
            })
    }

    pub fn task_property(&self, canonical: &str) -> Option<&str> {
        let mapping = &self.mapping.tasks;
        match canonical {
            "name" => mapping.name.as_deref(),
            "status" => mapping.status.as_deref(),
            "project" => mapping.project.as_deref(),
            "agent" => mapping.agent.as_deref(),
            "scheduled_at" => mapping.scheduled_at.as_deref(),
            "due" => mapping.due.as_deref(),
            "execution_id" => mapping.execution_id.as_deref(),
            "result" => mapping.result.as_deref(),
            _ => None,
        }
    }

    pub fn project_binding(&self, project_id: Option<&str>) -> Option<&LocalProjectBinding> {
        project_id
            .and_then(|id| self.project.bindings.get(id))
            .or(self.project.default.as_ref())
    }

    pub fn ensure_default_profiles(&mut self) {
        self.profiles
            .entry("supervisor".to_string())
            .or_insert_with(|| AgentProfile {
                kind: "codex".to_string(),
                model: Some("gpt-5.6-sol".to_string()),
                reasoning: Some("high".to_string()),
                agent: None,
                role: ExecutionRole::Supervisor,
            });
        self.profiles
            .entry("fast_worker".to_string())
            .or_insert_with(|| AgentProfile {
                kind: "codex".to_string(),
                model: Some("gpt-5.6-luna".to_string()),
                reasoning: Some("high".to_string()),
                agent: None,
                role: ExecutionRole::Worker,
            });
        self.profiles
            .entry("opencode_worker".to_string())
            .or_insert_with(|| AgentProfile {
                kind: "opencode".to_string(),
                model: None,
                reasoning: None,
                agent: Some("build".to_string()),
                role: ExecutionRole::Worker,
            });
        self.profiles
            .entry("grok_worker".to_string())
            .or_insert_with(|| AgentProfile {
                kind: "grok".to_string(),
                model: None,
                reasoning: None,
                agent: None,
                role: ExecutionRole::Worker,
            });
    }

    pub fn profile(&self, name: &str) -> Option<AgentProfile> {
        self.profiles.get(name).cloned().or_else(|| {
            matches!(name, "codex" | "opencode" | "grok").then(|| AgentProfile {
                kind: name.to_string(),
                ..AgentProfile::default()
            })
        })
    }
}

fn default_config_path() -> PathBuf {
    if let Some(path) = env::var_os("KBCTL_CONFIG") {
        return PathBuf::from(path);
    }
    home_dir().join(".config/kbctl/config.toml")
}

pub fn default_state_path() -> PathBuf {
    if let Some(path) = env::var_os("KBCTL_STATE") {
        return PathBuf::from(path);
    }
    home_dir().join(".local/share/kbctl/state.db")
}

pub fn default_lock_path() -> PathBuf {
    default_state_path().with_extension("lock")
}

fn home_dir() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn set_private_permissions(path: &Path) -> Result<(), KbctlError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)
            .map_err(|error| KbctlError::Config(format!("stat {}: {error}", path.display())))?
            .permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(path, permissions)
            .map_err(|error| KbctlError::Config(format!("protect {}: {error}", path.display())))?;
    }
    Ok(())
}
