use crate::{
    config::{Config, MappingConfig, PropertyMapping},
    domain::{Project, SchemaSnapshot, Task, TaskStatus},
    error::KbctlError,
};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use futures::TryStreamExt;
use notionrs::{Client, PaginateExt};
use notionrs_types::prelude::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

#[derive(Debug, Clone)]
pub enum DatabaseTarget {
    Database(String),
    DataSource(String),
}

#[derive(Debug, Clone, Default)]
pub struct TaskUpdate {
    pub id: String,
    pub status: Option<TaskStatus>,
    pub execution_id: Option<String>,
    pub clear_execution_id: bool,
    pub result: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskCreate {
    pub name: String,
    pub status: TaskStatus,
    pub due: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct ProjectUpdate {
    pub id: String,
    pub last_activity: Option<DateTime<Utc>>,
    pub active: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct InitializationResult {
    pub tasks_database_id: String,
    pub tasks_data_source_id: String,
    pub projects_database_id: String,
    pub projects_data_source_id: String,
    pub default_project_id: String,
    pub board_view_id: Option<String>,
}

#[async_trait]
pub trait KanbanProvider: Send + Sync {
    async fn discover_schema(&self, target: DatabaseTarget) -> Result<SchemaSnapshot, KbctlError>;
    async fn list_tasks(&self) -> Result<Vec<Task>, KbctlError>;
    async fn get_task(&self, id: &str) -> Result<Task, KbctlError>;
    async fn create_task(&self, create: TaskCreate) -> Result<Task, KbctlError> {
        let _ = create;
        Err(KbctlError::Validation(
            "this kanban provider does not support task creation".to_string(),
        ))
    }
    async fn update_task(&self, update: TaskUpdate) -> Result<(), KbctlError>;
    async fn append_result(&self, id: &str, result: &str) -> Result<(), KbctlError>;
    async fn update_project(&self, update: ProjectUpdate) -> Result<(), KbctlError>;
    async fn schema_is_current(&self) -> Result<(), KbctlError> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct NotionProvider {
    client: Arc<Client>,
    token: String,
    http: reqwest::Client,
    pub config: Config,
}

impl NotionProvider {
    pub fn new(config: Config) -> Result<Self, KbctlError> {
        let token = config.token()?;
        Ok(Self {
            client: Arc::new(Client::new(&token)),
            token,
            http: reqwest::Client::new(),
            config,
        })
    }

    pub async fn verify(&self) -> Result<Value, KbctlError> {
        let response = self
            .client
            .search_page()
            .filter_in_trash(false)
            .page_size(1)
            .send()
            .await
            .map_err(notion_error)?;
        serde_json::to_value(response).map_err(|error| KbctlError::Notion(error.to_string()))
    }

    pub async fn search_pages(&self, query: Option<&str>) -> Result<Vec<PageResponse>, KbctlError> {
        let client = self.client.search_page().filter_in_trash(false);
        let client = query
            .filter(|value| !value.trim().is_empty())
            .map_or(client.clone(), |value| client.query(value));
        client
            .into_stream()
            .try_collect::<Vec<PageResponse>>()
            .await
            .map_err(notion_error)
    }

    pub fn page_title_from_response(page: &PageResponse) -> String {
        serde_json::to_value(page)
            .ok()
            .and_then(|value| value.get("properties").cloned())
            .and_then(|properties| property_text(&properties, None, &["title", "Name", "Title"]))
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| page.id.clone())
    }

    async fn create_database(
        &self,
        parent_page_id: Option<&str>,
        title: &str,
        properties: HashMap<String, DataSourceProperty>,
    ) -> Result<DatabaseResponse, KbctlError> {
        let payload = database_create_payload(parent_page_id, title, &properties)?;
        let response = self
            .http
            .post("https://api.notion.com/v1/databases")
            .bearer_auth(&self.token)
            .header("Notion-Version", "2026-03-11")
            .json(&payload)
            .send()
            .await
            .map_err(|error| KbctlError::Notion(format!("database create request: {error}")))?;
        let status = response.status();
        let body = response.bytes().await.map_err(|error| {
            KbctlError::Notion(format!("database create response body: {error}"))
        })?;
        if !status.is_success() {
            let message = serde_json::from_slice::<Value>(&body)
                .ok()
                .and_then(|value| {
                    value
                        .get("message")
                        .and_then(Value::as_str)
                        .or_else(|| value.get("code").and_then(Value::as_str))
                        .map(ToOwned::to_owned)
                })
                .unwrap_or_else(|| String::from_utf8_lossy(&body).trim().to_string());
            return Err(KbctlError::Notion(format!(
                "database create HTTP {}: {}",
                status.as_u16(),
                if message.is_empty() {
                    "unknown Notion error"
                } else {
                    &message
                }
            )));
        }
        serde_json::from_slice::<DatabaseResponse>(&body).map_err(|error| {
            KbctlError::Notion(format!("decode database create response: {error}"))
        })
    }

    pub async fn initialize_workspace(
        &self,
        parent_page_id: Option<&str>,
        config: &mut Config,
    ) -> Result<InitializationResult, KbctlError> {
        let projects_properties = project_schema()?;
        let projects_database = self
            .create_database(parent_page_id, "Projects", projects_properties)
            .await?;
        let projects_data_source_id = projects_database
            .data_sources
            .first()
            .map(|source| source.id.clone())
            .ok_or_else(|| {
                KbctlError::Notion("Projects database returned no data source".to_string())
            })?;

        let default_path = std::env::current_dir()
            .map_err(|error| KbctlError::Config(format!("current directory: {error}")))?
            .display()
            .to_string();
        let default_project_properties = page_properties([
            ("Name", page_title("Default Project")?),
            ("Path", page_rich_text(&default_path)?),
            ("Default Agent", page_rich_text("codex")?),
            ("Active", page_checkbox(true)?),
        ])?;
        let default_project = match self
            .client
            .create_page::<HashMap<String, PageProperty>>()
            .data_source_id(projects_data_source_id.clone())
            .properties(default_project_properties)
            .send()
            .await
            .map_err(notion_error)
            .and_then(|response| response.into_page().map_err(notion_error))
        {
            Ok(page) => page,
            Err(error) => {
                let _ = self.archive_database(&projects_database.id).await;
                return Err(error);
            }
        };

        let tasks_properties = match task_schema(&projects_data_source_id) {
            Ok(properties) => properties,
            Err(error) => {
                let _ = self.archive_page(&default_project.id).await;
                let _ = self.archive_database(&projects_database.id).await;
                return Err(error);
            }
        };
        let tasks_database = match self
            .create_database(parent_page_id, "Tasks", tasks_properties)
            .await
        {
            Ok(database) => database,
            Err(error) => {
                let _ = self.archive_page(&default_project.id).await;
                let _ = self.archive_database(&projects_database.id).await;
                return Err(error);
            }
        };
        let tasks_data_source_id = tasks_database
            .data_sources
            .first()
            .map(|source| source.id.clone())
            .ok_or_else(|| {
                KbctlError::Notion("Tasks database returned no data source".to_string())
            })?;

        let tasks_schema_for_view = match self.retrieve_schema(&tasks_data_source_id).await {
            Ok(schema) => schema,
            Err(error) => {
                let _ = self.archive_page(&default_project.id).await;
                let _ = self.archive_database(&tasks_database.id).await;
                let _ = self.archive_database(&projects_database.id).await;
                return Err(error);
            }
        };
        let status_property_id = tasks_schema_for_view
            .properties
            .get("Status")
            .and_then(|value| value.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("Status")
            .to_string();
        let board_view = self
            .client
            .create_view()
            .database_id(tasks_database.id.clone())
            .data_source_id(tasks_data_source_id.clone())
            .name("Agent Board")
            .view_type(ViewType::Board)
            .configuration(json!({
                "type": "board",
                "group_by": {
                    "type": "status",
                    "property_id": status_property_id,
                    "group_by": "group",
                    "sort": { "type": "manual" }
                }
            }))
            .send()
            .await
            .map_err(notion_error)
            .ok();

        let tasks_schema = match self.retrieve_schema(&tasks_data_source_id).await {
            Ok(schema) => schema,
            Err(error) => {
                let _ = self.archive_page(&default_project.id).await;
                let _ = self.archive_database(&tasks_database.id).await;
                let _ = self.archive_database(&projects_database.id).await;
                return Err(error);
            }
        };
        let projects_schema = match self.retrieve_schema(&projects_data_source_id).await {
            Ok(schema) => schema,
            Err(error) => {
                let _ = self.archive_page(&default_project.id).await;
                let _ = self.archive_database(&tasks_database.id).await;
                let _ = self.archive_database(&projects_database.id).await;
                return Err(error);
            }
        };
        config.notion.parent_page_id = parent_page_id.map(ToOwned::to_owned);
        config.notion.tasks_database_id = Some(tasks_database.id.clone());
        config.notion.tasks_data_source_id = Some(tasks_data_source_id.clone());
        config.notion.projects_database_id = Some(projects_database.id.clone());
        config.notion.projects_data_source_id = Some(projects_data_source_id.clone());
        config.mapping = mapping_from_schemas(&tasks_schema, &projects_schema);
        config.project.default = Some(crate::config::LocalProjectBinding {
            id: default_project.id.clone(),
            name: "Default Project".to_string(),
            path: default_path,
            default_agent: "codex".to_string(),
            active: true,
        });
        Ok(InitializationResult {
            tasks_database_id: tasks_database.id,
            tasks_data_source_id,
            projects_database_id: projects_database.id,
            projects_data_source_id,
            default_project_id: default_project.id,
            board_view_id: board_view.map(|view| view.id),
        })
    }

    pub async fn archive_database(&self, database_id: &str) -> Result<(), KbctlError> {
        self.client
            .update_database()
            .database_id(database_id)
            .in_trash(true)
            .send()
            .await
            .map_err(notion_error)?;
        Ok(())
    }

    async fn archive_page(&self, page_id: &str) -> Result<(), KbctlError> {
        self.client
            .update_page::<HashMap<String, PageProperty>>()
            .page_id(page_id)
            .in_trash(true)
            .send()
            .await
            .map_err(notion_error)?;
        Ok(())
    }

    async fn data_source_schema(
        &self,
        target: DatabaseTarget,
    ) -> Result<SchemaSnapshot, KbctlError> {
        match target {
            DatabaseTarget::DataSource(id) => self.retrieve_schema(&id).await,
            DatabaseTarget::Database(id) => {
                let database = self
                    .client
                    .retrieve_database()
                    .database_id(id.clone())
                    .send()
                    .await
                    .map_err(notion_error)?;
                let source = database.data_sources.first().ok_or_else(|| {
                    KbctlError::Notion("database returned no data source".to_string())
                })?;
                self.retrieve_schema(&source.id).await
            }
        }
    }

    async fn retrieve_schema(&self, data_source_id: &str) -> Result<SchemaSnapshot, KbctlError> {
        retrieve_schema_from_client(&self.client, data_source_id).await
    }

    async fn query_tasks(&self) -> Result<Vec<Task>, KbctlError> {
        let data_source_id = self
            .config
            .notion
            .tasks_data_source_id
            .as_deref()
            .ok_or_else(|| {
                KbctlError::Config(
                    "tasks_data_source_id is not configured; run kbctl init".to_string(),
                )
            })?;
        let pages = self
            .client
            .query_data_source()
            .data_source_id(data_source_id)
            .into_stream()
            .try_collect::<Vec<PageResponse>>()
            .await
            .map_err(notion_error)?;
        pages
            .into_iter()
            .map(|page| self.page_to_task(page, None))
            .collect()
    }

    async fn query_projects(&self) -> Result<Vec<Project>, KbctlError> {
        let data_source_id = self
            .config
            .notion
            .projects_data_source_id
            .as_deref()
            .ok_or_else(|| {
                KbctlError::Config("projects_data_source_id is not configured".to_string())
            })?;
        let pages = self
            .client
            .query_data_source()
            .data_source_id(data_source_id)
            .into_stream()
            .try_collect::<Vec<PageResponse>>()
            .await
            .map_err(notion_error)?;
        pages
            .into_iter()
            .map(|page| page_to_project(page, &self.config.mapping.projects))
            .collect()
    }

    fn page_to_task(&self, page: PageResponse, body: Option<String>) -> Result<Task, KbctlError> {
        let value =
            serde_json::to_value(&page).map_err(|error| KbctlError::Notion(error.to_string()))?;
        let properties = value.get("properties").unwrap_or(&Value::Null);
        let mapping = &self.config.mapping.tasks;
        let name = property_text(properties, mapping.name.as_deref(), &["Name", "Title"])
            .unwrap_or_else(|| page.id.clone());
        let status_name =
            property_text(properties, mapping.status.as_deref(), &["Status", "State"])
                .unwrap_or_else(|| "backlog".to_string());
        let status = status_name
            .parse::<TaskStatus>()
            .unwrap_or(TaskStatus::Backlog);
        let project_id = property_relation(
            properties,
            mapping.project.as_deref(),
            &["Project", "Projects"],
        );
        let agent = property_text(properties, mapping.agent.as_deref(), &["Agent"]);
        let scheduled_at = property_date(
            properties,
            mapping.scheduled_at.as_deref(),
            &["Scheduled At", "Schedule At"],
        );
        let due = property_date(
            properties,
            mapping.due.as_deref(),
            &["Due", "Due Date", "Deadline"],
        );
        let execution_id = property_text(
            properties,
            mapping.execution_id.as_deref(),
            &["Execution ID", "Execution"],
        );
        let result = property_text(
            properties,
            mapping.result.as_deref(),
            &["Result", "Outcome"],
        );
        let last_edited_time = DateTime::parse_from_rfc3339(&page.last_edited_time.to_string())
            .ok()
            .map(|value| value.with_timezone(&Utc));
        Ok(Task {
            id: page.id,
            name,
            status,
            project_id,
            agent,
            scheduled_at,
            due,
            execution_id,
            result,
            body,
            last_edited_time,
        })
    }

    async fn fetch_task(&self, id: &str) -> Result<Task, KbctlError> {
        let page = self
            .client
            .get_page()
            .page_id(id)
            .send()
            .await
            .map_err(notion_error)?;
        let body = self
            .client
            .get_page_markdown()
            .page_id(id)
            .send()
            .await
            .map_err(notion_error)?
            .markdown;
        self.page_to_task(page, Some(body))
    }
}

#[async_trait]
impl KanbanProvider for NotionProvider {
    async fn discover_schema(&self, target: DatabaseTarget) -> Result<SchemaSnapshot, KbctlError> {
        self.data_source_schema(target).await
    }

    async fn list_tasks(&self) -> Result<Vec<Task>, KbctlError> {
        self.query_tasks().await
    }

    async fn get_task(&self, id: &str) -> Result<Task, KbctlError> {
        self.fetch_task(id).await
    }

    async fn create_task(&self, create: TaskCreate) -> Result<Task, KbctlError> {
        let data_source_id = self
            .config
            .notion
            .tasks_data_source_id
            .as_deref()
            .ok_or_else(|| {
                KbctlError::Config(
                    "tasks_data_source_id is not configured; run kbctl init".to_string(),
                )
            })?;
        let name_property = self.config.task_property("name").unwrap_or("Name");
        let status_property = self.config.task_property("status").unwrap_or("Status");
        let status_value = self
            .config
            .mapping
            .status_options
            .get(&create.status.to_string())
            .cloned()
            .unwrap_or_else(|| create.status.to_string());
        let mut properties = HashMap::new();
        properties.insert(name_property.to_string(), page_title(&create.name)?);
        properties.insert(
            status_property.to_string(),
            page_property_status(status_value)?,
        );
        if let Some(due) = create.due {
            let due_property = self.config.task_property("due").unwrap_or("Due");
            properties.insert(due_property.to_string(), page_property_date(&due)?);
        }
        let page = self
            .client
            .create_page::<HashMap<String, PageProperty>>()
            .data_source_id(data_source_id)
            .properties(properties)
            .send()
            .await
            .map_err(notion_error)
            .and_then(|response| response.into_page().map_err(notion_error))?;
        self.page_to_task(page, None)
    }

    async fn update_task(&self, update: TaskUpdate) -> Result<(), KbctlError> {
        let mut properties = HashMap::new();
        if let Some(status) = update.status {
            let property_name = self.config.task_property("status").unwrap_or("Status");
            let status_value = self
                .config
                .mapping
                .status_options
                .get(&status.to_string())
                .cloned()
                .unwrap_or_else(|| status.to_string());
            properties.insert(
                property_name.to_string(),
                page_property_status(status_value)?,
            );
        }
        if let Some(execution_id) = update.execution_id {
            let property_name = self
                .config
                .task_property("execution_id")
                .unwrap_or("Execution ID");
            properties.insert(
                property_name.to_string(),
                page_property_rich_text(&execution_id)?,
            );
        } else if update.clear_execution_id {
            let property_name = self
                .config
                .task_property("execution_id")
                .unwrap_or("Execution ID");
            properties.insert(property_name.to_string(), page_property_rich_text("")?);
        }
        if let Some(result) = update.result {
            let property_name = self.config.task_property("result").unwrap_or("Result");
            properties.insert(property_name.to_string(), page_property_rich_text(&result)?);
        }
        if properties.is_empty() {
            return Ok(());
        }
        self.client
            .update_page::<HashMap<String, PageProperty>>()
            .page_id(update.id)
            .properties(properties)
            .send()
            .await
            .map_err(notion_error)?;
        Ok(())
    }

    async fn append_result(&self, id: &str, result: &str) -> Result<(), KbctlError> {
        let marker = result.lines().next().unwrap_or_default().trim();
        if !marker.is_empty() {
            let existing = self
                .client
                .get_block_children()
                .block_id(id)
                .into_stream()
                .try_collect::<Vec<BlockResponse>>()
                .await
                .map_err(notion_error)?;
            if existing.iter().any(|block| {
                serde_json::to_value(block)
                    .map(|value| value.to_string().contains(marker))
                    .unwrap_or(false)
            }) {
                return Ok(());
            }
        }
        let block = Block::Paragraph {
            paragraph: ParagraphBlock::from(result),
        };
        self.client
            .append_block_children()
            .block_id(id)
            .children(vec![block])
            .position_end()
            .send()
            .await
            .map_err(notion_error)?;
        Ok(())
    }

    async fn update_project(&self, update: ProjectUpdate) -> Result<(), KbctlError> {
        let mut properties = HashMap::new();
        if let Some(last_activity) = update.last_activity {
            let name = self
                .config
                .mapping
                .projects
                .last_activity
                .as_deref()
                .unwrap_or("Last Activity");
            properties.insert(name.to_string(), page_property_date(&last_activity)?);
        }
        if let Some(active) = update.active {
            let name = self
                .config
                .mapping
                .projects
                .active
                .as_deref()
                .unwrap_or("Active");
            properties.insert(name.to_string(), page_property_checkbox(active)?);
        }
        if properties.is_empty() {
            return Ok(());
        }
        self.client
            .update_page::<HashMap<String, PageProperty>>()
            .page_id(update.id)
            .properties(properties)
            .send()
            .await
            .map_err(notion_error)?;
        Ok(())
    }

    async fn schema_is_current(&self) -> Result<(), KbctlError> {
        let tasks_id = self
            .config
            .notion
            .tasks_data_source_id
            .as_deref()
            .ok_or_else(|| {
                KbctlError::Config(
                    "tasks_data_source_id is not configured; run kbctl init".to_string(),
                )
            })?;
        let tasks = self.retrieve_schema(tasks_id).await?;
        missing_required_property(&tasks.properties, REQUIRED_TASK_PROPERTIES).map_err(
            |property| {
                KbctlError::Validation(format!(
                    "Tasks database is missing required property {property}"
                ))
            },
        )?;
        if let Some(projects_id) = self.config.notion.projects_data_source_id.as_deref() {
            let projects = self.retrieve_schema(projects_id).await?;
            missing_required_property(&projects.properties, REQUIRED_PROJECT_PROPERTIES).map_err(
                |property| {
                    KbctlError::Validation(format!(
                        "Projects database is missing required property {property}"
                    ))
                },
            )?;
        }
        Ok(())
    }
}

pub async fn list_projects(provider: &NotionProvider) -> Result<Vec<Project>, KbctlError> {
    provider.query_projects().await
}

fn default_mapping() -> MappingConfig {
    MappingConfig {
        tasks: PropertyMapping {
            name: Some("Name".to_string()),
            status: Some("Status".to_string()),
            project: Some("Project".to_string()),
            agent: Some("Agent".to_string()),
            scheduled_at: Some("Scheduled At".to_string()),
            due: Some("Due".to_string()),
            execution_id: Some("Execution ID".to_string()),
            result: Some("Result".to_string()),
            ..Default::default()
        },
        projects: PropertyMapping {
            name: Some("Name".to_string()),
            path: Some("Path".to_string()),
            default_agent: Some("Default Agent".to_string()),
            active: Some("Active".to_string()),
            last_activity: Some("Last Activity".to_string()),
            ..Default::default()
        },
        status_options: default_status_options(),
        schema_fingerprint: None,
    }
}

fn data_source_property_id(property: &DataSourceProperty) -> Option<&str> {
    match property {
        DataSourceProperty::Button(value) => value.id.as_deref(),
        DataSourceProperty::Checkbox(value) => value.id.as_deref(),
        DataSourceProperty::CreatedBy(value) => value.id.as_deref(),
        DataSourceProperty::CreatedTime(value) => value.id.as_deref(),
        DataSourceProperty::Date(value) => value.id.as_deref(),
        DataSourceProperty::Email(value) => value.id.as_deref(),
        DataSourceProperty::Files(value) => value.id.as_deref(),
        DataSourceProperty::Formula(value) => value.id.as_deref(),
        DataSourceProperty::LastEditedBy(value) => value.id.as_deref(),
        DataSourceProperty::LastEditedTime(value) => value.id.as_deref(),
        DataSourceProperty::MultiSelect(value) => value.id.as_deref(),
        DataSourceProperty::Number(value) => value.id.as_deref(),
        DataSourceProperty::People(value) => value.id.as_deref(),
        DataSourceProperty::PhoneNumber(value) => value.id.as_deref(),
        DataSourceProperty::Place(value) => value.id.as_deref(),
        DataSourceProperty::Relation(value) => value.id.as_deref(),
        DataSourceProperty::RichText(value) => value.id.as_deref(),
        DataSourceProperty::Rollup(value) => value.id.as_deref(),
        DataSourceProperty::Select(value) => value.id.as_deref(),
        DataSourceProperty::Status(value) => value.id.as_deref(),
        DataSourceProperty::Title(value) => value.id.as_deref(),
        DataSourceProperty::UniqueId(value) => value.id.as_deref(),
        DataSourceProperty::Url(value) => value.id.as_deref(),
        DataSourceProperty::Verification(value) => value.id.as_deref(),
    }
}

async fn retrieve_schema_from_client(
    client: &Client,
    data_source_id: &str,
) -> Result<SchemaSnapshot, KbctlError> {
    let response = client
        .retrieve_data_source()
        .data_source_id(data_source_id)
        .send()
        .await
        .map_err(notion_error)?;
    let mut properties = serde_json::to_value(&response.properties)
        .map_err(|error| KbctlError::Notion(error.to_string()))?;
    if let Some(property_map) = properties.as_object_mut() {
        for (name, property) in &response.properties {
            if let Some(id) = data_source_property_id(property)
                && let Some(value) = property_map.get_mut(name).and_then(Value::as_object_mut)
            {
                value.insert("id".to_string(), Value::String(id.to_string()));
            }
        }
    }
    let fingerprint = fingerprint(&properties);
    Ok(SchemaSnapshot {
        database_id: response.parent.database_id.clone(),
        data_source_id: Some(response.id),
        properties,
        fingerprint,
    })
}

fn page_to_project(page: PageResponse, mapping: &PropertyMapping) -> Result<Project, KbctlError> {
    let value =
        serde_json::to_value(&page).map_err(|error| KbctlError::Notion(error.to_string()))?;
    let properties = value.get("properties").unwrap_or(&Value::Null);
    Ok(Project {
        id: page.id,
        name: property_text(properties, mapping.name.as_deref(), &["Name", "Title"])
            .unwrap_or_else(|| "Unnamed Project".to_string()),
        path: property_text(properties, mapping.path.as_deref(), &["Path"]),
        default_agent: property_text(
            properties,
            mapping.default_agent.as_deref(),
            &["Default Agent", "Agent"],
        ),
        active: property_bool(properties, mapping.active.as_deref(), &["Active"]).unwrap_or(true),
        last_activity: property_date(
            properties,
            mapping.last_activity.as_deref(),
            &["Last Activity"],
        ),
    })
}

fn property_value<'a>(
    properties: &'a Value,
    configured: Option<&str>,
    aliases: &[&str],
) -> Option<&'a Value> {
    let map = properties.as_object()?;
    if let Some(configured) = configured {
        if let Some(value) = map.get(configured) {
            return Some(value);
        }
        if let Some(value) = map
            .values()
            .find(|value| value.get("id").and_then(Value::as_str) == Some(configured))
        {
            return Some(value);
        }
    }
    aliases
        .iter()
        .find_map(|alias| map.get(*alias))
        .or_else(|| {
            map.iter()
                .find(|(name, _)| aliases.iter().any(|alias| name.eq_ignore_ascii_case(alias)))
                .map(|(_, value)| value)
        })
}

fn property_text(properties: &Value, configured: Option<&str>, aliases: &[&str]) -> Option<String> {
    let value = property_value(properties, configured, aliases)?;
    if let Some(value) = value.get("title").and_then(Value::as_array) {
        let text = rich_text_plain(value);
        return (!text.trim().is_empty()).then_some(text);
    }
    if let Some(value) = value.get("rich_text").and_then(Value::as_array) {
        let text = rich_text_plain(value);
        return (!text.trim().is_empty()).then_some(text);
    }
    if let Some(value) = value
        .get("status")
        .and_then(|value| value.get("name"))
        .and_then(Value::as_str)
    {
        return Some(value.to_string());
    }
    if let Some(value) = value
        .get("select")
        .and_then(|value| value.get("name"))
        .and_then(Value::as_str)
    {
        return Some(value.to_string());
    }
    if let Some(value) = value.get("number").and_then(Value::as_f64) {
        return Some(value.to_string());
    }
    value
        .get("url")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn property_relation(
    properties: &Value,
    configured: Option<&str>,
    aliases: &[&str],
) -> Option<String> {
    property_value(properties, configured, aliases)?
        .get("relation")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn property_bool(properties: &Value, configured: Option<&str>, aliases: &[&str]) -> Option<bool> {
    property_value(properties, configured, aliases)?
        .get("checkbox")
        .and_then(Value::as_bool)
}

fn property_date(
    properties: &Value,
    configured: Option<&str>,
    aliases: &[&str],
) -> Option<DateTime<Utc>> {
    let start = property_value(properties, configured, aliases)?
        .get("date")?
        .get("start")?
        .as_str()?;
    DateTime::parse_from_rfc3339(start)
        .ok()
        .map(|value| value.with_timezone(&Utc))
        .or_else(|| {
            NaiveDate::parse_from_str(start, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc))
        })
}

fn rich_text_plain(items: &[Value]) -> String {
    items
        .iter()
        .filter_map(|item| {
            item.get("plain_text").and_then(Value::as_str).or_else(|| {
                item.get("text")
                    .and_then(|text| text.get("content"))
                    .and_then(Value::as_str)
            })
        })
        .collect::<Vec<_>>()
        .join("")
}

const REQUIRED_TASK_PROPERTIES: &[&str] = &[
    "Name",
    "Status",
    "Project",
    "Agent",
    "Scheduled At",
    "Due",
    "Execution ID",
    "Result",
];
const REQUIRED_PROJECT_PROPERTIES: &[&str] =
    &["Name", "Path", "Default Agent", "Active", "Last Activity"];

fn missing_required_property(properties: &Value, required: &[&str]) -> Result<(), String> {
    let Some(map) = properties.as_object() else {
        return Err("properties".to_string());
    };
    for property in required {
        let present = map.keys().any(|name| name.eq_ignore_ascii_case(property));
        if !present {
            return Err((*property).to_string());
        }
    }
    Ok(())
}

fn fingerprint(value: &Value) -> String {
    let mut digest = Sha256::new();
    digest.update(value.to_string().as_bytes());
    format!("{:x}", digest.finalize())
}

fn mapping_from_schemas(tasks: &SchemaSnapshot, projects: &SchemaSnapshot) -> MappingConfig {
    let mut mapping = default_mapping();
    mapping.tasks = mapping_for_schema(&tasks.properties, &mapping.tasks);
    mapping.projects = mapping_for_schema(&projects.properties, &mapping.projects);
    mapping.status_options = status_options_from_schema(tasks);
    mapping.schema_fingerprint = Some(fingerprint(
        &json!({"tasks": tasks.fingerprint, "projects": projects.fingerprint}),
    ));
    mapping
}

fn status_options_from_schema(schema: &SchemaSnapshot) -> BTreeMap<String, String> {
    let Some(properties) = schema.properties.as_object() else {
        return BTreeMap::new();
    };
    let Some(status) = properties
        .values()
        .find(|property| property.get("type").and_then(Value::as_str) == Some("status"))
    else {
        return BTreeMap::new();
    };
    status
        .get("status")
        .and_then(|value| value.get("options"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|option| {
            let name = option.get("name").and_then(Value::as_str)?;
            Some((name.to_ascii_lowercase(), name.to_string()))
        })
        .collect()
}

fn mapping_for_schema(properties: &Value, defaults: &PropertyMapping) -> PropertyMapping {
    let mut result = defaults.clone();
    let Some(map) = properties.as_object() else {
        return result;
    };
    let find = |aliases: &[&str]| -> Option<String> {
        aliases
            .iter()
            .find_map(|alias| {
                map.get(*alias)
                    .and_then(|value| value.get("id"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                map.iter()
                    .find(|(name, _)| aliases.iter().any(|alias| name.eq_ignore_ascii_case(alias)))
                    .and_then(|(_, value)| value.get("id"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
    };
    result.name = find(&["Name", "Title"]).or(result.name);
    result.status = find(&["Status", "State"]).or(result.status);
    result.project = find(&["Project", "Projects"]).or(result.project);
    result.agent = find(&["Agent"]).or(result.agent);
    result.scheduled_at = find(&["Scheduled At", "Schedule At"]).or(result.scheduled_at);
    result.due = find(&["Due", "Due Date", "Deadline"]).or(result.due);
    result.execution_id = find(&["Execution ID", "Execution"]).or(result.execution_id);
    result.result = find(&["Result", "Outcome"]).or(result.result);
    result.path = find(&["Path"]).or(result.path);
    result.default_agent = find(&["Default Agent"]).or(result.default_agent);
    result.active = find(&["Active"]).or(result.active);
    result.last_activity = find(&["Last Activity"]).or(result.last_activity);
    result
}

fn default_status_options() -> std::collections::BTreeMap<String, String> {
    [
        "backlog",
        "triage",
        "scheduled",
        "ready",
        "running",
        "review",
        "blocked",
        "done",
        "cancel",
        "archived",
    ]
    .into_iter()
    .map(|value| (value.to_string(), value.to_string()))
    .collect()
}

fn task_schema(
    project_data_source_id: &str,
) -> Result<HashMap<String, DataSourceProperty>, KbctlError> {
    let mut properties = HashMap::new();
    properties.insert(
        "Name".to_string(),
        data_source_property(json!({"type":"title","title":{}}))?,
    );
    properties.insert("Status".to_string(), data_source_property(json!({
        "type":"status",
        "status":{"options":[
            {"name":"backlog","color":"gray"},{"name":"triage","color":"yellow"},{"name":"scheduled","color":"blue"},{"name":"ready","color":"green"},{"name":"running","color":"purple"},{"name":"review","color":"orange"},{"name":"blocked","color":"red"},{"name":"done","color":"green"},{"name":"cancel","color":"gray"},{"name":"archived","color":"gray"}
        ],"groups":[]}
    }))?);
    properties.insert("Project".to_string(), data_source_property(json!({"type":"relation","relation":{"database_id":project_data_source_id,"single_property":{}}}))?);
    properties.insert(
        "Agent".to_string(),
        data_source_property(json!({"type":"rich_text","rich_text":{}}))?,
    );
    properties.insert(
        "Scheduled At".to_string(),
        data_source_property(json!({"type":"date","date":{}}))?,
    );
    properties.insert(
        "Due".to_string(),
        data_source_property(json!({"type":"date","date":{}}))?,
    );
    properties.insert(
        "Execution ID".to_string(),
        data_source_property(json!({"type":"rich_text","rich_text":{}}))?,
    );
    properties.insert(
        "Result".to_string(),
        data_source_property(json!({"type":"rich_text","rich_text":{}}))?,
    );
    Ok(properties)
}

fn project_schema() -> Result<HashMap<String, DataSourceProperty>, KbctlError> {
    let mut properties = HashMap::new();
    properties.insert(
        "Name".to_string(),
        data_source_property(json!({"type":"title","title":{}}))?,
    );
    properties.insert(
        "Path".to_string(),
        data_source_property(json!({"type":"rich_text","rich_text":{}}))?,
    );
    properties.insert(
        "Default Agent".to_string(),
        data_source_property(json!({"type":"rich_text","rich_text":{}}))?,
    );
    properties.insert(
        "Active".to_string(),
        data_source_property(json!({"type":"checkbox","checkbox":{}}))?,
    );
    properties.insert(
        "Last Activity".to_string(),
        data_source_property(json!({"type":"date","date":{}}))?,
    );
    Ok(properties)
}

fn data_source_property(value: Value) -> Result<DataSourceProperty, KbctlError> {
    let mut value = value;
    if let Some(object) = value.as_object_mut() {
        object
            .entry("name".to_string())
            .or_insert_with(|| Value::String(String::new()));
    }
    serde_json::from_value(value)
        .map_err(|error| KbctlError::Notion(format!("invalid data source property: {error}")))
}

fn page_properties<const N: usize>(
    properties: [(&str, PageProperty); N],
) -> Result<HashMap<String, PageProperty>, KbctlError> {
    Ok(properties
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect())
}

fn page_property(value: Value) -> Result<PageProperty, KbctlError> {
    serde_json::from_value(value)
        .map_err(|error| KbctlError::Notion(format!("invalid page property: {error}")))
}

fn page_title(value: &str) -> Result<PageProperty, KbctlError> {
    Ok(PageProperty::Title(PageTitleProperty::from(value)))
}

fn page_rich_text(value: &str) -> Result<PageProperty, KbctlError> {
    Ok(PageProperty::RichText(PageRichTextProperty::from(value)))
}

fn page_checkbox(value: bool) -> Result<PageProperty, KbctlError> {
    Ok(PageProperty::Checkbox(PageCheckboxProperty::from(value)))
}

fn page_property_status(value: String) -> Result<PageProperty, KbctlError> {
    Ok(PageProperty::Status(PageStatusProperty {
        id: None,
        status: Select::from(value),
    }))
}

fn page_property_rich_text(value: &str) -> Result<PageProperty, KbctlError> {
    if value.is_empty() {
        page_property(json!({"type":"rich_text","rich_text":[]}))
    } else {
        page_rich_text(value)
    }
}

fn page_property_checkbox(value: bool) -> Result<PageProperty, KbctlError> {
    page_checkbox(value)
}

fn page_property_date(value: &DateTime<Utc>) -> Result<PageProperty, KbctlError> {
    page_property(json!({"type":"date","date":{"start":value.to_rfc3339()}}))
}

fn notion_error(error: notionrs::Error) -> KbctlError {
    KbctlError::Notion(error.to_string())
}

fn database_create_payload(
    parent_page_id: Option<&str>,
    title: &str,
    properties: &HashMap<String, DataSourceProperty>,
) -> Result<Value, KbctlError> {
    let parent = parent_page_id
        .map(|page_id| json!({"type": "page_id", "page_id": page_id}))
        .unwrap_or_else(|| json!({"type": "workspace", "workspace": true}));
    let mut initial_properties = serde_json::to_value(properties)
        .map_err(|error| KbctlError::Notion(format!("encode database properties: {error}")))?;
    if let Some(property_map) = initial_properties.as_object_mut() {
        for property in property_map.values_mut() {
            if let Some(relation) = property.get_mut("relation").and_then(Value::as_object_mut)
                && let Some(database_id) = relation.remove("database_id")
            {
                relation.insert("data_source_id".to_string(), database_id);
            }
        }
    }
    Ok(json!({
        "parent": parent,
        "title": [RichText::from(title)],
        "initial_data_source": {"properties": initial_properties}
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_database_payload_uses_private_workspace_parent() {
        let properties = task_schema("project-data-source").unwrap();
        let payload = database_create_payload(None, "Tasks", &properties).unwrap();
        assert_eq!(
            payload["parent"],
            json!({"type": "workspace", "workspace": true})
        );
        assert_eq!(
            payload["initial_data_source"]["properties"]["Project"]["relation"]["data_source_id"],
            "project-data-source"
        );
        assert!(
            payload["initial_data_source"]["properties"]["Project"]["relation"]
                .get("database_id")
                .is_none()
        );
    }

    #[test]
    fn page_database_payload_preserves_explicit_parent() {
        let properties = project_schema().unwrap();
        let payload = database_create_payload(Some("page-id"), "Projects", &properties).unwrap();
        assert_eq!(
            payload["parent"],
            json!({"type": "page_id", "page_id": "page-id"})
        );
    }

    #[test]
    fn owned_schema_accepts_extra_properties() {
        let mut properties =
            serde_json::to_value(task_schema("project-data-source").unwrap()).unwrap();
        properties.as_object_mut().unwrap().insert(
            "Notes".to_string(),
            json!({"type": "rich_text", "rich_text": {}}),
        );
        assert!(missing_required_property(&properties, REQUIRED_TASK_PROPERTIES).is_ok());
    }

    #[test]
    fn owned_schema_rejects_missing_required_property() {
        let mut properties =
            serde_json::to_value(task_schema("project-data-source").unwrap()).unwrap();
        properties.as_object_mut().unwrap().remove("Status");
        assert_eq!(
            missing_required_property(&properties, REQUIRED_TASK_PROPERTIES).unwrap_err(),
            "Status"
        );
    }
}
