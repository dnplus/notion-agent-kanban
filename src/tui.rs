use crate::{
    config::{Config, default_state_path},
    domain::{Task, TaskStatus},
    error::KbctlError,
    herdr::{AgentRuntime, HerdrRuntime},
    notion::{KanbanProvider, NotionProvider, TaskCreate, TaskUpdate},
    store::Store,
};
use chrono::{Duration as ChronoDuration, Utc};
use crossterm::{
    cursor::{Hide, Show},
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, MouseButton,
        MouseEventKind,
    },
    execute,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear as ClearWidget, List, ListItem, ListState, Paragraph, Wrap},
};
use std::{
    io,
    time::{Duration, Instant},
};

const BOARD_STATUSES: [TaskStatus; 8] = [
    TaskStatus::Backlog,
    TaskStatus::Triage,
    TaskStatus::Scheduled,
    TaskStatus::Ready,
    TaskStatus::Running,
    TaskStatus::Review,
    TaskStatus::Blocked,
    TaskStatus::Done,
];
const NEW_TASK_STATUSES: [TaskStatus; 4] = [
    TaskStatus::Backlog,
    TaskStatus::Triage,
    TaskStatus::Scheduled,
    TaskStatus::Ready,
];

#[derive(Debug, Clone, Copy)]
enum CompactRow {
    Group(TaskStatus, usize),
    Task(usize),
}

#[derive(Debug, Clone)]
enum NewTaskState {
    Name(String),
    Status { name: String, selected: usize },
}

#[derive(Debug, Clone, Copy)]
struct ContextMenu {
    x: u16,
    y: u16,
    task_index: usize,
    width: u16,
    height: u16,
}

#[derive(Debug, Clone, Copy)]
enum MenuAction {
    Move(TaskStatus),
    Focus,
    Refresh,
    Close,
}

impl ContextMenu {
    const ITEMS: [(MenuAction, &'static str); 8] = [
        (MenuAction::Move(TaskStatus::Backlog), "1 backlog"),
        (MenuAction::Move(TaskStatus::Triage), "2 triage"),
        (MenuAction::Move(TaskStatus::Scheduled), "3 scheduled"),
        (MenuAction::Move(TaskStatus::Ready), "4 ready"),
        (MenuAction::Move(TaskStatus::Cancel), "c cancel"),
        (MenuAction::Focus, "f focus"),
        (MenuAction::Refresh, "r refresh"),
        (MenuAction::Close, "esc close"),
    ];

    fn at(column: u16, row: u16, task_index: usize, width: u16, height: u16) -> Option<Self> {
        if width < 12 || height < 4 {
            return None;
        }
        let menu_width = 22u16.min(width);
        let menu_height = (Self::ITEMS.len() as u16 + 2).min(height);
        Some(Self {
            x: column.min(width.saturating_sub(menu_width)),
            y: row.min(height.saturating_sub(menu_height)),
            task_index,
            width: menu_width,
            height: menu_height,
        })
    }

    fn for_selection(tasks: &[Task], selected: usize) -> Option<Self> {
        tasks.get(selected)?;
        let (width, height) = terminal::size().unwrap_or((120, 30));
        Self::at(0, 2, selected, width, height)
    }

    fn action_at(self, column: u16, row: u16) -> Option<MenuAction> {
        if column < self.x
            || column >= self.x.saturating_add(self.width)
            || row < self.y
            || row >= self.y.saturating_add(self.height)
        {
            return None;
        }
        let offset = row.saturating_sub(self.y);
        if offset == 0 || offset > Self::ITEMS.len() as u16 {
            return Some(MenuAction::Close);
        }
        Some(Self::ITEMS[(offset - 1) as usize].0)
    }
}

#[derive(Debug, Clone, Copy)]
struct BoardRegions {
    header: Rect,
    body: Rect,
    details: Rect,
    keys: Rect,
    message: Rect,
}

fn board_regions(area: Rect) -> BoardRegions {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(6),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(area);
    BoardRegions {
        header: chunks[0],
        body: chunks[1],
        details: chunks[2],
        keys: chunks[3],
        message: chunks[4],
    }
}

fn bordered_inner(area: Rect) -> Rect {
    Block::default().borders(Borders::ALL).inner(area)
}

fn compact_rows(tasks: &[Task]) -> Vec<CompactRow> {
    let mut rows = Vec::new();
    for status in BOARD_STATUSES {
        let indexes = tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| task.status == status)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        rows.push(CompactRow::Group(status, indexes.len()));
        rows.extend(indexes.into_iter().map(CompactRow::Task));
    }
    rows
}

fn list_offset(selected_row: usize, item_count: usize, viewport: usize) -> usize {
    if viewport == 0 {
        return 0;
    }
    selected_row
        .saturating_sub(viewport.saturating_sub(1))
        .min(item_count.saturating_sub(viewport))
}

fn compact_offset(rows: &[CompactRow], selected: usize, viewport: usize) -> usize {
    let selected_row = rows
        .iter()
        .position(|row| matches!(row, CompactRow::Task(index) if *index == selected))
        .unwrap_or(0);
    list_offset(selected_row, rows.len(), viewport)
}

fn task_at(
    tasks: &[Task],
    selected: usize,
    width: u16,
    height: u16,
    column: u16,
    row: u16,
) -> Option<usize> {
    let regions = board_regions(Rect::new(0, 0, width, height));
    if is_compact(width) {
        let inner = bordered_inner(regions.body);
        if column < inner.x
            || column >= inner.x.saturating_add(inner.width)
            || row < inner.y
            || row >= inner.y.saturating_add(inner.height)
        {
            return None;
        }
        let rows = compact_rows(tasks);
        let offset = compact_offset(&rows, selected, inner.height as usize);
        return match rows.get(offset + row.saturating_sub(inner.y) as usize) {
            Some(CompactRow::Task(index)) => Some(*index),
            _ => None,
        };
    }
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Ratio(1, 8); 8])
        .split(regions.body);
    for (column_index, area) in columns.iter().enumerate() {
        let inner = bordered_inner(*area);
        if column < inner.x
            || column >= inner.x.saturating_add(inner.width)
            || row < inner.y
            || row >= inner.y.saturating_add(inner.height)
        {
            continue;
        }
        let matching = tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| task.status == BOARD_STATUSES[column_index])
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let selected_row = matching
            .iter()
            .position(|index| *index == selected)
            .unwrap_or(0);
        let offset = list_offset(selected_row, matching.len(), inner.height as usize);
        return matching
            .get(offset + row.saturating_sub(inner.y) as usize)
            .copied();
    }
    None
}

fn is_compact(width: u16) -> bool {
    (width as usize) < BOARD_STATUSES.len() * 16
}

pub async fn run(config: Config) -> Result<(), KbctlError> {
    let store = Store::open(default_state_path())?;
    let provider = NotionProvider::new(config.clone()).ok();
    let runtime = HerdrRuntime::new(config.herdr.binary);
    let mut tasks = store.cached_tasks()?;
    let mut message = None;
    if tasks.is_empty() {
        message = refresh_tasks(provider.as_ref(), &store, &mut tasks).await;
    }
    terminal::enable_raw_mode()
        .map_err(|error| KbctlError::Runtime(format!("enable board terminal: {error}")))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, Hide, EnableMouseCapture)
        .map_err(|error| KbctlError::Runtime(format!("open board terminal: {error}")))?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)
        .map_err(|error| KbctlError::Runtime(format!("create board terminal: {error}")))?;
    let result = board_loop(
        provider.as_ref(),
        &store,
        &runtime,
        &mut tasks,
        &mut message,
        &mut terminal,
    )
    .await;
    let _ = execute!(
        terminal.backend_mut(),
        Show,
        DisableMouseCapture,
        LeaveAlternateScreen
    );
    let _ = terminal.show_cursor();
    let _ = terminal::disable_raw_mode();
    result
}

async fn board_loop(
    provider: Option<&NotionProvider>,
    store: &Store,
    runtime: &HerdrRuntime,
    tasks: &mut Vec<Task>,
    message: &mut Option<String>,
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
) -> Result<(), KbctlError> {
    let mut selected = 0usize;
    let mut cache_refresh_at = Instant::now() + Duration::from_secs(1);
    let mut menu = None;
    let mut new_task = None;
    loop {
        if Instant::now() >= cache_refresh_at {
            if let Ok(cached) = store.cached_tasks()
                && cached.as_slice() != tasks.as_slice()
            {
                *tasks = cached;
                *message = Some(format!("cache updated: {} tasks", tasks.len()));
            }
            cache_refresh_at = Instant::now() + Duration::from_secs(1);
        }
        selected = selected.min(tasks.len().saturating_sub(1));
        draw(
            tasks,
            selected,
            message.as_deref(),
            menu.as_ref(),
            new_task.as_ref(),
            terminal,
        )?;
        if event::poll(Duration::from_millis(500))
            .map_err(|error| KbctlError::Runtime(error.to_string()))?
        {
            match event::read().map_err(|error| KbctlError::Runtime(error.to_string()))? {
                Event::Key(KeyEvent { code, .. }) => {
                    if let Some(state) = new_task.take() {
                        match state {
                            NewTaskState::Name(mut value) => match code {
                                KeyCode::Esc => {}
                                KeyCode::Enter => {
                                    if value.trim().is_empty() {
                                        *message = Some("task title cannot be empty".to_string());
                                        new_task = Some(NewTaskState::Name(value));
                                    } else {
                                        new_task = Some(NewTaskState::Status {
                                            name: value.trim().to_string(),
                                            selected: 0,
                                        });
                                    }
                                }
                                KeyCode::Backspace => {
                                    value.pop();
                                    new_task = Some(NewTaskState::Name(value));
                                }
                                KeyCode::Char(character) if !character.is_control() => {
                                    value.push(character);
                                    new_task = Some(NewTaskState::Name(value));
                                }
                                _ => new_task = Some(NewTaskState::Name(value)),
                            },
                            NewTaskState::Status {
                                name,
                                selected: mut status_index,
                            } => match code {
                                KeyCode::Esc => {}
                                KeyCode::Up => {
                                    status_index = status_index.saturating_sub(1);
                                    new_task = Some(NewTaskState::Status {
                                        name,
                                        selected: status_index,
                                    });
                                }
                                KeyCode::Down => {
                                    status_index = status_index
                                        .saturating_add(1)
                                        .min(NEW_TASK_STATUSES.len().saturating_sub(1));
                                    new_task = Some(NewTaskState::Status {
                                        name,
                                        selected: status_index,
                                    });
                                }
                                KeyCode::Char(character @ '1'..='4') => {
                                    new_task = Some(NewTaskState::Status {
                                        name,
                                        selected: character as usize - '1' as usize,
                                    });
                                }
                                KeyCode::Enter => {
                                    let previous_count = tasks.len();
                                    *message = create_task(
                                        provider,
                                        store,
                                        tasks,
                                        name,
                                        NEW_TASK_STATUSES[status_index],
                                    )
                                    .await;
                                    if tasks.len() > previous_count {
                                        selected = tasks.len() - 1;
                                    }
                                }
                                _ => {
                                    new_task = Some(NewTaskState::Status {
                                        name,
                                        selected: status_index,
                                    })
                                }
                            },
                        }
                        continue;
                    }
                    match code {
                        KeyCode::Char('q') => break,
                        KeyCode::Esc => {
                            if menu.take().is_none() {
                                break;
                            }
                        }
                        KeyCode::Char('n') => {
                            menu = None;
                            *message = None;
                            new_task = Some(NewTaskState::Name(String::new()));
                        }
                        KeyCode::Char('r') => {
                            menu = None;
                            *message = refresh_tasks(provider, store, tasks).await;
                        }
                        KeyCode::Char('m') => {
                            menu = ContextMenu::for_selection(tasks, selected);
                        }
                        KeyCode::Down => {
                            menu = None;
                            selected = selected
                                .saturating_add(1)
                                .min(tasks.len().saturating_sub(1));
                        }
                        KeyCode::Up => {
                            menu = None;
                            selected = selected.saturating_sub(1);
                        }
                        KeyCode::Char('1') => {
                            menu = None;
                            *message = move_selected(
                                provider,
                                store,
                                runtime,
                                tasks,
                                selected,
                                TaskStatus::Backlog,
                            )
                            .await;
                        }
                        KeyCode::Char('2') => {
                            menu = None;
                            *message = move_selected(
                                provider,
                                store,
                                runtime,
                                tasks,
                                selected,
                                TaskStatus::Triage,
                            )
                            .await;
                        }
                        KeyCode::Char('3') => {
                            menu = None;
                            *message = move_selected(
                                provider,
                                store,
                                runtime,
                                tasks,
                                selected,
                                TaskStatus::Scheduled,
                            )
                            .await;
                        }
                        KeyCode::Char('4') => {
                            menu = None;
                            *message = move_selected(
                                provider,
                                store,
                                runtime,
                                tasks,
                                selected,
                                TaskStatus::Ready,
                            )
                            .await;
                        }
                        KeyCode::Char('c') => {
                            menu = None;
                            *message = move_selected(
                                provider,
                                store,
                                runtime,
                                tasks,
                                selected,
                                TaskStatus::Cancel,
                            )
                            .await;
                        }
                        KeyCode::Char('f') => {
                            menu = None;
                            *message = focus_selected(store, runtime, tasks, selected).await;
                        }
                        _ => {}
                    }
                }
                Event::Mouse(mouse) => {
                    if new_task.is_some() {
                        continue;
                    }
                    if let Some(active_menu) = menu {
                        if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                            match active_menu.action_at(mouse.column, mouse.row) {
                                Some(MenuAction::Close) => menu = None,
                                Some(MenuAction::Refresh) => {
                                    menu = None;
                                    *message = refresh_tasks(provider, store, tasks).await;
                                }
                                Some(MenuAction::Move(status)) => {
                                    menu = None;
                                    *message = move_selected(
                                        provider,
                                        store,
                                        runtime,
                                        tasks,
                                        active_menu.task_index,
                                        status,
                                    )
                                    .await;
                                    selected =
                                        active_menu.task_index.min(tasks.len().saturating_sub(1));
                                }
                                Some(MenuAction::Focus) => {
                                    menu = None;
                                    selected =
                                        active_menu.task_index.min(tasks.len().saturating_sub(1));
                                    *message =
                                        focus_selected(store, runtime, tasks, selected).await;
                                }
                                None => menu = None,
                            }
                        }
                    } else {
                        let area = terminal
                            .size()
                            .map_err(|error| KbctlError::Runtime(error.to_string()))?;
                        let width = area.width;
                        let height = area.height;
                        match mouse.kind {
                            MouseEventKind::Down(MouseButton::Left) => {
                                if let Some(index) =
                                    task_at(tasks, selected, width, height, mouse.column, mouse.row)
                                {
                                    selected = index;
                                    menu = ContextMenu::at(
                                        mouse.column,
                                        mouse.row,
                                        index,
                                        width,
                                        height,
                                    );
                                }
                            }
                            MouseEventKind::ScrollUp => {
                                selected = selected.saturating_sub(1);
                            }
                            MouseEventKind::ScrollDown => {
                                selected = selected
                                    .saturating_add(1)
                                    .min(tasks.len().saturating_sub(1));
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

async fn create_task(
    provider: Option<&NotionProvider>,
    store: &Store,
    tasks: &mut Vec<Task>,
    name: String,
    status: TaskStatus,
) -> Option<String> {
    let Some(provider) = provider else {
        return Some("Notion credential unavailable; task was not created".to_string());
    };
    let created = match provider
        .create_task(TaskCreate {
            name,
            status,
            due: Some(Utc::now() + ChronoDuration::days(1)),
        })
        .await
    {
        Ok(task) => task,
        Err(error) => return Some(format!("task create failed: {error}")),
    };
    let status = created.status;
    let cache_error = store.cache_task(&created).err();
    tasks.push(created);
    cache_error.map_or_else(
        || Some(format!("task created in {status}")),
        |error| {
            Some(format!(
                "task created in {status}; cache write failed: {error}"
            ))
        },
    )
}

fn render_new_task(frame: &mut Frame, state: &NewTaskState) {
    let area = frame.area();
    match state {
        NewTaskState::Name(value) => {
            let popup = centered_rect(area, 56, 5);
            frame.render_widget(ClearWidget, popup);
            let inner_width = bordered_inner(popup).width as usize;
            let lines = vec![
                Line::from("title (enter to choose status)"),
                Line::from(fit_cells(&format!("> {}_", value), inner_width)),
                Line::from("esc cancel"),
            ];
            let input = Paragraph::new(lines)
                .style(Style::default().fg(Color::White))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Cyan))
                        .title(" new task "),
                );
            frame.render_widget(input, popup);
        }
        NewTaskState::Status { name, selected } => {
            let popup = centered_rect(area, 44, (NEW_TASK_STATUSES.len() as u16) + 2);
            frame.render_widget(ClearWidget, popup);
            let items = NEW_TASK_STATUSES
                .iter()
                .enumerate()
                .map(|(index, status)| ListItem::new(format!("{} {status}", index + 1)))
                .collect::<Vec<_>>();
            let mut list_state = ListState::default();
            list_state.select(Some(*selected));
            let title = fit_cells(
                &format!(" new task: {} · enter ", single_line(name)),
                bordered_inner(popup).width as usize,
            );
            let list = List::new(items)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Cyan))
                        .title(title),
                )
                .highlight_style(
                    Style::default()
                        .fg(Color::White)
                        .bg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                )
                .highlight_symbol("> ");
            frame.render_stateful_widget(list, popup, &mut list_state);
        }
    }
}

fn centered_rect(area: Rect, requested_width: u16, requested_height: u16) -> Rect {
    let width = requested_width.min(area.width).max(1);
    let height = requested_height.min(area.height).max(1);
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

async fn refresh_tasks(
    provider: Option<&NotionProvider>,
    store: &Store,
    tasks: &mut Vec<Task>,
) -> Option<String> {
    let Some(provider) = provider else {
        return Some("Notion credential unavailable; showing local cache".to_string());
    };
    match provider.list_tasks().await {
        Ok(fresh) => match store.cache_tasks(&fresh) {
            Ok(()) => {
                *tasks = fresh;
                Some(format!("refreshed {} tasks", tasks.len()))
            }
            Err(error) => Some(format!("cache write failed: {error}")),
        },
        Err(error) => Some(format!(
            "Notion refresh failed; showing local cache: {error}"
        )),
    }
}

async fn move_selected(
    provider: Option<&NotionProvider>,
    store: &Store,
    runtime: &HerdrRuntime,
    tasks: &mut [Task],
    selected: usize,
    status: TaskStatus,
) -> Option<String> {
    let Some(provider) = provider else {
        return Some("Notion credential unavailable; status was not changed".to_string());
    };
    let Some(task) = tasks.get(selected).cloned() else {
        return Some("no task selected".to_string());
    };
    if status == TaskStatus::Cancel
        && let Some(execution_id) = task.execution_id.as_deref()
        && let Ok(Some(execution)) = store.execution(execution_id)
        && let Some(runtime_id) = execution.runtime_id.as_deref()
    {
        let _ = runtime.cancel(runtime_id).await;
    }
    if let Err(error) = provider
        .update_task(TaskUpdate {
            id: task.id.clone(),
            status: Some(status),
            clear_execution_id: status == TaskStatus::Cancel,
            ..Default::default()
        })
        .await
    {
        return Some(format!("task update failed: {error}"));
    }
    let mut updated = task;
    updated.status = status;
    if status == TaskStatus::Cancel {
        updated.execution_id = None;
    }
    if let Err(error) = store.cache_task(&updated) {
        return Some(format!("status updated; cache write failed: {error}"));
    }
    if let Some(slot) = tasks.get_mut(selected) {
        *slot = updated;
    }
    Some(format!("task moved to {status}"))
}

async fn focus_selected(
    store: &Store,
    runtime: &HerdrRuntime,
    tasks: &[Task],
    selected: usize,
) -> Option<String> {
    let Some(task) = tasks.get(selected) else {
        return Some("no task selected".to_string());
    };
    let Some(execution_id) = task.execution_id.as_deref() else {
        return Some("selected task has no active execution".to_string());
    };
    let Ok(Some(execution)) = store.execution(execution_id) else {
        return Some("execution is not in local state".to_string());
    };
    let Some(runtime_id) = execution.runtime_id.as_deref() else {
        return Some("execution has no Herdr runtime yet".to_string());
    };
    match runtime.focus(runtime_id).await {
        Ok(()) => Some(format!("focused {execution_id}")),
        Err(error) => Some(format!("focus failed: {error}")),
    }
}

fn draw(
    tasks: &[Task],
    selected: usize,
    message: Option<&str>,
    menu: Option<&ContextMenu>,
    new_task: Option<&NewTaskState>,
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
) -> Result<(), KbctlError> {
    terminal
        .draw(|frame| render_board(frame, tasks, selected, message, menu, new_task))
        .map(|_| ())
        .map_err(|error| KbctlError::Runtime(format!("draw board: {error}")))
}

fn render_board(
    frame: &mut Frame,
    tasks: &[Task],
    selected: usize,
    message: Option<&str>,
    menu: Option<&ContextMenu>,
    new_task: Option<&NewTaskState>,
) {
    let area = frame.area();
    let regions = board_regions(area);
    let compact = is_compact(area.width);
    let header = if area.width < 48 {
        "kbctl · click menu · q quit"
    } else if area.width < 72 {
        "kbctl · click menu · right-click Herdr · q quit"
    } else if compact {
        "kbctl board · cache · click menu · right-click Herdr · q quit"
    } else {
        "kbctl board · local cache · click menu · right-click Herdr · q quit"
    };
    frame.render_widget(
        Paragraph::new(fit_cells(header, area.width as usize)).style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        regions.header,
    );
    if compact {
        render_compact(frame, tasks, selected, regions.body);
    } else {
        render_wide(frame, tasks, selected, regions.body);
    }
    let details = tasks
        .get(selected)
        .map(|task| task_detail_lines(task, area.width as usize))
        .unwrap_or_else(|| vec![Line::from(" selected: none")]);
    frame.render_widget(
        Paragraph::new(details)
            .style(Style::default().fg(Color::Gray))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .borders(Borders::TOP)
                    .border_style(Style::default().fg(Color::DarkGray)),
            ),
        regions.details,
    );
    let keys = if area.width < 48 {
        "↑↓ · n new · 1-4 · c · f · m · q"
    } else if area.width < 72 {
        "↑↓ select · n new · 1-4 · c · f · m · q"
    } else if compact {
        "↑↓ select · n new · 1-4 move · c cancel · f focus · m menu · q quit"
    } else {
        "↑/↓ select · n new · 1-4 move · c cancel · f focus · m menu · q quit"
    };
    frame.render_widget(
        Paragraph::new(fit_cells(keys, area.width as usize))
            .style(Style::default().fg(Color::DarkGray)),
        regions.keys,
    );
    if let Some(message) = message {
        frame.render_widget(
            Paragraph::new(fit_cells(message, area.width as usize))
                .style(Style::default().fg(Color::Yellow)),
            regions.message,
        );
    }
    if let Some(menu) = menu {
        render_menu(frame, *menu);
    }
    if let Some(new_task) = new_task {
        render_new_task(frame, new_task);
    }
}

fn render_compact(frame: &mut Frame, tasks: &[Task], selected: usize, area: Rect) {
    let rows = compact_rows(tasks);
    let mut items = rows
        .iter()
        .map(|row| match row {
            CompactRow::Group(status, count) => ListItem::new(Line::from(Span::styled(
                format!("▾ {status} ({count})"),
                Style::default()
                    .fg(status_color(*status))
                    .add_modifier(Modifier::BOLD),
            ))),
            CompactRow::Task(index) => ListItem::new(single_line(&tasks[*index].name)),
        })
        .collect::<Vec<_>>();
    if tasks.is_empty() {
        items.push(ListItem::new(Span::styled(
            "  (no tasks in cache)",
            Style::default().fg(Color::DarkGray),
        )));
    }
    let inner = bordered_inner(area);
    let selected_row = rows
        .iter()
        .position(|row| matches!(row, CompactRow::Task(index) if *index == selected));
    let mut state = ListState::default();
    if let Some(selected_row) = selected_row {
        state.select(Some(selected_row));
        *state.offset_mut() = list_offset(selected_row, rows.len(), inner.height as usize);
    }
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray))
                .title(format!(" tasks: {} ", tasks.len())),
        )
        .highlight_style(
            Style::default()
                .fg(Color::White)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ")
        .scroll_padding(1);
    frame.render_stateful_widget(list, area, &mut state);
}

fn render_wide(frame: &mut Frame, tasks: &[Task], selected: usize, area: Rect) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Ratio(1, 8); 8])
        .split(area);
    for (column_index, status) in BOARD_STATUSES.iter().copied().enumerate() {
        let matching = tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| task.status == status)
            .collect::<Vec<_>>();
        let items = matching
            .iter()
            .map(|(_, task)| ListItem::new(single_line(&task.name)))
            .collect::<Vec<_>>();
        let selected_row = matching.iter().position(|(index, _)| *index == selected);
        let inner = bordered_inner(columns[column_index]);
        let mut state = ListState::default();
        if let Some(selected_row) = selected_row {
            state.select(Some(selected_row));
            *state.offset_mut() = list_offset(selected_row, matching.len(), inner.height as usize);
        }
        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::DarkGray))
                    .title(Line::from(Span::styled(
                        format!(" {status} ({}) ", matching.len()),
                        Style::default()
                            .fg(status_color(status))
                            .add_modifier(Modifier::BOLD),
                    ))),
            )
            .highlight_style(
                Style::default()
                    .fg(Color::White)
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ")
            .scroll_padding(1);
        frame.render_stateful_widget(list, columns[column_index], &mut state);
    }
}

fn render_menu(frame: &mut Frame, menu: ContextMenu) {
    let area = Rect::new(menu.x, menu.y, menu.width, menu.height);
    frame.render_widget(ClearWidget, area);
    let items = ContextMenu::ITEMS
        .iter()
        .map(|(_, label)| ListItem::new(*label))
        .collect::<Vec<_>>();
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan))
                .title(" actions "),
        )
        .style(Style::default().fg(Color::White).bg(Color::DarkGray));
    frame.render_widget(list, area);
}

fn single_line(value: &str) -> String {
    value.lines().collect::<Vec<_>>().join(" ")
}

fn task_detail_lines(task: &Task, width: usize) -> Vec<Line<'static>> {
    let lines = [
        format!(" task: {}", single_line(&task.name)),
        format!(
            " status: {} · agent: {}",
            task.status,
            task.agent.as_deref().unwrap_or("default")
        ),
        format!(
            " project: {} · sched: {}",
            compact_identifier(task.project_id.as_deref()),
            compact_timestamp(task.scheduled_at)
        ),
        format!(" due: {}", compact_timestamp(task.due)),
        format!(
            " exec: {} · result: {}",
            compact_identifier(task.execution_id.as_deref()),
            task.result
                .as_deref()
                .map(single_line)
                .unwrap_or_else(|| "none".to_string())
        ),
    ];
    lines
        .into_iter()
        .enumerate()
        .map(|(index, line)| {
            let style = if index == 0 {
                Style::default().add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Line::from(Span::styled(fit_cells(&line, width), style))
        })
        .collect()
}

fn compact_identifier(value: Option<&str>) -> String {
    value
        .map(|value| fit_cells(value, 12))
        .unwrap_or_else(|| "none".to_string())
}

fn compact_timestamp(value: Option<chrono::DateTime<chrono::Utc>>) -> String {
    value
        .map(|value| value.format("%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn fit_cells(value: &str, max_cells: usize) -> String {
    if max_cells == 0 {
        return String::new();
    }
    if Line::from(value).width() <= max_cells {
        return value.to_string();
    }
    if max_cells == 1 {
        return "…".to_string();
    }
    let mut result = String::new();
    let mut width = 0usize;
    for character in value.chars() {
        let character_width = Line::from(character.to_string()).width();
        if width + character_width > max_cells - 1 {
            break;
        }
        result.push(character);
        width += character_width;
    }
    result.push('…');
    result
}

fn status_color(status: TaskStatus) -> Color {
    match status {
        TaskStatus::Backlog => Color::DarkGray,
        TaskStatus::Triage => Color::Magenta,
        TaskStatus::Scheduled => Color::Yellow,
        TaskStatus::Ready => Color::Green,
        TaskStatus::Running => Color::Blue,
        TaskStatus::Review => Color::Cyan,
        TaskStatus::Blocked => Color::Red,
        TaskStatus::Done => Color::Rgb(0, 150, 100),
        TaskStatus::Cancel | TaskStatus::Archived => Color::DarkGray,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};

    fn task(id: &str, status: TaskStatus) -> Task {
        Task {
            id: id.to_string(),
            name: id.to_string(),
            status,
            ..Task::default()
        }
    }

    #[test]
    fn compact_rows_include_empty_status_groups() {
        let tasks = vec![
            task("backlog item", TaskStatus::Backlog),
            task("ready item", TaskStatus::Ready),
        ];
        assert_eq!(task_at(&tasks, 0, 80, 30, 2, 3), Some(0));
        assert_eq!(task_at(&tasks, 0, 80, 30, 2, 7), Some(1));
        assert_eq!(task_at(&tasks, 0, 80, 30, 2, 4), None);
        assert_eq!(
            compact_rows(&tasks).len(),
            BOARD_STATUSES.len() + tasks.len()
        );
    }

    #[test]
    fn context_menu_maps_clicks_to_actions() {
        let menu = ContextMenu::at(2, 3, 0, 80, 30).expect("menu fits");
        assert!(matches!(
            menu.action_at(2, 4),
            Some(MenuAction::Move(TaskStatus::Backlog))
        ));
        assert!(matches!(menu.action_at(2, 10), Some(MenuAction::Refresh)));
        assert!(matches!(menu.action_at(2, 12), Some(MenuAction::Close)));
        assert!(menu.action_at(1, 4).is_none());
    }

    #[test]
    fn compact_render_uses_bounded_terminal_cells() {
        let backend = TestBackend::new(36, 14);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        let tasks = vec![Task {
            id: "task".to_string(),
            name: "a very long task title that must stay inside the pane".to_string(),
            status: TaskStatus::Backlog,
            ..Task::default()
        }];
        terminal
            .draw(|frame| render_board(frame, &tasks, 0, None, None, None))
            .expect("render board");
        let buffer = terminal.backend().buffer();
        let rendered = buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("backlog"));
        assert!(rendered.contains("task"));
        assert_eq!(buffer.area.width, 36);
        assert_eq!(buffer.area.height, 14);
    }

    #[test]
    fn wide_mouse_regions_match_column_widgets() {
        let tasks = vec![
            task("backlog item", TaskStatus::Backlog),
            task("ready item", TaskStatus::Ready),
        ];
        assert_eq!(task_at(&tasks, 0, 160, 20, 2, 2), Some(0));
        assert_eq!(task_at(&tasks, 0, 160, 20, 62, 2), Some(1));
        assert_eq!(task_at(&tasks, 0, 160, 20, 22, 2), None);
    }
}
