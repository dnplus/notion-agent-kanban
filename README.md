# kbctl

Notion-backed Kanban control plane for local coding agents. A daemon polls Tasks, starts a Herdr agent (Grok, Codex, Claude, …), and writes the validated result back to Notion.

The release binary is a single executable. SQLite is bundled; TLS uses rustls. No extra runtime.

## Install

### GitHub Release

Tag `v*` builds are published by GitHub Actions. Pick the asset for your machine and put it on `PATH`:

```sh
# macOS Apple Silicon
curl -L -o ~/.local/bin/kbctl \
  https://github.com/dnplus/notion-agent-kanban/releases/latest/download/kbctl-aarch64-apple-darwin
chmod +x ~/.local/bin/kbctl
```

Other assets: `kbctl-x86_64-apple-darwin`, `kbctl-x86_64-unknown-linux-gnu`, `kbctl-x86_64-pc-windows-msvc.exe`.

### From source

```sh
git clone https://github.com/dnplus/notion-agent-kanban
cd notion-agent-kanban
cargo build --release
# single binary: target/release/kbctl
cp target/release/kbctl ~/.local/bin/kbctl
```

Or `cargo install --path . --locked` (installs to `~/.cargo/bin`).

### Agent wiring

Once `kbctl` is on `PATH`, install report skills / the Herdr plugin. This also copies the current binary to `~/.local/bin/kbctl`:

```sh
kbctl install              # binary only
kbctl install --grok
kbctl install --codex
kbctl install --herdr
```

`--grok` writes `~/.grok/skills/kbctl-report/SKILL.md` (`GROK_HOME` overrides the Grok home). `--codex` writes `~/.codex/skills/kbctl-report/SKILL.md`. `--herdr` links the board plugin.

## Setup

```sh
export NOTION_API_TOKEN=ntn_...
kbctl init
```

`kbctl init` always creates a new Tasks database, Projects database, default Project, and Agent Board. Without `--parent` it uses a workspace-level parent (Private, for PATs that support that). After init you can move those databases to another workspace; kbctl keeps using the stored database ids as long as the current token can still reach them.

`--parent <page-id-or-url>` only sets the page for that first create.

Bind a local directory and choose the Herdr agent kind (default is `codex`):

```sh
kbctl project bind default . --agent grok
```

A task’s Agent field overrides the project default.

## Workflow

```text
Notion Task (ready / triage / scheduled)
        -> kbctl daemon run
        -> Herdr agent
        -> kbctl report done|review|blocked
        -> validation + Notion writeback
```

`done` and `review` need a non-empty `--summary`. `blocked` needs a non-empty `--reason`. If the agent process exits without a valid report, the daemon returns the task to `ready` and retries after a delay, up to `max_attempts`, then sends it to `review`. Human `cancel` / `archived` is not retried.

Herdr-dispatched agents write `kbctl report` to `.kbctl/reports/<execution-id>.json` in the project directory. The daemon ingests that spool, then owns SQLite and Notion writeback, so a Codex sandbox does not need `~/.local/share/kbctl/state.db`. A report from a normal terminal still uses local state/outbox directly.

Herdr idle/done is not process exit. The daemon treats the execution as finished only when the pane is gone or the foreground agent process has left.

## Board

`kbctl board` is an offline SQLite cache (ratatui). The daemon refreshes it after each successful Notion poll; `r` refreshes from Notion. Narrow panes use a grouped list (empty groups stay visible); wide panes use Kanban columns.

`n` creates a task in Notion: title, then backlog / triage / scheduled / ready. New tasks get a due date of tomorrow so the daemon can dispatch them. Left-click opens the action menu; right-click stays Herdr’s pane menu. `1`/`2`/`3`/`4` move to backlog/triage/scheduled/ready, `c` cancels, `f` focuses the Herdr execution. Status changes need a Notion token; without a network the last cache remains.

`kbctl install --herdr` docks a narrow board split on the current Herdr tab. When the plugin is linked, each daemon-created execution workspace gets the same board pane beside the agent. A failed board pane is a warning only. Reinstall closes the current tab’s old board pane first so the new binary loads.

## Commands

```sh
kbctl doctor
kbctl board
kbctl daemon run
kbctl project bind <project-id-or-default> <directory>
kbctl task move <task-id> ready
kbctl report done --execution <execution-id> --summary "what changed and how it was verified"
kbctl install --grok
```

Config: `~/.config/kbctl/config.toml`. State: `~/.local/share/kbctl/state.db`. Override with `KBCTL_CONFIG` and `KBCTL_STATE`.

```toml
[daemon]
poll_interval_seconds = 15
max_concurrency = 1
max_attempts = 3
retry_delay_seconds = 15
```

## Releases

Push a version tag to build and attach binaries:

```sh
git tag v0.1.0
git push origin v0.1.0
```
