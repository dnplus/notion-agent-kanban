# kbctl

`kbctl` 是以 Notion 為看板、以本機 daemon 驅動 Agent 的 Kanban CLI。V1 使用 `notionrs` 0.32，Herdr 是第一個可替換的 Agent runtime。支援的 Herdr agent kind 包含 `grok`、`codex` 與 `claude`。

## 初始化

先提供 Notion PAT：

```sh
export NOTION_API_TOKEN=ntn_...
```

`kbctl init` 一律建立全新的 Tasks、Projects、預設 Project 與 Agent Board。不指定 `--parent` 時用 Notion workspace-level parent，對支援 workspace-level PAT 的連線會出現在 Private 區域。之後把這些 database 搬到哪個 workspace 都不必重跑 init；kbctl 用設定裡的 database id 存取，只要目前的 Notion token 仍能打到它們即可。

```sh
kbctl init
```

`--parent <page-id-or-url>` 只決定**第一次建立**時放在哪個頁面底下。

## 工作流程

```text
Notion Task (ready/triage/scheduled)
        -> kbctl daemon run
        -> Herdr agent
        -> kbctl report done|review|blocked
        -> validation + Notion writeback
```

`done` 必須附非空摘要，`blocked` 必須附原因。Agent process 結束但沒有合法 report 時，daemon 會清除舊 execution、將工作放回 `ready`，並依重試間隔重新開啟；達到 `max_attempts` 後才送到 `review`。人為 `cancel`/`archived` 的工作不會重試。

Herdr 派出的 agent 會把 `kbctl report` 寫入 Project 目錄下的 `.kbctl/reports/<execution-id>.json`，daemon 讀取後才寫入 SQLite outbox 並同步 Notion；因此 Codex sandbox 不需要直接開啟全域 `~/.local/share/kbctl/state.db`。手動在一般終端執行 report 時仍會直接使用本機 state/outbox。

Herdr 的 idle/done 狀態不等於 process 已退出。daemon 會檢查 execution pane 的前景 agent process；只有 pane 消失或 agent process 確實離開時才進入重試流程。

## 本地快取看板

`kbctl board` 是可離線查看的本地 SQLite 快取看板，畫面使用 `ratatui` widgets 依 terminal cell 重新排版。daemon 每次成功從 Notion poll 後會更新快取；看板會自動讀取快取，`r` 仍可手動從 Notion 重新整理。窄 pane 會自動使用依 status 分組的可捲動列表排版，空的 group 也會保留顯示；寬 pane 使用 Kanban 欄位。按 `n` 輸入 task 名稱，按 Enter 後用上下鍵選擇 backlog、triage、scheduled 或 ready，再按 Enter 建立到 Notion 並寫入本地快取；可連續重複新增。看板新增的工作會預設 Due 為隔日，daemon 才會派工。左鍵點工作會開啟操作選單，右鍵留給 Herdr 的 pane 選單；下方會顯示選取 task 的五行摘要（名稱、狀態/Agent、Project/排程、截止、Execution/Result）。選取工作後也可用 `1`/`2`/`3`/`4` 移動到 backlog、triage、scheduled、ready，`c` 取消，`f` 聚焦 Herdr execution。狀態操作仍需 Notion token，沒有網路時看板會保留最後一次快取。

`kbctl install --herdr` 會寫入並 link `kbctl` Herdr plugin，然後以 plugin action 呼叫 Herdr 的 managed plugin pane API，在目前 tab 建立固定右側的窄 `kbctl board` split pane。plugin 已安裝時，背景 daemon 建立每個 execution 的 Herdr workspace，也會在該 workspace 的 agent pane 旁建立同一個窄 board pane；因此從 Herdr agents 切換到背景工作時仍能直接操作看板。看板 pane 建立失敗只會記錄 warning，不會阻止 agent 派工。重新安裝時會先關閉目前 tab 的舊 board pane，確保重建後載入新的 binary。這是 Herdr v1 plugin 能做到的可停靠 terminal sidebar；Herdr 原生 sidebar 本身仍由 Herdr host 管理。看板接收左鍵點擊與滾輪；右鍵維持 Herdr 預設 pane 選單，不再轉送給看板。工作目錄會優先使用已確認的本機 Project path，否則使用目前 pane 的 cwd，因此 `.env` 與本地快取可以被讀取。`kbctl install --grok` 與 `kbctl install --codex` 只安裝 report skill，分別寫入 `~/.grok/skills/kbctl-report/SKILL.md` 與 `~/.codex/skills/kbctl-report/SKILL.md`。Grok home 可用 `GROK_HOME` 覆寫。

Herdr agent 名稱會使用 provider 與 task title 的合法 slug（例如 `grok-ok-120bdfac`、`codex-ok-120bdfac`）；中文標題會保留在 workspace label，末尾短 ID 只用來避免同名工作互相覆蓋。Notion Task 的 Agent 欄位或 Project 的 Default Agent 決定 Herdr `--kind`；未設定時預設 `codex`。要派出 Grok：

```sh
kbctl project bind default . --agent grok
```

重新安裝後也可以直接從 Herdr 觸發同一個 action：

```sh
herdr plugin action invoke kbctl.open-board
```

daemon 設定可在 `config.toml` 調整：

```toml
[daemon]
poll_interval_seconds = 15
max_concurrency = 1
max_attempts = 3
retry_delay_seconds = 15
```

## 常用命令

```sh
kbctl doctor
kbctl board
kbctl daemon run
kbctl project bind <project-id-or-default> <directory>
kbctl task move <task-id> ready
kbctl report done --execution <execution-id> --summary "完成內容與驗證方式"
kbctl install --grok
kbctl install --codex
kbctl install --herdr
```

設定存放於 `~/.config/kbctl/config.toml`，SQLite 狀態存放於 `~/.local/share/kbctl/state.db`。可以用 `KBCTL_CONFIG`、`KBCTL_STATE` 指定替代路徑。
