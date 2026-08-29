use crate::{config::Config, error::KbctlError};
use serde_json::Value;
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

pub fn run(config: Config, grok: bool, codex: bool, herdr: bool) -> Result<(), KbctlError> {
    if !grok && !codex && !herdr {
        return Err(KbctlError::Validation(
            "請指定 --grok、--codex 或 --herdr".to_string(),
        ));
    }
    install_cli_binary()?;
    if grok {
        let path = grok_skill_path();
        write_atomic(&path, REPORT_SKILL)?;
        println!("Grok skill 已安裝：{}", path.display());
        println!(
            "將 Notion Agent 或 `kbctl project bind <project> <dir> --agent grok` 設為 grok，Herdr 就會派出 Grok。"
        );
    }
    if codex {
        let path = codex_skill_path();
        write_atomic(&path, REPORT_SKILL)?;
        println!("Codex skill 已安裝：{}", path.display());
    }
    if herdr {
        let path = herdr_plugin_path(&config);
        let executable = env::current_exe()
            .map_err(|error| KbctlError::Runtime(format!("找不到 kbctl executable: {error}")))?;
        let manifest = format!(
            "id = \"kbctl\"\nname = \"kbctl\"\nversion = \"{}\"\nmin_herdr_version = \"0.8.0\"\nplatforms = [\"macos\", \"linux\", \"windows\"]\n\n[[panes]]\nid = \"board\"\ntitle = \"kbctl board\"\nplacement = \"split\"\ncommand = [{} , \"board\"]\n",
            env!("CARGO_PKG_VERSION"),
            toml_string(&executable.display().to_string()),
        );
        let manifest = format!(
            "{manifest}\n[[actions]]\nid = \"open-board\"\ntitle = \"Open kbctl board\"\ndescription = \"Open the kbctl board as a docked Herdr sidebar.\"\ncommand = [{} , \"_herdr-open-board\"]\n",
            toml_string(&executable.display().to_string()),
        );
        write_atomic(&path, &manifest)?;
        link_and_open_herdr(&config.herdr.binary, &path)?;
        println!("Herdr plugin manifest 已安裝：{}", path.display());
    }
    Ok(())
}

fn link_and_open_herdr(binary: &str, manifest_path: &Path) -> Result<(), KbctlError> {
    let plugins = run_herdr_json(binary, &["plugin", "list", "--json"])?;
    let linked = plugins
        .get("result")
        .and_then(|value| value.get("plugins"))
        .and_then(Value::as_array)
        .is_some_and(|plugins| {
            plugins
                .iter()
                .any(|plugin| plugin.get("plugin_id").and_then(Value::as_str) == Some("kbctl"))
        });
    if linked {
        run_herdr_json(binary, &["plugin", "unlink", "kbctl"])?;
    }
    let path = manifest_path.to_string_lossy().to_string();
    run_herdr_json(binary, &["plugin", "link", &path])?;
    let panes = run_herdr_json(binary, &["pane", "list"])?;
    let focused_tab = panes
        .get("result")
        .and_then(|value| value.get("panes"))
        .and_then(Value::as_array)
        .and_then(|panes| {
            panes.iter().find_map(|pane| {
                pane.get("focused")
                    .and_then(Value::as_bool)
                    .filter(|focused| *focused)
                    .and_then(|_| pane.get("tab_id").and_then(Value::as_str))
            })
        });
    let board_panes = panes
        .get("result")
        .and_then(|value| value.get("panes"))
        .and_then(Value::as_array)
        .map(|panes| {
            panes
                .iter()
                .filter(|pane| {
                    let in_focused_tab = focused_tab.is_none()
                        || focused_tab == pane.get("tab_id").and_then(Value::as_str);
                    (pane.get("terminal_title_stripped").and_then(Value::as_str)
                        == Some("kbctl board")
                        || pane.get("label").and_then(Value::as_str) == Some("kbctl board"))
                        && in_focused_tab
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for pane_id in board_panes.iter().filter_map(|pane| {
        pane.get("pane_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    }) {
        run_herdr_json(binary, &["pane", "close", &pane_id])?;
    }
    run_herdr_json(binary, &["plugin", "action", "invoke", "kbctl.open-board"])?;
    Ok(())
}

pub fn open_herdr_board() -> Result<(), KbctlError> {
    let binary = env::var("HERDR_BIN_PATH").unwrap_or_else(|_| "herdr".to_string());
    let active_pane = env::var("HERDR_ACTIVE_PANE_ID")
        .or_else(|_| env::var("HERDR_PANE_ID"))
        .map_err(|_| {
            KbctlError::Runtime(
                "Herdr did not provide an active pane; run this action from Herdr".to_string(),
            )
        })?;
    let panes = run_herdr_json(&binary, &["pane", "list"])?;
    let pane_list = panes
        .get("result")
        .and_then(|value| value.get("panes"))
        .and_then(Value::as_array)
        .ok_or_else(|| KbctlError::Runtime("Herdr did not return pane list".to_string()))?;
    let active_pane_info = pane_list
        .iter()
        .find(|pane| pane.get("pane_id").and_then(Value::as_str) == Some(active_pane.as_str()));
    let active_tab = env::var("HERDR_ACTIVE_TAB_ID")
        .or_else(|_| env::var("HERDR_TAB_ID"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            active_pane_info
                .and_then(|pane| pane.get("tab_id").and_then(Value::as_str))
                .map(ToOwned::to_owned)
        });
    let configured_cwd = Config::load(None)
        .ok()
        .and_then(|config| config.project.default.map(|binding| binding.path))
        .map(PathBuf::from)
        .filter(|path| path.is_dir())
        .map(|path| path.to_string_lossy().to_string());
    let cwd = configured_cwd
        .or_else(|| env::var("HERDR_ACTIVE_PANE_CWD").ok())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            active_pane_info.and_then(|pane| {
                pane.get("foreground_cwd")
                    .or_else(|| pane.get("cwd"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
        })
        .or_else(|| {
            env::current_dir()
                .ok()
                .map(|path| path.to_string_lossy().to_string())
        })
        .ok_or_else(|| KbctlError::Runtime("resolve Herdr board cwd".to_string()))?;
    let board_exists = pane_list.iter().any(|pane| {
        let title = pane
            .get("terminal_title_stripped")
            .or_else(|| pane.get("label"))
            .and_then(Value::as_str);
        title == Some("kbctl board")
            && active_tab.as_deref() == pane.get("tab_id").and_then(Value::as_str)
    });
    if board_exists {
        return Ok(());
    }
    let args = vec![
        "plugin".to_string(),
        "pane".to_string(),
        "open".to_string(),
        "--plugin".to_string(),
        "kbctl".to_string(),
        "--entrypoint".to_string(),
        "board".to_string(),
        "--placement".to_string(),
        "split".to_string(),
        "--target-pane".to_string(),
        active_pane.clone(),
        "--direction".to_string(),
        "right".to_string(),
        "--cwd".to_string(),
        cwd,
        "--no-focus".to_string(),
    ];
    let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
    run_herdr_json(&binary, &arg_refs)?;
    run_herdr_json(
        &binary,
        &[
            "pane",
            "resize",
            "--pane",
            active_pane.as_str(),
            "--direction",
            "right",
            "--amount",
            "0.25",
        ],
    )?;
    Ok(())
}

fn run_herdr_json(binary: &str, args: &[&str]) -> Result<Value, KbctlError> {
    let output = Command::new(binary)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| KbctlError::Runtime(format!("run {binary}: {error}")))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let value = serde_json::from_str::<Value>(&stdout).map_err(|error| {
        KbctlError::Runtime(format!(
            "Herdr returned non-JSON output for {}: {}",
            args.join(" "),
            if stderr.is_empty() {
                error.to_string()
            } else {
                stderr.clone()
            }
        ))
    })?;
    if !output.status.success() {
        return Err(KbctlError::Runtime(
            value
                .get("error")
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("Herdr command failed: {}", args.join(" "))),
        ));
    }
    if let Some(error) = value.get("error") {
        return Err(KbctlError::Runtime(error.to_string()));
    }
    Ok(value)
}

fn install_cli_binary() -> Result<(), KbctlError> {
    let executable = env::current_exe()
        .map_err(|error| KbctlError::Runtime(format!("找不到 kbctl executable: {error}")))?;
    let dest = cli_binary_path();
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| KbctlError::Config(format!("create {}: {error}", parent.display())))?;
    }
    if same_path(&executable, &dest) {
        println!("kbctl 已在 PATH：{}", dest.display());
        return Ok(());
    }
    fs::copy(&executable, &dest)
        .map_err(|error| KbctlError::Config(format!("install {}: {error}", dest.display())))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&dest)
            .map_err(|error| KbctlError::Config(format!("stat {}: {error}", dest.display())))?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&dest, permissions)
            .map_err(|error| KbctlError::Config(format!("chmod {}: {error}", dest.display())))?;
    }
    println!("kbctl 已安裝到 PATH：{}", dest.display());
    Ok(())
}

fn cli_binary_path() -> PathBuf {
    home_dir().join(".local/bin/kbctl")
}

fn same_path(left: &Path, right: &Path) -> bool {
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

fn grok_skill_path() -> PathBuf {
    grok_skill_path_from(&grok_home())
}

fn grok_home() -> PathBuf {
    env::var_os("GROK_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home_dir().join(".grok"))
}

fn grok_skill_path_from(home: &Path) -> PathBuf {
    home.join("skills/kbctl-report/SKILL.md")
}

fn codex_skill_path() -> PathBuf {
    home_dir().join(".codex/skills/kbctl-report/SKILL.md")
}

fn herdr_plugin_path(config: &Config) -> PathBuf {
    config
        .herdr
        .plugin_directory
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| home_dir().join(".config/herdr/plugins/kbctl"))
        .join("herdr-plugin.toml")
}

fn write_atomic(path: &Path, content: &str) -> Result<(), KbctlError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| KbctlError::Config(format!("create {}: {error}", parent.display())))?;
    }
    let temporary = match path.file_name() {
        Some(name) => path.with_file_name(format!("{}.tmp", name.to_string_lossy())),
        None => path.with_extension("tmp"),
    };
    fs::write(&temporary, content)
        .map_err(|error| KbctlError::Config(format!("write {}: {error}", temporary.display())))?;
    fs::rename(&temporary, path)
        .map_err(|error| KbctlError::Config(format!("replace {}: {error}", path.display())))?;
    Ok(())
}

fn toml_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn home_dir() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

const REPORT_SKILL: &str = r#"---
name: kbctl-report
description: Report a kbctl task result after completing a work contract. Use when KBCTL_EXECUTION_ID is set, a kbctl work contract is present, or the user asks to kbctl report done, review, or blocked.
---

# kbctl report

Keep `KBCTL_EXECUTION_ID` and `KBCTL_TASK_ID` from the environment. kbctl validates the report and writes business status back to Notion.

- Success: `kbctl report done --summary "what changed and how it was verified"`
- Human review: `kbctl report review --summary "what needs review"`
- Cannot continue: `kbctl report blocked --reason "the blocking condition"`

In a Herdr work contract, the report is written to the project-local spool file supplied by kbctl; the daemon consumes that file and owns the SQLite/Notion writeback. If the process exits without a valid report, the daemon retries up to its attempt limit, then sends the task to review.
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn grok_skill_is_written_under_grok_home() {
        let directory = tempfile::tempdir().unwrap();
        let path = grok_skill_path_from(directory.path());
        write_atomic(&path, REPORT_SKILL).unwrap();
        let contents = fs::read_to_string(&path).unwrap();
        assert!(contents.contains("name: kbctl-report"));
        assert!(contents.contains("kbctl report done"));
        assert!(contents.contains("KBCTL_EXECUTION_ID"));
        assert!(!path.with_file_name("SKILL.md.tmp").exists());
    }

    #[test]
    fn install_requires_a_target() {
        let error = run(Config::default(), false, false, false).unwrap_err();
        assert!(error.to_string().contains("--grok"));
    }

    #[test]
    fn cli_binary_installs_under_local_bin() {
        assert!(cli_binary_path().ends_with(".local/bin/kbctl"));
    }
}
