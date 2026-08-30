use crate::{config::Config, error::KbctlError, herdr::run_sync_json};
use serde_json::Value;
use std::{
    env, fs,
    path::{Path, PathBuf},
};

pub fn run(config: Config, grok: bool, codex: bool, herdr: bool) -> Result<(), KbctlError> {
    install_cli_binary()?;
    if !grok && !codex && !herdr {
        return Ok(());
    }
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
        let executable = cli_binary_path();
        let manifest = herdr_manifest(&executable);
        write_atomic(&path, &manifest)?;
        link_and_open_herdr(&config.herdr.binary, &path)?;
        println!("Herdr plugin manifest 已安裝：{}", path.display());
    }
    Ok(())
}

fn link_and_open_herdr(binary: &str, manifest_path: &Path) -> Result<(), KbctlError> {
    let plugins = run_sync_json(binary, &["plugin", "list", "--json"])?;
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
        run_sync_json(binary, &["plugin", "unlink", "kbctl"])?;
    }
    let path = manifest_path.to_string_lossy().to_string();
    run_sync_json(binary, &["plugin", "link", &path])?;
    run_sync_json(binary, &["plugin", "action", "invoke", "kbctl.open-board"])?;
    Ok(())
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
    let temporary = dest.with_file_name(".kbctl.installing");
    fs::copy(&executable, &temporary)
        .map_err(|error| KbctlError::Config(format!("install {}: {error}", temporary.display())))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&temporary)
            .map_err(|error| KbctlError::Config(format!("stat {}: {error}", temporary.display())))?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&temporary, permissions).map_err(|error| {
            KbctlError::Config(format!("chmod {}: {error}", temporary.display()))
        })?;
    }
    fs::rename(&temporary, &dest)
        .map_err(|error| KbctlError::Config(format!("activate {}: {error}", dest.display())))?;
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

fn herdr_manifest(executable: &Path) -> String {
    let executable = toml_string(&executable.display().to_string());
    format!(
        "id = \"kbctl\"\nname = \"kbctl\"\nversion = \"{}\"\nmin_herdr_version = \"0.8.2\"\ndescription = \"Context-aware Notion task control for Herdr\"\nplatforms = [\"macos\", \"linux\", \"windows\"]\n\n[[panes]]\nid = \"board\"\ntitle = \"kbctl board\"\nplacement = \"split\"\ncommand = [{executable} , \"board\"]\n\n[[actions]]\nid = \"open-board\"\ntitle = \"Open kbctl board\"\ndescription = \"Open the board and select the task associated with the focused Herdr pane.\"\ncontexts = [\"global\", \"workspace\", \"tab\", \"pane\"]\ncommand = [{executable} , \"_herdr-open-board\"]\n\n[[actions]]\nid = \"task-detail\"\ntitle = \"Open current task\"\ndescription = \"Open the focused pane's kbctl task in the board.\"\ncontexts = [\"workspace\", \"tab\", \"pane\"]\ncommand = [{executable} , \"_herdr-task-detail\"]\n\n[[actions]]\nid = \"focus-task\"\ntitle = \"Focus current task\"\ndescription = \"Focus the Herdr agent for the focused pane's kbctl task.\"\ncontexts = [\"pane\"]\ncommand = [{executable} , \"_herdr-focus-task\"]\n\n[[actions]]\nid = \"cancel-task\"\ntitle = \"Cancel current task\"\ndescription = \"Cancel the kbctl task associated with the focused Herdr pane.\"\ncontexts = [\"pane\"]\ncommand = [{executable} , \"_herdr-cancel-task\"]\n",
        env!("CARGO_PKG_VERSION")
    )
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
description: Submit a kbctl work contract result. Use when KBCTL_EXECUTION_ID is set or a kbctl Supervisor, Worker, or standalone contract is present.
---

# kbctl report

Keep `KBCTL_EXECUTION_ID` and `KBCTL_TASK_ID` from the environment. kbctl validates the report and writes business status back to Notion.

When `KBCTL_TRANSPORT` is `herdr` and `KBCTL_EXECUTION_ROLE` is `supervisor`, `reviewer`, or `worker`, do not run `kbctl report` and do not write a manifest. Serialize the envelope as JSON, encode those exact JSON bytes with standard Base64, and return that text between the exact `KBCTL_ENVELOPE_BEGIN` and `KBCTL_ENVELOPE_END` marker lines required by the work contract. Line wrapping inside the Base64 payload is allowed. Supervisors return Plan and Review envelopes. Workers return Completion envelopes and must commit write work before returning a successful write result.

Outside Herdr, `kbctl report submit --execution "$KBCTL_EXECUTION_ID" --manifest <file>` remains available for a manually managed Supervisor, Reviewer, or Worker.

For standalone contracts:

- Success: `kbctl report done --summary "what changed and how it was verified"`
- Human review: `kbctl report review --summary "what needs review"`
- Cannot continue: `kbctl report blocked --reason "the blocking condition"`

In a Herdr work contract, Herdr transports the final response and lifecycle events. The daemon reads and validates the marked envelope, then owns SQLite and Notion writeback. If the process settles without a valid envelope, the daemon retries up to its attempt limit, then sends the task to review.
"#;

#[cfg(test)]
mod tests {
    use super::*;

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
    fn cli_binary_installs_under_local_bin() {
        assert!(cli_binary_path().ends_with(".local/bin/kbctl"));
    }

    #[test]
    fn herdr_manifest_declares_context_actions() {
        let manifest = herdr_manifest(Path::new("/tmp/kbctl"));
        assert!(manifest.contains("contexts = [\"global\", \"workspace\", \"tab\", \"pane\"]"));
        assert!(manifest.contains("contexts = [\"workspace\", \"tab\", \"pane\"]"));
        assert!(manifest.contains("id = \"task-detail\""));
        assert!(manifest.contains("id = \"focus-task\""));
        assert!(manifest.contains("id = \"cancel-task\""));
        assert!(manifest.contains("_herdr-task-detail"));
    }
}
