//! One Telegram line after a published epoch.
//!
//! Write the two defaults below. An empty token or an empty chat id sends
//! nothing. The archive command still exits 0 when Telegram fails.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, Result};

/// Bot token from BotFather. Leave this empty to send nothing.
pub(crate) const DEFAULT_TELEGRAM_BOT_TOKEN: &str = "8127218375:AAEP6oECzjqNaLaH2VHY_HA_7VYDuS4SL6Y";

/// Channel id, for example "-100…". Leave this empty to send nothing.
pub(crate) const DEFAULT_TELEGRAM_CHAT_ID: &str = "-4561803275";

const CURL_ARGS: &[&str] = &["--config", "-"];

pub(crate) fn notify_published_epoch(
    epoch: u64,
    output: &Path,
    bot_token: &str,
    chat_id: &str,
) -> Result<()> {
    let token = bot_token.trim();
    let chat = chat_id.trim();
    if !credentials_ready(token, chat) {
        eprintln!("telegram: skipped, bot token or chat id is empty");
        return Ok(());
    }
    let size = match directory_size(output) {
        Ok(bytes) => format_size(bytes),
        Err(err) => {
            eprintln!("telegram: directory size: {err:#}");
            "unknown".to_string()
        }
    };
    let text = published_line(epoch, &size);
    if let Err(err) = send_telegram(token, chat, &text) {
        eprintln!("telegram: {}", redact(&format!("{err:#}"), token));
    }
    Ok(())
}

pub(crate) fn credentials_ready(bot_token: &str, chat_id: &str) -> bool {
    !bot_token.trim().is_empty() && !chat_id.trim().is_empty()
}

pub(crate) fn published_line(epoch: u64, size: &str) -> String {
    format!("✅ epoch {epoch} archived, size {size}")
}

pub(crate) fn directory_size(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(path).with_context(|| format!("read {}", path.display()))? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_file() {
            total = total.saturating_add(meta.len());
        }
    }
    Ok(total)
}

pub(crate) fn format_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * 1024 * 1024;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn send_telegram(bot_token: &str, chat_id: &str, text: &str) -> Result<()> {
    let config = curl_config(bot_token, chat_id, text);
    let mut child = Command::new("curl")
        .args(CURL_ARGS)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("start curl")?;
    child
        .stdin
        .take()
        .context("curl stdin")?
        .write_all(config.as_bytes())
        .context("write curl config")?;
    let output = child.wait_with_output().context("wait for curl")?;
    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("curl status {}: {}", output.status, err.trim());
    }
    Ok(())
}

pub(crate) fn curl_config(bot_token: &str, chat_id: &str, text: &str) -> String {
    format!(
        "\
silent
show-error
fail
connect-timeout = 5
max-time = 15
request = \"POST\"
data-urlencode = \"chat_id={chat}\"
data-urlencode = \"text={text}\"
url = \"https://api.telegram.org/bot{token}/sendMessage\"
",
        chat = curl_quote(chat_id),
        text = curl_quote(text),
        token = curl_quote(bot_token),
    )
}

fn curl_quote(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

pub(crate) fn redact(text: &str, token: &str) -> String {
    let token = token.trim();
    if token.is_empty() {
        text.to_string()
    } else {
        text.replace(token, "[token]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_line_names_the_epoch_and_the_size() {
        assert_eq!(
            published_line(1084, "66.2 GiB"),
            "✅ epoch 1084 archived, size 66.2 GiB"
        );
    }

    #[test]
    fn format_size_uses_binary_units() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(1536), "1.5 KiB");
        assert_eq!(format_size(66 * 1024 * 1024 * 1024), "66.0 GiB");
    }

    #[test]
    fn blank_credentials_do_not_send() {
        assert!(!credentials_ready(" ", ""));
        assert!(!credentials_ready("token", "  "));
        assert!(credentials_ready("token", "-100"));
    }

    #[test]
    fn curl_config_keeps_the_token_out_of_the_arguments() {
        let token = "123456:ABC";
        let text = "epoch 1 slots 2-3 size 1.0 GiB";
        let config = curl_config(token, "-100123", text);
        assert_eq!(CURL_ARGS, ["--config", "-"]);
        assert!(CURL_ARGS.iter().all(|arg| !arg.contains(token)));
        assert!(config.contains(
            "url = \"https://api.telegram.org/bot123456:ABC/sendMessage\""
        ));
        assert!(config.contains("data-urlencode = \"chat_id=-100123\""));
        assert!(config.contains(&format!("data-urlencode = \"text={text}\"")));
    }

    #[test]
    fn an_error_text_loses_the_bot_token() {
        assert_eq!(
            redact("failed bot 123:ABC here", "123:ABC"),
            "failed bot [token] here"
        );
    }

    #[test]
    fn directory_size_sums_regular_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a"), [1, 2, 3]).unwrap();
        std::fs::write(dir.path().join("b"), [4, 5]).unwrap();
        std::fs::create_dir(dir.path().join("nested")).unwrap();
        assert_eq!(directory_size(dir.path()).unwrap(), 5);
    }
}
