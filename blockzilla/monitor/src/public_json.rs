use anyhow::{Context, Result, anyhow, ensure};
use regex::Regex;
use serde_json::Value;
use std::{borrow::Cow, sync::OnceLock};

pub const MAX_JSON_BYTES: usize = 64 * 1024 * 1024;

const PATH_KEY_SUFFIXES: &[&str] = &["_dir", "_path", "_root"];
const SENSITIVE_PUBLIC_KEYS: &[&str] = &[
    "access_key",
    "access_token",
    "api_key",
    "authorization",
    "client_secret",
    "credential",
    "credentials",
    "endpoint",
    "password",
    "passwd",
    "private_key",
    "proxy_authorization",
    "refresh_token",
    "secret",
    "session_key",
    "token",
];
const SENSITIVE_COMPACT_KEYS: &[&str] = &[
    "accesskey",
    "accesstoken",
    "apikey",
    "authorization",
    "clientsecret",
    "credential",
    "credentials",
    "endpoint",
    "password",
    "passwd",
    "privatekey",
    "proxyauthorization",
    "refreshtoken",
    "secret",
    "sessionkey",
    "token",
];
const SENSITIVE_PUBLIC_KEY_SUFFIXES: &[&str] = &[
    "_access_key",
    "_access_token",
    "_api_key",
    "_authorization",
    "_client_secret",
    "_credential",
    "_credentials",
    "_password",
    "_private_key",
    "_refresh_token",
    "_secret",
    "_session_key",
    "_token",
];

struct RedactionRegexes {
    absolute_private_path: Regex,
    authorization_token: Regex,
    credentialed_url: Regex,
    query_credential: Regex,
    inline_credential: Regex,
}

fn redactions() -> &'static RedactionRegexes {
    static REDACTIONS: OnceLock<RedactionRegexes> = OnceLock::new();
    REDACTIONS.get_or_init(|| RedactionRegexes {
        // Rust regex does not support look-behind. Capture and preserve the
        // boundary byte instead of using Python's negative look-behind.
        absolute_private_path: Regex::new(
            r"(^|[^A-Za-z0-9:])(/(?:Applications|Users|bin|boot|data|dev|etc|home|lib|lib64|media|mnt|opt|proc|root|run|sbin|srv|storage|sys|tmp|usr|var|volume[0-9]*)(?:/[^\s,;:()\[\]{}]+)*)",
        )
        .expect("valid private path regex"),
        authorization_token: Regex::new(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+")
            .expect("valid authorization regex"),
        credentialed_url: Regex::new(r"(?i)\b([a-z][a-z0-9+.-]*://)[^/\s?#]+@")
            .expect("valid credentialed URL regex"),
        query_credential: Regex::new(
            r"(?i)([?&](?:access[_-]?key|access[_-]?token|api[_-]?key|client[_-]?secret|credential|password|private[_-]?key|refresh[_-]?token|secret|token)=)[^&#\s]*",
        )
        .expect("valid query credential regex"),
        inline_credential: Regex::new(
            r"(?i)\b(access[_-]?key|access[_-]?token|api[_-]?key|client[_-]?secret|credential|password|private[_-]?key|refresh[_-]?token|secret|token)\s*[:=]\s*[^\s,;&#]+",
        )
        .expect("valid inline credential regex"),
    })
}

fn normalized_key(value: &str) -> String {
    let mut separated = String::with_capacity(value.len() + 4);
    let mut previous: Option<char> = None;
    for character in value.chars() {
        if character.is_ascii_uppercase()
            && previous.is_some_and(|item| item.is_ascii_lowercase() || item.is_ascii_digit())
        {
            separated.push('_');
        }
        separated.extend(character.to_lowercase());
        previous = Some(character);
    }

    let mut normalized = String::with_capacity(separated.len());
    let mut separator = false;
    for character in separated.chars() {
        if character.is_ascii_lowercase() || character.is_ascii_digit() {
            if separator && !normalized.is_empty() {
                normalized.push('_');
            }
            normalized.push(character);
            separator = false;
        } else {
            separator = true;
        }
    }
    normalized
}

pub fn is_sensitive_public_key(value: &str) -> bool {
    // The Python boundary uses Unicode case-folding before matching sensitive
    // names. Rust's standard lowercase conversion is deliberately weaker (for
    // example, `paßword`.to_lowercase() does not become `password`). Public
    // producer schemas use ASCII field names, so dropping non-ASCII keys is the
    // conservative, fail-closed equivalent and prevents a case-fold bypass.
    if !value.is_ascii() {
        return true;
    }
    let normalized = normalized_key(value);
    let compact = normalized.replace('_', "");
    SENSITIVE_PUBLIC_KEYS.contains(&normalized.as_str())
        || SENSITIVE_COMPACT_KEYS.contains(&compact.as_str())
        || normalized.split('_').any(|token| {
            matches!(
                token,
                "authorization"
                    | "credential"
                    | "credentials"
                    | "password"
                    | "passwd"
                    | "secret"
                    | "token"
            )
        })
        || SENSITIVE_PUBLIC_KEY_SUFFIXES
            .iter()
            .any(|suffix| normalized.ends_with(suffix))
}

fn safe_basename(value: &str) -> &str {
    value
        .trim_end_matches(['/', '\\'])
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or("")
}

fn replace_if_match<'a>(value: Cow<'a, str>, regex: &Regex, replacement: &str) -> Cow<'a, str> {
    if regex.is_match(value.as_ref()) {
        Cow::Owned(regex.replace_all(value.as_ref(), replacement).into_owned())
    } else {
        value
    }
}

fn sanitize_public_string_cow(value: &str) -> Cow<'_, str> {
    let regexes = redactions();
    let mut public = Cow::Borrowed(value);
    public = replace_if_match(public, &regexes.absolute_private_path, "$1<redacted-path>");
    public = replace_if_match(public, &regexes.authorization_token, "$1 <redacted>");
    public = replace_if_match(public, &regexes.credentialed_url, "$1<redacted>@");
    public = replace_if_match(public, &regexes.query_credential, "$1<redacted>");
    replace_if_match(public, &regexes.inline_credential, "$1=<redacted>")
}

pub fn sanitize_public_string(value: &str) -> String {
    sanitize_public_string_cow(value).into_owned()
}

pub fn sanitize_public_value(value: &mut Value, key: Option<&str>) {
    match value {
        Value::Object(object) => {
            object.retain(|child_key, _| !is_sensitive_public_key(child_key));
            for (child_key, child_value) in object {
                sanitize_public_value(child_value, Some(child_key));
            }
        }
        Value::Array(items) => {
            for item in items {
                sanitize_public_value(item, None);
            }
        }
        Value::String(text) => {
            let path_value = key.is_some_and(|name| {
                name == "path"
                    || PATH_KEY_SUFFIXES
                        .iter()
                        .any(|suffix| name.ends_with(suffix))
            });
            let source = if path_value {
                safe_basename(text)
            } else {
                text.as_str()
            };
            let replacement = match sanitize_public_string_cow(source) {
                Cow::Owned(public) => Some(public),
                Cow::Borrowed(public) if public != text => Some(public.to_owned()),
                Cow::Borrowed(_) => None,
            };
            if let Some(public) = replacement {
                *text = public;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

pub fn public_json_bytes(raw: &[u8]) -> Result<Vec<u8>> {
    ensure!(
        raw.len() <= MAX_JSON_BYTES,
        "upstream JSON exceeds the public proxy limit"
    );
    let mut value: Value = serde_json::from_slice(raw).context("decode upstream JSON")?;
    sanitize_public_value(&mut value, None);
    let encoded = serde_json::to_vec(&value).context("encode public JSON")?;
    let encoded_text = std::str::from_utf8(&encoded).map_err(|_| anyhow!("invalid JSON UTF-8"))?;
    ensure!(
        !redactions().absolute_private_path.is_match(encoded_text),
        "public JSON still contains an absolute private path"
    );
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn path_values_are_reduced_and_messages_are_redacted() {
        let raw = serde_json::to_vec(&json!({
            "epochs": [{
                "input_path": "/volume1/archive/cars/epoch-341.car.zst",
                "output_path": "/volume1/archive/v2/epoch-341"
            }],
            "live": [{"capture_dir": "/volume1/private/live/capture-1"}],
            "errors": [{"message": "open /home/operator/private/state failed"}]
        }))
        .unwrap();
        let public: Value = serde_json::from_slice(&public_json_bytes(&raw).unwrap()).unwrap();
        assert_eq!(public["epochs"][0]["input_path"], "epoch-341.car.zst");
        assert_eq!(public["epochs"][0]["output_path"], "epoch-341");
        assert_eq!(public["live"][0]["capture_dir"], "capture-1");
        assert!(
            public["errors"][0]["message"]
                .as_str()
                .unwrap()
                .contains("<redacted-path>")
        );
    }

    #[test]
    fn secret_keys_and_strings_are_redacted() {
        let raw = br#"{
            "ok":true,
            "token":"TOPSECRET",
            "nested":{"apiKey":"TOPSECRET","receiverTokenValue":"TOPSECRET"},
            "message":"request used Bearer TOPSECRET",
            "credentialed_url":"https://operator:p@ss@example.invalid/status",
            "query_url":"https://example.invalid/status?token=TOPSECRET&mode=public",
            "diagnostic":"api_key=TOPSECRET"
        }"#;
        let public: Value = serde_json::from_slice(&public_json_bytes(raw).unwrap()).unwrap();
        let serialized = serde_json::to_string(&public).unwrap();
        assert_eq!(public["ok"], true);
        assert!(public.get("token").is_none());
        assert!(public["nested"].get("apiKey").is_none());
        assert!(public["nested"].get("receiverTokenValue").is_none());
        assert!(!serialized.contains("TOPSECRET"));
        assert_eq!(public["message"], "request used Bearer <redacted>");
        assert_eq!(
            public["credentialed_url"],
            "https://<redacted>@example.invalid/status"
        );
        assert!(
            public["query_url"]
                .as_str()
                .unwrap()
                .contains("token=<redacted>")
        );
        assert_eq!(public["diagnostic"], "api_key=<redacted>");
    }

    #[test]
    fn non_ascii_keys_cannot_bypass_python_casefold_semantics() {
        let raw = r#"{"paßword":"TOPSECRET","safe":"public"}"#.as_bytes();
        let public: Value = serde_json::from_slice(&public_json_bytes(raw).unwrap()).unwrap();
        assert!(public.get("paßword").is_none());
        assert_eq!(public["safe"], "public");
        assert!(
            !serde_json::to_string(&public)
                .unwrap()
                .contains("TOPSECRET")
        );
    }

    #[test]
    fn ordinary_strings_remain_borrowed_through_the_redaction_pipeline() {
        assert!(matches!(
            sanitize_public_string_cow("ordinary public value"),
            Cow::Borrowed("ordinary public value")
        ));
    }

    #[test]
    fn public_size_limit_is_inclusive() {
        assert!(public_json_bytes(&vec![b' '; MAX_JSON_BYTES + 1]).is_err());
    }
}
