use crate::error::{SourceError, SourceResult};
use worker::{Fetch, Headers, Method, Request, RequestInit};

const OLD_FAITHFUL_URL: &str = "https://files.old-faithful.net";

pub async fn get_text(path: &str) -> SourceResult<String> {
    let bytes = get_bytes(path).await?;
    String::from_utf8(bytes).map_err(|err| SourceError::SourceBody {
        path: path.to_string(),
        reason: err.to_string(),
    })
}

pub async fn get_bytes(path: &str) -> SourceResult<Vec<u8>> {
    let url = format!("{OLD_FAITHFUL_URL}/{}", path.trim_start_matches('/'));
    let mut http_response = fetch(&url, None).await?;
    let status = http_response.status_code();
    if status != 200 {
        return Err(SourceError::SourceStatus {
            path: url,
            range: None,
            status,
        });
    }
    http_response
        .bytes()
        .await
        .map_err(|err| SourceError::SourceBody {
            path: path.to_string(),
            reason: err.to_string(),
        })
}

pub async fn get_range(path: &str, offset: u64, len: usize) -> SourceResult<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    let end = offset
        .checked_add(len as u64)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| SourceError::RangeOverflow {
            path: path.to_string(),
            offset,
            len,
        })?;
    let range_header = format!("bytes={offset}-{end}");

    let url = format!("{OLD_FAITHFUL_URL}/{}", path.trim_start_matches('/'));
    let mut http_response = fetch(&url, Some(&range_header)).await?;
    let status = http_response.status_code();
    if status != 206 {
        return Err(SourceError::SourceStatus {
            path: url,
            range: Some(range_header),
            status,
        });
    }
    let bytes = http_response
        .bytes()
        .await
        .map_err(|err| SourceError::SourceBody {
            path: path.to_string(),
            reason: err.to_string(),
        })?;
    if bytes.len() == len {
        Ok(bytes)
    } else {
        Err(SourceError::SourceBody {
            path: path.to_string(),
            reason: format!(
                "{range_header} returned {} bytes, expected {len}",
                bytes.len()
            ),
        })
    }
}

async fn fetch(url: &str, range_header: Option<&str>) -> SourceResult<worker::Response> {
    let headers = Headers::new();
    if let Some(range_header) = range_header {
        headers.set("Range", range_header)?;
    }
    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    init.with_headers(headers);
    let request = Request::new_with_init(url, &init)?;
    Fetch::Request(request).send().await.map_err(Into::into)
}
