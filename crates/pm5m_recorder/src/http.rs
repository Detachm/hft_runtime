use crate::types::RecorderConfig;
use market_data_etl_core::{now_unix_ns, sha256_bytes};
use std::thread;
use std::time::Duration;

pub trait HttpFetcher: Sync {
    fn fetch(&self, url: &str, config: HttpFetchConfig) -> HttpFetchOutcome;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BlockingHttpFetcher;

#[derive(Debug, Clone, Copy)]
pub struct HttpFetchConfig {
    pub timeout_ms: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

impl From<&RecorderConfig> for HttpFetchConfig {
    fn from(config: &RecorderConfig) -> Self {
        Self {
            timeout_ms: config.http_timeout_ms,
            max_retries: config.http_max_retries,
            retry_backoff_ms: config.http_retry_backoff_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpFetchOutcome {
    pub url: String,
    pub request_start_ts_ns: i64,
    pub request_end_ts_ns: i64,
    pub status_code: Option<u16>,
    pub body: Vec<u8>,
    pub body_sha256: Option<String>,
    pub attempt_count: u32,
    pub final_error: Option<String>,
}

impl HttpFetchOutcome {
    pub fn success(url: &str, body: impl Into<Vec<u8>>) -> Self {
        let body = body.into();
        Self {
            url: url.to_string(),
            request_start_ts_ns: now_unix_ns() as i64,
            request_end_ts_ns: now_unix_ns() as i64,
            status_code: Some(200),
            body_sha256: Some(sha256_bytes(&body)),
            body,
            attempt_count: 1,
            final_error: None,
        }
    }

    pub fn failure(
        url: &str,
        status_code: Option<u16>,
        body: impl Into<Vec<u8>>,
        attempt_count: u32,
        final_error: impl Into<String>,
    ) -> Self {
        let body = body.into();
        Self {
            url: url.to_string(),
            request_start_ts_ns: now_unix_ns() as i64,
            request_end_ts_ns: now_unix_ns() as i64,
            status_code,
            body_sha256: Some(sha256_bytes(&body)),
            body,
            attempt_count,
            final_error: Some(final_error.into()),
        }
    }

    pub(crate) fn is_success(&self) -> bool {
        self.final_error.is_none()
            && self
                .status_code
                .is_some_and(|code| (200..300).contains(&code))
    }

    pub(crate) fn retry_count(&self) -> u32 {
        self.attempt_count.saturating_sub(1)
    }
}

impl HttpFetcher for BlockingHttpFetcher {
    fn fetch(&self, url: &str, config: HttpFetchConfig) -> HttpFetchOutcome {
        let request_start_ts_ns = now_unix_ns() as i64;
        let client = match reqwest::blocking::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                return HttpFetchOutcome {
                    url: url.to_string(),
                    request_start_ts_ns,
                    request_end_ts_ns: now_unix_ns() as i64,
                    status_code: None,
                    body: Vec::new(),
                    body_sha256: Some(sha256_bytes(&[])),
                    attempt_count: 0,
                    final_error: Some(format!("build HTTP client: {err}")),
                };
            }
        };

        let max_attempts = config.max_retries.saturating_add(1);
        let mut attempt_count = 0;
        let mut last_status = None;
        let mut last_body = Vec::new();
        let mut last_error = None;
        while attempt_count < max_attempts {
            attempt_count += 1;
            match client.get(url).send() {
                Ok(response) => {
                    let status = response.status().as_u16();
                    last_status = Some(status);
                    match response.bytes() {
                        Ok(bytes) => {
                            last_body = bytes.to_vec();
                            if (200..300).contains(&status) {
                                return HttpFetchOutcome {
                                    url: url.to_string(),
                                    request_start_ts_ns,
                                    request_end_ts_ns: now_unix_ns() as i64,
                                    status_code: Some(status),
                                    body_sha256: Some(sha256_bytes(&last_body)),
                                    body: last_body,
                                    attempt_count,
                                    final_error: None,
                                };
                            }
                            last_error = Some(format!("HTTP status {status}"));
                            if !is_retryable_status(status) || attempt_count >= max_attempts {
                                break;
                            }
                        }
                        Err(err) => {
                            last_body = Vec::new();
                            last_error = Some(format!("read HTTP response body: {err}"));
                            if attempt_count >= max_attempts {
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    last_status = None;
                    last_body = Vec::new();
                    last_error = Some(format!("GET {url}: {err}"));
                    if attempt_count >= max_attempts {
                        break;
                    }
                }
            }
            thread::sleep(Duration::from_millis(config.retry_backoff_ms));
        }

        HttpFetchOutcome {
            url: url.to_string(),
            request_start_ts_ns,
            request_end_ts_ns: now_unix_ns() as i64,
            status_code: last_status,
            body_sha256: Some(sha256_bytes(&last_body)),
            body: last_body,
            attempt_count,
            final_error: last_error,
        }
    }
}

fn is_retryable_status(status: u16) -> bool {
    status == 429 || (500..600).contains(&status)
}
