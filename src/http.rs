//! The one HTTP client every publisher shares.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::header::{HeaderMap, AUTHORIZATION, CONTENT_TYPE};
use serde::Serialize;
use serde_json::Value;
use tokio::time::Instant;

use crate::lock;

pub(crate) const USER_AGENT: &str = concat!(
    "TheDiscordList-serenity/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/TheDiscordList/integration-serenity)"
);

const TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(60);
/// A hostile or broken rate-limit header must not park the client forever.
const MAX_SUSPENSION_SECS: u64 = 3600;

#[derive(Debug)]
pub(crate) enum Outcome {
    Ok {
        status: u16,
        body: Value,
    },
    /// 400/422: this payload will never succeed.
    Payload(u16),
    Auth,
    Plan,
    /// 404/409. A 404 means an unknown id or a server listing, never a bad token.
    Config(u16),
    Throttled,
    Transient {
        status: Option<u16>,
        message: String,
    },
    /// Not sent: the client-wide 429 suspension is active.
    Dropped,
}

/// One suspension for the whole client, because the server keeps a single
/// rate-limit counter per client IP. Heartbeats bypass it: a dropped stats
/// post is a stale number, a dropped heartbeat is a false "down".
pub(crate) struct Api {
    client: reqwest::Client,
    base_url: String,
    authorization: String,
    suspended_until: Mutex<Option<Instant>>,
}

impl Api {
    pub(crate) fn new(base_url: &str, token: &str) -> reqwest::Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(TIMEOUT)
                .user_agent(USER_AGENT)
                .build()?,
            base_url: base_url.trim_end_matches('/').to_owned(),
            authorization: format!("Bearer {token}"),
            suspended_until: Mutex::new(None),
        })
    }

    #[cfg(test)]
    pub(crate) fn suspended_until(&self) -> Option<Instant> {
        *lock(&self.suspended_until)
    }

    pub(crate) async fn post(
        &self,
        path: &str,
        body: &(impl Serialize + Sync),
        heartbeat: bool,
    ) -> Outcome {
        if !heartbeat && lock(&self.suspended_until).is_some_and(|until| Instant::now() < until) {
            return Outcome::Dropped;
        }

        let body = match serde_json::to_vec(body) {
            Ok(body) => body,
            Err(error) => {
                return Outcome::Transient {
                    status: None,
                    message: error.to_string(),
                }
            }
        };

        let response = self
            .client
            .post(format!("{}{path}", self.base_url))
            .header(AUTHORIZATION, &self.authorization)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                return Outcome::Transient {
                    status: None,
                    message: error.to_string(),
                }
            }
        };

        let status = response.status().as_u16();
        if status == 429 {
            self.suspend(response.headers());
        }

        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                return Outcome::Transient {
                    status: Some(status),
                    message: error.to_string(),
                }
            }
        };

        match status {
            200..=299 => Outcome::Ok {
                status,
                body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            },
            401 => Outcome::Auth,
            403 => Outcome::Plan,
            404 | 409 => Outcome::Config(status),
            400 | 422 => Outcome::Payload(status),
            429 => Outcome::Throttled,
            _ => Outcome::Transient {
                status: Some(status),
                message: format!("TheDiscordList responded {status}."),
            },
        }
    }

    /// Honours `Retry-After` (seconds, default 60) and `X-RateLimit-Reset`
    /// (unix seconds), whichever ends later, plus up to 2 s of jitter.
    fn suspend(&self, headers: &HeaderMap) {
        let seconds = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<u64>().ok())
                .filter(|value| *value > 0)
        };

        let now = Instant::now();
        let retry_after = seconds("retry-after").map_or(DEFAULT_RETRY_AFTER, |secs| {
            Duration::from_secs(secs.min(MAX_SUSPENSION_SECS))
        });
        let mut until = now + retry_after;

        if let Some(reset) = seconds("x-ratelimit-reset") {
            let unix_now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let wait = reset.saturating_sub(unix_now).min(MAX_SUSPENSION_SECS);
            until = until.max(now + Duration::from_secs(wait));
        }

        *lock(&self.suspended_until) = Some(until + Duration::from_secs(2).mul_f64(random_unit()));
    }
}

/// `min(base * 2^attempt, 30 * base)`, scaled by a random factor in `[0.5, 1.0]`.
/// With the default 1 s base that is the spec's `min(1000 * 2^n, 30000)` ms.
pub(crate) fn backoff(attempt: u32, base: Duration) -> Duration {
    let ceiling = base.saturating_mul(1 << attempt.min(16)).min(base * 30);
    ceiling.mul_f64(0.5 + random_unit() * 0.5)
}

/// A uniform float in `[0, 1)` from the std hasher's per-instance random keys.
pub(crate) fn random_unit() -> f64 {
    let bits = RandomState::new().build_hasher().finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_stays_within_the_jittered_window() {
        let base = Duration::from_secs(1);
        for attempt in 0..8 {
            let ceiling = Duration::from_millis((1000u64 << attempt).min(30_000));
            for _ in 0..50 {
                let delay = backoff(attempt, base);
                assert!(
                    delay >= ceiling / 2 && delay <= ceiling,
                    "{attempt}: {delay:?}"
                );
            }
        }
    }

    #[test]
    fn random_unit_is_in_range_and_varies() {
        let samples: Vec<f64> = (0..100).map(|_| random_unit()).collect();
        assert!(samples.iter().all(|x| (0.0..1.0).contains(x)));
        assert!(samples.windows(2).any(|pair| pair[0] != pair[1]));
    }

    #[test]
    fn user_agent_names_library_version_and_repository() {
        assert_eq!(
            USER_AGENT,
            format!(
                "TheDiscordList-serenity/{} (+https://github.com/TheDiscordList/integration-serenity)",
                env!("CARGO_PKG_VERSION")
            )
        );
    }
}
