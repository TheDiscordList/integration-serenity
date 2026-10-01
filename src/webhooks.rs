//! Verification for TheDiscordList's outbound webhooks.
//!
//! ```
//! use thediscordlist_serenity::webhooks::{self, EventData};
//!
//! fn handle(raw_body: &[u8], signature: Option<&str>, secret: &str) -> u16 {
//!     match webhooks::verify(raw_body, signature, secret) {
//!         Ok(event) => {
//!             if let EventData::ListingUpvoted(vote) = &event.data {
//!                 println!("{} upvotes", vote.total_upvotes);
//!             }
//!             204 // every verified delivery gets a 2xx, unknown events included
//!         }
//!         Err(_) => 400,
//!     }
//! }
//! ```
//!
//! `raw_body` must be the exact bytes that arrived. The signature covers
//! bytes, not meaning: re-serialising a parsed body can never verify.

use std::fmt::{self, Write as _};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use subtle::ConstantTimeEq;

/// `X-TDL-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256>`.
pub const SIGNATURE_HEADER: &str = "X-TDL-Signature";
/// `X-TDL-Delivery`: the delivery uuid, equal to [`WebhookEvent::id`] and stable across retries.
pub const DELIVERY_HEADER: &str = "X-TDL-Delivery";
/// `X-TDL-Event`: the event name, equal to [`WebhookEvent::event`].
pub const EVENT_HEADER: &str = "X-TDL-Event";
/// `X-TDL-Attempt`: the 1-based delivery attempt.
pub const ATTEMPT_HEADER: &str = "X-TDL-Attempt";
/// How far `t` may sit from the current time, in either direction.
pub const DEFAULT_TOLERANCE: Duration = Duration::from_secs(300);

/// Why a delivery failed verification. Answer it with a `400`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebhookError {
    /// No `X-TDL-Signature` header.
    MissingSignature,
    /// The header has no numeric `t` or no `v1`.
    MalformedSignature,
    /// `t` is further than the tolerance from now.
    StaleTimestamp,
    /// The HMAC does not match: wrong secret, or the body was altered.
    InvalidSignature,
    /// The signature is valid but the body is not a webhook envelope.
    MalformedBody,
}

impl WebhookError {
    /// A stable machine-readable code, e.g. `"INVALID_SIGNATURE"`.
    #[must_use]
    pub fn code(self) -> &'static str {
        match self {
            Self::MissingSignature => "MISSING_SIGNATURE",
            Self::MalformedSignature => "MALFORMED_SIGNATURE",
            Self::StaleTimestamp => "STALE_TIMESTAMP",
            Self::InvalidSignature => "INVALID_SIGNATURE",
            Self::MalformedBody => "MALFORMED_BODY",
        }
    }
}

impl fmt::Display for WebhookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::MissingSignature => "missing X-TDL-Signature header",
            Self::MalformedSignature => "malformed X-TDL-Signature header",
            Self::StaleTimestamp => "webhook timestamp outside the tolerance window",
            Self::InvalidSignature => "webhook signature did not match",
            Self::MalformedBody => "webhook body was not a valid event envelope",
        })
    }
}

impl std::error::Error for WebhookError {}

/// A verified delivery.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WebhookEvent {
    /// The delivery uuid. Stable across all retries of one delivery: deduplicate on it.
    pub id: String,
    /// The event name, e.g. `"listing.upvoted"`. Unknown names parse too.
    pub event: String,
    /// ISO 8601 enqueue time, not send time. Do not use it for freshness.
    pub created_at: String,
    /// The listing the event is about.
    pub listing: Listing,
    /// The event's payload, typed for the events this version knows.
    pub data: EventData,
}

/// The listing an event is about.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct Listing {
    /// Guild id for a server, application id for a bot.
    pub id: String,
    /// Server or bot.
    #[serde(rename = "type")]
    pub kind: ListingKind,
    /// Display name.
    pub name: String,
    /// Public listing URL.
    pub url: String,
}

/// Whether a listing is a server or a bot.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ListingKind {
    /// A Discord server listing.
    Server,
    /// A bot listing.
    Bot,
    /// A listing type this version does not know.
    #[serde(other)]
    Other,
}

/// An event's `data`, typed by event name.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum EventData {
    /// `listing.bumped`
    ListingBumped(ListingBumped),
    /// `listing.upvoted`
    ListingUpvoted(ListingUpvoted),
    /// `server.joined`: server listings only.
    ServerJoined(ServerJoined),
    /// `bot.added`: a click on the install hand-off. Discord never reports whether the install completed.
    BotAdded(BotAdded),
    /// `bot.stats_reported`: also fires on owner edits in the web UI, not only on your own posts.
    BotStatsReported(BotStatsReported),
    /// `webhook.ping`
    WebhookPing(WebhookPing),
    /// An event this version does not know, or a known one whose shape changed. Answer it with a 2xx.
    Unknown(Value),
}

/// The member behind an event. Every field reflects their last sign-in.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct WebhookUser {
    /// Discord user id; can be `None` even when the user is present.
    pub id: Option<String>,
    /// Username snapshotted at last sign-in.
    pub username: String,
    /// Full avatar CDN URL.
    pub avatar: Option<String>,
}

/// `data` of `listing.bumped`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct ListingBumped {
    /// Points awarded for the bump.
    pub points: i64,
    /// ISO 8601.
    pub bumped_at: String,
    /// ISO 8601.
    pub next_bump_at: String,
    /// Who bumped.
    pub user: Option<WebhookUser>,
}

/// `data` of `listing.upvoted`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct ListingUpvoted {
    /// ISO 8601.
    pub voted_at: String,
    /// The listing's upvote total after this vote.
    pub total_upvotes: u64,
    /// Who voted.
    pub user: Option<WebhookUser>,
}

/// `data` of `server.joined`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerJoined {
    /// ISO 8601.
    pub joined_at: String,
    /// Who joined.
    pub user: Option<WebhookUser>,
}

/// `data` of `bot.added`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct BotAdded {
    /// ISO 8601.
    pub added_at: String,
    /// Who clicked.
    pub user: Option<WebhookUser>,
}

/// `data` of `bot.stats_reported`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct BotStatsReported {
    /// Reported server count.
    pub guild_count: Option<u64>,
    /// Reported shard count.
    pub shard_count: Option<u64>,
    /// ISO 8601.
    pub reported_at: String,
}

/// `data` of `webhook.ping`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct WebhookPing {
    /// A human-readable message.
    pub message: String,
}

#[derive(Deserialize)]
struct Envelope {
    id: String,
    event: String,
    created_at: String,
    listing: Listing,
    #[serde(default)]
    data: Value,
}

/// Verifies a delivery against the current time with the default 300 s tolerance.
///
/// `signature` is the `X-TDL-Signature` header value. `raw_body` must be the
/// exact bytes received.
///
/// # Errors
///
/// A [`WebhookError`] naming the first check that failed. Answer it with a `400`.
pub fn verify(
    raw_body: &[u8],
    signature: Option<&str>,
    secret: &str,
) -> Result<WebhookEvent, WebhookError> {
    verify_at(
        raw_body,
        signature,
        secret,
        DEFAULT_TOLERANCE,
        SystemTime::now(),
    )
}

/// [`verify`] with an explicit tolerance and clock.
///
/// # Errors
///
/// A [`WebhookError`] naming the first check that failed.
pub fn verify_at(
    raw_body: &[u8],
    signature: Option<&str>,
    secret: &str,
    tolerance: Duration,
    now: SystemTime,
) -> Result<WebhookEvent, WebhookError> {
    let header = signature
        .filter(|header| !header.trim().is_empty())
        .ok_or(WebhookError::MissingSignature)?;

    // Split on the first `=` only: a value may itself contain `=`.
    let mut timestamp = None;
    let mut digest = None;
    for part in header.split(',') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        let value = value.trim();
        match key.trim() {
            "t" if timestamp.is_none() => timestamp = Some(value),
            "v1" if digest.is_none() && !value.is_empty() => digest = Some(value),
            _ => {}
        }
    }
    let (Some(timestamp), Some(digest)) = (timestamp, digest) else {
        return Err(WebhookError::MalformedSignature);
    };
    let timestamp: i64 = timestamp
        .parse()
        .map_err(|_| WebhookError::MalformedSignature)?;

    // Two-sided, against `t` and never `created_at`, which is stamped at enqueue time.
    let now_ms = now
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis()) as i128;
    if (now_ms - i128::from(timestamp) * 1000).abs() > tolerance.as_millis() as i128 {
        return Err(WebhookError::StaleTimestamp);
    }

    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|_| WebhookError::InvalidSignature)?;
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(raw_body);
    let mut expected = String::with_capacity(64);
    for byte in mac.finalize().into_bytes() {
        let _ = write!(expected, "{byte:02x}");
    }

    // Compare hex strings: decoding first would turn odd or invalid hex into a length mismatch.
    if !bool::from(
        digest
            .to_ascii_lowercase()
            .as_bytes()
            .ct_eq(expected.as_bytes()),
    ) {
        return Err(WebhookError::InvalidSignature);
    }

    let envelope: Envelope =
        serde_json::from_slice(raw_body).map_err(|_| WebhookError::MalformedBody)?;
    let data = typed(&envelope.event, envelope.data);
    Ok(WebhookEvent {
        id: envelope.id,
        event: envelope.event,
        created_at: envelope.created_at,
        listing: envelope.listing,
        data,
    })
}

fn typed(event: &str, data: Value) -> EventData {
    fn parse<T: DeserializeOwned>(data: &Value, wrap: fn(T) -> EventData) -> Option<EventData> {
        T::deserialize(data).ok().map(wrap)
    }
    let known = match event {
        "listing.bumped" => parse(&data, EventData::ListingBumped),
        "listing.upvoted" => parse(&data, EventData::ListingUpvoted),
        "server.joined" => parse(&data, EventData::ServerJoined),
        "bot.added" => parse(&data, EventData::BotAdded),
        "bot.stats_reported" => parse(&data, EventData::BotStatsReported),
        "webhook.ping" => parse(&data, EventData::WebhookPing),
        _ => None,
    };
    known.unwrap_or(EventData::Unknown(data))
}
