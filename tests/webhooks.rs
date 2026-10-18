use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use thediscordlist_serenity::webhooks::{
    verify, verify_at, EventData, ListingKind, WebhookError, DEFAULT_TOLERANCE,
};

// The shared vector every SDK asserts, computed with the server's PHP formula.
const SECRET: &str = "whsec_test";
const T: u64 = 1_700_000_000;
const BODY: &str = r#"{"id":"d1","event":"webhook.ping","created_at":"2023-11-14T22:13:20+00:00","listing":{"id":"1","type":"bot","name":"Bot","url":"https://thediscordlist.com/bots/1"},"data":{"message":"hi"}}"#;
const V1: &str = "b6eeea31d04c3f86990dcbb706dfaf988c9ff2fc234316ae77e168831127dc59";

fn at(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

fn sign(secret: &str, t: u64, body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{t}.{body}").as_bytes());
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn check(
    body: &str,
    header: &str,
) -> Result<thediscordlist_serenity::webhooks::WebhookEvent, WebhookError> {
    verify_at(
        body.as_bytes(),
        Some(header),
        SECRET,
        DEFAULT_TOLERANCE,
        at(T),
    )
}

#[test]
fn the_shared_vector_matches_an_independent_hmac() {
    assert_eq!(sign(SECRET, T, BODY), V1);
}

#[test]
fn the_shared_vector_verifies_and_parses() {
    let event = check(BODY, &format!("t={T},v1={V1}")).unwrap();

    assert_eq!(event.id, "d1");
    assert_eq!(event.event, "webhook.ping");
    assert_eq!(event.created_at, "2023-11-14T22:13:20+00:00");
    assert_eq!(event.listing.id, "1");
    assert_eq!(event.listing.kind, ListingKind::Bot);
    assert_eq!(event.listing.url, "https://thediscordlist.com/bots/1");
    assert!(matches!(&event.data, EventData::WebhookPing(ping) if ping.message == "hi"));
}

#[test]
fn a_tampered_body_is_rejected() {
    let tampered = BODY.replace("\"hi\"", "\"hello\"");
    assert_eq!(
        check(&tampered, &format!("t={T},v1={V1}")).unwrap_err(),
        WebhookError::InvalidSignature
    );
}

#[test]
fn the_wrong_secret_is_rejected() {
    let header = format!("t={T},v1={}", sign("whsec_other", T, BODY));
    assert_eq!(
        check(BODY, &header).unwrap_err(),
        WebhookError::InvalidSignature
    );
}

#[test]
fn an_empty_secret_is_a_configuration_error() {
    // A delivery forged with the empty key must not verify against an unset secret.
    let forged = format!("t={T},v1={}", sign("", T, BODY));
    for secret in ["", "   "] {
        let result = verify_at(
            BODY.as_bytes(),
            Some(&forged),
            secret,
            DEFAULT_TOLERANCE,
            at(T),
        );
        assert_eq!(result.unwrap_err(), WebhookError::MissingSecret);
    }
}

#[test]
fn stale_timestamps_are_rejected_in_both_directions() {
    let header = format!("t={T},v1={V1}");
    for now in [T + 301, T - 301] {
        let result = verify_at(
            BODY.as_bytes(),
            Some(&header),
            SECRET,
            DEFAULT_TOLERANCE,
            at(now),
        );
        assert_eq!(
            result.unwrap_err(),
            WebhookError::StaleTimestamp,
            "now = {now}"
        );
    }
    for now in [T + 300, T - 300] {
        let result = verify_at(
            BODY.as_bytes(),
            Some(&header),
            SECRET,
            DEFAULT_TOLERANCE,
            at(now),
        );
        assert!(result.is_ok(), "now = {now} is inside the window");
    }
}

#[test]
fn verify_uses_the_real_clock() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let fresh = format!("t={now},v1={}", sign(SECRET, now, BODY));
    assert!(verify(BODY.as_bytes(), Some(&fresh), SECRET).is_ok());

    let vector = format!("t={T},v1={V1}");
    assert_eq!(
        verify(BODY.as_bytes(), Some(&vector), SECRET).unwrap_err(),
        WebhookError::StaleTimestamp
    );
}

#[test]
fn a_missing_header_is_reported_as_such() {
    for header in [None, Some(""), Some("  ")] {
        let result = verify_at(BODY.as_bytes(), header, SECRET, DEFAULT_TOLERANCE, at(T));
        assert_eq!(result.unwrap_err(), WebhookError::MissingSignature);
    }
}

#[test]
fn malformed_headers_are_reported_as_such() {
    for header in [
        "garbage".to_owned(),
        format!("v1={V1}"),
        format!("t={T}"),
        format!("t={T},v1="),
        format!("t=abc,v1={V1}"),
        format!("t=NaN,v1={V1}"),
        format!("t=1.7e9,v1={V1}"),
        format!("t=,v1={V1}"),
    ] {
        assert_eq!(
            check(BODY, &header).unwrap_err(),
            WebhookError::MalformedSignature,
            "{header}"
        );
    }
}

#[test]
fn header_parts_split_on_the_first_equals_sign_only() {
    // Unknown parts whose values contain `=` are skipped, not misparsed.
    let header = format!("t={T},v0=abc==,x=a=b=c,v1={V1}");
    assert!(check(BODY, &header).is_ok());

    // A `v1` whose value carries an extra `=` part is that whole value, so it cannot match.
    let header = format!("t={T},v1={V1}=extra");
    assert_eq!(
        check(BODY, &header).unwrap_err(),
        WebhookError::InvalidSignature
    );
}

#[test]
fn the_first_t_and_first_non_empty_v1_win() {
    let header = format!("t={T},t=1,v1=,v1={V1},v1=deadbeef");
    assert!(check(BODY, &header).is_ok());
}

#[test]
fn an_uppercase_hex_signature_verifies() {
    let header = format!("t={T},v1={}", V1.to_ascii_uppercase());
    assert!(check(BODY, &header).is_ok());
}

#[test]
fn whitespace_around_parts_is_tolerated() {
    let header = format!(" t = {T} , v1 = {V1} ");
    assert!(check(BODY, &header).is_ok());
}

#[test]
fn a_signed_body_that_is_not_json_is_malformed() {
    let body = "not json";
    let header = format!("t={T},v1={}", sign(SECRET, T, body));
    assert_eq!(
        check(body, &header).unwrap_err(),
        WebhookError::MalformedBody
    );
}

#[test]
fn unknown_events_parse_for_forward_compatibility() {
    let body = r#"{"id":"d2","event":"listing.reviewed","created_at":"2026-01-01T00:00:00+00:00","listing":{"id":"9","type":"server","name":"S","url":"https://thediscordlist.com/servers/9"},"data":{"stars":5}}"#;
    let header = format!("t={T},v1={}", sign(SECRET, T, body));

    let event = check(body, &header).unwrap();

    assert_eq!(event.event, "listing.reviewed");
    assert_eq!(event.listing.kind, ListingKind::Server);
    assert!(matches!(&event.data, EventData::Unknown(data) if data["stars"] == 5));
}

#[test]
fn known_events_are_typed() {
    let body = r#"{"id":"d3","event":"listing.upvoted","created_at":"2026-01-01T00:00:00+00:00","listing":{"id":"1","type":"bot","name":"Bot","url":"https://thediscordlist.com/bots/1"},"data":{"voted_at":"2026-01-01T00:00:00+00:00","total_upvotes":42,"user":{"id":null,"username":"sam","avatar":null}}}"#;
    let header = format!("t={T},v1={}", sign(SECRET, T, body));

    let EventData::ListingUpvoted(vote) = check(body, &header).unwrap().data else {
        panic!("expected a typed upvote");
    };

    assert_eq!(vote.total_upvotes, 42);
    let user = vote.user.unwrap();
    assert_eq!(user.username, "sam");
    assert_eq!(user.id, None);
}

#[test]
fn bump_ready_is_typed() {
    let body = r#"{"id":"d4","event":"listing.bump_ready","created_at":"2026-01-01T00:00:00+00:00","listing":{"id":"9","type":"server","name":"S","url":"https://thediscordlist.com/servers/9"},"data":{"ready_at":"2026-01-01T00:00:00+00:00"}}"#;
    let header = format!("t={T},v1={}", sign(SECRET, T, body));

    let EventData::ListingBumpReady(ready) = check(body, &header).unwrap().data else {
        panic!("expected a typed bump_ready");
    };

    assert_eq!(ready.ready_at, "2026-01-01T00:00:00+00:00");
}

#[test]
fn error_codes_are_machine_readable() {
    assert_eq!(WebhookError::MissingSecret.code(), "MISSING_SECRET");
    assert_eq!(WebhookError::MissingSignature.code(), "MISSING_SIGNATURE");
    assert_eq!(
        WebhookError::MalformedSignature.code(),
        "MALFORMED_SIGNATURE"
    );
    assert_eq!(WebhookError::StaleTimestamp.code(), "STALE_TIMESTAMP");
    assert_eq!(WebhookError::InvalidSignature.code(), "INVALID_SIGNATURE");
    assert_eq!(WebhookError::MalformedBody.code(), "MALFORMED_BODY");
}
