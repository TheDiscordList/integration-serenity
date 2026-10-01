//! The public surface, against a local mock of TheDiscordList.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use support::{MockServer, Reply};
use thediscordlist_serenity::{Autoposter, Event, PostResult, Publisher, Skip};

#[tokio::test]
async fn a_missing_token_gives_an_inert_autoposter() {
    let server = MockServer::ok().await;

    for token in ["", "   "] {
        let poster = Autoposter::builder(token)
            .base_url(&server.url)
            .bot_id(1u64)
            .build();

        let no_token = PostResult {
            ok: false,
            status: None,
            skipped: Some(Skip::NoToken),
        };
        assert_eq!(poster.post_stats().await, no_token);
        assert_eq!(poster.post_heartbeat().await, no_token);
        assert_eq!(poster.post_commands().await, no_token);
        poster.stop();
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn stats_can_be_posted_before_ready_with_a_bot_id_and_a_count() {
    let server = MockServer::ok().await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    let poster = Autoposter::builder("tdl_token")
        .base_url(format!("{}/", server.url))
        .bot_id(161_660_517_914_509_312u64)
        .server_count(|| async { Some(42) })
        .on_event(move |event| sink.lock().unwrap().push(event.clone()))
        .build();

    let result = poster.post_stats().await;

    assert_eq!(
        result,
        PostResult {
            ok: true,
            status: Some(200),
            skipped: None
        }
    );
    let post = &server.posts()[0];
    assert_eq!(post.path, "/bots/161660517914509312/stats");
    assert_eq!(post.header("authorization"), Some("Bearer tdl_token"));
    assert_eq!(post.header("content-type"), Some("application/json"));
    assert!(post
        .header("user-agent")
        .unwrap()
        .starts_with("TheDiscordList-serenity/"));
    // No READY yet, so the shard count is unknown and the key is omitted, not null.
    assert_eq!(post.body_text(), r#"{"server_count":42}"#);
    assert_eq!(
        *events.lock().unwrap(),
        [Event::Posted {
            publisher: Publisher::Stats,
            status: 200
        }]
    );
}

#[tokio::test]
async fn heartbeats_and_commands_wait_for_ready() {
    let server = MockServer::ok().await;
    let poster = Autoposter::builder("tdl_token")
        .base_url(&server.url)
        .bot_id(1u64)
        .build();

    assert_eq!(poster.post_heartbeat().await.skipped, Some(Skip::NoShards));
    assert_eq!(poster.post_commands().await.skipped, Some(Skip::NotReady));
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn a_publisher_turned_off_in_the_builder_stays_off() {
    let server = MockServer::ok().await;
    let poster = Autoposter::builder("tdl_token")
        .base_url(&server.url)
        .bot_id(1u64)
        .stats(false)
        .build();

    assert_eq!(poster.post_stats().await.skipped, Some(Skip::Disabled));
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn a_server_listing_id_disables_stats_and_says_why() {
    let server = MockServer::start(|_| Reply::new(404, r#"{"message":"Not found."}"#)).await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    let poster = Autoposter::builder("tdl_token")
        .base_url(&server.url)
        .bot_id(1u64)
        .server_count(|| async { Some(1) })
        .on_event(move |event| sink.lock().unwrap().push(event.clone()))
        .build();

    assert_eq!(poster.post_stats().await.status, Some(404));

    let events = events.lock().unwrap();
    assert_eq!(
        events[..],
        [Event::Disabled {
            publisher: Publisher::Stats,
            status: Some(404),
            reason: "not-found"
        }]
    );
    let message = events[0].to_string();
    assert!(
        message.contains("server listing") && !message.contains("token"),
        "{message}"
    );
}
