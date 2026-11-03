//! The public surface, against a local mock of TheDiscordList.

mod support;

use std::time::Duration;

use support::MockServer;
use thediscordlist_serenity::{Autoposter, PostResult, Skip};

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
async fn stats_wait_for_ready_even_with_a_bot_id_and_a_count() {
    let server = MockServer::ok().await;
    let poster = Autoposter::builder("tdl_token")
        .base_url(&server.url)
        .bot_id(161_660_517_914_509_312u64)
        .server_count(|| async { Some(42) })
        .build();

    // No READY yet, so the shard count is unknown, and the server would null the stored one.
    assert_eq!(poster.post_stats().await.skipped, Some(Skip::NotReady));
    assert!(server.requests().is_empty());
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
