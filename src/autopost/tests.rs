use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::SystemTime;

use serde_json::json;
use serenity::all::{ApplicationId, HttpBuilder};

use super::*;

#[path = "../../tests/support/mod.rs"]
mod support;

use support::{MockServer, Reply};

const APP_ID: u64 = 161_660_517_914_509_312;

const COMMANDS_JSON: &str = r#"[
  {"id":"1","application_id":"161660517914509312","version":"1","type":1,"name":"config",
   "description":"Configure the bot","default_member_permissions":"32","dm_permission":true,
   "nsfw":false,"integration_types":[0],"contexts":null,
   "options":[{"type":2,"name":"logging","description":"Logging","options":[
     {"type":1,"name":"set","description":"Set the channel","options":[
       {"type":3,"name":"level","description":"Level","required":true,
        "choices":[{"name":"High","value":"high"},{"name":"Low","value":"low"}]},
       {"type":4,"name":"days","description":"Days","choices":[{"name":"Week","value":7}]}
     ]}
   ]}]},
  {"id":"2","application_id":"161660517914509312","version":"1","type":3,"name":"Report message",
   "description":"","default_member_permissions":null,"nsfw":false}
]"#;

type Events = Arc<Mutex<Vec<Event>>>;

fn builder(server: &MockServer, events: &Events) -> AutoposterBuilder {
    let events = Arc::clone(events);
    let mut builder = Autoposter::builder("tdl_test_token")
        .base_url(&server.url)
        .on_event(move |event| events.lock().unwrap().push(event.clone()));
    builder.config.retry_base = Duration::from_millis(5);
    builder
}

fn handles(server: &MockServer) -> Handles {
    Handles {
        cache: Arc::new(Cache::new()),
        http: Arc::new(
            HttpBuilder::new("discord-token")
                .proxy(&server.url)
                .ratelimiter_disabled(true)
                .application_id(ApplicationId::new(APP_ID))
                .build(),
        ),
    }
}

/// READY for one shard, without starting the background loops.
fn ready(poster: &Autoposter, server: &MockServer, shard_id: u32, total_shards: u32) {
    poster
        .inner
        .record_ready(handles(server), APP_ID, shard_id, total_shards);
}

fn taken(events: &Events) -> Vec<Event> {
    std::mem::take(&mut *events.lock().unwrap())
}

fn throttled() -> PostResult {
    PostResult {
        ok: false,
        status: Some(429),
        skipped: Some(Skip::Throttled),
    }
}

#[tokio::test]
async fn stats_request_carries_headers_and_body() {
    let server = MockServer::ok().await;
    let events = Events::default();
    let poster = builder(&server, &events).build();
    ready(&poster, &server, 0, 1);

    let result = poster.post_stats().await;

    assert_eq!(
        result,
        PostResult {
            ok: true,
            status: Some(200),
            skipped: None
        }
    );
    let posts = server.posts();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].path, format!("/bots/{APP_ID}/stats"));
    assert_eq!(
        posts[0].header("authorization"),
        Some("Bearer tdl_test_token")
    );
    assert_eq!(posts[0].header("content-type"), Some("application/json"));
    assert_eq!(posts[0].header("user-agent"), Some(crate::http::USER_AGENT));
    assert_eq!(
        posts[0].body_text(),
        r#"{"server_count":0,"shard_count":1}"#
    );
    assert_eq!(
        taken(&events),
        [Event::Posted {
            publisher: Publisher::Stats,
            status: 200
        }]
    );
}

#[tokio::test]
async fn a_bearer_prefix_on_the_token_is_not_doubled() {
    let server = MockServer::ok().await;
    let poster = Autoposter::builder(" Bearer abc ")
        .base_url(&server.url)
        .build();
    ready(&poster, &server, 0, 1);

    poster.post_stats().await;

    assert_eq!(
        server.posts()[0].header("authorization"),
        Some("Bearer abc")
    );
}

#[tokio::test]
async fn heartbeat_reports_every_local_shard_and_omits_unknown_latency() {
    let server = MockServer::ok().await;
    let poster = builder(&server, &Events::default()).build();
    ready(&poster, &server, 0, 2);
    poster.inner.on_stage(1, ConnectionStage::Identifying);

    assert!(poster.post_heartbeat().await.ok);

    let post = &server.posts()[0];
    assert_eq!(post.path, format!("/bots/{APP_ID}/heartbeat"));
    assert_eq!(
        post.json(),
        json!({
            "cluster": 0,
            "cluster_count": 1,
            "shards": [
                {"id": 0, "status": "ready", "guilds": 0},
                {"id": 1, "status": "connecting", "guilds": 0}
            ]
        })
    );
}

#[tokio::test]
async fn heartbeat_cadence_follows_the_server_grace_window() {
    for (seconds, expected) in [(90, 30), (600, 60), (3, 15)] {
        let server = MockServer::start(move |_| {
            Reply::new(200, format!(r#"{{"next_beat_within_seconds":{seconds}}}"#))
        })
        .await;
        let poster = builder(&server, &Events::default()).build();
        ready(&poster, &server, 0, 1);

        poster.post_heartbeat().await;

        assert_eq!(
            poster.inner.state().heartbeat_interval,
            Duration::from_secs(expected)
        );
    }
}

#[tokio::test]
async fn a_cluster_override_is_reported_verbatim() {
    let server = MockServer::ok().await;
    let poster = builder(&server, &Events::default()).cluster(2, 4).build();
    ready(&poster, &server, 8, 16);

    assert!(poster.post_heartbeat().await.ok);

    let body = server.posts()[0].json();
    assert_eq!(body["cluster"], 2);
    assert_eq!(body["cluster_count"], 4);
    assert_eq!(
        body["shards"],
        json!([{"id": 8, "status": "ready", "guilds": 0}])
    );
}

#[tokio::test]
async fn an_invalid_cluster_override_is_ignored() {
    for (id, count) in [(4, 4), (0, 0), (0, 1025)] {
        let poster = Autoposter::builder("token").cluster(id, count).build();
        assert_eq!(poster.inner.config.cluster, None);
    }
}

#[tokio::test]
async fn an_unknown_cluster_count_sends_no_heartbeat() {
    let server = MockServer::ok().await;
    let events = Events::default();
    let poster = builder(&server, &events).build();
    // Shard 1 of 2 runs in some other process this one cannot see.
    ready(&poster, &server, 0, 2);

    assert_eq!(
        poster.post_heartbeat().await,
        PostResult::skip(Skip::ClusterCountUnknown)
    );
    assert_eq!(
        poster.post_stats().await,
        PostResult::skip(Skip::Unmeasurable)
    );
    assert!(taken(&events).is_empty());

    // Once every shard has had time to boot, the loop disables itself.
    poster.inner.state().ready_at = Instant::now().checked_sub(Duration::from_secs(120));
    assert_eq!(poster.inner.heartbeat_tick().await, None);

    assert!(server.requests().is_empty());
    assert_eq!(
        taken(&events),
        [Event::Disabled {
            publisher: Publisher::Heartbeat,
            status: None,
            reason: "cluster-count-unknown"
        }]
    );
}

#[tokio::test]
async fn unauthorized_disables_every_publisher() {
    let server = MockServer::start(|_| Reply::new(401, r#"{"message":"Unauthenticated."}"#)).await;
    let events = Events::default();
    let poster = builder(&server, &events).build();
    ready(&poster, &server, 0, 1);

    assert_eq!(poster.post_stats().await, PostResult::failed(Some(401)));

    for publisher in [Publisher::Stats, Publisher::Heartbeat, Publisher::Commands] {
        assert!(poster.inner.is_disabled(publisher), "{publisher}");
    }
    let disabled = PostResult::skip(Skip::Disabled);
    assert_eq!(poster.inner.publish_stats(false).await, disabled);
    assert_eq!(poster.inner.beat(false).await, disabled);
    assert_eq!(poster.inner.publish_commands(false).await, disabled);
    assert_eq!(server.requests().len(), 1);
    assert_eq!(
        taken(&events),
        [Event::Disabled {
            publisher: Publisher::Stats,
            status: Some(401),
            reason: "unauthorized"
        }]
    );

    // A manual post re-arms its own publisher.
    assert_eq!(poster.post_heartbeat().await.status, Some(401));
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn plan_403_suspends_heartbeats_without_disabling_them() {
    let server = MockServer::start(|_| Reply::new(403, "{}")).await;
    let events = Events::default();
    let poster = builder(&server, &events).build();
    ready(&poster, &server, 0, 1);

    assert_eq!(
        poster.post_heartbeat().await,
        PostResult {
            ok: false,
            status: Some(403),
            skipped: Some(Skip::Plan)
        }
    );
    assert!(!poster.inner.is_disabled(Publisher::Heartbeat));
    let until = poster.inner.state().gates[Publisher::Heartbeat as usize]
        .suspended_until
        .unwrap();
    assert!(until >= Instant::now() + Duration::from_secs(29 * 60));

    let next = poster.inner.heartbeat_tick().await.unwrap();
    assert!(next >= until, "the next beat must wait out the suspension");

    let events = taken(&events);
    assert_eq!(events.len(), 1, "a plan error is reported once: {events:?}");
    assert!(matches!(
        events[0],
        Event::Error {
            publisher: Publisher::Heartbeat,
            status: Some(403),
            retryable: false,
            ..
        }
    ));
}

#[tokio::test]
async fn not_found_disables_only_that_publisher() {
    let server = MockServer::start(|request| {
        Reply::new(
            if request.path.ends_with("/stats") {
                404
            } else {
                200
            },
            "{}",
        )
    })
    .await;
    let events = Events::default();
    let poster = builder(&server, &events).build();
    ready(&poster, &server, 0, 1);

    assert_eq!(poster.post_stats().await, PostResult::failed(Some(404)));

    assert!(poster.inner.is_disabled(Publisher::Stats));
    assert!(!poster.inner.is_disabled(Publisher::Heartbeat));
    assert!(poster.post_heartbeat().await.ok);
    assert_eq!(
        taken(&events)[0],
        Event::Disabled {
            publisher: Publisher::Stats,
            status: Some(404),
            reason: "not-found"
        }
    );
}

#[tokio::test]
async fn conflict_disables_the_commands_publisher() {
    let server = MockServer::start(|request| match request.method.as_str() {
        "GET" => Reply::new(200, COMMANDS_JSON),
        _ => Reply::new(409, "{}"),
    })
    .await;
    let events = Events::default();
    let poster = builder(&server, &events).build();
    ready(&poster, &server, 0, 1);

    assert_eq!(poster.post_commands().await, PostResult::failed(Some(409)));

    assert!(poster.inner.is_disabled(Publisher::Commands));
    assert_eq!(
        taken(&events),
        [Event::Disabled {
            publisher: Publisher::Commands,
            status: Some(409),
            reason: "conflict"
        }]
    );
}

#[tokio::test]
async fn throttling_suspends_stats_and_commands_but_not_heartbeats() {
    let server = MockServer::start(|request| {
        if request.path.ends_with("/stats") {
            Reply::new(429, "{}").header("retry-after", "120")
        } else if request.method == "GET" {
            Reply::new(200, COMMANDS_JSON)
        } else {
            Reply::new(200, "{}")
        }
    })
    .await;
    let poster = builder(&server, &Events::default()).build();
    ready(&poster, &server, 0, 1);

    assert_eq!(poster.post_stats().await, throttled());

    let until = poster
        .inner
        .api
        .as_ref()
        .unwrap()
        .suspended_until()
        .unwrap();
    assert!(until >= Instant::now() + Duration::from_secs(119));
    assert!(until <= Instant::now() + Duration::from_secs(123));
    assert_eq!(
        poster.post_commands().await,
        PostResult::skip(Skip::Suspended)
    );
    assert!(poster.post_heartbeat().await.ok);

    let paths: Vec<String> = server.posts().into_iter().map(|post| post.path).collect();
    assert_eq!(
        paths,
        [
            format!("/bots/{APP_ID}/stats"),
            format!("/bots/{APP_ID}/heartbeat")
        ]
    );
}

#[tokio::test]
async fn throttling_honours_the_rate_limit_reset_header() {
    let reset = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 600;
    let server = MockServer::start(move |_| {
        Reply::new(429, "{}")
            .header("retry-after", "1")
            .header("x-ratelimit-reset", reset.to_string())
    })
    .await;
    let poster = builder(&server, &Events::default()).build();
    ready(&poster, &server, 0, 1);

    assert_eq!(poster.post_stats().await, throttled());

    let until = poster
        .inner
        .api
        .as_ref()
        .unwrap()
        .suspended_until()
        .unwrap();
    assert!(until >= Instant::now() + Duration::from_secs(598));
}

#[tokio::test]
async fn server_errors_are_retried() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let server = MockServer::start(move |_| {
        if counter.fetch_add(1, Ordering::SeqCst) < 2 {
            Reply::new(503, "")
        } else {
            Reply::new(200, "{}")
        }
    })
    .await;
    let poster = builder(&server, &Events::default()).build();
    ready(&poster, &server, 0, 1);

    assert!(poster.post_stats().await.ok);
    assert_eq!(server.posts().len(), 3);
}

#[tokio::test]
async fn persistent_server_errors_stop_after_three_attempts() {
    let server = MockServer::start(|_| Reply::new(500, "")).await;
    let events = Events::default();
    let poster = builder(&server, &events).build();
    ready(&poster, &server, 0, 1);

    assert_eq!(poster.post_heartbeat().await, PostResult::failed(Some(500)));

    assert_eq!(server.posts().len(), 3);
    assert!(!poster.inner.is_disabled(Publisher::Heartbeat));
    assert!(matches!(
        taken(&events)[..],
        [Event::Error {
            publisher: Publisher::Heartbeat,
            status: Some(500),
            retryable: true,
            ..
        }]
    ));
}

#[tokio::test]
async fn payload_errors_are_reported_once_and_keep_the_publisher() {
    let server = MockServer::start(|_| Reply::new(422, r#"{"message":"invalid"}"#)).await;
    let events = Events::default();
    let poster = builder(&server, &events).build();
    ready(&poster, &server, 0, 1);

    assert_eq!(poster.post_heartbeat().await, PostResult::failed(Some(422)));
    assert_eq!(poster.post_heartbeat().await, PostResult::failed(Some(422)));

    assert_eq!(server.posts().len(), 2);
    assert!(!poster.inner.is_disabled(Publisher::Heartbeat));
    assert_eq!(taken(&events).len(), 1);
}

#[tokio::test]
async fn commands_are_forwarded_with_only_the_fields_the_api_accepts() {
    let server = MockServer::start(|request| match request.method.as_str() {
        "GET" => Reply::new(200, COMMANDS_JSON),
        _ => Reply::new(200, "{}"),
    })
    .await;
    let poster = builder(&server, &Events::default()).build();
    ready(&poster, &server, 0, 1);

    assert!(poster.post_commands().await.ok);

    let requests = server.requests();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(
        requests[0].path,
        format!("/api/v10/applications/{APP_ID}/commands")
    );
    let post = &requests[1];
    assert_eq!(post.path, format!("/bots/{APP_ID}/commands"));
    assert!(!post.body_text().contains("null"), "{}", post.body_text());
    assert_eq!(
        post.json(),
        json!({"commands": [
            {
                "type": 1, "name": "config", "description": "Configure the bot", "nsfw": false,
                "default_member_permissions": "32",
                "options": [{"type": 2, "name": "logging", "description": "Logging", "options": [
                    {"type": 1, "name": "set", "description": "Set the channel", "options": [
                        {"type": 3, "name": "level", "description": "Level", "required": true,
                         "choices": [{"name": "High", "value": "high"}, {"name": "Low", "value": "low"}]},
                        {"type": 4, "name": "days", "description": "Days",
                         "choices": [{"name": "Week", "value": 7}]}
                    ]}
                ]}]
            },
            {"type": 3, "name": "Report message", "nsfw": false}
        ]})
    );
}

#[tokio::test]
async fn an_empty_command_list_is_only_posted_on_request() {
    let server = MockServer::start(|request| match request.method.as_str() {
        "GET" => Reply::new(200, "[]"),
        _ => Reply::new(200, "{}"),
    })
    .await;

    let poster = builder(&server, &Events::default()).build();
    ready(&poster, &server, 0, 1);
    assert_eq!(poster.post_commands().await, PostResult::skip(Skip::Empty));
    assert!(server.posts().is_empty());

    let poster = builder(&server, &Events::default())
        .post_empty_commands(true)
        .build();
    ready(&poster, &server, 0, 1);
    assert!(poster.post_commands().await.ok);
    assert_eq!(server.posts()[0].body_text(), r#"{"commands":[]}"#);
}

#[tokio::test]
async fn stats_respect_the_minimum_gap_and_skip_unchanged_counts() {
    let server = MockServer::ok().await;
    let poster = builder(&server, &Events::default()).build();
    ready(&poster, &server, 0, 1);
    assert!(poster.post_stats().await.ok);
    assert_eq!(poster.post_stats().await, PostResult::skip(Skip::MinGap));

    let count = Arc::new(AtomicU64::new(7));
    let source = Arc::clone(&count);
    let mut gapless = builder(&server, &Events::default())
        .server_count(move || std::future::ready(Some(source.load(Ordering::SeqCst))));
    gapless.config.stats_min_gap = Duration::ZERO;
    let poster = gapless.build();
    ready(&poster, &server, 0, 1);

    assert!(poster.post_stats().await.ok);
    assert_eq!(poster.post_stats().await, PostResult::skip(Skip::Unchanged));
    count.store(8, Ordering::SeqCst);
    assert!(poster.post_stats().await.ok);
    assert_eq!(server.posts().len(), 3);
}

#[tokio::test]
async fn only_cluster_zero_publishes_stats() {
    let server = MockServer::ok().await;

    let other = builder(&server, &Events::default())
        .cluster(1, 2)
        .server_count(|| async { Some(10) })
        .build();
    ready(&other, &server, 2, 4);
    assert_eq!(other.post_stats().await, PostResult::skip(Skip::NotElected));

    let unsummed = builder(&server, &Events::default()).cluster(0, 2).build();
    ready(&unsummed, &server, 0, 4);
    assert_eq!(
        unsummed.post_stats().await,
        PostResult::skip(Skip::Unmeasurable)
    );

    let summed = builder(&server, &Events::default())
        .cluster(0, 2)
        .server_count(|| async { Some(1234) })
        .build();
    ready(&summed, &server, 0, 4);
    assert!(summed.post_stats().await.ok);

    let posts = server.posts();
    assert_eq!(posts.len(), 1);
    assert_eq!(
        posts[0].body_text(),
        r#"{"server_count":1234,"shard_count":4}"#
    );
}

#[tokio::test]
async fn ready_starts_all_three_publishers_and_stop_ends_them() {
    let server = MockServer::start(|request| match request.method.as_str() {
        "GET" => Reply::new(200, COMMANDS_JSON),
        _ => Reply::new(200, "{}"),
    })
    .await;
    let mut builder = builder(&server, &Events::default());
    builder.config.stats_first_delay = Duration::from_millis(20);
    builder.config.commands_delay = Duration::from_millis(40);
    let poster = builder.build();

    poster.inner.on_ready(handles(&server), APP_ID, 0, 1);

    let posts = server.wait_for_posts(3, Duration::from_secs(5)).await;
    let mut kinds: Vec<&str> = posts
        .iter()
        .map(|post| post.path.rsplit('/').next().unwrap())
        .collect();
    kinds.sort_unstable();
    assert_eq!(kinds, ["commands", "heartbeat", "stats"]);

    poster.stop();
    assert!(lock(&poster.inner.tasks).is_empty());
    // A second READY after stop() starts nothing.
    poster.inner.on_ready(handles(&server), APP_ID, 0, 1);
    assert!(lock(&poster.inner.tasks).is_empty());
}

#[tokio::test]
async fn dropping_the_last_handle_ends_the_background_tasks() {
    let server = MockServer::ok().await;
    let poster = builder(&server, &Events::default()).build();
    poster.inner.on_ready(handles(&server), APP_ID, 0, 1);
    server.wait_for_posts(1, Duration::from_secs(5)).await;

    let weak = Arc::downgrade(&poster.inner);
    drop(poster);

    let deadline = Instant::now() + Duration::from_secs(5);
    while weak.upgrade().is_some() {
        assert!(
            Instant::now() < deadline,
            "the tasks kept the autoposter alive"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn guild_changes_debounce_into_one_stats_post() {
    let server = MockServer::ok().await;
    let count = Arc::new(AtomicU64::new(1));
    let source = Arc::clone(&count);
    let mut builder = builder(&server, &Events::default())
        .heartbeat(false)
        .commands(false)
        .server_count(move || std::future::ready(Some(source.load(Ordering::SeqCst))));
    builder.config.stats_first_delay = Duration::from_millis(10);
    builder.config.stats_debounce = Duration::from_millis(30);
    builder.config.stats_min_gap = Duration::from_millis(200);
    let poster = builder.build();
    poster.inner.on_ready(handles(&server), APP_ID, 0, 1);
    server.wait_for_posts(1, Duration::from_secs(5)).await;

    count.store(4, Ordering::SeqCst);
    for _ in 0..3 {
        poster.inner.stats_wake.notify_one();
    }

    let posts = server.wait_for_posts(2, Duration::from_secs(5)).await;
    assert_eq!(posts[1].json()["server_count"], 4);
    sleep(Duration::from_millis(300)).await;
    assert_eq!(server.posts().len(), 2);
}

#[test]
fn connection_stages_map_to_api_statuses() {
    let cases = [
        (ConnectionStage::Connected, "ready"),
        (ConnectionStage::Connecting, "connecting"),
        (ConnectionStage::Handshake, "connecting"),
        (ConnectionStage::Identifying, "connecting"),
        (ConnectionStage::Resuming, "resuming"),
        (ConnectionStage::Disconnected, "disconnected"),
    ];
    for (stage, status) in cases {
        assert_eq!(
            serde_json::to_value(ShardStatus::from(stage)).unwrap(),
            status
        );
    }
}
