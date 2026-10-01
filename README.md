# thediscordlist-serenity

Publish your [serenity](https://github.com/serenity-rs/serenity) or
[poise](https://github.com/serenity-rs/poise) bot's server count, shard
heartbeats and application commands to [TheDiscordList](https://thediscordlist.com)
automatically, and verify its inbound webhooks.

Adds nothing to a serenity bot's dependency tree: it reuses serenity's
`reqwest`, TLS stack and tokio runtime. The optional `webhooks` feature adds
`hmac` and `sha2`.

```sh
cargo add thediscordlist-serenity
```

## Quickstart

```rust
use serenity::all::{Client, GatewayIntents};
use thediscordlist_serenity::Autoposter;

#[tokio::main]
async fn main() {
    let poster = Autoposter::new(std::env::var("TDL_TOKEN").unwrap_or_default());

    let mut client = Client::builder(std::env::var("DISCORD_TOKEN").unwrap(), GatewayIntents::GUILDS)
        .event_handler(Handler) // your own handler, unchanged
        .event_handler(poster.clone())
        .await
        .unwrap();
    poster.attach_shard_manager(client.shard_manager.clone());

    client.start_autosharded().await.unwrap();
}
```

That is the whole integration, and it is also the recommended production
setup. serenity calls every registered event handler, so the autoposter sits
next to yours and starts on the first `READY`. It runs three publishers:

| Publisher | What it sends | Cadence |
| --- | --- | --- |
| Stats | `server_count`, `shard_count` | 5 s after `READY`, then every 5 min, debounced 20 s on guild join/leave |
| Heartbeat | per-shard status, latency and guild counts | right after `READY`, then every ~30 s, plus ~1 s after any shard stage change |
| Commands | your global application commands | once, ~10 s after `READY` |

`attach_shard_manager` is optional. With it, heartbeats read each shard's
stage and latency from serenity's shard runners; without it they track stages
from gateway events and leave `latency_ms` out.

Your **stats token** is on the listing's manage page, under the API tab. It is
scoped to one listing, so it is the only credential this crate needs. With a
missing or empty token, `Autoposter::new` logs one warning and returns an inert
autoposter: a forgotten environment variable never takes down a deploy.

### poise

poise runs on serenity, so the same handler works next to a poise framework:

```rust
use poise::serenity_prelude as serenity;
use thediscordlist_serenity::Autoposter;

let poster = Autoposter::new(std::env::var("TDL_TOKEN").unwrap_or_default());

let framework = poise::Framework::builder()
    .options(poise::FrameworkOptions { commands: vec![ping()], ..Default::default() })
    .setup(|ctx, _ready, framework| {
        Box::pin(async move {
            poise::builtins::register_globally(ctx, &framework.options().commands).await?;
            Ok(Data)
        })
    })
    .build();

let mut client = serenity::ClientBuilder::new(token, serenity::GatewayIntents::GUILDS)
    .framework(framework)
    .event_handler(poster.clone())
    .await?;
poster.attach_shard_manager(client.shard_manager.clone());
client.start().await?;
```

Commands registered in `setup` are on Discord before the commands publisher
reads them ten seconds later.

## Sharding

serenity has no multi-process sharding manager or IPC, so the topology comes
from what this process can see:

| Topology | `server_count` comes from | Stats and commands posted by | Heartbeats |
| --- | --- | --- | --- |
| One shard | the cache | this process | this process, as cluster 0 of 1 |
| Several shards, one process (`start_autosharded`, `start_shards`) | the cache | this process | this process, once every shard has connected |
| Shard ranges split across processes (`start_shard_range`) | your `server_count` | cluster 0, or the process running shard 0 | each process for itself, with `.cluster(id, count)` |

For a split deployment, name the cluster and supply the total:

```rust
let poster = Autoposter::builder(token)
    .cluster(cluster_id, cluster_count)
    .server_count(|| async { my_bus.sum_guild_counts().await })
    .build();
```

Every process heartbeats for **itself** on purpose. A manager reporting "all
fine" for a cluster whose host has died is exactly the failure the status page
exists to catch.

> **If the cluster count cannot be determined, heartbeats do not start.** The
> API hard-deletes every cluster row whose id is `>= cluster_count` in the
> request, so one process guessing `1` would erase all of its siblings and
> their monitoring history. Without `.cluster(..)`, a process counts as the
> whole bot only once it has seen every shard id below the shard total. If it
> still has not after the boot window (5 s per shard + 60 s), the heartbeat
> publisher disables itself with reason `cluster-count-unknown`, pointing at
> `.cluster(id, count)`. Losing a status page beats losing data.

## Options

```rust
use std::time::Duration;

let poster = Autoposter::builder(token)
    .bot_id(161660517914509312u64) // default: the application id from READY
    .base_url("https://thediscordlist.com/api")
    .stats(true)
    .stats_interval(Duration::from_secs(300))
    .stats_debounce(Duration::from_secs(20))
    .stats_min_gap(Duration::from_secs(60)) // also the minimum
    .heartbeat(true)
    .heartbeat_interval(Duration::from_secs(30)) // 15–60 s, retuned by the API
    .commands(true)
    .post_empty_commands(false)
    .cluster(0, 1)
    .server_count(|| async { Some(1234) })
    .on_event(|event| println!("{event}"))
    .build();
```

`stats_min_gap` is a hard floor between two stats POSTs. A join storm, a mass
reconnect and a manual `post_stats()` all route through it, so the ceiling is
structurally one request per minute.

`post_empty_commands` is off because posting an empty array **clears** the
command list on your listing. Turn it on only if that is what you mean.

| Cargo feature | Default | Adds |
| --- | --- | --- |
| `rustls-tls` | yes | the autoposter, on serenity's `rustls_backend` (serenity's default) |
| `native-tls` | no | the autoposter, on serenity's `native_tls_backend` |
| `webhooks` | no | `webhooks::verify` |

Pick the TLS feature that matches your serenity backend so both share one
HTTP and TLS build, e.g. `default-features = false, features = ["native-tls"]`.

## Events and manual posting

```rust
use thediscordlist_serenity::{Autoposter, Event};

let poster = Autoposter::builder(token)
    .on_event(|event| match event {
        Event::Posted { publisher, status } => {}
        Event::Error { publisher, status, retryable, message } => {}
        Event::Disabled { publisher, status, reason } => {}
        _ => {}
    })
    .build();

let result = poster.post_stats().await; // PostResult { ok, status, skipped }
poster.post_heartbeat().await;
poster.post_commands().await;

poster.stop();
```

Nothing in this crate panics or returns an `Err` at your bot. Background tasks
handle their own failures, and `post_*` always resolves to a `PostResult`
whose `skipped` says why nothing was sent (`MinGap`, `Unchanged`,
`Suspended`, `NotReady`, …). Without `on_event`, events go to `tracing`:
`debug` for posts, `warn` for errors and disabled publishers.

An explicit `post_*` also re-arms a publisher that disabled itself, so you can
retry after fixing a token or a listing id without restarting.

The background tasks end on `stop()`, or when the last `Autoposter` clone is
dropped (serenity holds the one you registered until the client is dropped).

## Webhooks

```sh
cargo add thediscordlist-serenity --features webhooks
# a webhook-only service, without serenity:
cargo add thediscordlist-serenity --no-default-features --features webhooks
```

### axum

```rust
use axum::{body::Bytes, http::{HeaderMap, StatusCode}, routing::post, Router};
use thediscordlist_serenity::webhooks::{self, EventData, SIGNATURE_HEADER};

async fn tdl_webhook(headers: HeaderMap, body: Bytes) -> StatusCode {
    let signature = headers.get(SIGNATURE_HEADER).and_then(|value| value.to_str().ok());
    let secret = std::env::var("TDL_WEBHOOK_SECRET").unwrap_or_default();

    let Ok(event) = webhooks::verify(&body, signature, &secret) else {
        return StatusCode::BAD_REQUEST;
    };
    if let EventData::ListingUpvoted(vote) = &event.data {
        println!("{} upvotes", vote.total_upvotes);
    }
    StatusCode::NO_CONTENT // ping and unknown events get a 2xx too
}

let app = Router::new().route("/webhooks/tdl", post(tdl_webhook));
```

Take the body as `Bytes` (or `String`), never as `Json<T>`: the extractor
consumes the bytes the signature covers.

### Verifying by hand

```rust
use thediscordlist_serenity::webhooks::{self, WebhookError};

match webhooks::verify(raw_body, signature_header, secret) {
    Ok(event) => { /* event.id, event.event, event.listing, event.data */ }
    Err(error) => { /* answer 400; error.code() is e.g. "INVALID_SIGNATURE" */ }
}
```

`raw_body` must be the exact bytes that arrived. Re-serialising a parsed body
re-escapes slashes and non-ASCII and can never verify: the signature covers
bytes, not meaning. The timestamp must be within 300 s of now in either
direction; `webhooks::verify_at` takes another tolerance and clock.

### Events

| `event.event` | `event.data` |
| --- | --- |
| `listing.bumped` | `EventData::ListingBumped` |
| `listing.upvoted` | `EventData::ListingUpvoted` |
| `server.joined` | `EventData::ServerJoined`, server listings only |
| `bot.added` | `EventData::BotAdded` |
| `bot.stats_reported` | `EventData::BotStatsReported` |
| `webhook.ping` | `EventData::WebhookPing` |
| anything newer | `EventData::Unknown(serde_json::Value)` |

Two of them are not what their names suggest. `bot.added` is a **click**
event, dispatched on the install hand-off; Discord never tells anyone whether
the install completed. And `bot.stats_reported` is not an echo of your own
POST: it also fires when the owner edits the listing in the web UI, and from
the heartbeat's internal stats forward.

### Rules for a healthy endpoint

- **Always answer 2xx** to a verified delivery, including `webhook.ping` and
  event names you do not recognise. The sender disables an endpoint after 10
  consecutive failed deliveries, about 50 requests over 11.5 hours.
- A disabled endpoint is probed with a `webhook.ping` every 6 hours for 7
  days, and **a 2xx to that probe re-enables it**.
- **Register an exact `https://` URL with no redirect.** The sender treats 3xx
  as a failure, so an `http → https` or trailing-slash redirect burns all five
  retries and counts toward the disable streak.
- **Deduplicate on `event.id`.** It equals `X-TDL-Delivery` and is stable
  across all five retries of one delivery. There is no in-process cache here
  on purpose: it would be wrong under the multi-process deployments this crate
  serves. A manual "redeliver" from the UI mints a new id deliberately.
- **Rotating the secret has no grace window.** Deliveries already queued under
  the old secret become unverifiable.
- **Answer fast.** Hand slow work to `tokio::spawn` and return the 2xx.

## Plans, limits and known behaviour

- **Heartbeats need Gold or higher.** On a `403` the heartbeat publisher
  suspends and reprobes every 30 minutes rather than disabling: the tier is
  derived per request, so a bot can gain it mid-run.
- **Rate limits are shared.** Stats and commands share 60 requests per minute
  per client IP; heartbeats have 120. This crate keeps one client-side `429`
  suspension (`Retry-After`, `X-RateLimit-Reset`, plus jitter) for stats and
  commands, and heartbeats bypass it: a stale number beats a false "down" on
  your status page. On shared hosting, other bots behind the same egress IP
  count against you.
- **`404` never means "bad token".** It means the id is unknown or belongs to
  a server listing, and disables only that publisher. A `401` disables all
  three.
- **Unchanged counts are not re-sent** for 30 minutes: every stats POST
  re-indexes the listing and fires a `bot.stats_reported` webhook.
- **Only global commands are published.** Guild-scoped commands are not part
  of a listing.
- The server count includes unavailable guilds: the bot is still in them.
  Without `GatewayIntents::GUILDS`, serenity's cache freezes at the `READY`
  snapshot and the count silently stops moving.

## Requirements

- Rust 1.74 or newer (CI builds and tests on 1.74 and stable)
- serenity 0.12 (tested with 0.12.5), and therefore poise 0.6 or 0.7
- serenity's tokio runtime

## Publishing (maintainers)

Releases publish to crates.io through trusted publishing (OIDC); there is no
long-lived `CARGO_REGISTRY_TOKEN` secret. crates.io only accepts a trusted
publisher for a crate that already exists, so the first version goes up by
hand. One-time setup:

1. Create a crates.io API token with the `publish-new` scope, then run
   `cargo publish` from a clean checkout of the `0.1.0` commit.
2. On crates.io, open the crate's **Settings → Trusted Publishing** and add a
   GitHub publisher: owner `TheDiscordList`, repository `integration-serenity`,
   workflow `release.yml`, environment `release`.
3. In the GitHub repository, create the `release` environment (optionally
   limited to `v*` tags), then revoke the token from step 1.
4. Push the `v0.1.0` tag. The release workflow sees `0.1.0` already on
   crates.io and skips the publish.

Every release after that:

1. Bump `version` in `Cargo.toml` and move the `Unreleased` notes in
   `CHANGELOG.md` under it.
2. Commit, tag `vX.Y.Z` and push the tag. `release.yml` checks the tag matches
   `Cargo.toml`, runs the tests, exchanges the workflow's OIDC token for a
   short-lived crates.io token with `rust-lang/crates-io-auth-action` and runs
   `cargo publish`.

## License

MIT
