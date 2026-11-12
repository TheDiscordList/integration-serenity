# Changelog

All notable changes to this crate are documented here. It follows
[Semantic Versioning](https://semver.org/).

## Unreleased

## 0.2.1

- A shard never reads `disconnected`: serenity's `Disconnected` stage is a runner
  that has not started or is about to restart, so it reads `connecting`, and a
  routine reconnect no longer opens a public incident.
- A shard's `guilds` is left out until that shard is ready, so a restart's first
  beat can no longer zero the public server count.
- The local server count waits for every shard to be ready, so a boot no longer
  posts an undercount.
- A shard that serenity is restarting stays in the heartbeat as `connecting`.
  Missing it could disable the heartbeat publisher for good on a bot with several
  shards.
- The commands post comes back 5 minutes after a server error, a rate limit, a
  plan suspension or a failed fetch from Discord, until it lands or a terminal
  `4xx` ends it.
- A `post_commands` that lands ends a pending retry, which would only post the
  same list again.
- A scheduled post that gets a `429` retries once the window lifts, if that lands
  before its next run. A manual post still returns `Skip::Throttled` at once.
- A heartbeat `429` no longer suspends stats and commands.
- A burst of shard stage changes can no longer postpone the heartbeat.
- A panicking `on_event` callback no longer ends the publisher that raised it.
- `post_stats` before `READY` returns `Skip::NotReady` instead of posting without
  a `shard_count`, which nulled the stored value.
- The release workflow refuses a tag that is not on `main`.
- The tests and examples build against tokio 1.53, and CI runs on
  `actions/checkout` v7.

## 0.2.0

- `webhooks::verify` refuses an empty or blank secret with the new
  `WebhookError::MissingSecret` instead of checking signatures against an
  empty key.
- The `listing.bump_ready` webhook event: `EventData::ListingBumpReady`.
- Retries never run past the next scheduled post, and only network errors,
  timeouts, `408`, `425` and `5xx` are retried. Any other `4xx` is reported once.
- Out-of-band heartbeats after a shard stage change wait at least 10 seconds
  after the previous beat.

## 0.1.0

First release.

- `Autoposter`: a serenity `EventHandler` that publishes the server count,
  per-shard heartbeats and global application commands to TheDiscordList,
  with the shared rate-limit suspension, retries and status handling of the
  official integrations.
- Cluster-safe heartbeats: no heartbeat is sent until the cluster count is
  known, either because this process runs every shard or from `.cluster(id, count)`.
- `webhooks::verify` (feature `webhooks`): `X-TDL-Signature` verification with
  typed event payloads.
- Works with poise 0.6 and 0.7.
