# Changelog

All notable changes to this crate are documented here. It follows
[Semantic Versioning](https://semver.org/).

## Unreleased

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
