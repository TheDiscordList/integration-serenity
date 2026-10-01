# Changelog

All notable changes to this crate are documented here. It follows
[Semantic Versioning](https://semver.org/).

## Unreleased

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
