//! The autoposter: three publishers driven by serenity's gateway events.
//!
//! Every public method returns a [`PostResult`] and every background loop
//! swallows its own failures, so nothing in here can take down the host bot.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use serenity::all::{
    Cache, Command, CommandOption, ConnectionStage, Context, EventHandler, Guild, Http, Ready,
    ResumedEvent, ShardManager, ShardStageUpdateEvent, UnavailableGuild,
};
use serenity::async_trait;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{sleep, sleep_until, Instant};

use crate::http::{backoff, random_unit, Api, Outcome};
use crate::{lock, DEFAULT_BASE_URL};

const STATS_INTERVAL: Duration = Duration::from_secs(300);
const STATS_DEBOUNCE: Duration = Duration::from_secs(20);
const STATS_MIN_GAP: Duration = Duration::from_secs(60);
/// Republish an unchanged count this often, so a listing never looks abandoned.
const STATS_FORCE_AFTER: Duration = Duration::from_secs(1800);
const STATS_REMEASURE: Duration = Duration::from_secs(60);
const STATS_FIRST_DELAY: Duration = Duration::from_secs(5);

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_MIN_INTERVAL: Duration = Duration::from_secs(15);
const HEARTBEAT_MAX_INTERVAL: Duration = Duration::from_secs(60);
const HEARTBEAT_JITTER: Duration = Duration::from_secs(5);
const HEARTBEAT_MIN_DELAY: Duration = Duration::from_secs(1);
/// The floor between an out-of-band beat and the one before it.
const HEARTBEAT_OUT_OF_BAND_GAP: Duration = Duration::from_secs(10);

/// Offset from the first stats post: the two share one server-side rate budget.
const COMMANDS_DELAY: Duration = Duration::from_secs(10);
/// The Gold tier is derived per request, so a bot can gain it mid-run.
const PLAN_REPROBE: Duration = Duration::from_secs(1800);
/// serenity's shard queuer boots one shard per 5 s.
const SHARD_BOOT_PACE: Duration = Duration::from_secs(5);
const SHARD_BOOT_GRACE: Duration = Duration::from_secs(60);

const MAX_ATTEMPTS: u32 = 3;
const MAX_LATENCY_MS: u64 = 600_000;
const MAX_CLUSTERS: u32 = 1024;

/// One of the three things the autoposter publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Publisher {
    /// `POST /bots/{id}/stats`: server and shard count.
    Stats = 0,
    /// `POST /bots/{id}/heartbeat`: per-shard status for the status page.
    Heartbeat = 1,
    /// `POST /bots/{id}/commands`: the global application commands.
    Commands = 2,
}

impl fmt::Display for Publisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stats => "stats",
            Self::Heartbeat => "heartbeat",
            Self::Commands => "commands",
        })
    }
}

/// Something the autoposter wants you to know about. See [`AutoposterBuilder::on_event`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// TheDiscordList accepted a request.
    Posted {
        /// Which publisher sent it.
        publisher: Publisher,
        /// The 2xx status.
        status: u16,
    },
    /// A request failed. Payload and plan errors are reported once per status.
    Error {
        /// Which publisher failed.
        publisher: Publisher,
        /// The HTTP status, or `None` for a network failure or timeout.
        status: Option<u16>,
        /// Whether the next scheduled run may succeed without a change on your side.
        retryable: bool,
        /// A human-readable description.
        message: String,
    },
    /// A publisher stopped itself. Calling its `post_*` method re-arms it.
    Disabled {
        /// Which publisher stopped.
        publisher: Publisher,
        /// The status that caused it, if any.
        status: Option<u16>,
        /// `"unauthorized"`, `"not-found"`, `"conflict"` or `"cluster-count-unknown"`.
        reason: &'static str,
    },
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Posted { publisher, status } => write!(f, "{publisher}: posted ({status})"),
            Self::Error {
                publisher, message, ..
            } => write!(f, "{publisher}: {message}"),
            Self::Disabled {
                publisher, reason, ..
            } => {
                let hint = match *reason {
                    "unauthorized" => {
                        "the stats token was rejected (401), so every publisher is disabled. Copy it again from the listing's API tab"
                    }
                    "not-found" => {
                        "the bot id is unknown or belongs to a server listing (404). Check bot_id"
                    }
                    "conflict" => "the listing is not configured for slash commands (409)",
                    "cluster-count-unknown" => {
                        "not every shard runs in this process, so the cluster count is unknown. A wrong cluster_count deletes sibling clusters server-side; configure .cluster(id, count) to enable heartbeats"
                    }
                    other => other,
                };
                write!(f, "{publisher} disabled: {hint}")
            }
        }
    }
}

/// Why a `post_*` call sent nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Skip {
    /// No stats token was configured; the autoposter is inert.
    NoToken,
    /// The publisher is turned off in the builder, or disabled itself and this was not a manual post.
    Disabled,
    /// No `READY` yet: the bot id, cache or HTTP client is not known.
    NotReady,
    /// The same publisher already has a request in flight.
    InFlight,
    /// Less than the minimum gap since the last stats post.
    MinGap,
    /// Same counts as the last successful post, which is under 30 minutes old.
    Unchanged,
    /// Another process (cluster or shard 0) publishes stats and commands.
    NotElected,
    /// The server count cannot be trusted yet.
    Unmeasurable,
    /// The client-wide rate-limit suspension dropped the request.
    Suspended,
    /// TheDiscordList answered 429.
    Throttled,
    /// The listing's plan does not include this endpoint (403); reprobing later.
    Plan,
    /// Heartbeats would need a cluster count this process cannot determine.
    ClusterCountUnknown,
    /// No shard has reported in yet.
    NoShards,
    /// No global commands are registered; posting `[]` would clear the listing's.
    Empty,
    /// Fetching the global commands from Discord failed.
    FetchFailed,
}

/// The outcome of one publish. Never an `Err`, never a panic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PostResult {
    /// TheDiscordList answered 2xx.
    pub ok: bool,
    /// The final HTTP status, if a request got an answer.
    pub status: Option<u16>,
    /// Set when nothing was sent, or the answer was a 403 or 429.
    pub skipped: Option<Skip>,
}

impl PostResult {
    fn skip(reason: Skip) -> Self {
        Self {
            ok: false,
            status: None,
            skipped: Some(reason),
        }
    }

    fn failed(status: Option<u16>) -> Self {
        Self {
            ok: false,
            status,
            skipped: None,
        }
    }
}

type EventCallback = Arc<dyn Fn(&Event) + Send + Sync>;
type ServerCount = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Option<u64>> + Send>> + Send + Sync>;

#[derive(Clone)]
struct Config {
    base_url: String,
    bot_id: Option<u64>,
    stats: bool,
    heartbeat: bool,
    commands: bool,
    stats_interval: Duration,
    stats_debounce: Duration,
    stats_min_gap: Duration,
    heartbeat_interval: Duration,
    post_empty_commands: bool,
    cluster: Option<(u32, u32)>,
    stats_first_delay: Duration,
    commands_delay: Duration,
    retry_base: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_owned(),
            bot_id: None,
            stats: true,
            heartbeat: true,
            commands: true,
            stats_interval: STATS_INTERVAL,
            stats_debounce: STATS_DEBOUNCE,
            stats_min_gap: STATS_MIN_GAP,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            post_empty_commands: false,
            cluster: None,
            stats_first_delay: STATS_FIRST_DELAY,
            commands_delay: COMMANDS_DELAY,
            retry_base: Duration::from_secs(1),
        }
    }
}

/// Configures an [`Autoposter`]. Created by [`Autoposter::builder`].
#[must_use]
pub struct AutoposterBuilder {
    token: String,
    config: Config,
    on_event: Option<EventCallback>,
    server_count: Option<ServerCount>,
}

impl AutoposterBuilder {
    /// API base URL. Default: `https://thediscordlist.com/api`.
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.config.base_url = base_url.into();
        self
    }

    /// Discord application id. Default: the one in the `READY` payload.
    /// Setting it also lets [`Autoposter::post_stats`] work before `READY`.
    pub fn bot_id(mut self, bot_id: impl Into<u64>) -> Self {
        self.config.bot_id = Some(bot_id.into());
        self
    }

    /// Turns the stats publisher on or off. Default: on.
    pub fn stats(mut self, enabled: bool) -> Self {
        self.config.stats = enabled;
        self
    }

    /// Stats cadence. Default: 5 minutes.
    pub fn stats_interval(mut self, interval: Duration) -> Self {
        self.config.stats_interval = interval;
        self
    }

    /// Trailing debounce after a guild join or leave. Default: 20 seconds.
    pub fn stats_debounce(mut self, debounce: Duration) -> Self {
        self.config.stats_debounce = debounce;
        self
    }

    /// Hard floor between two stats posts, manual ones included. Default and minimum: 60 seconds.
    pub fn stats_min_gap(mut self, min_gap: Duration) -> Self {
        self.config.stats_min_gap = min_gap.max(STATS_MIN_GAP);
        self
    }

    /// Turns the heartbeat publisher on or off. Default: on.
    pub fn heartbeat(mut self, enabled: bool) -> Self {
        self.config.heartbeat = enabled;
        self
    }

    /// Starting heartbeat cadence, clamped to 15–60 s and retuned from the
    /// API's `next_beat_within_seconds`. Default: 30 seconds.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval =
            interval.clamp(HEARTBEAT_MIN_INTERVAL, HEARTBEAT_MAX_INTERVAL);
        self
    }

    /// Turns the commands publisher on or off. Default: on.
    pub fn commands(mut self, enabled: bool) -> Self {
        self.config.commands = enabled;
        self
    }

    /// Posting an empty list **clears** the listing's commands, so it is
    /// skipped unless this is set. Default: off.
    pub fn post_empty_commands(mut self, post_empty: bool) -> Self {
        self.config.post_empty_commands = post_empty;
        self
    }

    /// Names this process's cluster when shards are split across processes.
    /// `count` is 1–1024 and `id < count`; anything else is ignored with a warning.
    pub fn cluster(mut self, id: u32, count: u32) -> Self {
        self.config.cluster = Some((id, count));
        self
    }

    /// Supplies the server count instead of reading the local cache, for
    /// multi-process bots that can sum across processes. `None` means
    /// "unknown right now"; the publisher measures again a minute later.
    pub fn server_count<F, Fut>(mut self, server_count: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<u64>> + Send + 'static,
    {
        self.server_count = Some(Arc::new(move || Box::pin(server_count())));
        self
    }

    /// Observes posted, error and disabled events. Without a callback they
    /// are logged through `tracing` (`debug` for posts, `warn` otherwise).
    pub fn on_event(mut self, on_event: impl Fn(&Event) + Send + Sync + 'static) -> Self {
        self.on_event = Some(Arc::new(on_event));
        self
    }

    /// Builds the autoposter. A missing or empty token logs one warning and
    /// returns an inert autoposter, so a forgotten environment variable never
    /// takes down a deploy.
    #[must_use]
    pub fn build(mut self) -> Autoposter {
        let token = self.token.trim();
        let token = token.strip_prefix("Bearer ").unwrap_or(token).trim();

        let mut warnings = Vec::new();
        let api = if token.is_empty() {
            warnings.push((
                "no-token",
                "No stats token supplied; auto-posting is disabled.".to_owned(),
            ));
            None
        } else {
            match Api::new(&self.config.base_url, token) {
                Ok(api) => Some(api),
                Err(error) => {
                    warnings.push((
                        "http-client",
                        format!(
                            "Could not build the HTTP client ({error}); auto-posting is disabled."
                        ),
                    ));
                    None
                }
            }
        };

        if let Some((id, count)) = self.config.cluster {
            if count == 0 || count > MAX_CLUSTERS || id >= count {
                warnings.push(("cluster", format!("Ignoring .cluster({id}, {count}): count must be 1-{MAX_CLUSTERS} and id below it.")));
                self.config.cluster = None;
            }
        }

        let inner = Inner {
            state: Mutex::new(State::new(self.config.heartbeat_interval)),
            config: self.config,
            api,
            on_event: self.on_event,
            server_count: self.server_count,
            shard_manager: OnceLock::new(),
            stats_busy: tokio::sync::Mutex::new(()),
            heartbeat_busy: tokio::sync::Mutex::new(()),
            stats_wake: Arc::new(Notify::new()),
            heartbeat_wake: Arc::new(Notify::new()),
            tasks: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
        };
        for (key, message) in warnings {
            inner.warn_once(key, &message);
        }

        Autoposter {
            inner: Arc::new(inner),
        }
    }
}

/// Publishes a serenity bot's stats, heartbeats and commands to TheDiscordList.
///
/// Cheap to clone; every clone drives the same publishers. Register one clone
/// with [`ClientBuilder::event_handler`](serenity::all::ClientBuilder::event_handler)
/// and it starts on the first `READY`. Background tasks end on [`stop`](Self::stop)
/// or when the last clone is dropped.
#[derive(Clone)]
pub struct Autoposter {
    inner: Arc<Inner>,
}

impl Autoposter {
    /// An autoposter with every default. Same as `Autoposter::builder(token).build()`.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self::builder(token).build()
    }

    /// Starts configuring an autoposter. `token` is the listing's stats token.
    pub fn builder(token: impl Into<String>) -> AutoposterBuilder {
        AutoposterBuilder {
            token: token.into(),
            config: Config::default(),
            on_event: None,
            server_count: None,
        }
    }

    /// Lets heartbeats read shard stages and latency from the shard runners.
    /// Without it, heartbeats track stages from gateway events and omit `latency_ms`.
    pub fn attach_shard_manager(&self, shard_manager: Arc<ShardManager>) {
        let _ = self.inner.shard_manager.set(shard_manager);
    }

    /// Posts the server count now. Still bound by the 60 s minimum gap.
    /// Re-arms a publisher that disabled itself.
    pub async fn post_stats(&self) -> PostResult {
        self.inner.publish_stats(true).await
    }

    /// Sends one heartbeat now. Re-arms a publisher that disabled itself.
    pub async fn post_heartbeat(&self) -> PostResult {
        self.inner.beat(true).await
    }

    /// Fetches the global commands from Discord and posts them now.
    /// Re-arms a publisher that disabled itself.
    pub async fn post_commands(&self) -> PostResult {
        self.inner.publish_commands(true).await
    }

    /// Stops every background task. Not restartable; manual posts still work.
    pub fn stop(&self) {
        self.inner.stopped.store(true, Ordering::SeqCst);
        self.inner.abort_tasks();
    }
}

#[async_trait]
impl EventHandler for Autoposter {
    async fn ready(&self, ctx: Context, ready: Ready) {
        let handles = Handles {
            cache: Arc::clone(&ctx.cache),
            http: Arc::clone(&ctx.http),
        };
        let total_shards = ready.shard.as_ref().map_or(1, |shard| shard.total);
        self.inner.on_ready(
            handles,
            ready.application.id.get(),
            ctx.shard_id.0,
            total_shards,
        );
    }

    async fn resume(&self, ctx: Context, _: ResumedEvent) {
        self.inner
            .on_stage(ctx.shard_id.0, ConnectionStage::Connected);
    }

    async fn shard_stage_update(&self, _: Context, event: ShardStageUpdateEvent) {
        self.inner.on_stage(event.shard_id.0, event.new);
    }

    async fn guild_create(&self, _: Context, _: Guild, is_new: Option<bool>) {
        if is_new == Some(true) {
            self.inner.stats_wake.notify_one();
        }
    }

    async fn guild_delete(&self, _: Context, incomplete: UnavailableGuild, _: Option<Guild>) {
        // An outage marks guilds unavailable; only a real leave moves the count.
        if !incomplete.unavailable {
            self.inner.stats_wake.notify_one();
        }
    }
}

#[derive(Clone)]
struct Handles {
    cache: Arc<Cache>,
    http: Arc<Http>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum ShardStatus {
    Ready,
    Connecting,
    Resuming,
    Disconnected,
}

impl From<ConnectionStage> for ShardStatus {
    fn from(stage: ConnectionStage) -> Self {
        match stage {
            ConnectionStage::Connected => Self::Ready,
            ConnectionStage::Resuming => Self::Resuming,
            ConnectionStage::Disconnected => Self::Disconnected,
            _ => Self::Connecting,
        }
    }
}

struct LocalShard {
    id: u32,
    status: ShardStatus,
    latency: Option<Duration>,
}

#[derive(Default)]
struct Gate {
    disabled: Option<&'static str>,
    suspended_until: Option<Instant>,
    reported: HashSet<u16>,
}

struct State {
    gates: [Gate; 3],
    bot_id: Option<u64>,
    total_shards: Option<u32>,
    shards: BTreeMap<u32, ShardStatus>,
    handles: Option<Handles>,
    ready_at: Option<Instant>,
    last_stats_at: Option<Instant>,
    last_stats: Option<(u64, Option<u32>)>,
    last_beat_at: Option<Instant>,
    heartbeat_interval: Duration,
    warned: HashSet<&'static str>,
}

impl State {
    fn new(heartbeat_interval: Duration) -> Self {
        Self {
            gates: Default::default(),
            bot_id: None,
            total_shards: None,
            shards: BTreeMap::new(),
            handles: None,
            ready_at: None,
            last_stats_at: None,
            last_stats: None,
            last_beat_at: None,
            heartbeat_interval,
            warned: HashSet::new(),
        }
    }
}

struct Inner {
    config: Config,
    api: Option<Api>,
    on_event: Option<EventCallback>,
    server_count: Option<ServerCount>,
    shard_manager: OnceLock<Arc<ShardManager>>,
    state: Mutex<State>,
    stats_busy: tokio::sync::Mutex<()>,
    heartbeat_busy: tokio::sync::Mutex<()>,
    stats_wake: Arc<Notify>,
    heartbeat_wake: Arc<Notify>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    stopped: AtomicBool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    fn abort_tasks(&self) {
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
    }

    fn emit(&self, event: Event) {
        match &self.on_event {
            Some(on_event) => on_event(&event),
            None if matches!(event, Event::Posted { .. }) => tracing::debug!("{event}"),
            None => tracing::warn!("{event}"),
        }
    }

    fn warn_once(&self, key: &'static str, message: &str) {
        if self.state().warned.insert(key) {
            tracing::warn!("{message}");
        }
    }

    /// Records a shard's `READY`; returns whether it was the first one.
    fn record_ready(
        &self,
        handles: Handles,
        application_id: u64,
        shard_id: u32,
        total_shards: u32,
    ) -> bool {
        let mut state = self.state();
        state.bot_id = Some(application_id);
        state.total_shards = Some(total_shards.max(1));
        state.shards.insert(shard_id, ShardStatus::Ready);
        state.handles.get_or_insert(handles);
        if state.ready_at.is_some() {
            return false;
        }
        state.ready_at = Some(Instant::now());
        true
    }

    fn on_ready(
        self: &Arc<Self>,
        handles: Handles,
        application_id: u64,
        shard_id: u32,
        total_shards: u32,
    ) {
        if self.api.is_none() || self.stopped.load(Ordering::SeqCst) {
            return;
        }
        if self.record_ready(handles, application_id, shard_id, total_shards) {
            self.spawn_tasks();
        } else {
            self.heartbeat_wake.notify_one();
        }
    }

    fn on_stage(&self, shard_id: u32, stage: ConnectionStage) {
        if self.api.is_none() {
            return;
        }
        self.state().shards.insert(shard_id, stage.into());
        self.heartbeat_wake.notify_one();
    }

    fn spawn_tasks(self: &Arc<Self>) {
        let mut tasks = lock(&self.tasks);
        if self.config.heartbeat {
            tasks.push(tokio::spawn(heartbeat_loop(
                Arc::downgrade(self),
                Arc::clone(&self.heartbeat_wake),
            )));
        }
        if self.config.stats {
            tasks.push(tokio::spawn(stats_loop(
                Arc::downgrade(self),
                Arc::clone(&self.stats_wake),
            )));
        }
        if self.config.commands {
            tasks.push(tokio::spawn(commands_once(Arc::downgrade(self))));
        }
    }

    fn enabled(&self, publisher: Publisher) -> bool {
        match publisher {
            Publisher::Stats => self.config.stats,
            Publisher::Heartbeat => self.config.heartbeat,
            Publisher::Commands => self.config.commands,
        }
    }

    fn is_disabled(&self, publisher: Publisher) -> bool {
        self.state().gates[publisher as usize].disabled.is_some()
    }

    fn suspended(&self, publisher: Publisher) -> bool {
        self.state().gates[publisher as usize]
            .suspended_until
            .is_some_and(|until| Instant::now() < until)
    }

    /// Shared preconditions. A manual post re-arms a disabled publisher.
    fn gate(&self, publisher: Publisher, manual: bool) -> Option<Skip> {
        if self.api.is_none() {
            return Some(Skip::NoToken);
        }
        if !self.enabled(publisher) {
            return Some(Skip::Disabled);
        }
        if manual {
            let was_disabled = self.state().gates[publisher as usize]
                .disabled
                .take()
                .is_some();
            if was_disabled {
                match publisher {
                    Publisher::Stats => self.stats_wake.notify_one(),
                    Publisher::Heartbeat => self.heartbeat_wake.notify_one(),
                    Publisher::Commands => {}
                }
            }
            return None;
        }
        self.is_disabled(publisher).then_some(Skip::Disabled)
    }

    fn disable(&self, publisher: Publisher, status: Option<u16>, reason: &'static str) {
        let newly = self.state().gates[publisher as usize]
            .disabled
            .replace(reason)
            .is_none();
        if newly {
            self.emit(Event::Disabled {
                publisher,
                status,
                reason,
            });
        }
    }

    fn report_once(&self, publisher: Publisher, status: u16, message: String) {
        let first = self.state().gates[publisher as usize]
            .reported
            .insert(status);
        if first {
            self.emit(Event::Error {
                publisher,
                status: Some(status),
                retryable: false,
                message,
            });
        }
    }

    fn bot_id(&self) -> Option<u64> {
        self.config.bot_id.or(self.state().bot_id)
    }

    async fn local_shards(&self) -> Vec<LocalShard> {
        if let Some(shard_manager) = self.shard_manager.get() {
            let runners = shard_manager.runners.lock().await;
            let mut shards: Vec<LocalShard> = runners
                .iter()
                .map(|(id, runner)| LocalShard {
                    id: id.0,
                    status: runner.stage.into(),
                    latency: runner.latency,
                })
                .collect();
            shards.sort_by_key(|shard| shard.id);
            return shards;
        }
        self.state()
            .shards
            .iter()
            .map(|(&id, &status)| LocalShard {
                id,
                status,
                latency: None,
            })
            .collect()
    }

    /// `(cluster, cluster_count)` when it can be trusted. Without an override
    /// that means this process runs every shard; serenity has no way to say
    /// how a split deployment is laid out, and the server deletes every
    /// cluster at or above a reported `cluster_count`.
    fn cluster(&self, total_shards: Option<u32>, local: &[LocalShard]) -> Option<(u32, u32)> {
        if self.config.cluster.is_some() {
            return self.config.cluster;
        }
        let total = total_shards?;
        let present = local.iter().filter(|shard| shard.id < total).count();
        (present == total as usize).then_some((0, 1))
    }

    /// Exactly one process publishes stats and commands: cluster 0, or whichever runs shard 0.
    fn elected(&self, local: &[LocalShard]) -> bool {
        match self.config.cluster {
            Some((id, _)) => id == 0,
            None => local.is_empty() || local.iter().any(|shard| shard.id == 0),
        }
    }

    /// Whether every local shard has had time to boot since the first `READY`.
    fn shards_settled(&self) -> bool {
        let state = self.state();
        let Some(ready_at) = state.ready_at else {
            return false;
        };
        let boot = SHARD_BOOT_PACE * state.total_shards.unwrap_or(1) + SHARD_BOOT_GRACE;
        Instant::now() >= ready_at + boot
    }

    async fn measure(&self, local: &[LocalShard], total_shards: Option<u32>) -> Result<u64, Skip> {
        if !self.elected(local) {
            return Err(Skip::NotElected);
        }
        if let Some(server_count) = &self.server_count {
            return server_count().await.ok_or(Skip::Unmeasurable);
        }
        if self.cluster(total_shards, local) != Some((0, 1)) {
            if self.config.cluster.is_some() || self.shards_settled() {
                self.warn_once(
                    "server-count",
                    "This process does not run every shard, so its cache cannot give the server count. Configure .server_count(..) to post stats.",
                );
            }
            return Err(Skip::Unmeasurable);
        }
        let cache = self
            .state()
            .handles
            .as_ref()
            .map(|handles| Arc::clone(&handles.cache));
        // Unavailable guilds are included: the bot is still in them.
        cache
            .map(|cache| cache.guilds().len() as u64)
            .ok_or(Skip::NotReady)
    }

    async fn publish_stats(&self, manual: bool) -> PostResult {
        if let Some(skip) = self.gate(Publisher::Stats, manual) {
            return PostResult::skip(skip);
        }
        let Ok(_busy) = self.stats_busy.try_lock() else {
            return PostResult::skip(Skip::InFlight);
        };
        if !manual && self.suspended(Publisher::Stats) {
            return PostResult::skip(Skip::Plan);
        }
        // A hard floor: join storms, reconnects and manual posts all route through it.
        let last_stats_at = self.state().last_stats_at;
        if last_stats_at.is_some_and(|at| Instant::now() < at + self.config.stats_min_gap) {
            return PostResult::skip(Skip::MinGap);
        }
        let Some(bot_id) = self.bot_id() else {
            return PostResult::skip(Skip::NotReady);
        };

        let local = self.local_shards().await;
        let total_shards = self.state().total_shards;
        let server_count = match self.measure(&local, total_shards).await {
            Ok(count) => count,
            Err(skip) => return PostResult::skip(skip),
        };

        {
            // Every stats post re-indexes the listing and fires a webhook, and
            // the server does not dedupe, so an unchanged count is skipped here.
            let mut state = self.state();
            let now = Instant::now();
            let fresh = state
                .last_stats_at
                .is_some_and(|at| now < at + STATS_FORCE_AFTER);
            if fresh && state.last_stats == Some((server_count, total_shards)) {
                return PostResult::skip(Skip::Unchanged);
            }
            state.last_stats_at = Some(now);
        }

        let body = StatsBody {
            server_count,
            shard_count: total_shards,
        };
        let (result, _) = self
            .send(
                Publisher::Stats,
                &format!("/bots/{bot_id}/stats"),
                &body,
                Some(self.config.stats_interval),
            )
            .await;
        if result.ok {
            self.state().last_stats = Some((server_count, total_shards));
        }
        result
    }

    fn next_stats_run(&self, result: &PostResult) -> Option<Instant> {
        let state = self.state();
        let gate = &state.gates[Publisher::Stats as usize];
        if gate.disabled.is_some() {
            return None;
        }
        let now = Instant::now();
        Some(match result.skipped {
            Some(Skip::MinGap) => state
                .last_stats_at
                .map_or(now, |at| at + self.config.stats_min_gap),
            Some(Skip::Plan) => gate.suspended_until.unwrap_or(now).max(now),
            Some(Skip::Unmeasurable | Skip::NotElected | Skip::NotReady) => now + STATS_REMEASURE,
            _ => now + self.config.stats_interval,
        })
    }

    async fn beat(&self, manual: bool) -> PostResult {
        if let Some(skip) = self.gate(Publisher::Heartbeat, manual) {
            return PostResult::skip(skip);
        }
        let Ok(_busy) = self.heartbeat_busy.try_lock() else {
            return PostResult::skip(Skip::InFlight);
        };
        let Some(bot_id) = self.bot_id() else {
            return PostResult::skip(Skip::NotReady);
        };

        // The API requires at least one shard.
        let local = self.local_shards().await;
        if local.is_empty() {
            return PostResult::skip(Skip::NoShards);
        }
        let (total_shards, handles) = {
            let state = self.state();
            (state.total_shards, state.handles.clone())
        };
        let Some((cluster, cluster_count)) = self.cluster(total_shards, &local) else {
            if !manual && self.shards_settled() {
                self.disable(Publisher::Heartbeat, None, "cluster-count-unknown");
            }
            return PostResult::skip(Skip::ClusterCountUnknown);
        };

        let guilds = match (&handles, total_shards) {
            (Some(handles), Some(total)) => guilds_per_shard(&handles.cache, total),
            _ => HashMap::new(),
        };
        let body = HeartbeatBody {
            cluster,
            cluster_count,
            shards: local
                .iter()
                .map(|shard| ShardReport {
                    id: shard.id,
                    status: shard.status,
                    // `latency_ms` is capped server-side; a stalled shard can exceed it.
                    latency_ms: shard.latency.map(|latency| {
                        u64::try_from(latency.as_millis())
                            .map_or(MAX_LATENCY_MS, |ms| ms.min(MAX_LATENCY_MS))
                    }),
                    guilds: handles
                        .as_ref()
                        .map(|_| guilds.get(&shard.id).copied().unwrap_or(0)),
                })
                .collect(),
        };

        let interval = {
            let mut state = self.state();
            state.last_beat_at = Some(Instant::now());
            state.heartbeat_interval
        };
        let (result, response) = self
            .send(
                Publisher::Heartbeat,
                &format!("/bots/{bot_id}/heartbeat"),
                &body,
                Some(interval),
            )
            .await;

        // The cadence follows the server's grace window rather than a constant.
        let next_within = response
            .as_ref()
            .and_then(|body| body.get("next_beat_within_seconds"))
            .and_then(Value::as_f64)
            .filter(|seconds| seconds.is_finite() && *seconds > 0.0);
        if let Some(seconds) = next_within {
            self.state().heartbeat_interval = Duration::from_secs_f64((seconds / 3.0).min(3600.0))
                .clamp(HEARTBEAT_MIN_INTERVAL, HEARTBEAT_MAX_INTERVAL);
        }
        result
    }

    async fn heartbeat_tick(&self) -> Option<Instant> {
        let started = Instant::now();
        self.beat(false).await;

        let state = self.state();
        let gate = &state.gates[Publisher::Heartbeat as usize];
        if gate.disabled.is_some() {
            return None;
        }
        let jitter = HEARTBEAT_JITTER.mul_f64(random_unit());
        let next =
            (started + state.heartbeat_interval + jitter).max(Instant::now() + HEARTBEAT_MIN_DELAY);
        Some(gate.suspended_until.map_or(next, |until| next.max(until)))
    }

    /// When a shard transition's out-of-band beat goes out: soon, but never within
    /// 10 s of the previous beat, so a flapping shard cannot outpace the rate limit.
    fn out_of_band_beat(&self) -> Option<Instant> {
        let state = self.state();
        let gate = &state.gates[Publisher::Heartbeat as usize];
        if gate.disabled.is_some() {
            return None;
        }
        let soon = Instant::now() + HEARTBEAT_MIN_DELAY;
        let next = state
            .last_beat_at
            .map_or(soon, |at| soon.max(at + HEARTBEAT_OUT_OF_BAND_GAP));
        Some(gate.suspended_until.map_or(next, |until| next.max(until)))
    }

    async fn publish_commands(&self, manual: bool) -> PostResult {
        if let Some(skip) = self.gate(Publisher::Commands, manual) {
            return PostResult::skip(skip);
        }
        if self.suspended(Publisher::Commands) {
            return PostResult::skip(Skip::Plan);
        }
        let Some(bot_id) = self.bot_id() else {
            return PostResult::skip(Skip::NotReady);
        };
        if !manual && !self.elected(&self.local_shards().await) {
            return PostResult::skip(Skip::NotElected);
        }
        let Some(http) = self
            .state()
            .handles
            .as_ref()
            .map(|handles| Arc::clone(&handles.http))
        else {
            return PostResult::skip(Skip::NotReady);
        };

        let commands = match Command::get_global_commands(&http).await {
            Ok(commands) => commands,
            Err(error) => {
                self.emit(Event::Error {
                    publisher: Publisher::Commands,
                    status: None,
                    retryable: true,
                    message: format!("Could not fetch the global commands from Discord: {error}"),
                });
                return PostResult::skip(Skip::FetchFailed);
            }
        };
        if commands.is_empty() && !self.config.post_empty_commands {
            return PostResult::skip(Skip::Empty);
        }

        let body = CommandsBody {
            commands: commands.iter().map(CommandPayload::from).collect(),
        };
        self.send(
            Publisher::Commands,
            &format!("/bots/{bot_id}/commands"),
            &body,
            None,
        )
        .await
        .0
    }

    /// One request, retrying only transient failures, at most three attempts.
    /// A retry that would land past the next scheduled run is dropped: that
    /// run carries fresher data, and a failed request still burns rate-limit budget.
    async fn send(
        &self,
        publisher: Publisher,
        path: &str,
        body: &(impl Serialize + Sync),
        next_run_in: Option<Duration>,
    ) -> (PostResult, Option<Value>) {
        let Some(api) = &self.api else {
            return (PostResult::skip(Skip::NoToken), None);
        };
        let next_run = next_run_in.map(|delay| Instant::now() + delay);
        let mut attempt = 0;
        loop {
            let outcome = api
                .post(path, body, publisher == Publisher::Heartbeat)
                .await;
            attempt += 1;
            if attempt < MAX_ATTEMPTS && matches!(outcome, Outcome::Transient { .. }) {
                let delay = backoff(attempt - 1, self.config.retry_base);
                if !next_run.is_some_and(|at| Instant::now() + delay >= at) {
                    sleep(delay).await;
                    continue;
                }
            }
            return self.react(publisher, outcome);
        }
    }

    fn react(&self, publisher: Publisher, outcome: Outcome) -> (PostResult, Option<Value>) {
        match outcome {
            Outcome::Ok { status, body } => {
                self.emit(Event::Posted { publisher, status });
                (
                    PostResult {
                        ok: true,
                        status: Some(status),
                        skipped: None,
                    },
                    Some(body),
                )
            }
            Outcome::Dropped => (PostResult::skip(Skip::Suspended), None),
            Outcome::Payload(status) => {
                // Stays enabled: the next run may carry valid data.
                self.report_once(
                    publisher,
                    status,
                    format!("TheDiscordList rejected the {publisher} payload ({status})."),
                );
                (PostResult::failed(Some(status)), None)
            }
            Outcome::Auth => {
                for gate in &mut self.state().gates {
                    gate.disabled = Some("unauthorized");
                }
                self.emit(Event::Disabled {
                    publisher,
                    status: Some(401),
                    reason: "unauthorized",
                });
                (PostResult::failed(Some(401)), None)
            }
            Outcome::Plan => {
                self.state().gates[publisher as usize].suspended_until =
                    Some(Instant::now() + PLAN_REPROBE);
                self.report_once(
                    publisher,
                    403,
                    format!(
                        "The {publisher} endpoint needs a higher plan. Reprobing in 30 minutes."
                    ),
                );
                (
                    PostResult {
                        ok: false,
                        status: Some(403),
                        skipped: Some(Skip::Plan),
                    },
                    None,
                )
            }
            Outcome::Config(status) => {
                self.disable(
                    publisher,
                    Some(status),
                    if status == 409 {
                        "conflict"
                    } else {
                        "not-found"
                    },
                );
                (PostResult::failed(Some(status)), None)
            }
            Outcome::Throttled => (
                PostResult {
                    ok: false,
                    status: Some(429),
                    skipped: Some(Skip::Throttled),
                },
                None,
            ),
            Outcome::Transient { status, message } => {
                self.emit(Event::Error {
                    publisher,
                    status,
                    retryable: true,
                    message,
                });
                (PostResult::failed(status), None)
            }
        }
    }
}

async fn sleep_until_some(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Tasks hold a `Weak`, so dropping the last [`Autoposter`] ends them.
async fn stats_loop(inner: Weak<Inner>, wake: Arc<Notify>) {
    let Some(first_delay) = inner.upgrade().map(|inner| inner.config.stats_first_delay) else {
        return;
    };
    let mut next = Some(Instant::now() + first_delay);
    let mut debounce_cap: Option<Instant> = None;
    loop {
        tokio::select! {
            () = sleep_until_some(next) => {
                debounce_cap = None;
                let Some(inner) = inner.upgrade() else { return };
                let result = inner.publish_stats(false).await;
                next = inner.next_stats_run(&result);
            }
            () = wake.notified() => {
                let Some(inner) = inner.upgrade() else { return };
                if inner.is_disabled(Publisher::Stats) {
                    next = None;
                    continue;
                }
                // Trailing debounce, capped so a join storm cannot starve the post.
                let now = Instant::now();
                let cap = *debounce_cap.get_or_insert(now + inner.config.stats_min_gap);
                next = Some((now + inner.config.stats_debounce).min(cap));
            }
        }
    }
}

async fn heartbeat_loop(inner: Weak<Inner>, wake: Arc<Notify>) {
    let mut next = Some(Instant::now());
    loop {
        tokio::select! {
            () = sleep_until_some(next) => {
                let Some(inner) = inner.upgrade() else { return };
                next = inner.heartbeat_tick().await;
            }
            () = wake.notified() => {
                // Shard transitions beat out of band; a burst coalesces into one.
                let Some(inner) = inner.upgrade() else { return };
                next = inner.out_of_band_beat();
            }
        }
    }
}

async fn commands_once(inner: Weak<Inner>) {
    let Some(delay) = inner.upgrade().map(|inner| inner.config.commands_delay) else {
        return;
    };
    sleep(delay).await;
    if let Some(inner) = inner.upgrade() {
        inner.publish_commands(false).await;
    }
}

fn guilds_per_shard(cache: &Cache, total_shards: u32) -> HashMap<u32, u64> {
    let mut counts = HashMap::new();
    for guild in cache.guilds() {
        let shard = (guild.get() >> 22) % u64::from(total_shards);
        *counts.entry(shard as u32).or_insert(0) += 1;
    }
    counts
}

#[derive(Serialize)]
struct StatsBody {
    server_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    shard_count: Option<u32>,
}

#[derive(Serialize)]
struct HeartbeatBody {
    cluster: u32,
    cluster_count: u32,
    shards: Vec<ShardReport>,
}

#[derive(Serialize)]
struct ShardReport {
    id: u32,
    status: ShardStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    guilds: Option<u64>,
}

#[derive(Serialize)]
struct CommandsBody<'a> {
    commands: Vec<CommandPayload<'a>>,
}

/// Only the fields the API accepts. serenity's own serialisation writes
/// `null` for absent optionals, which the API rejects with a 422.
#[derive(Serialize)]
struct CommandPayload<'a> {
    #[serde(rename = "type")]
    kind: u8,
    name: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    description: &'a str,
    nsfw: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_member_permissions: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    options: Vec<OptionPayload<'a>>,
}

impl<'a> From<&'a Command> for CommandPayload<'a> {
    fn from(command: &'a Command) -> Self {
        Self {
            kind: command.kind.into(),
            name: &command.name,
            description: &command.description,
            nsfw: command.nsfw,
            default_member_permissions: command
                .default_member_permissions
                .map(|permissions| permissions.bits().to_string()),
            options: command.options.iter().map(OptionPayload::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct OptionPayload<'a> {
    #[serde(rename = "type")]
    kind: u8,
    name: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    description: &'a str,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    required: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    choices: Vec<ChoicePayload<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    options: Vec<OptionPayload<'a>>,
}

impl<'a> From<&'a CommandOption> for OptionPayload<'a> {
    fn from(option: &'a CommandOption) -> Self {
        Self {
            kind: option.kind.into(),
            name: &option.name,
            description: &option.description,
            required: option.required,
            choices: option
                .choices
                .iter()
                .map(|choice| ChoicePayload {
                    name: &choice.name,
                    value: &choice.value,
                })
                .collect(),
            options: option.options.iter().map(OptionPayload::from).collect(),
        }
    }
}

#[derive(Serialize)]
struct ChoicePayload<'a> {
    name: &'a str,
    value: &'a Value,
}

#[cfg(test)]
mod tests;
