//! Official [TheDiscordList](https://thediscordlist.com) integration for
//! [serenity](https://docs.rs/serenity) and [poise](https://docs.rs/poise).
//!
//! The [`Autoposter`] publishes your bot's server count, per-shard heartbeats
//! and global slash commands. Register it as an extra event handler and it
//! starts on the first `READY`:
//!
//! ```no_run
//! use serenity::all::{Client, GatewayIntents};
//! use thediscordlist_serenity::Autoposter;
//!
//! # async fn run() -> serenity::Result<()> {
//! let poster = Autoposter::new(std::env::var("TDL_TOKEN").unwrap_or_default());
//!
//! let mut client = Client::builder(std::env::var("DISCORD_TOKEN").unwrap_or_default(), GatewayIntents::GUILDS)
//!     .event_handler(poster.clone())
//!     .await?;
//! poster.attach_shard_manager(client.shard_manager.clone());
//!
//! client.start().await
//! # }
//! ```
//!
//! With the `webhooks` feature, [`webhooks::verify`] checks the signature of
//! TheDiscordList's outbound webhooks. It does not need serenity:
//! `default-features = false, features = ["webhooks"]` builds it alone.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[cfg(feature = "autopost")]
mod autopost;
#[cfg(feature = "autopost")]
mod http;
#[cfg(feature = "webhooks")]
pub mod webhooks;

#[cfg(feature = "autopost")]
pub use autopost::{Autoposter, AutoposterBuilder, Event, PostResult, Publisher, Skip};

/// The API base URL used unless [`AutoposterBuilder::base_url`] overrides it.
#[cfg(feature = "autopost")]
pub const DEFAULT_BASE_URL: &str = "https://thediscordlist.com/api";

/// Poisoning only means another thread panicked mid-update; the state is still usable.
#[cfg(feature = "autopost")]
fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
