//! A serenity bot that publishes its stats, heartbeats and commands.
//!
//! ```sh
//! DISCORD_TOKEN=... TDL_TOKEN=... cargo run --example bot
//! ```

use std::env;

use serenity::all::{Client, Context, EventHandler, GatewayIntents, Ready};
use serenity::async_trait;
use thediscordlist_serenity::Autoposter;

struct Handler;

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, _: Context, ready: Ready) {
        println!("{} is connected", ready.user.name);
    }
}

#[tokio::main]
async fn main() {
    let poster = Autoposter::builder(env::var("TDL_TOKEN").unwrap_or_default())
        .on_event(|event| println!("TheDiscordList: {event}"))
        .build();

    let token = env::var("DISCORD_TOKEN").expect("set DISCORD_TOKEN");
    let mut client = Client::builder(token, GatewayIntents::GUILDS)
        .event_handler(Handler)
        .event_handler(poster.clone())
        .await
        .expect("create the client");
    poster.attach_shard_manager(client.shard_manager.clone());

    if let Err(error) = client.start_autosharded().await {
        eprintln!("client error: {error}");
    }
}
