//! Pub/Sub, ported from Redis's pubsub.c: channels, patterns and shard
//! channels. Messages go straight to each subscriber's connection, as
//! RESP3 pushes or RESP2 arrays.

use std::collections::HashMap;

use super::engine::{Command, Ctx, Engine, Reply, cmd, container, help_reply};
use super::glob;
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    cmd("subscribe", subscribe),
    cmd("unsubscribe", unsubscribe),
    cmd("psubscribe", psubscribe),
    cmd("punsubscribe", punsubscribe),
    cmd("ssubscribe", ssubscribe),
    cmd("sunsubscribe", sunsubscribe),
    cmd("publish", publish),
    cmd("spublish", spublish),
    container("pubsub", pubsub_help, PUBSUB),
];

static PUBSUB: &[Command] = &[
    cmd("help", pubsub_help),
    cmd("channels", pubsub_channels),
    cmd("numsub", pubsub_numsub),
    cmd("numpat", pubsub_numpat),
    cmd("shardchannels", pubsub_shardchannels),
    cmd("shardnumsub", pubsub_shardnumsub),
];

/// The three kinds of subscription.
#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Channel,
    Pattern,
    Shard,
}

/// Server-side subscriptions: name -> subscribers in subscription order.
#[derive(Default)]
pub struct PubSub {
    channels: HashMap<Vec<u8>, Vec<u64>>,
    patterns: HashMap<Vec<u8>, Vec<u64>>,
    shard: HashMap<Vec<u8>, Vec<u64>>,
}

impl PubSub {
    fn map(&mut self, kind: Kind) -> &mut HashMap<Vec<u8>, Vec<u64>> {
        match kind {
            Kind::Channel => &mut self.channels,
            Kind::Pattern => &mut self.patterns,
            Kind::Shard => &mut self.shard,
        }
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        (self.channels.len(), self.patterns.len(), self.shard.len())
    }
}

/// A client's subscriptions, in the order it made them.
#[derive(Default)]
pub struct Subscriptions {
    pub channels: Vec<Vec<u8>>,
    pub patterns: Vec<Vec<u8>>,
    pub shard: Vec<Vec<u8>>,
}

impl Subscriptions {
    fn list(&mut self, kind: Kind) -> &mut Vec<Vec<u8>> {
        match kind {
            Kind::Channel => &mut self.channels,
            Kind::Pattern => &mut self.patterns,
            Kind::Shard => &mut self.shard,
        }
    }

    /// Any subscription at all (Redis's CLIENT_PUBSUB flag).
    pub fn active(&self) -> bool {
        !(self.channels.is_empty() && self.patterns.is_empty() && self.shard.is_empty())
    }

    /// The count in (un)subscribe replies: channels and patterns share
    /// one, shard channels have their own.
    fn count(&self, kind: Kind) -> i64 {
        match kind {
            Kind::Shard => self.shard.len() as i64,
            _ => (self.channels.len() + self.patterns.len()) as i64,
        }
    }
}

/// Commands a RESP2 client may still send while subscribed.
pub fn allowed_while_subscribed(name: &str) -> bool {
    matches!(
        name,
        "ping"
            | "subscribe"
            | "ssubscribe"
            | "unsubscribe"
            | "sunsubscribe"
            | "psubscribe"
            | "punsubscribe"
            | "quit"
            | "reset"
    )
}

fn push(kind: &str, a: Value, b: Value) -> Value {
    Value::Push(vec![Value::bulk(kind), a, b])
}

impl Engine {
    /// Sends a push to a client's connection.
    pub fn deliver(&mut self, id: u64, v: Value) {
        let Some(c) = self.clients.get_mut(&id) else { return };
        match &c.conn.push {
            Some(send) => {
                let mut out = Vec::new();
                super::resp::encode(&v, c.resp, &mut out);
                send(out);
            }
            None => c.pushed.push(v),
        }
    }

    /// Pushes delivered to a connection without a socket (tests).
    pub fn take_pushes(&mut self, id: u64) -> Vec<Value> {
        self.clients.get_mut(&id).map(|c| std::mem::take(&mut c.pushed)).unwrap_or_default()
    }

    fn subscribe(&mut self, id: u64, kind: Kind, name: &[u8]) -> Value {
        let c = self.clients.get_mut(&id).expect("registered");
        let list = c.subs.list(kind);
        if !list.iter().any(|n| n == name) {
            list.push(name.to_vec());
            self.pubsub.map(kind).entry(name.to_vec()).or_default().push(id);
        }
        let c = &self.clients[&id];
        let word = match kind {
            Kind::Channel => "subscribe",
            Kind::Pattern => "psubscribe",
            Kind::Shard => "ssubscribe",
        };
        push(word, Value::bulk(name), Value::Integer(c.subs.count(kind)))
    }

    fn unsubscribe(&mut self, id: u64, kind: Kind, name: Option<&[u8]>) -> Value {
        let c = self.clients.get_mut(&id).expect("registered");
        if let Some(name) = name {
            let list = c.subs.list(kind);
            if let Some(i) = list.iter().position(|n| n == name) {
                list.remove(i);
                let map = self.pubsub.map(kind);
                if let Some(ids) = map.get_mut(name) {
                    ids.retain(|x| *x != id);
                    if ids.is_empty() {
                        map.remove(name);
                    }
                }
            }
        }
        let word = match kind {
            Kind::Channel => "unsubscribe",
            Kind::Pattern => "punsubscribe",
            Kind::Shard => "sunsubscribe",
        };
        let count = self.clients[&id].subs.count(kind);
        push(word, name.map_or(Value::Null, Value::bulk), Value::Integer(count))
    }

    /// Unsubscribes from everything of `kind`, one reply per name (or a
    /// single nil reply when there was nothing).
    fn unsubscribe_all(&mut self, id: u64, kind: Kind) -> Vec<Value> {
        let names = self.clients[&id].subs.clone_list(kind);
        if names.is_empty() {
            return vec![self.unsubscribe(id, kind, None)];
        }
        names.iter().map(|n| self.unsubscribe(id, kind, Some(n))).collect()
    }

    /// Drops every subscription silently (RESET, disconnect).
    pub(crate) fn unsubscribe_everything(&mut self, id: u64) {
        if !self.clients.get(&id).is_some_and(|c| c.subs.active()) {
            return;
        }
        for kind in [Kind::Channel, Kind::Pattern, Kind::Shard] {
            self.unsubscribe_all(id, kind);
        }
    }

    /// PUBLISH / SPUBLISH: delivers and counts receivers.
    fn publish(&mut self, channel: &[u8], msg: &[u8], shard: bool) -> i64 {
        let mut n = 0;
        let kind = if shard { Kind::Shard } else { Kind::Channel };
        let word = if shard { "smessage" } else { "message" };
        let ids = self.pubsub.map(kind).get(channel).cloned().unwrap_or_default();
        for id in ids {
            self.deliver(id, push(word, Value::bulk(channel), Value::bulk(msg)));
            n += 1;
        }
        if shard {
            return n;
        }
        let matching: Vec<(Vec<u8>, Vec<u64>)> = self
            .pubsub
            .patterns
            .iter()
            .filter(|(p, _)| glob::matches(p, channel, false))
            .map(|(p, ids)| (p.clone(), ids.clone()))
            .collect();
        for (pattern, ids) in matching {
            for id in ids {
                let m = Value::Push(vec![
                    Value::bulk("pmessage"),
                    Value::bulk(&pattern),
                    Value::bulk(channel),
                    Value::bulk(msg),
                ]);
                self.deliver(id, m);
                n += 1;
            }
        }
        n
    }
}

impl Subscriptions {
    fn clone_list(&self, kind: Kind) -> Vec<Vec<u8>> {
        match kind {
            Kind::Channel => self.channels.clone(),
            Kind::Pattern => self.patterns.clone(),
            Kind::Shard => self.shard.clone(),
        }
    }
}

fn many(replies: Vec<Value>) -> Value {
    if replies.len() == 1 { replies.into_iter().next().unwrap() } else { Value::Many(replies) }
}

fn subscribe_generic(ctx: &mut Ctx, a: &[Vec<u8>], kind: Kind, name: &str) -> Reply {
    // In EXEC a SUBSCRIBE is tolerated for backward compatibility, but
    // SSUBSCRIBE isn't.
    if ctx.deny_blocking && (!ctx.in_exec || kind == Kind::Shard) {
        return Err(Value::err(format!("ERR {name} isn't allowed for a DENY BLOCKING client")));
    }
    let id = ctx.session.id;
    Ok(many(a[1..].iter().map(|n| ctx.engine.subscribe(id, kind, n)).collect()))
}

fn unsubscribe_generic(ctx: &mut Ctx, a: &[Vec<u8>], kind: Kind) -> Reply {
    let id = ctx.session.id;
    let replies = if a.len() == 1 {
        ctx.engine.unsubscribe_all(id, kind)
    } else {
        a[1..].iter().map(|n| ctx.engine.unsubscribe(id, kind, Some(n))).collect()
    };
    Ok(many(replies))
}

fn subscribe(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    subscribe_generic(ctx, a, Kind::Channel, "SUBSCRIBE")
}

fn psubscribe(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    subscribe_generic(ctx, a, Kind::Pattern, "PSUBSCRIBE")
}

fn ssubscribe(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    subscribe_generic(ctx, a, Kind::Shard, "SSUBSCRIBE")
}

fn unsubscribe(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    unsubscribe_generic(ctx, a, Kind::Channel)
}

fn punsubscribe(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    unsubscribe_generic(ctx, a, Kind::Pattern)
}

fn sunsubscribe(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    unsubscribe_generic(ctx, a, Kind::Shard)
}

fn publish(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(ctx.engine.publish(&a[1], &a[2], false)))
}

fn spublish(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(ctx.engine.publish(&a[1], &a[2], true)))
}

fn pubsub_help(_: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(help_reply(
        "pubsub",
        &[
            "CHANNELS [<pattern>]",
            "    Return the currently active channels matching a <pattern> (default: '*').",
            "NUMPAT",
            "    Return number of subscriptions to patterns.",
            "NUMSUB [<channel> ...]",
            "    Return the number of subscribers for the specified channels, excluding",
            "    pattern subscriptions(default: no channels).",
            "SHARDCHANNELS [<pattern>]",
            "    Return the currently active shard level channels matching a <pattern> (default: '*').",
            "SHARDNUMSUB [<shardchannel> ...]",
            "    Return the number of subscribers for the specified shard level channel(s)",
        ],
    ))
}

fn channel_list(map: &HashMap<Vec<u8>, Vec<u64>>, a: &[Vec<u8>]) -> Reply {
    if a.len() > 3 {
        return Err(subcommand_syntax(a));
    }
    let pattern = a.get(2);
    let mut names: Vec<&Vec<u8>> =
        map.keys().filter(|c| pattern.is_none_or(|p| glob::matches(p, c, false))).collect();
    names.sort();
    Ok(Value::Array(names.into_iter().map(Value::bulk).collect()))
}

fn numsub(map: &HashMap<Vec<u8>, Vec<u64>>, a: &[Vec<u8>]) -> Reply {
    let mut out = Vec::new();
    for ch in &a[2..] {
        out.push(Value::bulk(ch));
        out.push(Value::Integer(map.get(ch).map_or(0, |v| v.len()) as i64));
    }
    Ok(Value::Array(out))
}

/// `addReplySubcommandSyntaxError`.
fn subcommand_syntax(a: &[Vec<u8>]) -> Value {
    Value::err(format!(
        "ERR unknown subcommand or wrong number of arguments for '{}'. Try PUBSUB HELP.",
        String::from_utf8_lossy(&a[1])
    ))
}

fn pubsub_channels(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    channel_list(&ctx.engine.pubsub.channels, a)
}

fn pubsub_shardchannels(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    channel_list(&ctx.engine.pubsub.shard, a)
}

fn pubsub_numsub(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    numsub(&ctx.engine.pubsub.channels, a)
}

fn pubsub_shardnumsub(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    numsub(&ctx.engine.pubsub.shard, a)
}

fn pubsub_numpat(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    Ok(Value::Integer(ctx.engine.pubsub.patterns.len() as i64))
}
