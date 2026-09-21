//! Connection commands: PING, ECHO, SELECT, QUIT.

use super::engine::{Command, Ctx, Reply, arity_error, db_arg};
use super::resp::Value;

pub static COMMANDS: &[Command] = &[
    Command { name: "ping", arity: -1, handler: ping },
    Command { name: "echo", arity: 2, handler: echo },
    Command { name: "select", arity: 2, handler: select },
    Command { name: "quit", arity: -1, handler: quit },
];

fn ping(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    match a.len() {
        1 => Ok(Value::Simple("PONG".into())),
        2 => Ok(Value::bulk(&a[1])),
        _ => Err(arity_error("ping")),
    }
}

fn echo(_: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    Ok(Value::bulk(&a[1]))
}

fn select(ctx: &mut Ctx, a: &[Vec<u8>]) -> Reply {
    ctx.session.db = db_arg(&a[1])?;
    Ok(Value::ok())
}

fn quit(ctx: &mut Ctx, _: &[Vec<u8>]) -> Reply {
    ctx.session.closing = true;
    Ok(Value::ok())
}
