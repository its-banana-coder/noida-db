use crate::rabbitmq::codec::write_frame;
use crate::rabbitmq::engine::{Engine, Message};
use amq_protocol::frame::{AMQPContentHeader, AMQPFrame};
use amq_protocol::protocol::{AMQPClass, basic, channel, confirm, connection, exchange, queue};
use amq_protocol::types::{AMQPValue, FieldTable};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Write};
use std::net::TcpStream;

fn send_method(stream: &mut TcpStream, channel_id: u16, method: AMQPClass) -> std::io::Result<()> {
    write_frame(stream, channel_id, &AMQPFrame::Method(channel_id, method))
}

/// Sends `basic.ack` for a just-stored publish, if `channel_id` called
/// `confirm.select`. This is a single in-memory node, so a publish that
/// reached `engine.publish` always "succeeds" — there's no real failure
/// mode to `nack` here.
fn ack_if_confirming(
    stream: &mut TcpStream,
    channel_id: u16,
    confirm_channels: &HashSet<u16>,
    delivery_tags: &mut HashMap<u16, u64>,
) {
    if confirm_channels.contains(&channel_id) {
        let tag = delivery_tags.entry(channel_id).or_insert(0);
        *tag += 1;
        let _ = send_method(
            stream,
            channel_id,
            AMQPClass::Basic(basic::AMQPMethod::Ack(basic::Ack {
                delivery_tag: *tag,
                multiple: false,
            })),
        );
    }
}

pub fn handle_connection(mut stream: TcpStream, mut engine: Engine) {
    let mut header = [0u8; 8];
    if stream.read_exact(&mut header).is_err() {
        return;
    }

    if header != b"AMQP\x00\x00\x09\x01"[..] {
        let _ = stream.write_all(b"AMQP\x00\x00\x09\x01");
        return;
    }

    let mut sp_map = BTreeMap::new();
    sp_map.insert("product".into(), AMQPValue::LongString("RabbitMQ".into()));
    sp_map.insert("version".into(), AMQPValue::LongString("3.13.7".into()));

    let mut cap_map = BTreeMap::new();
    cap_map.insert("publisher_confirms".into(), AMQPValue::Boolean(true));
    cap_map.insert("exchange_exchange_bindings".into(), AMQPValue::Boolean(true));
    cap_map.insert("basic.nack".into(), AMQPValue::Boolean(true));
    cap_map.insert("consumer_cancel_notify".into(), AMQPValue::Boolean(true));
    cap_map.insert("connection.blocked".into(), AMQPValue::Boolean(true));
    cap_map.insert("consumer_priorities".into(), AMQPValue::Boolean(true));
    cap_map.insert("authentication_failure_close".into(), AMQPValue::Boolean(true));
    cap_map.insert("per_consumer_qos".into(), AMQPValue::Boolean(true));
    cap_map.insert("direct_reply_to".into(), AMQPValue::Boolean(true));

    sp_map.insert("capabilities".into(), AMQPValue::FieldTable(FieldTable::from(cap_map)));

    let start = connection::Start {
        version_major: 0,
        version_minor: 9,
        server_properties: FieldTable::from(sp_map),
        mechanisms: "PLAIN AMQPLAIN".into(),
        locales: "en_US".into(),
    };
    if send_method(&mut stream, 0, AMQPClass::Connection(connection::AMQPMethod::Start(start)))
        .is_err()
    {
        return;
    }

    let mut pending_publish: Option<(
        u16,
        amq_protocol::types::ShortString,
        amq_protocol::types::ShortString,
    )> = None;
    let mut pending_expiration: Option<std::time::Instant> = None;
    let mut pending_body = Vec::new();
    let mut expected_body_size = 0;

    let mut consumers: Vec<(amq_protocol::types::ShortString, u16)> = Vec::new();

    // Channels that called confirm.select — every publish on one of these
    // gets a basic.ack back once stored (this is a single in-memory node,
    // so "stored" is immediate and never fails/nacks).
    let mut confirm_channels: HashSet<u16> = HashSet::new();
    let mut delivery_tags: HashMap<u16, u64> = HashMap::new();
    
    // For nack/reject, we need to track unacked messages
    // (channel_id, delivery_tag) -> (queue, Message)
    let mut unacked_messages: HashMap<(u16, u64), (amq_protocol::types::ShortString, Message)> = HashMap::new();
    let mut next_delivery_tag: HashMap<u16, u64> = HashMap::new();

    stream.set_nonblocking(true).unwrap();
    let mut incomplete_buffer = Vec::new();

    loop {
        let mut buffer = [0u8; 8192];
        match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(bytes_read) => {
                incomplete_buffer.extend_from_slice(&buffer[..bytes_read]);

                while !incomplete_buffer.is_empty() {
                    let mut parse_success = false;
                    let mut remaining = Vec::new();

                    if let Ok((rest, frame)) =
                        amq_protocol::frame::parse_frame(incomplete_buffer.as_slice())
                    {
                        parse_success = true;
                        remaining = rest.to_vec();
                        match frame {
                            AMQPFrame::Method(channel_id, method) => {
                                if let AMQPClass::Basic(basic::AMQPMethod::Publish(args)) = &method
                                {
                                    pending_publish = Some((
                                        channel_id,
                                        args.exchange.clone(),
                                        args.routing_key.clone(),
                                    ));
                                } else if let AMQPClass::Confirm(confirm::AMQPMethod::Select(_)) =
                                    &method
                                {
                                    confirm_channels.insert(channel_id);
                                    let _ = send_method(
                                        &mut stream,
                                        channel_id,
                                        AMQPClass::Confirm(confirm::AMQPMethod::SelectOk(
                                            confirm::SelectOk {},
                                        )),
                                    );
                                } else if let AMQPClass::Basic(basic::AMQPMethod::Consume(args)) =
                                    &method
                                {
                                    let consumer_tag = if args.consumer_tag.as_str().is_empty() {
                                        "amq.ctag-123".into()
                                    } else {
                                        args.consumer_tag.clone()
                                    };
                                    consumers.push((args.queue.clone(), channel_id));
                                    let _ = send_method(
                                        &mut stream,
                                        channel_id,
                                        AMQPClass::Basic(basic::AMQPMethod::ConsumeOk(
                                            basic::ConsumeOk { consumer_tag },
                                        )),
                                    );
                                } else if let AMQPClass::Basic(basic::AMQPMethod::Get(args)) =
                                    &method
                                {
                                    if let Some(msg) = engine.basic_get(&args.queue) {
                                        let tag = next_delivery_tag.entry(channel_id).or_insert(1);
                                        let current_tag = *tag;
                                        *tag += 1;
                                        unacked_messages.insert((channel_id, current_tag), (args.queue.clone(), msg.clone()));

                                        let _ = send_method(
                                            &mut stream,
                                            channel_id,
                                            AMQPClass::Basic(basic::AMQPMethod::GetOk(
                                                basic::GetOk {
                                                    delivery_tag: current_tag,
                                                    redelivered: msg.redelivered,
                                                    exchange: "".into(),
                                                    routing_key: args.queue.clone(),
                                                    message_count: 0,
                                                },
                                            )),
                                        );
                                        let header = AMQPContentHeader {
                                            class_id: 60,
                                            body_size: msg.data.len() as u64,
                                            properties: amq_protocol::protocol::basic::AMQPProperties::default(),
                                        };
                                        let _ = write_frame(
                                            &mut stream,
                                            channel_id,
                                            &AMQPFrame::Header(channel_id, header),
                                        );
                                        let _ = write_frame(
                                            &mut stream,
                                            channel_id,
                                            &AMQPFrame::Body(channel_id, msg.data),
                                        );
                                    } else {
                                        let _ = send_method(
                                            &mut stream,
                                            channel_id,
                                            AMQPClass::Basic(basic::AMQPMethod::GetEmpty(
                                                basic::GetEmpty {},
                                            )),
                                        );
                                    }
                                } else if let AMQPClass::Basic(basic::AMQPMethod::Ack(args)) = &method {
                                    if args.multiple {
                                        unacked_messages.retain(|&(c, tag), _| c != channel_id || tag > args.delivery_tag);
                                    } else {
                                        unacked_messages.remove(&(channel_id, args.delivery_tag));
                                    }
                                } else if let AMQPClass::Basic(basic::AMQPMethod::Nack(args)) = &method {
                                    let mut to_nack = Vec::new();
                                    if args.multiple {
                                        let tags: Vec<_> = unacked_messages.keys().filter(|&&(c, tag)| c == channel_id && tag <= args.delivery_tag).copied().collect();
                                        for k in tags {
                                            if let Some(v) = unacked_messages.remove(&k) {
                                                to_nack.push(v);
                                            }
                                        }
                                    } else {
                                        if let Some(v) = unacked_messages.remove(&(channel_id, args.delivery_tag)) {
                                            to_nack.push(v);
                                        }
                                    }
                                    for (q, msg) in to_nack {
                                        if args.requeue {
                                            engine.requeue(&q, msg);
                                        } else {
                                            engine.dead_letter(&q, msg);
                                        }
                                    }
                                } else if let AMQPClass::Basic(basic::AMQPMethod::Reject(args)) = &method {
                                    if let Some((q, msg)) = unacked_messages.remove(&(channel_id, args.delivery_tag)) {
                                        if args.requeue {
                                            engine.requeue(&q, msg);
                                        } else {
                                            engine.dead_letter(&q, msg);
                                        }
                                    }
                                } else if let AMQPClass::Basic(basic::AMQPMethod::Qos(_args)) =
                                    &method
                                {
                                    let _ = send_method(
                                        &mut stream,
                                        channel_id,
                                        AMQPClass::Basic(basic::AMQPMethod::QosOk(basic::QosOk {})),
                                    );
                                } else {
                                    handle_method(&mut stream, channel_id, method, &mut engine);
                                }
                            }
                            AMQPFrame::Heartbeat => {
                                let _ = write_frame(&mut stream, 0, &AMQPFrame::Heartbeat);
                            }
                            AMQPFrame::Header(_channel_id, header) => {
                                if pending_publish.is_some() {
                                    expected_body_size = header.body_size;
                                    pending_body.clear();
                                    
                                    pending_expiration = None;
                                    if let Some(exp_str) = header.properties.expiration() {
                                        if let Ok(millis) = exp_str.as_str().parse::<u64>() {
                                            pending_expiration = Some(std::time::Instant::now() + std::time::Duration::from_millis(millis));
                                        }
                                    }

                                    if expected_body_size == 0
                                        && let Some((pub_channel, exchange, rk)) =
                                            pending_publish.take()
                                    {
                                        engine.publish(
                                            exchange,
                                            rk.clone(),
                                            Message {
                                                content_type: None,
                                                routing_key: rk,
                                                data: pending_body.clone(),
                                                expiration: pending_expiration.take(),
                                                redelivered: false,
                                            },
                                        );
                                        ack_if_confirming(
                                            &mut stream,
                                            pub_channel,
                                            &confirm_channels,
                                            &mut delivery_tags,
                                        );
                                    }
                                }
                            }
                            AMQPFrame::Body(_channel_id, payload) if pending_publish.is_some() => {
                                pending_body.extend_from_slice(&payload);
                                if pending_body.len() as u64 >= expected_body_size
                                    && let Some((pub_channel, exchange, rk)) =
                                        pending_publish.take()
                                {
                                    engine.publish(
                                        exchange,
                                        rk.clone(),
                                        Message { content_type: None, routing_key: rk, data: pending_body.clone(), expiration: pending_expiration.take(), redelivered: false },
                                    );
                                    ack_if_confirming(
                                        &mut stream,
                                        pub_channel,
                                        &confirm_channels,
                                        &mut delivery_tags,
                                    );
                                    pending_body.clear();
                                }
                            }
                            _ => {}
                        }
                    }

                    if parse_success {
                        incomplete_buffer = remaining;
                    } else {
                        break;
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                for (queue, channel_id) in &consumers {
                    if let Some(msg) = engine.basic_get(queue) {
                        let tag = next_delivery_tag.entry(*channel_id).or_insert(1);
                        let current_tag = *tag;
                        *tag += 1;
                        unacked_messages.insert((*channel_id, current_tag), (queue.clone(), msg.clone()));

                        let deliver = basic::Deliver {
                            consumer_tag: "test_consumer".into(),
                            delivery_tag: current_tag,
                            redelivered: msg.redelivered,
                            exchange: "".into(),
                            routing_key: queue.clone(),
                        };
                        let _ = send_method(
                            &mut stream,
                            *channel_id,
                            AMQPClass::Basic(basic::AMQPMethod::Deliver(deliver)),
                        );

                        let header = AMQPContentHeader {
                            class_id: 60,
                            body_size: msg.data.len() as u64,
                            properties: amq_protocol::protocol::basic::AMQPProperties::default(),
                        };
                        let _ = write_frame(
                            &mut stream,
                            *channel_id,
                            &AMQPFrame::Header(*channel_id, header),
                        );

                        let _ = write_frame(
                            &mut stream,
                            *channel_id,
                            &AMQPFrame::Body(*channel_id, msg.data),
                        );
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(_) => return,
        }
    }
}

fn handle_method(stream: &mut TcpStream, channel_id: u16, method: AMQPClass, engine: &mut Engine) {
    match method {
        AMQPClass::Connection(connection::AMQPMethod::StartOk(_)) => {
            let tune = connection::Tune { channel_max: 2047, frame_max: 131072, heartbeat: 60 };
            let _ =
                send_method(stream, 0, AMQPClass::Connection(connection::AMQPMethod::Tune(tune)));
        }
        AMQPClass::Connection(connection::AMQPMethod::TuneOk(_)) => {}
        AMQPClass::Connection(connection::AMQPMethod::Open(_)) => {
            let open_ok = connection::OpenOk {};
            let _ = send_method(
                stream,
                0,
                AMQPClass::Connection(connection::AMQPMethod::OpenOk(open_ok)),
            );
        }
        AMQPClass::Connection(connection::AMQPMethod::Close(_)) => {
            let _ = send_method(
                stream,
                0,
                AMQPClass::Connection(connection::AMQPMethod::CloseOk(connection::CloseOk {})),
            );
        }
        AMQPClass::Channel(channel::AMQPMethod::Open(_)) => {
            let _ = send_method(
                stream,
                channel_id,
                AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk {})),
            );
        }
        AMQPClass::Channel(channel::AMQPMethod::Close(_)) => {
            let _ = send_method(
                stream,
                channel_id,
                AMQPClass::Channel(channel::AMQPMethod::CloseOk(channel::CloseOk {})),
            );
        }
        AMQPClass::Exchange(exchange::AMQPMethod::Declare(args)) => {
            engine.declare_exchange(args.exchange, args.kind);
            let _ = send_method(
                stream,
                channel_id,
                AMQPClass::Exchange(exchange::AMQPMethod::DeclareOk(exchange::DeclareOk {})),
            );
        }
        AMQPClass::Queue(queue::AMQPMethod::Declare(args)) => {
            let queue = engine.declare_queue(args.queue, args.arguments);
            let _ = send_method(
                stream,
                channel_id,
                AMQPClass::Queue(queue::AMQPMethod::DeclareOk(queue::DeclareOk {
                    queue,
                    message_count: 0,
                    consumer_count: 0,
                })),
            );
        }
        AMQPClass::Queue(queue::AMQPMethod::Bind(args)) => {
            engine.bind_queue(args.queue, args.exchange, args.routing_key);
            let _ = send_method(
                stream,
                channel_id,
                AMQPClass::Queue(queue::AMQPMethod::BindOk(queue::BindOk {})),
            );
        }
        _ => {}
    }
}
