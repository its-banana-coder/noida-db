use crate::rabbitmq::codec::write_frame;
use crate::rabbitmq::engine::{Engine, Message};
use amq_protocol::frame::{AMQPContentHeader, AMQPFrame};
use amq_protocol::protocol::{AMQPClass, basic, channel, connection, exchange, queue};
use amq_protocol::types::{AMQPValue, FieldTable};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;

fn send_method(stream: &mut TcpStream, channel_id: u16, method: AMQPClass) -> std::io::Result<()> {
    write_frame(stream, channel_id, &AMQPFrame::Method(channel_id, method))
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
        amq_protocol::types::ShortString,
        amq_protocol::types::ShortString,
    )> = None;
    let mut pending_body = Vec::new();
    let mut expected_body_size = 0;

    let mut consumers: Vec<(amq_protocol::types::ShortString, u16)> = Vec::new();

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
                                    pending_publish =
                                        Some((args.exchange.clone(), args.routing_key.clone()));
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
                                        let _ = send_method(
                                            &mut stream,
                                            channel_id,
                                            AMQPClass::Basic(basic::AMQPMethod::GetOk(
                                                basic::GetOk {
                                                    delivery_tag: 1,
                                                    redelivered: false,
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
                                    if expected_body_size == 0
                                        && let Some((exchange, rk)) = pending_publish.take()
                                    {
                                        engine.publish(
                                            exchange,
                                            rk,
                                            Message {
                                                content_type: None,
                                                data: pending_body.clone(),
                                            },
                                        );
                                    }
                                }
                            }
                            AMQPFrame::Body(_channel_id, payload) => {
                                if pending_publish.is_some() {
                                    pending_body.extend_from_slice(&payload);
                                    if pending_body.len() as u64 >= expected_body_size
                                        && let Some((exchange, rk)) = pending_publish.take()
                                    {
                                        engine.publish(
                                            exchange,
                                            rk,
                                            Message {
                                                content_type: None,
                                                data: pending_body.clone(),
                                            },
                                        );
                                        pending_body.clear();
                                    }
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
                        let deliver = basic::Deliver {
                            consumer_tag: "test_consumer".into(),
                            delivery_tag: 1,
                            redelivered: false,
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
            let queue = engine.declare_queue(args.queue);
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
        AMQPClass::Basic(basic::AMQPMethod::Ack(_)) => {}
        AMQPClass::Basic(basic::AMQPMethod::Nack(_)) => {}
        AMQPClass::Basic(basic::AMQPMethod::Reject(_)) => {}
        _ => {}
    }
}
