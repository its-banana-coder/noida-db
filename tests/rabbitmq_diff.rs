#![cfg(feature = "rabbitmq")]

use lapin::{Connection, ConnectionProperties};
use std::net::SocketAddr;
use std::time::Duration;

async fn get_reference_uri() -> Option<String> {
    if let Ok(addr) = std::env::var("NOIDA_RABBITMQ_REF") {
        Some(format!("amqp://{}/%2f", addr))
    } else if std::net::TcpStream::connect_timeout(
        &"127.0.0.1:5672".parse::<SocketAddr>().unwrap(),
        Duration::from_millis(50),
    )
    .is_ok()
    {
        Some("amqp://127.0.0.1:5672/%2f".to_string())
    } else {
        None
    }
}

fn start_noida_rabbitmq() -> SocketAddr {
    noida::services::start("rabbitmq", "127.0.0.1:0").unwrap().unwrap()
}

#[tokio::test]
async fn test_rabbitmq_diff() {
    let Some(ref_uri) = get_reference_uri().await else {
        println!(
            "SKIPPED: no reference RabbitMQ server (set NOIDA_RABBITMQ_REF or run one on 127.0.0.1:5672)"
        );
        return;
    };

    let noida_addr = start_noida_rabbitmq();
    let noida_uri = format!("amqp://127.0.0.1:{}/%2f", noida_addr.port());

    let noida_conn =
        Connection::connect(&noida_uri, ConnectionProperties::default()).await.unwrap();
    let ref_conn = Connection::connect(&ref_uri, ConnectionProperties::default()).await.unwrap();

    let noida_chan = noida_conn.create_channel().await.unwrap();
    let ref_chan = ref_conn.create_channel().await.unwrap();

    // 1. Basic declare + publish + get
    let _ = noida_chan
        .queue_delete("test_diff_queue".into(), lapin::options::QueueDeleteOptions::default())
        .await;
    let _ = ref_chan
        .queue_delete("test_diff_queue".into(), lapin::options::QueueDeleteOptions::default())
        .await;

    noida_chan
        .queue_declare(
            "test_diff_queue".into(),
            lapin::options::QueueDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();
    ref_chan
        .queue_declare(
            "test_diff_queue".into(),
            lapin::options::QueueDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .unwrap();

    noida_chan
        .basic_publish(
            "".into(),
            "test_diff_queue".into(),
            lapin::options::BasicPublishOptions::default(),
            b"diff msg",
            lapin::BasicProperties::default(),
        )
        .await
        .unwrap();
    ref_chan
        .basic_publish(
            "".into(),
            "test_diff_queue".into(),
            lapin::options::BasicPublishOptions::default(),
            b"diff msg",
            lapin::BasicProperties::default(),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let noida_get = noida_chan
        .basic_get("test_diff_queue".into(), lapin::options::BasicGetOptions::default())
        .await
        .unwrap();
    let ref_get = ref_chan
        .basic_get("test_diff_queue".into(), lapin::options::BasicGetOptions::default())
        .await
        .unwrap();

    assert!(noida_get.is_some() && ref_get.is_some());
    assert_eq!(noida_get.unwrap().delivery.data, ref_get.unwrap().delivery.data);

    // 2. Error responses (e.g., publishing to nonexistent exchange)
    // Wait, rabbitmq doesn't typically error channel on publish to nonexistent exchange immediately with AMQP 0-9-1 basic.publish unless mandatory is set, but let's check declare exchange invalid behavior

    // We will expand these in following steps as we add the features
}
