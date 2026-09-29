#[cfg(feature = "rabbitmq")]
mod test {
    use lapin::{Connection, ConnectionProperties};
    use noida::services::start;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn rabbitmq_connect_declare() {
        let addr = start("rabbitmq", "127.0.0.1:0").unwrap().unwrap();
        let uri = format!("amqp://127.0.0.1:{}/%2f", addr.port());
        let conn = Connection::connect(&uri, ConnectionProperties::default()).await.unwrap();
        let channel = conn.create_channel().await.unwrap();
        channel
            .exchange_declare(
                "test_exchange".into(),
                lapin::ExchangeKind::Topic,
                lapin::options::ExchangeDeclareOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();
        channel
            .queue_declare(
                "test_queue".into(),
                lapin::options::QueueDeclareOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();
        channel
            .queue_bind(
                "test_queue".into(),
                "test_exchange".into(),
                "test_routing_key".into(),
                lapin::options::QueueBindOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn rabbitmq_publish_consume() {
        let addr = start("rabbitmq", "127.0.0.1:0").unwrap().unwrap();
        let uri = format!("amqp://127.0.0.1:{}/%2f", addr.port());
        let conn = Connection::connect(&uri, ConnectionProperties::default()).await.unwrap();
        let channel = conn.create_channel().await.unwrap();

        channel
            .queue_declare(
                "test_pub_queue".into(),
                lapin::options::QueueDeclareOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();

        let mut consumer = channel
            .basic_consume(
                "test_pub_queue".into(),
                "test_consumer".into(),
                lapin::options::BasicConsumeOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();

        channel
            .basic_publish(
                "".into(),
                "test_pub_queue".into(),
                lapin::options::BasicPublishOptions::default(),
                b"hello world",
                lapin::BasicProperties::default(),
            )
            .await
            .unwrap();

        let delivery = consumer.next().await.unwrap().unwrap();
        assert_eq!(delivery.data, b"hello world");
        delivery.ack(lapin::options::BasicAckOptions::default()).await.unwrap();
    }

    /// The server advertises `publisher_confirms` in its connection
    /// capabilities. This exercises that it's real: confirm_select() has to
    /// get a real SelectOk back (or this hangs), and each publish has to
    /// get a real basic.ack (or the inner await on the returned Confirmation
    /// never resolves and this test times out instead of completing).
    #[tokio::test]
    async fn rabbitmq_publisher_confirms() {
        let addr = start("rabbitmq", "127.0.0.1:0").unwrap().unwrap();
        let uri = format!("amqp://127.0.0.1:{}/%2f", addr.port());
        let conn = Connection::connect(&uri, ConnectionProperties::default()).await.unwrap();
        let channel = conn.create_channel().await.unwrap();

        channel
            .confirm_select(lapin::options::ConfirmSelectOptions::default())
            .await
            .expect("confirm.select must get a real SelectOk, not hang");

        channel
            .queue_declare(
                "test_confirm_queue".into(),
                lapin::options::QueueDeclareOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();

        let confirm = channel
            .basic_publish(
                "".into(),
                "test_confirm_queue".into(),
                lapin::options::BasicPublishOptions::default(),
                b"confirmed message",
                lapin::BasicProperties::default(),
            )
            .await
            .unwrap()
            .await
            .expect("publish must get a real basic.ack, not hang");
        assert!(confirm.is_ack(), "expected an ack, got {confirm:?}");
    }

    #[tokio::test]
    async fn rabbitmq_get() {
        let addr = start("rabbitmq", "127.0.0.1:0").unwrap().unwrap();
        let uri = format!("amqp://127.0.0.1:{}/%2f", addr.port());
        let conn =
            lapin::Connection::connect(&uri, lapin::ConnectionProperties::default()).await.unwrap();
        let channel = conn.create_channel().await.unwrap();

        channel
            .queue_declare(
                "test_get_queue".into(),
                lapin::options::QueueDeclareOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();

        channel
            .basic_publish(
                "".into(),
                "test_get_queue".into(),
                lapin::options::BasicPublishOptions::default(),
                b"get msg",
                lapin::BasicProperties::default(),
            )
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let get_res = channel
            .basic_get("test_get_queue".into(), lapin::options::BasicGetOptions::default())
            .await
            .unwrap();
        if let Some(msg) = get_res {
            assert_eq!(msg.delivery.data, b"get msg");
            msg.delivery.ack(lapin::options::BasicAckOptions::default()).await.unwrap();
        } else {
            panic!("Message not found");
        }
    }

    #[tokio::test]
    async fn rabbitmq_qos() {
        let addr = start("rabbitmq", "127.0.0.1:0").unwrap().unwrap();
        let uri = format!("amqp://127.0.0.1:{}/%2f", addr.port());
        let conn =
            lapin::Connection::connect(&uri, lapin::ConnectionProperties::default()).await.unwrap();
        let channel = conn.create_channel().await.unwrap();

        channel
            .queue_declare(
                "test_qos_queue".into(),
                lapin::options::QueueDeclareOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();

        channel.basic_qos(10, lapin::options::BasicQosOptions::default()).await.unwrap();
    }

    #[tokio::test]
    async fn rabbitmq_http_management() {
        let _addr = start("rabbitmq", "127.0.0.1:5672").unwrap().unwrap();

        let client = ureq::Agent::new_with_defaults();
        let res = client.get("http://127.0.0.1:15672/api/overview").call().unwrap();

        assert_eq!(res.status(), 200);
        let body: serde_json::Value =
            res.into_body().read_to_string().unwrap().parse::<serde_json::Value>().unwrap();
        assert_eq!(body["management_version"], "3.13.7");
        assert_eq!(body["rabbitmq_version"], "3.13.7");
    }

    #[tokio::test]
    async fn rabbitmq_nack_requeue() {
        let addr = start("rabbitmq", "127.0.0.1:0").unwrap().unwrap();
        let uri = format!("amqp://127.0.0.1:{}/%2f", addr.port());
        let conn = Connection::connect(&uri, ConnectionProperties::default()).await.unwrap();
        let channel = conn.create_channel().await.unwrap();

        channel
            .queue_declare(
                "nack_requeue_q".into(),
                lapin::options::QueueDeclareOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();

        channel
            .basic_publish(
                "".into(),
                "nack_requeue_q".into(),
                lapin::options::BasicPublishOptions::default(),
                b"msg1",
                lapin::BasicProperties::default(),
            )
            .await
            .unwrap();

        let mut consumer = channel
            .basic_consume(
                "nack_requeue_q".into(),
                "tag1".into(),
                lapin::options::BasicConsumeOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();

        if let Some(delivery) = consumer.next().await {
            let delivery = delivery.unwrap();
            assert_eq!(delivery.data, b"msg1");
            assert!(!delivery.redelivered);
            delivery
                .nack(lapin::options::BasicNackOptions { multiple: false, requeue: true })
                .await
                .unwrap();
        }

        // Consume again, it should be there and redelivered=true
        if let Some(delivery) = consumer.next().await {
            let delivery = delivery.unwrap();
            assert_eq!(delivery.data, b"msg1");
            assert!(delivery.redelivered);
            delivery.ack(lapin::options::BasicAckOptions::default()).await.unwrap();
        }
    }

    #[tokio::test]
    async fn rabbitmq_reject_dead_letter() {
        let addr = start("rabbitmq", "127.0.0.1:0").unwrap().unwrap();
        let uri = format!("amqp://127.0.0.1:{}/%2f", addr.port());
        let conn = Connection::connect(&uri, ConnectionProperties::default()).await.unwrap();
        let channel = conn.create_channel().await.unwrap();

        let mut args = lapin::types::FieldTable::default();
        args.insert("x-dead-letter-exchange".into(), lapin::types::AMQPValue::LongString("dlx2".into()));
        args.insert(
            "x-dead-letter-routing-key".into(),
            lapin::types::AMQPValue::LongString("dlrk2".into()),
        );

        channel
            .exchange_declare(
                "dlx2".into(),
                lapin::ExchangeKind::Direct,
                lapin::options::ExchangeDeclareOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();
        channel
            .queue_declare(
                "dlq2".into(),
                lapin::options::QueueDeclareOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();
        channel
            .queue_bind(
                "dlq2".into(),
                "dlx2".into(),
                "dlrk2".into(),
                lapin::options::QueueBindOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();

        channel
            .queue_declare("reject_dl_q".into(), lapin::options::QueueDeclareOptions::default(), args)
            .await
            .unwrap();

        channel
            .basic_publish(
                "".into(),
                "reject_dl_q".into(),
                lapin::options::BasicPublishOptions::default(),
                b"msg2",
                lapin::BasicProperties::default(),
            )
            .await
            .unwrap();

        let mut consumer = channel
            .basic_consume(
                "reject_dl_q".into(),
                "tag2".into(),
                lapin::options::BasicConsumeOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();

        if let Some(delivery) = consumer.next().await {
            let delivery = delivery.unwrap();
            delivery.reject(lapin::options::BasicRejectOptions { requeue: false }).await.unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Check it's in DLQ
        let get_res = channel
            .basic_get("dlq2".into(), lapin::options::BasicGetOptions::default())
            .await
            .unwrap();
        assert!(get_res.is_some());
        assert_eq!(get_res.unwrap().delivery.data, b"msg2");
    }
}
