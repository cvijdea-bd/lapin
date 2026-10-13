#![cfg(feature = "tokio")]

//! Checks that closing a connection completes the publishes still queued on it.
//!
//! Runs against a real broker. The close is sent ahead of the queued publish
//! frames, so the connection is closed while most of them are still waiting to
//! be written. Each publish must then complete, written or failed, instead of
//! staying pending.

use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use std::time::Duration;

const PUBLISHES: usize = 64;
const PAYLOAD: usize = 1 << 20;
const TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread")]
async fn close_completes_queued_publishes() {
    let addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
    let conn = Connection::connect(&addr, ConnectionProperties::default())
        .await
        .unwrap();
    let channel = conn.create_channel().await.unwrap();
    let queue = channel
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    let payload = vec![0; PAYLOAD];
    let publishes: Vec<_> = (0..PUBLISHES)
        .map(|_| {
            let (channel, queue, payload) =
                (channel.clone(), queue.name().clone(), payload.clone());
            tokio::spawn(async move {
                channel
                    .basic_publish(
                        "".into(),
                        queue,
                        BasicPublishOptions::default(),
                        &payload,
                        BasicProperties::default(),
                    )
                    .await
                    .map(drop)
            })
        })
        .collect();
    // Let every publish queue its frames before the close goes out.
    tokio::time::sleep(Duration::from_millis(50)).await;
    conn.close(200, "bye".into()).await.unwrap();

    for publish in publishes {
        tokio::time::timeout(TIMEOUT, publish)
            .await
            .expect("a publish never completed")
            .unwrap()
            .ok();
    }
}
