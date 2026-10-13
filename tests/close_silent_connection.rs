#![cfg(feature = "tokio")]

//! Checks that closing a connection whose broker went silent completes.
//!
//! Runs against a real broker through a relay. Once the relay goes silent it
//! swallows every byte in both directions and never closes the client's socket,
//! like a network that stopped delivering. The connections use a one-second
//! heartbeat, so the missing heartbeats are noticed within seconds.

mod common;

use common::Relay;
use lapin::{Connection, ConnectionProperties, ErrorKind};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);

async fn close_once_silent(properties: ConnectionProperties) {
    let relay = Relay::start();
    let mut uri = relay.uri();
    uri.query.heartbeat = Some(1);
    let conn = Connection::connect_uri(uri, properties).await.unwrap();
    relay.go_silent();

    let error = tokio::time::timeout(TIMEOUT, conn.close(200, "bye".into()))
        .await
        .expect("the close never completed")
        .unwrap_err();
    assert!(
        matches!(error.kind(), ErrorKind::MissingHeartbeatError),
        "{error}"
    );
}

#[tokio::test]
async fn close_on_a_silent_connection_completes() {
    close_once_silent(ConnectionProperties::default()).await;
}

#[tokio::test]
async fn close_on_a_silent_connection_completes_with_auto_recovery() {
    close_once_silent(ConnectionProperties::default().enable_auto_recover()).await;
}
