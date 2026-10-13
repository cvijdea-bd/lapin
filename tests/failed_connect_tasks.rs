#![cfg(feature = "tokio")]

//! Checks that a connection attempt that fails leaves no task behind.
//!
//! No broker is needed: the attempts go to a port nothing listens on.

use lapin::{Connection, ConnectionProperties};
use std::{net::TcpListener, time::Duration};

const ATTEMPTS: usize = 3;
/// Lets the tasks of the failed attempts notice and end.
const SETTLE: Duration = Duration::from_secs(1);

#[tokio::test(flavor = "multi_thread")]
async fn failed_connections_leave_no_tasks_behind() {
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let addr = format!("amqp://127.0.0.1:{port}/%2f");
    let metrics = tokio::runtime::Handle::current().metrics();
    let before = metrics.num_alive_tasks();

    for _ in 0..ATTEMPTS {
        Connection::connect(&addr, ConnectionProperties::default())
            .await
            .expect_err("nothing listens on the port");
    }
    tokio::time::sleep(SETTLE).await;

    assert_eq!(metrics.num_alive_tasks(), before);
}
