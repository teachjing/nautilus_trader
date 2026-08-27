// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
// -------------------------------------------------------------------------------------------------

//! Bounded WebSocket I/O and connection liveness supervision.
//!
//! The design is adapted from the vendored `rithmic-rs` transport helpers under
//! `rithmic-rs/src/ws.rs` and `rithmic-rs/src/ping_manager.rs`, while keeping
//! the Nautilus adapter's own protocol and session types.

use std::time::Duration;

use futures_util::{Sink, SinkExt};
use tokio::time::{Instant, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;

pub(crate) const WEBSOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const WEBSOCKET_PING_INTERVAL: Duration = Duration::from_secs(60);
pub(crate) const WEBSOCKET_PING_TIMEOUT: Duration = Duration::from_secs(50);

#[derive(Debug)]
pub(crate) struct PingManager {
    pending_since: Option<Instant>,
    timeout: Duration,
}

impl PingManager {
    pub(crate) const fn new(timeout: Duration) -> Self {
        Self {
            pending_since: None,
            timeout,
        }
    }

    pub(crate) fn sent(&mut self) {
        if self.pending_since.replace(Instant::now()).is_some() {
            log::warn!("Sent a new Rithmic WebSocket ping before receiving the previous pong");
        }
    }

    pub(crate) fn received(&mut self) {
        self.pending_since = None;
    }

    pub(crate) async fn timed_out(&mut self) {
        match self.pending_since {
            Some(sent_at) => {
                sleep_until(sent_at + self.timeout).await;
                self.pending_since = None;
            }
            None => std::future::pending().await,
        }
    }
}

pub(crate) async fn send_with_timeout<S>(
    sink: &mut S,
    message: WebSocketMessage,
) -> anyhow::Result<()>
where
    S: Sink<WebSocketMessage> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    timeout(WEBSOCKET_WRITE_TIMEOUT, sink.send(message))
        .await
        .map_err(|_| anyhow::anyhow!(
            "Rithmic WebSocket write timed out after {WEBSOCKET_WRITE_TIMEOUT:?}"
        ))??;
    Ok(())
}

pub(crate) async fn close_with_timeout<S>(sink: &mut S) -> anyhow::Result<()>
where
    S: Sink<WebSocketMessage> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    timeout(WEBSOCKET_WRITE_TIMEOUT, sink.close())
        .await
        .map_err(|_| anyhow::anyhow!(
            "Rithmic WebSocket close timed out after {WEBSOCKET_WRITE_TIMEOUT:?}"
        ))??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_manager_starts_idle() {
        let manager = PingManager::new(Duration::from_secs(1));
        assert!(manager.pending_since.is_none());
    }

    #[test]
    fn pong_clears_pending_ping() {
        let mut manager = PingManager::new(Duration::from_secs(1));
        manager.sent();
        manager.received();
        assert!(manager.pending_since.is_none());
    }
}
