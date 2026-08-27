// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
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
pub(crate) struct LivenessWatchdog {
    label: &'static str,
    pending_since: Option<Instant>,
    timeout: Duration,
}

impl LivenessWatchdog {
    pub(crate) const fn new(label: &'static str, timeout: Duration) -> Self {
        Self {
            label,
            pending_since: None,
            timeout,
        }
    }

    pub(crate) fn sent(&mut self) {
        if self.pending_since.is_none() {
            self.pending_since = Some(Instant::now());
        } else {
            log::warn!(
                "Sent a new Rithmic {} before receiving the previous response",
                self.label,
            );
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
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn liveness_watchdog_starts_idle() {
        let manager = LivenessWatchdog::new("test", Duration::from_secs(1));
        assert!(manager.pending_since.is_none());
    }

    #[rstest]
    fn response_clears_pending_request() {
        let mut manager = LivenessWatchdog::new("test", Duration::from_secs(1));
        manager.sent();
        manager.received();
        assert!(manager.pending_since.is_none());
    }

    #[rstest]
    fn repeated_send_preserves_original_deadline() {
        let mut manager = LivenessWatchdog::new("test", Duration::from_secs(1));
        manager.sent();
        let pending_since = manager.pending_since;
        manager.sent();
        assert_eq!(manager.pending_since, pending_since);
    }
}
