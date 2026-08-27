// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Live connection diagnostics for the Rithmic ticker-plant adapter.

use std::time::Duration;

use nautilus_common::messages::DataEvent;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::data::Data;
use tokio_util::sync::CancellationToken;

use crate::{
    config::RithmicDataClientConfig,
    flow::{LoginCredentials, MarketSubscription},
    protocol::update_bits,
    session::RithmicSession,
};

/// Results from one live Rithmic ticker-plant connection probe.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RithmicConnectionProbeResult {
    /// Contracts used for the actual Rithmic market-data subscriptions.
    pub resolved_subscriptions: Vec<String>,
    /// Number of native Nautilus trade ticks received.
    pub trades: u64,
    /// Number of native Nautilus quote ticks received.
    pub quotes: u64,
    /// Number of native Nautilus order-book batches received.
    pub order_book_batches: u64,
    /// Total number of native Nautilus order-book deltas received.
    pub order_book_deltas: u64,
}

impl RithmicConnectionProbeResult {
    /// Returns the total number of native market-data events received.
    #[must_use]
    pub const fn total_events(&self) -> u64 {
        self.trades + self.quotes + self.order_book_batches
    }
}

/// Connects to Rithmic, collects native Nautilus events, then logs out cleanly.
///
/// # Errors
///
/// Returns an error for invalid configuration, discovery/login/subscription failures, an early
/// session failure, or a graceful-shutdown failure.
pub async fn run_connection_probe(
    config: RithmicDataClientConfig,
    duration: Duration,
) -> anyhow::Result<RithmicConnectionProbeResult> {
    anyhow::ensure!(!duration.is_zero(), "Probe duration must be positive");
    let credentials = credentials(&config)?;
    let subscriptions = subscriptions(&config)?;
    anyhow::ensure!(
        !subscriptions.is_empty(),
        "At least one Rithmic live probe subscription is required"
    );
    let connect_timeout = Duration::from_secs(config.connect_timeout_secs);
    let (session, resolved) = RithmicSession::connect_subscribed(
        &config.gateway_url,
        &credentials,
        &subscriptions,
        connect_timeout,
    )
    .await?;

    let mut result = RithmicConnectionProbeResult {
        resolved_subscriptions: resolved
            .iter()
            .map(|subscription| format!("{}.{}", subscription.exchange, subscription.symbol))
            .collect(),
        ..Default::default()
    };
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let mut session_task = tokio::spawn(async move {
        session
            .run(
                resolved,
                sender,
                get_atomic_clock_realtime(),
                task_cancel,
            )
            .await
    });
    let deadline = tokio::time::sleep(duration);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            () = &mut deadline => break,
            event = receiver.recv() => {
                let Some(event) = event else {
                    anyhow::bail!("Rithmic live probe event channel closed unexpectedly")
                };
                count_event(&mut result, event);
            }
            outcome = &mut session_task => {
                let outcome = outcome?;
                outcome?;
                anyhow::bail!("Rithmic live probe session stopped before its deadline")
            }
        }
    }

    cancel.cancel();
    session_task.await??;
    Ok(result)
}

fn count_event(result: &mut RithmicConnectionProbeResult, event: DataEvent) {
    match event {
        DataEvent::Data(Data::Trade(_)) => result.trades += 1,
        DataEvent::Data(Data::Quote(_)) => result.quotes += 1,
        DataEvent::Data(Data::Deltas(deltas)) => {
            result.order_book_batches += 1;
            result.order_book_deltas += deltas.deltas.len() as u64;
        }
        DataEvent::Data(Data::Delta(_)) => {
            result.order_book_batches += 1;
            result.order_book_deltas += 1;
        }
        _ => {}
    }
}

fn credentials(config: &RithmicDataClientConfig) -> anyhow::Result<LoginCredentials> {
    let user = config
        .username
        .clone()
        .or_else(|| std::env::var("RITHMIC_USER").ok())
        .ok_or_else(|| anyhow::anyhow!("Set RITHMIC_USER for the live connection probe"))?;
    let password = config
        .password
        .clone()
        .or_else(|| std::env::var("RITHMIC_PASSWORD").ok())
        .ok_or_else(|| anyhow::anyhow!("Set RITHMIC_PASSWORD for the live connection probe"))?;
    Ok(LoginCredentials {
        user,
        password,
        system_name: config.system_name.clone(),
        app_name: "NautilusTrader".to_string(),
        app_version: env!("CARGO_PKG_VERSION").to_string(),
        aggregated_quotes: false,
    })
}

fn subscriptions(config: &RithmicDataClientConfig) -> anyhow::Result<Vec<MarketSubscription>> {
    let mut bits = 0;
    if config.subscribe_trades {
        bits |= update_bits::LAST_TRADE;
    }
    if config.subscribe_quotes {
        bits |= update_bits::BBO;
    }
    if config.subscribe_book_deltas {
        bits |= update_bits::ORDER_BOOK;
    }
    anyhow::ensure!(bits != 0, "At least one Rithmic market-data type must be enabled");

    config
        .market_subscriptions
        .iter()
        .map(|value| {
            let (exchange, symbol) = value.split_once('.').ok_or_else(|| {
                anyhow::anyhow!("Invalid Rithmic subscription '{value}': expected EXCHANGE.SYMBOL")
            })?;
            anyhow::ensure!(
                !exchange.is_empty() && !symbol.is_empty() && !symbol.contains('.'),
                "Invalid Rithmic subscription '{value}': expected EXCHANGE.SYMBOL"
            );
            Ok(MarketSubscription::new(symbol, exchange, bits))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn counts_native_market_data_events() {
        let mut result = RithmicConnectionProbeResult::default();
        let instrument_id = nautilus_model::identifiers::InstrumentId::from("MESU6.CME");
        let ts = nautilus_core::UnixNanos::from(1);
        let trade = nautilus_model::data::TradeTick::new(
            instrument_id,
            nautilus_model::types::Price::from("6000.25"),
            nautilus_model::types::Quantity::from(1),
            nautilus_model::enums::AggressorSide::Buy,
            nautilus_model::identifiers::TradeId::from("test-trade"),
            ts,
            ts,
        );

        count_event(&mut result, DataEvent::Data(Data::Trade(trade)));
        assert_eq!(result.trades, 1);
        assert_eq!(result.total_events(), 1);
    }
}
