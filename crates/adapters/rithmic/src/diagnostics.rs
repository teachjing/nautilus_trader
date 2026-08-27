// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Live connection diagnostics for the Rithmic ticker-plant adapter.

use std::{
    collections::HashSet,
    fs::OpenOptions,
    io::Write,
    time::Duration,
};

use nautilus_common::messages::DataEvent;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    data::{Data, OrderBookDelta},
    enums::{BookAction, BookType, OrderSide, RecordFlag},
    identifiers::InstrumentId,
    types::Price,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::RithmicDataClientConfig,
    flow::{LoginCredentials, MarketSubscription},
    protocol::update_bits,
    session::{RawOrderBookMetrics, RithmicSession},
};

/// Results from one live Rithmic ticker-plant connection probe.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RithmicConnectionProbeResult {
    /// Systems returned by Rithmic system discovery before login.
    pub available_systems: Vec<String>,
    /// JSONL file containing credential-safe discovery and connection responses.
    pub diagnostic_log_path: Option<String>,
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
    /// Granularity proven by the received native book data.
    pub order_book_type: Option<BookType>,
    /// Maximum number of bid price levels simultaneously observed.
    pub max_bid_levels: usize,
    /// Maximum number of ask price levels simultaneously observed.
    pub max_ask_levels: usize,
    /// Number of order-book add actions received.
    pub book_adds: u64,
    /// Number of order-book update actions received.
    pub book_updates: u64,
    /// Number of order-book delete actions received.
    pub book_deletes: u64,
    /// Number of order-book clear actions received.
    pub book_clears: u64,
    /// Number of book deltas carrying a nonzero exchange order ID.
    pub book_deltas_with_order_ids: u64,
    /// Whether individual order cancellations are visible in the received feed.
    pub individual_cancels_visible: bool,
    /// Number of raw Rithmic template 156 messages received.
    pub raw_book_messages: u64,
    /// Maximum bid entries carried by one raw template 156 message.
    pub max_raw_bid_entries: u64,
    /// Maximum ask entries carried by one raw template 156 message.
    pub max_raw_ask_entries: u64,
    /// Raw book messages containing aggregate order-count metadata.
    pub book_messages_with_order_counts: u64,
    /// Raw book messages containing nonzero implied-liquidity metadata.
    pub book_messages_with_implicit_liquidity: u64,
}

#[derive(Default)]
struct OrderBookProbeState {
    levels: HashSet<(InstrumentId, OrderSide, Price)>,
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
        config.front_month_fallback.as_deref(),
        config.diagnostic_log_dir.as_deref(),
    )
    .await?;

    let mut result = RithmicConnectionProbeResult {
        available_systems: session.available_systems().to_vec(),
        diagnostic_log_path: session
            .diagnostic_log_path()
            .map(|path| path.display().to_string()),
        resolved_subscriptions: resolved
            .iter()
            .map(|subscription| format!("{}.{}", subscription.exchange, subscription.symbol))
            .collect(),
        ..Default::default()
    };
    let mut book_state = OrderBookProbeState::default();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let raw_book_metrics = std::sync::Arc::new(RawOrderBookMetrics::default());
    let task_raw_book_metrics = std::sync::Arc::clone(&raw_book_metrics);
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let mut session_task = tokio::spawn(async move {
        session
            .run(
                resolved,
                sender,
                get_atomic_clock_realtime(),
                task_cancel,
                Some(task_raw_book_metrics),
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
                count_event(&mut result, &mut book_state, event);
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
    (
        result.raw_book_messages,
        result.max_raw_bid_entries,
        result.max_raw_ask_entries,
        result.book_messages_with_order_counts,
        result.book_messages_with_implicit_liquidity,
    ) = raw_book_metrics.snapshot();
    if result.order_book_type.is_none() && result.quotes > 0 {
        result.order_book_type = Some(BookType::L1_MBP);
    }
    result.individual_cancels_visible = result.order_book_type == Some(BookType::L3_MBO)
        && result.book_deletes > 0
        && result.book_deltas_with_order_ids > 0;
    write_capability_summary(&result)?;
    Ok(result)
}

fn write_capability_summary(result: &RithmicConnectionProbeResult) -> anyhow::Result<()> {
    let Some(path) = &result.diagnostic_log_path else {
        return Ok(());
    };
    let record = serde_json::json!({
        "stage": "order_book_capabilities",
        "status": "observed",
        "details": {
            "order_book_type": result.order_book_type.map(|value| value.to_string()),
            "max_bid_levels": result.max_bid_levels,
            "max_ask_levels": result.max_ask_levels,
            "book_adds": result.book_adds,
            "book_updates": result.book_updates,
            "book_deletes": result.book_deletes,
            "book_clears": result.book_clears,
            "book_deltas_with_order_ids": result.book_deltas_with_order_ids,
            "individual_cancels_visible": result.individual_cancels_visible,
            "raw_book_messages": result.raw_book_messages,
            "max_raw_bid_entries": result.max_raw_bid_entries,
            "max_raw_ask_entries": result.max_raw_ask_entries,
            "book_messages_with_order_counts": result.book_messages_with_order_counts,
            "book_messages_with_implicit_liquidity":
                result.book_messages_with_implicit_liquidity,
        },
    });
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{record}")?;
    Ok(())
}

fn count_event(
    result: &mut RithmicConnectionProbeResult,
    state: &mut OrderBookProbeState,
    event: DataEvent,
) {
    match event {
        DataEvent::Data(Data::Trade(_)) => result.trades += 1,
        DataEvent::Data(Data::Quote(_)) => result.quotes += 1,
        DataEvent::Data(Data::Deltas(deltas)) => {
            result.order_book_batches += 1;
            result.order_book_deltas += deltas.deltas.len() as u64;
            for delta in deltas.deltas {
                count_book_delta(result, state, delta);
            }
        }
        DataEvent::Data(Data::Delta(delta)) => {
            result.order_book_batches += 1;
            result.order_book_deltas += 1;
            count_book_delta(result, state, delta);
        }
        _ => {}
    }
}

fn count_book_delta(
    result: &mut RithmicConnectionProbeResult,
    state: &mut OrderBookProbeState,
    delta: OrderBookDelta,
) {
    if RecordFlag::F_MBP.matches(delta.flags) {
        result.order_book_type = Some(BookType::L2_MBP);
    } else if delta.order.order_id != 0 {
        result.order_book_type = Some(BookType::L3_MBO);
    }

    match delta.action {
        BookAction::Add => result.book_adds += 1,
        BookAction::Update => result.book_updates += 1,
        BookAction::Delete => result.book_deletes += 1,
        BookAction::Clear => result.book_clears += 1,
    }
    if delta.order.order_id != 0 {
        result.book_deltas_with_order_ids += 1;
    }

    let key = (delta.instrument_id, delta.order.side, delta.order.price);
    match delta.action {
        BookAction::Add | BookAction::Update => {
            state.levels.insert(key);
        }
        BookAction::Delete => {
            state.levels.remove(&key);
        }
        BookAction::Clear => {
            state
                .levels
                .retain(|(instrument_id, _, _)| *instrument_id != delta.instrument_id);
        }
    }
    result.max_bid_levels = result.max_bid_levels.max(
        state
            .levels
            .iter()
            .filter(|(_, side, _)| *side == OrderSide::Buy)
            .count(),
    );
    result.max_ask_levels = result.max_ask_levels.max(
        state
            .levels
            .iter()
            .filter(|(_, side, _)| *side == OrderSide::Sell)
            .count(),
    );
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

        let mut state = OrderBookProbeState::default();
        count_event(&mut result, &mut state, DataEvent::Data(Data::Trade(trade)));
        assert_eq!(result.trades, 1);
        assert_eq!(result.total_events(), 1);
    }

    #[rstest]
    fn classifies_aggregated_book_levels_as_l2_mbp() {
        let instrument_id = InstrumentId::from("MESU6.CME");
        let ts = nautilus_core::UnixNanos::from(1);
        let delta = OrderBookDelta::new(
            instrument_id,
            BookAction::Update,
            nautilus_model::data::BookOrder::new(
                OrderSide::Buy,
                Price::from("6000.25"),
                nautilus_model::types::Quantity::from(10),
                0,
            ),
            RecordFlag::F_MBP as u8,
            1,
            ts,
            ts,
        );
        let mut result = RithmicConnectionProbeResult::default();
        let mut state = OrderBookProbeState::default();

        count_event(&mut result, &mut state, DataEvent::Data(Data::Delta(delta)));

        assert_eq!(result.order_book_type, Some(BookType::L2_MBP));
        assert_eq!(result.max_bid_levels, 1);
        assert_eq!(result.book_updates, 1);
        assert_eq!(result.book_deltas_with_order_ids, 0);
        assert!(!result.individual_cancels_visible);
    }
}
