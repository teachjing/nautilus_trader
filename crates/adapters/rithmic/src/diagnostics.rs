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
    instruments::Instrument,
    types::Price,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::RithmicDataClientConfig,
    discovery::{RithmicExchangeInfo, RithmicInstrumentInfo},
    flow::{LoginCredentials, MarketSubscription},
    protocol::update_bits,
    session::{
        RawOrderBookMetrics, RithmicSession, RithmicSessionCommand, RuntimeDataType,
        RuntimeSubscription,
    },
};

/// Result of connecting idle, hydrating one contract, and toggling its live subscription.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RithmicDynamicSubscriptionProbeResult {
    pub instrument_id: InstrumentId,
    pub connected_idle: bool,
    pub instrument_hydrated: bool,
    pub market_data_events: u64,
    pub unsubscribed_cleanly: bool,
}

/// Connects without instruments, then hydrates and subscribes to a contract supplied later.
///
/// # Errors
///
/// Returns an error for invalid configuration or instrument IDs, connection, hydration,
/// subscription, timeout, or graceful-shutdown failures.
pub async fn run_dynamic_subscription_probe(
    config: RithmicDataClientConfig,
    instrument_id: InstrumentId,
    idle_duration: Duration,
    hydration_timeout: Duration,
) -> anyhow::Result<RithmicDynamicSubscriptionProbeResult> {
    anyhow::ensure!(!hydration_timeout.is_zero(), "Hydration timeout must be positive");
    let credentials = credentials(&config)?;
    let connect_timeout = Duration::from_secs(config.connect_timeout_secs);
    let (session, resolved) = RithmicSession::connect_subscribed(
        &config.gateway_url,
        &credentials,
        &[],
        connect_timeout,
        config.front_month_fallback.as_deref(),
        config.diagnostic_log_dir.as_deref(),
    )
    .await?;
    anyhow::ensure!(resolved.is_empty(), "Idle Rithmic connection resolved subscriptions");

    let (data_sender, mut data_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (runtime_sender, mut runtime_receiver) = tokio::sync::mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let mut session_task = tokio::spawn(async move {
        session
            .run(
                Vec::new(),
                &mut runtime_receiver,
                data_sender,
                get_atomic_clock_realtime(),
                task_cancel,
                None,
                false,
                None,
            )
            .await
    });

    tokio::time::sleep(idle_duration).await;
    anyhow::ensure!(!session_task.is_finished(), "Rithmic idle session stopped unexpectedly");
    let subscription = RuntimeSubscription {
        instrument_id,
        data_type: RuntimeDataType::Quote,
    };
    runtime_sender.send(RithmicSessionCommand::Subscribe(subscription.clone()))?;

    let mut result = RithmicDynamicSubscriptionProbeResult {
        instrument_id,
        connected_idle: true,
        ..Default::default()
    };
    let hydration = tokio::time::sleep(hydration_timeout);
    tokio::pin!(hydration);
    loop {
        tokio::select! {
            () = &mut hydration => {
                anyhow::bail!("Timed out hydrating Rithmic instrument {instrument_id}")
            }
            event = data_receiver.recv() => {
                let Some(event) = event else {
                    anyhow::bail!("Rithmic dynamic probe event channel closed")
                };
                match event {
                    DataEvent::Instrument(instrument) if instrument.id() == instrument_id => {
                        result.instrument_hydrated = true;
                        break;
                    }
                    DataEvent::Data(data) if data.instrument_id() == instrument_id => {
                        result.market_data_events = result.market_data_events.saturating_add(1);
                    }
                    _ => {}
                }
            }
            outcome = &mut session_task => {
                outcome??;
                anyhow::bail!("Rithmic dynamic probe stopped during hydration")
            }
        }
    }

    runtime_sender.send(RithmicSessionCommand::Unsubscribe(subscription))?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    anyhow::ensure!(
        !session_task.is_finished(),
        "Rithmic session stopped while unsubscribing {instrument_id}"
    );
    result.unsubscribed_cleanly = true;
    cancel.cancel();
    session_task.await??;
    Ok(result)
}

/// Result of a focused Rithmic futures-contract search for one exchange.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct RithmicInstrumentSearchResult {
    pub output_path: String,
    pub exchange: String,
    pub search_text: String,
    pub match_count: usize,
    pub instruments: Vec<RithmicInstrumentInfo>,
}

/// Searches futures contracts on one selected market and saves the results as JSON.
///
/// # Errors
///
/// Returns an error for an empty exchange/search string, unavailable credentials, connection or
/// protocol failures, timeout, or an output-file failure.
pub async fn run_instrument_search(
    config: &RithmicDataClientConfig,
    exchange: &str,
    search_text: &str,
) -> anyhow::Result<RithmicInstrumentSearchResult> {
    let exchange = exchange.trim().to_ascii_uppercase();
    let search_text = search_text.trim().to_ascii_uppercase();
    anyhow::ensure!(!exchange.is_empty(), "Rithmic instrument search exchange is required");
    anyhow::ensure!(
        !search_text.is_empty(),
        "Rithmic instrument search text is required"
    );
    let credentials = credentials(config)?;
    let timeout_secs = std::env::var("RITHMIC_DISCOVERY_TIMEOUT_SECS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|e| anyhow::anyhow!("Invalid RITHMIC_DISCOVERY_TIMEOUT_SECS: {e}"))?
        .unwrap_or(120);
    let instruments = tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        RithmicSession::search_instruments(
            &config.gateway_url,
            &credentials,
            &exchange,
            &search_text,
            config.diagnostic_log_dir.as_deref(),
        ),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "Rithmic instrument search for {exchange}.{search_text} timed out after {timeout_secs}s"
        )
    })??;
    let output_dir = config
        .diagnostic_log_dir
        .as_deref()
        .unwrap_or("target/rithmic-diagnostics");
    let output_path = std::path::Path::new(output_dir).join(format!(
        "rithmic-instrument-search-{exchange}-{search_text}.json"
    ));
    let result = RithmicInstrumentSearchResult {
        output_path: output_path.display().to_string(),
        exchange,
        search_text,
        match_count: instruments.len(),
        instruments,
    };
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&output_path, serde_json::to_vec_pretty(&result)?)?;
    Ok(result)
}

/// Result of querying every market-data exchange permission for the authenticated user.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct RithmicMarketEntitlementProbeResult {
    pub output_path: String,
    pub available_markets: usize,
    pub entitled_markets: usize,
    pub exchanges: Vec<RithmicExchangeInfo>,
}

/// Queries and saves all Rithmic exchange market-data entitlements without subscribing to data.
///
/// # Errors
///
/// Returns an error when credentials are unavailable, discovery fails, or JSON cannot be saved.
pub async fn run_market_entitlement_probe(
    config: &RithmicDataClientConfig,
) -> anyhow::Result<RithmicMarketEntitlementProbeResult> {
    let credentials = credentials(config)?;
    let timeout_secs = std::env::var("RITHMIC_DISCOVERY_TIMEOUT_SECS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|e| anyhow::anyhow!("Invalid RITHMIC_DISCOVERY_TIMEOUT_SECS: {e}"))?
        .unwrap_or(120);
    let catalog = tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        RithmicSession::discover_catalog_sequential(
            &config.gateway_url,
            &credentials,
            false,
            &[],
            config.diagnostic_log_dir.as_deref(),
        ),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!("Rithmic market entitlement discovery timed out after {timeout_secs}s")
    })??;
    let output_dir = config
        .diagnostic_log_dir
        .as_deref()
        .unwrap_or("target/rithmic-diagnostics");
    let output_path = std::path::Path::new(output_dir).join("rithmic-market-entitlements.json");
    let available_markets = catalog.exchanges.len();
    let entitled_markets = catalog.exchanges.iter().filter(|value| value.entitled).count();
    let result = RithmicMarketEntitlementProbeResult {
        output_path: output_path.display().to_string(),
        available_markets,
        entitled_markets,
        exchanges: catalog.exchanges,
    };
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&output_path, serde_json::to_vec_pretty(&result)?)?;
    Ok(result)
}

/// Results from one live Rithmic ticker-plant connection probe.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RithmicConnectionProbeResult {
    /// Systems returned by Rithmic system discovery before login.
    pub available_systems: Vec<String>,
    /// JSONL file containing credential-safe discovery and connection responses.
    pub diagnostic_log_path: Option<String>,
    /// Stable JSON catalog containing discovered exchanges and instruments.
    pub discovery_catalog_path: Option<String>,
    /// Number of exchanges returned by template 343.
    pub discovered_exchanges: usize,
    /// Number of exchanges enabled for this user.
    pub entitled_exchanges: usize,
    /// Number of futures instruments returned by template 110.
    pub discovered_instruments: usize,
    /// Whether the opt-in concurrent plant capacity probe ran.
    pub plant_capacity_probe_enabled: bool,
    /// Whether an Order Plant login succeeded while Ticker Plant remained connected.
    pub order_with_ticker_connected: bool,
    /// Whether a History Plant login succeeded while both Ticker and Order were connected.
    pub history_with_ticker_and_order_connected: bool,
    /// Whether a History Plant login succeeded while only Ticker remained connected.
    pub history_with_ticker_only_connected: bool,
    /// Credential-safe Order Plant connection error, when rejected.
    pub order_connection_error: Option<String>,
    /// Credential-safe History Plant connection error with Order Plant held open.
    pub history_with_order_connection_error: Option<String>,
    /// Credential-safe History Plant connection error with only Ticker Plant held open.
    pub history_ticker_only_connection_error: Option<String>,
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
    /// Template 156 messages rejected during native MBP conversion.
    pub mbp_parse_errors: u64,
    /// Template 156 messages rejected for mismatched price/size arrays.
    pub mbp_array_mismatch_errors: u64,
    /// Template 156 messages rejected for invalid event timestamps.
    pub mbp_timestamp_errors: u64,
    /// Whether the dedicated depth-by-order probe was enabled.
    pub mbo_probe_enabled: bool,
    /// Price selected for the Rithmic depth-by-order request.
    pub mbo_selected_price: Option<String>,
    /// Whether Rithmic accepted the template 117 subscription.
    pub mbo_subscription_accepted: bool,
    /// Whether Rithmic rejected the template 117 subscription.
    pub mbo_subscription_rejected: bool,
    /// Number of template 116 depth-by-order snapshot messages.
    pub mbo_snapshot_messages: u64,
    /// Number of template 160 depth-by-order update messages.
    pub mbo_update_messages: u64,
    /// Number of template 161 depth-by-order end events.
    pub mbo_end_events: u64,
    /// Individual new orders observed in template 160 updates.
    pub mbo_new_orders: u64,
    /// Individual changed orders observed in template 160 updates.
    pub mbo_changed_orders: u64,
    /// Individual deleted orders observed in template 160 updates.
    pub mbo_deleted_orders: u64,
    /// Individual delete events carrying an exchange order ID.
    pub mbo_deletes_with_order_ids: u64,
    /// Depth-by-order entries carrying exchange order IDs.
    pub mbo_entries_with_order_ids: u64,
    /// Depth-by-order entries carrying queue priority.
    pub mbo_entries_with_priority: u64,
    /// Depth-by-order entries carrying a previous price.
    pub mbo_entries_with_previous_price: u64,
    /// Number of detected exchange-sequence discontinuities.
    pub mbo_sequence_gaps: u64,
    /// Number of automatic MBO snapshot recovery requests.
    pub mbo_resnapshots: u64,
    /// Whether received data proves true market-by-order capability.
    pub mbo_capable: bool,
}

/// Result of testing simultaneous authenticated Rithmic infrastructure-plant sockets.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RithmicPlantCapacityProbeResult {
    pub ticker_connected: bool,
    pub order_with_ticker_connected: bool,
    pub history_with_ticker_and_order_connected: bool,
    pub history_with_ticker_only_connected: bool,
    pub order_error: Option<String>,
    pub history_with_order_error: Option<String>,
    pub history_ticker_only_error: Option<String>,
}

/// Tests the account's concurrent Ticker, Order, and History Plant connection capacity.
///
/// # Errors
///
/// Returns an error if system discovery or the baseline Ticker Plant login fails.
pub async fn run_plant_capacity_probe(
    config: &RithmicDataClientConfig,
) -> anyhow::Result<RithmicPlantCapacityProbeResult> {
    let credentials = credentials(config)?;
    let capacity = RithmicSession::probe_plant_connection_capacity(
        &config.gateway_url,
        &credentials,
        config.diagnostic_log_dir.as_deref(),
    )
    .await?;
    Ok(RithmicPlantCapacityProbeResult {
        ticker_connected: capacity.ticker_connected,
        order_with_ticker_connected: capacity.order_with_ticker_connected,
        history_with_ticker_and_order_connected: capacity
            .history_with_ticker_and_order_connected,
        history_with_ticker_only_connected: capacity.history_with_ticker_only_connected,
        order_error: capacity.order_error,
        history_with_order_error: capacity.history_with_order_error,
        history_ticker_only_error: capacity.history_ticker_only_error,
    })
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
    let mbo_probe_enabled = config.effective_book_feed() == crate::config::RithmicBookFeed::L3Mbo
        || env_flag("RITHMIC_TEST_MBO");
    anyhow::ensure!(
        !(mbo_probe_enabled
            && config.effective_book_feed() == crate::config::RithmicBookFeed::L2Mbp),
        "Rithmic L2 market-by-price and L3 market-by-order subscriptions are mutually exclusive"
    );
    let discover_markets = env_flag("RITHMIC_DISCOVER_MARKETS")
        || env_flag("RITHMIC_DISCOVER_INSTRUMENTS");
    let discover_instruments = env_flag("RITHMIC_DISCOVER_INSTRUMENTS");
    let discovery_exchanges = std::env::var("RITHMIC_DISCOVERY_EXCHANGES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_ascii_uppercase)
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| {
            let mut exchanges = subscriptions
                .iter()
                .map(|subscription| subscription.exchange.to_ascii_uppercase())
                .collect::<Vec<_>>();
            exchanges.sort();
            exchanges.dedup();
            exchanges
        });
    let plant_capacity_probe_enabled = env_flag("RITHMIC_TEST_PLANT_CAPACITY");
    let discovery_timeout_secs = std::env::var("RITHMIC_DISCOVERY_TIMEOUT_SECS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|e| anyhow::anyhow!("Invalid RITHMIC_DISCOVERY_TIMEOUT_SECS: {e}"))?
        .unwrap_or(120);
    let catalog_dir = config
        .diagnostic_log_dir
        .clone()
        .unwrap_or_else(|| "target/rithmic-diagnostics".to_string());
    let discovered_catalog = if discover_markets {
        Some(
            tokio::time::timeout(
                Duration::from_secs(discovery_timeout_secs),
                RithmicSession::discover_catalog_sequential(
                    &config.gateway_url,
                    &credentials,
                    discover_instruments,
                    &discovery_exchanges,
                    config.diagnostic_log_dir.as_deref(),
                ),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Rithmic catalog discovery timed out after {discovery_timeout_secs}s"
                )
            })??,
        )
    } else {
        None
    };

    let plant_capacity = if plant_capacity_probe_enabled {
        Some(run_plant_capacity_probe(&config).await?)
    } else {
        None
    };

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
        mbo_probe_enabled,
        plant_capacity_probe_enabled,
        ..Default::default()
    };
    if let Some(capacity) = plant_capacity {
        result.order_with_ticker_connected = capacity.order_with_ticker_connected;
        result.history_with_ticker_and_order_connected =
            capacity.history_with_ticker_and_order_connected;
        result.history_with_ticker_only_connected = capacity.history_with_ticker_only_connected;
        result.order_connection_error = capacity.order_error;
        result.history_with_order_connection_error = capacity.history_with_order_error;
        result.history_ticker_only_connection_error = capacity.history_ticker_only_error;
    }
    if let Some(catalog) = discovered_catalog {
        let path = std::path::Path::new(&catalog_dir).join("rithmic-discovery.json");
        catalog.save(&path)?;
        result.discovery_catalog_path = Some(path.display().to_string());
        result.discovered_exchanges = catalog.exchanges.len();
        result.entitled_exchanges = catalog
            .exchanges
            .iter()
            .filter(|exchange| exchange.entitled)
            .count();
        result.discovered_instruments = catalog.instruments.len();
    }
    let mut book_state = OrderBookProbeState::default();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let raw_book_metrics = std::sync::Arc::new(RawOrderBookMetrics::default());
    let task_raw_book_metrics = std::sync::Arc::clone(&raw_book_metrics);
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let (_runtime_sender, mut runtime_receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut session_task = tokio::spawn(async move {
        session
            .run(
                resolved,
                &mut runtime_receiver,
                sender,
                get_atomic_clock_realtime(),
                task_cancel,
                Some(task_raw_book_metrics),
                mbo_probe_enabled,
                None,
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
    let raw = raw_book_metrics.snapshot();
    result.raw_book_messages = raw.messages;
    result.max_raw_bid_entries = raw.max_bid_entries;
    result.max_raw_ask_entries = raw.max_ask_entries;
    result.book_messages_with_order_counts = raw.messages_with_order_counts;
    result.book_messages_with_implicit_liquidity = raw.messages_with_implicit_liquidity;
    result.mbp_parse_errors = raw.mbp_parse_errors;
    result.mbp_array_mismatch_errors = raw.mbp_array_mismatch_errors;
    result.mbp_timestamp_errors = raw.mbp_timestamp_errors;
    result.mbo_selected_price = raw.mbo_selected_price.map(|price| price.to_string());
    result.mbo_subscription_accepted = raw.mbo_subscription_accepted;
    result.mbo_subscription_rejected = raw.mbo_subscription_rejected;
    result.mbo_snapshot_messages = raw.mbo_snapshot_messages;
    result.mbo_update_messages = raw.mbo_update_messages;
    result.mbo_end_events = raw.mbo_end_events;
    result.mbo_new_orders = raw.mbo_new_orders;
    result.mbo_changed_orders = raw.mbo_changed_orders;
    result.mbo_deleted_orders = raw.mbo_deleted_orders;
    result.mbo_deletes_with_order_ids = raw.mbo_deletes_with_order_ids;
    result.mbo_entries_with_order_ids = raw.mbo_entries_with_order_ids;
    result.mbo_entries_with_priority = raw.mbo_entries_with_priority;
    result.mbo_entries_with_previous_price = raw.mbo_entries_with_previous_price;
    result.mbo_sequence_gaps = raw.mbo_sequence_gaps;
    result.mbo_resnapshots = raw.mbo_resnapshots;
    result.mbo_capable = result.mbo_entries_with_order_ids > 0
        && (result.mbo_snapshot_messages > 0 || result.mbo_update_messages > 0);
    if result.mbo_capable {
        result.order_book_type = Some(BookType::L3_MBO);
    } else if result.raw_book_messages > 0 {
        result.order_book_type = Some(BookType::L2_MBP);
    } else if result.order_book_type.is_none() && result.quotes > 0 {
        result.order_book_type = Some(BookType::L1_MBP);
    }
    result.individual_cancels_visible = result.mbo_capable
        && result.mbo_deletes_with_order_ids > 0;
    write_capability_summary(&result)?;
    Ok(result)
}

fn write_capability_summary(result: &RithmicConnectionProbeResult) -> anyhow::Result<()> {
    let Some(path) = &result.diagnostic_log_path else {
        return Ok(());
    };
    // Build the large details object in smaller groups. A single deeply nested
    // `json!` invocation can exceed rustc's default macro recursion limit.
    let mut details = serde_json::json!({
            "order_book_type": result.order_book_type.map(|value| value.to_string()),
            "discovery_catalog_path": result.discovery_catalog_path,
            "discovered_exchanges": result.discovered_exchanges,
            "entitled_exchanges": result.entitled_exchanges,
            "discovered_instruments": result.discovered_instruments,
    });
    let capacity = serde_json::json!({
            "plant_capacity_probe_enabled": result.plant_capacity_probe_enabled,
            "order_with_ticker_connected": result.order_with_ticker_connected,
            "history_with_ticker_and_order_connected": result.history_with_ticker_and_order_connected,
            "history_with_ticker_only_connected": result.history_with_ticker_only_connected,
            "order_connection_error": result.order_connection_error,
            "history_with_order_connection_error": result.history_with_order_connection_error,
            "history_ticker_only_connection_error": result.history_ticker_only_connection_error,
    });
    let market_by_price = serde_json::json!({
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
            "mbp_parse_errors": result.mbp_parse_errors,
            "mbp_array_mismatch_errors": result.mbp_array_mismatch_errors,
            "mbp_timestamp_errors": result.mbp_timestamp_errors,
    });
    let market_by_order = serde_json::json!({
            "mbo_probe_enabled": result.mbo_probe_enabled,
            "mbo_selected_price": result.mbo_selected_price,
            "mbo_subscription_accepted": result.mbo_subscription_accepted,
            "mbo_subscription_rejected": result.mbo_subscription_rejected,
            "mbo_snapshot_messages": result.mbo_snapshot_messages,
            "mbo_update_messages": result.mbo_update_messages,
            "mbo_end_events": result.mbo_end_events,
            "mbo_new_orders": result.mbo_new_orders,
            "mbo_changed_orders": result.mbo_changed_orders,
            "mbo_deleted_orders": result.mbo_deleted_orders,
            "mbo_deletes_with_order_ids": result.mbo_deletes_with_order_ids,
            "mbo_entries_with_order_ids": result.mbo_entries_with_order_ids,
            "mbo_entries_with_priority": result.mbo_entries_with_priority,
            "mbo_entries_with_previous_price": result.mbo_entries_with_previous_price,
            "mbo_sequence_gaps": result.mbo_sequence_gaps,
            "mbo_resnapshots": result.mbo_resnapshots,
            "mbo_capable": result.mbo_capable,
    });
    let details = details
        .as_object_mut()
        .expect("Rithmic diagnostic details must be a JSON object");
    details.extend(
        capacity
            .as_object()
            .expect("Rithmic capacity details must be a JSON object")
            .clone(),
    );
    details.extend(
        market_by_price
            .as_object()
            .expect("Rithmic MBP details must be a JSON object")
            .clone(),
    );
    details.extend(
        market_by_order
            .as_object()
            .expect("Rithmic MBO details must be a JSON object")
            .clone(),
    );
    let record = serde_json::json!({
        "stage": "order_book_capabilities",
        "status": "observed",
        "details": details,
    });
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{record}")?;
    Ok(())
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
    })
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
    if config.effective_book_feed() == crate::config::RithmicBookFeed::L2Mbp {
        bits |= update_bits::ORDER_BOOK;
    }
    anyhow::ensure!(
        bits != 0
            || config.effective_book_feed() == crate::config::RithmicBookFeed::L3Mbo,
        "At least one Rithmic market-data type must be enabled"
    );

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
