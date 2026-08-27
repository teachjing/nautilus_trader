// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Rithmic ticker-plant WebSocket session.

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{create_dir_all, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use nautilus_common::messages::DataEvent;
use nautilus_core::time::AtomicTime;
use nautilus_model::{data::Data, identifiers::InstrumentId};
use prost::Message as ProstMessage;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::Message as WebSocketMessage,
};
use tokio_util::sync::CancellationToken;

use crate::{
    discovery::{RithmicDiscoveryCatalog, RithmicExchangeInfo, RithmicInstrumentInfo},
    flow::{
        LoginCredentials, MarketSubscription, ensure_response_success, heartbeat_interval,
        heartbeat_request,
    },
    history::{RithmicHistoricalBarType, parse_time_bar},
    parse::{
        QuoteState, mbo_order_id, parse_depth_by_order_snapshot, parse_depth_by_order_update,
        parse_order_book, parse_quote, parse_trade,
    },
    protocol::{
        EntitlementFlag, FORCED_LOGOUT_TEMPLATE_ID, InboundMessage, InfrastructureType,
        LOGOUT_REQUEST_TEMPLATE_ID,
        LOGIN_RESPONSE_TEMPLATE_ID, REJECT_TEMPLATE_ID, RequestLogout, RequestSystemInfo,
        RequestFrontMonthContract, ResponseCode, ResponseFrontMonthContract, ResponseSystemInfo,
        SYSTEM_INFO_REQUEST_TEMPLATE_ID, SYSTEM_INFO_RESPONSE_TEMPLATE_ID, SubscriptionRequest,
        FRONT_MONTH_REQUEST_TEMPLATE_ID, FRONT_MONTH_RESPONSE_TEMPLATE_ID, decode_inbound,
        encode_frame, DepthByOrder, DepthUpdateType, RequestDepthByOrderSnapshot,
        RequestDepthByOrderUpdates, ResponseDepthByOrderSnapshot,
        DEPTH_BY_ORDER_SNAPSHOT_REQUEST_TEMPLATE_ID, DEPTH_BY_ORDER_UPDATES_REQUEST_TEMPLATE_ID,
        LIST_EXCHANGE_PERMISSIONS_REQUEST_TEMPLATE_ID, RequestListExchangePermissions,
        RequestSearchSymbols, SEARCH_SYMBOLS_REQUEST_TEMPLATE_ID, SearchInstrumentType,
        SearchPattern, ReplayDirection, ReplayTimeOrder, RequestTimeBarReplay,
        TIME_BAR_REPLAY_REQUEST_TEMPLATE_ID, TimeBarType,
    },
};

type RithmicWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Default)]
pub(crate) struct RawOrderBookMetrics {
    messages: AtomicU64,
    max_bid_entries: AtomicU64,
    max_ask_entries: AtomicU64,
    messages_with_order_counts: AtomicU64,
    messages_with_implicit_liquidity: AtomicU64,
    mbp_parse_errors: AtomicU64,
    mbp_array_mismatch_errors: AtomicU64,
    mbp_timestamp_errors: AtomicU64,
    mbo_snapshot_messages: AtomicU64,
    mbo_update_messages: AtomicU64,
    mbo_end_events: AtomicU64,
    mbo_subscription_accepted: AtomicU64,
    mbo_subscription_rejected: AtomicU64,
    mbo_new_orders: AtomicU64,
    mbo_changed_orders: AtomicU64,
    mbo_deleted_orders: AtomicU64,
    mbo_deletes_with_order_ids: AtomicU64,
    mbo_entries_with_order_ids: AtomicU64,
    mbo_entries_with_priority: AtomicU64,
    mbo_entries_with_previous_price: AtomicU64,
    mbo_selected_price_bits: AtomicU64,
}

#[derive(Debug, Default)]
pub(crate) struct RawOrderBookMetricsSnapshot {
    pub(crate) messages: u64,
    pub(crate) max_bid_entries: u64,
    pub(crate) max_ask_entries: u64,
    pub(crate) messages_with_order_counts: u64,
    pub(crate) messages_with_implicit_liquidity: u64,
    pub(crate) mbp_parse_errors: u64,
    pub(crate) mbp_array_mismatch_errors: u64,
    pub(crate) mbp_timestamp_errors: u64,
    pub(crate) mbo_snapshot_messages: u64,
    pub(crate) mbo_update_messages: u64,
    pub(crate) mbo_end_events: u64,
    pub(crate) mbo_subscription_accepted: bool,
    pub(crate) mbo_subscription_rejected: bool,
    pub(crate) mbo_new_orders: u64,
    pub(crate) mbo_changed_orders: u64,
    pub(crate) mbo_deleted_orders: u64,
    pub(crate) mbo_deletes_with_order_ids: u64,
    pub(crate) mbo_entries_with_order_ids: u64,
    pub(crate) mbo_entries_with_priority: u64,
    pub(crate) mbo_entries_with_previous_price: u64,
    pub(crate) mbo_selected_price: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PlantConnectionCapacity {
    pub(crate) ticker_connected: bool,
    pub(crate) order_with_ticker_connected: bool,
    pub(crate) history_with_ticker_and_order_connected: bool,
    pub(crate) history_with_ticker_only_connected: bool,
    pub(crate) order_error: Option<String>,
    pub(crate) history_with_order_error: Option<String>,
    pub(crate) history_ticker_only_error: Option<String>,
}

impl RawOrderBookMetrics {
    fn observe(&self, update: &crate::protocol::OrderBook) {
        self.messages.fetch_add(1, Ordering::Relaxed);
        self.max_bid_entries
            .fetch_max(update.bid_price.len() as u64, Ordering::Relaxed);
        self.max_ask_entries
            .fetch_max(update.ask_price.len() as u64, Ordering::Relaxed);
        if update.bid_orders.iter().any(|count| *count > 0)
            || update.ask_orders.iter().any(|count| *count > 0)
        {
            self.messages_with_order_counts
                .fetch_add(1, Ordering::Relaxed);
        }
        if update.implicit_bid_size.iter().any(|size| *size > 0)
            || update.implicit_ask_size.iter().any(|size| *size > 0)
        {
            self.messages_with_implicit_liquidity
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn observe_mbo_snapshot(&self, response: &ResponseDepthByOrderSnapshot) {
        self.mbo_snapshot_messages.fetch_add(1, Ordering::Relaxed);
        self.mbo_entries_with_order_ids.fetch_add(
            response
                .exchange_order_id
                .iter()
                .filter(|value| !value.is_empty())
                .count() as u64,
            Ordering::Relaxed,
        );
        self.mbo_entries_with_priority.fetch_add(
            response
                .depth_order_priority
                .iter()
                .filter(|value| **value > 0)
                .count() as u64,
            Ordering::Relaxed,
        );
    }

    fn observe_mbp_parse_error(&self, error: &anyhow::Error) {
        self.mbp_parse_errors.fetch_add(1, Ordering::Relaxed);
        let message = error.to_string();
        if message.contains("array lengths differ") {
            self.mbp_array_mismatch_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        if message.contains("timestamp is invalid") {
            self.mbp_timestamp_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn observe_mbo_update(&self, update: &DepthByOrder) {
        self.mbo_update_messages.fetch_add(1, Ordering::Relaxed);
        for update_type in &update.update_type {
            match DepthUpdateType::try_from(*update_type).unwrap_or_default() {
                DepthUpdateType::New => self.mbo_new_orders.fetch_add(1, Ordering::Relaxed),
                DepthUpdateType::Change => {
                    self.mbo_changed_orders.fetch_add(1, Ordering::Relaxed)
                }
                DepthUpdateType::Delete => {
                    self.mbo_deleted_orders.fetch_add(1, Ordering::Relaxed)
                }
                DepthUpdateType::Unspecified => 0,
            };
        }
        self.mbo_deletes_with_order_ids.fetch_add(
            update
                .update_type
                .iter()
                .zip(&update.exchange_order_id)
                .filter(|(update_type, order_id)| {
                    matches!(
                        DepthUpdateType::try_from(**update_type),
                        Ok(DepthUpdateType::Delete)
                    )
                        && !order_id.is_empty()
                })
                .count() as u64,
            Ordering::Relaxed,
        );
        self.mbo_entries_with_order_ids.fetch_add(
            update
                .exchange_order_id
                .iter()
                .filter(|value| !value.is_empty())
                .count() as u64,
            Ordering::Relaxed,
        );
        self.mbo_entries_with_priority.fetch_add(
            update
                .depth_order_priority
                .iter()
                .filter(|value| **value > 0)
                .count() as u64,
            Ordering::Relaxed,
        );
        self.mbo_entries_with_previous_price.fetch_add(
            update
                .prev_depth_price_flag
                .iter()
                .filter(|value| **value)
                .count() as u64,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn snapshot(&self) -> RawOrderBookMetricsSnapshot {
        let selected_price_bits = self.mbo_selected_price_bits.load(Ordering::Relaxed);
        RawOrderBookMetricsSnapshot {
            messages: self.messages.load(Ordering::Relaxed),
            max_bid_entries: self.max_bid_entries.load(Ordering::Relaxed),
            max_ask_entries: self.max_ask_entries.load(Ordering::Relaxed),
            messages_with_order_counts: self.messages_with_order_counts.load(Ordering::Relaxed),
            messages_with_implicit_liquidity: self
                .messages_with_implicit_liquidity
                .load(Ordering::Relaxed),
            mbp_parse_errors: self.mbp_parse_errors.load(Ordering::Relaxed),
            mbp_array_mismatch_errors: self
                .mbp_array_mismatch_errors
                .load(Ordering::Relaxed),
            mbp_timestamp_errors: self.mbp_timestamp_errors.load(Ordering::Relaxed),
            mbo_snapshot_messages: self.mbo_snapshot_messages.load(Ordering::Relaxed),
            mbo_update_messages: self.mbo_update_messages.load(Ordering::Relaxed),
            mbo_end_events: self.mbo_end_events.load(Ordering::Relaxed),
            mbo_subscription_accepted: self.mbo_subscription_accepted.load(Ordering::Relaxed) > 0,
            mbo_subscription_rejected: self.mbo_subscription_rejected.load(Ordering::Relaxed) > 0,
            mbo_new_orders: self.mbo_new_orders.load(Ordering::Relaxed),
            mbo_changed_orders: self.mbo_changed_orders.load(Ordering::Relaxed),
            mbo_deleted_orders: self.mbo_deleted_orders.load(Ordering::Relaxed),
            mbo_deletes_with_order_ids: self
                .mbo_deletes_with_order_ids
                .load(Ordering::Relaxed),
            mbo_entries_with_order_ids: self.mbo_entries_with_order_ids.load(Ordering::Relaxed),
            mbo_entries_with_priority: self.mbo_entries_with_priority.load(Ordering::Relaxed),
            mbo_entries_with_previous_price: self
                .mbo_entries_with_previous_price
                .load(Ordering::Relaxed),
            mbo_selected_price: (selected_price_bits != 0)
                .then(|| f64::from_bits(selected_price_bits)),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ReconnectBackoff {
    initial: Duration,
    maximum: Duration,
    next: Duration,
}

impl ReconnectBackoff {
    pub(crate) fn new(initial: Duration, maximum: Duration) -> anyhow::Result<Self> {
        anyhow::ensure!(!initial.is_zero(), "Reconnect delay must be positive");
        anyhow::ensure!(
            initial <= maximum,
            "Initial reconnect delay cannot exceed maximum delay"
        );
        Ok(Self {
            initial,
            maximum,
            next: initial,
        })
    }

    pub(crate) fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = self.next.saturating_add(self.initial).min(self.maximum);
        delay
    }

    pub(crate) fn reset(&mut self) {
        self.next = self.initial;
    }
}

pub(crate) struct RithmicSession {
    socket: RithmicWebSocket,
    heartbeat_interval: Duration,
    available_systems: Vec<String>,
    diagnostic_log: Option<Arc<DiagnosticLog>>,
}

#[derive(Debug)]
struct DiagnosticLog {
    path: PathBuf,
}

impl DiagnosticLog {
    fn create(directory: Option<&str>) -> anyhow::Result<Option<Arc<Self>>> {
        let Some(directory) = directory else {
            return Ok(None);
        };
        create_dir_all(directory)?;
        let epoch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let path = Path::new(directory).join(format!("rithmic-live-{epoch}.jsonl"));
        let log = Arc::new(Self { path });
        log.record("diagnostic_log", "created", serde_json::json!({}));
        log::info!("Writing Rithmic diagnostics to {}", log.path.display());
        Ok(Some(log))
    }

    fn record(&self, stage: &str, status: &str, details: serde_json::Value) {
        let record = serde_json::json!({
            "stage": stage,
            "status": status,
            "details": details,
        });
        let result = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .and_then(|mut file| writeln!(file, "{record}"));
        if let Err(e) = result {
            log::warn!("Failed to write Rithmic diagnostic log: {e}");
        }
    }
}

impl Debug for RithmicSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicSession))
            .field("heartbeat_interval", &self.heartbeat_interval)
            .finish_non_exhaustive()
    }
}

impl RithmicSession {
    async fn connect_without_discovery(
        gateway_url: &str,
        credentials: &LoginCredentials,
        diagnostic_log: Option<Arc<DiagnosticLog>>,
        infrastructure: InfrastructureType,
        available_systems: Vec<String>,
    ) -> anyhow::Result<Self> {
        let (mut socket, _) = connect_async(gateway_url).await?;
        let login_request = match infrastructure {
            InfrastructureType::TickerPlant => credentials.ticker_plant_request(),
            InfrastructureType::OrderPlant => credentials.order_plant_request(),
            InfrastructureType::HistoryPlant => credentials.history_plant_request(),
            _ => anyhow::bail!("Unsupported Rithmic infrastructure {infrastructure:?}"),
        };
        Self::send_protobuf(&mut socket, &login_request).await?;
        let response = Self::receive_expected(&mut socket, LOGIN_RESPONSE_TEMPLATE_ID).await?;
        let InboundMessage::Login(login) = response else {
            anyhow::bail!("Rithmic login returned an unexpected response")
        };
        let heartbeat_interval = heartbeat_interval(&login)?;
        if let Some(log) = &diagnostic_log {
            log.record(
                "plant_capacity_login",
                "success",
                serde_json::json!({
                    "infrastructure": format!("{infrastructure:?}"),
                    "rp_code": login.rp_code,
                }),
            );
        }
        Ok(Self {
            socket,
            heartbeat_interval,
            available_systems,
            diagnostic_log,
        })
    }

    pub(crate) async fn probe_plant_connection_capacity(
        gateway_url: &str,
        credentials: &LoginCredentials,
        diagnostic_log_dir: Option<&str>,
    ) -> anyhow::Result<PlantConnectionCapacity> {
        nautilus_cryptography::providers::install_cryptographic_provider();
        let diagnostic_log = DiagnosticLog::create(diagnostic_log_dir)?;
        let systems = Self::discover_systems(gateway_url, diagnostic_log.as_ref()).await?;
        anyhow::ensure!(
            systems.system_name.iter().any(|name| name == &credentials.system_name),
            "Rithmic system '{}' is unavailable",
            credentials.system_name
        );
        let available_systems = systems.system_name;
        let mut result = PlantConnectionCapacity::default();
        let mut ticker = Self::connect_without_discovery(
            gateway_url,
            credentials,
            diagnostic_log.clone(),
            InfrastructureType::TickerPlant,
            available_systems.clone(),
        )
        .await?;
        result.ticker_connected = true;

        let mut order = match Self::connect_without_discovery(
            gateway_url,
            credentials,
            diagnostic_log.clone(),
            InfrastructureType::OrderPlant,
            available_systems.clone(),
        )
        .await
        {
            Ok(session) => {
                result.order_with_ticker_connected = true;
                Some(session)
            }
            Err(error) => {
                result.order_error = Some(format!("{error:#}"));
                None
            }
        };

        let mut history = match Self::connect_without_discovery(
            gateway_url,
            credentials,
            diagnostic_log.clone(),
            InfrastructureType::HistoryPlant,
            available_systems.clone(),
        )
        .await
        {
            Ok(session) => {
                result.history_with_ticker_and_order_connected = order.is_some();
                result.history_with_ticker_only_connected = order.is_none();
                Some(session)
            }
            Err(error) => {
                result.history_with_order_error = Some(format!("{error:#}"));
                None
            }
        };
        if let Some(session) = &mut history {
            session.logout_and_close().await?;
        }
        if let Some(session) = &mut order {
            session.logout_and_close().await?;
        }

        // Always test the ticker + history combination independently. When the
        // three-plant attempt succeeds, skipping this step leaves the default
        // `false` value looking like a rejected connection rather than an
        // untested scenario.
        match Self::connect_without_discovery(
            gateway_url,
            credentials,
            diagnostic_log,
            InfrastructureType::HistoryPlant,
            available_systems,
        )
        .await
        {
            Ok(mut session) => {
                result.history_with_ticker_only_connected = true;
                session.logout_and_close().await?;
            }
            Err(error) => result.history_ticker_only_error = Some(format!("{error:#}")),
        }
        ticker.logout_and_close().await?;
        Ok(result)
    }
    async fn connect_inner(
        gateway_url: &str,
        credentials: &LoginCredentials,
        diagnostic_log: Option<Arc<DiagnosticLog>>,
        infrastructure: InfrastructureType,
    ) -> anyhow::Result<Self> {
        nautilus_cryptography::providers::install_cryptographic_provider();

        let systems = Self::discover_systems(gateway_url, diagnostic_log.as_ref())
            .await
            .context("Rithmic system discovery failed")?;
        anyhow::ensure!(
            systems.system_name.iter().any(|name| name == &credentials.system_name),
            "Rithmic system '{}' is unavailable, available systems: {}",
            credentials.system_name,
            systems.system_name.join(", ")
        );

        let (mut socket, _) = connect_async(gateway_url).await?;
        let login_request = match infrastructure {
            InfrastructureType::TickerPlant => credentials.ticker_plant_request(),
            InfrastructureType::OrderPlant => credentials.order_plant_request(),
            InfrastructureType::HistoryPlant => credentials.history_plant_request(),
            _ => anyhow::bail!("Unsupported Rithmic infrastructure {infrastructure:?}"),
        };
        Self::send_protobuf(&mut socket, &login_request).await?;
        let response = Self::receive_expected(&mut socket, LOGIN_RESPONSE_TEMPLATE_ID).await?;
        let InboundMessage::Login(login) = response else {
            anyhow::bail!("Rithmic login returned an unexpected response")
        };
        if let Some(log) = &diagnostic_log {
            log.record(
                "plant_login",
                if login.rp_code.first().is_some_and(|code| code == "0") {
                    "success"
                } else {
                    "error"
                },
                serde_json::json!({
                    "system_name": credentials.system_name,
                    "infrastructure": format!("{infrastructure:?}"),
                    "rp_code": login.rp_code,
                    "heartbeat_interval": login.heartbeat_interval,
                }),
            );
        }
        let heartbeat_interval = heartbeat_interval(&login)?;

        log::info!(
            "Connected to Rithmic {infrastructure:?} '{}'",
            credentials.system_name,
        );

        Ok(Self {
            socket,
            heartbeat_interval,
            available_systems: systems.system_name,
            diagnostic_log,
        })
    }

    pub(crate) async fn connect_subscribed(
        gateway_url: &str,
        credentials: &LoginCredentials,
        subscriptions: &[MarketSubscription],
        timeout: Duration,
        front_month_fallback: Option<&str>,
        diagnostic_log_dir: Option<&str>,
    ) -> anyhow::Result<(Self, Vec<MarketSubscription>)> {
        tokio::time::timeout(timeout, async {
            let diagnostic_log = DiagnosticLog::create(diagnostic_log_dir)?;
            let mut session = Self::connect_inner(
                gateway_url,
                credentials,
                diagnostic_log,
                InfrastructureType::TickerPlant,
            )
            .await?;
            let mut resolved = Vec::with_capacity(subscriptions.len());
            for subscription in subscriptions {
                let subscription = session
                    .resolve_subscription(subscription, front_month_fallback)
                    .await?;
                session.subscribe(&subscription).await?;
                resolved.push(subscription);
            }
            Ok((session, resolved))
        })
        .await
        .map_err(|_| anyhow::anyhow!("Rithmic setup timed out after {timeout:?}"))?
    }

    async fn resolve_subscription(
        &mut self,
        subscription: &MarketSubscription,
        front_month_fallback: Option<&str>,
    ) -> anyhow::Result<MarketSubscription> {
        if looks_like_futures_contract(&subscription.symbol) {
            return Ok(subscription.clone());
        }

        let request = RequestFrontMonthContract {
            template_id: FRONT_MONTH_REQUEST_TEMPLATE_ID,
            symbol: subscription.symbol.clone(),
            exchange: subscription.exchange.clone(),
            need_updates: false,
            ..Default::default()
        };
        Self::send_protobuf(&mut self.socket, &request).await?;
        let response = Self::receive_expected(&mut self.socket, FRONT_MONTH_RESPONSE_TEMPLATE_ID)
            .await?;
        let InboundMessage::FrontMonth(response) = response else {
            anyhow::bail!("Rithmic front-month lookup returned an unexpected response")
        };
        if let Some(log) = &self.diagnostic_log {
            log.record(
                "front_month_discovery",
                if response.rp_code.first().is_some_and(|code| code == "0") {
                    "success"
                } else {
                    "error"
                },
                serde_json::json!({
                    "requested": format!("{}.{}", subscription.exchange, subscription.symbol),
                    "rp_code": response.rp_code,
                    "trading_exchange": response.trading_exchange,
                    "trading_symbol": response.trading_symbol,
                }),
            );
        }
        match resolved_subscription(subscription, &response) {
            Ok(resolved) => Ok(resolved),
            Err(e) if front_month_unavailable(&response) => {
                let fallback = parse_fallback(front_month_fallback, subscription)?;
                log::warn!(
                    "Rithmic front-month discovery unavailable for {}.{} ({e}); using {}.{}",
                    subscription.exchange,
                    subscription.symbol,
                    fallback.exchange,
                    fallback.symbol,
                );
                if let Some(log) = &self.diagnostic_log {
                    log.record(
                        "front_month_fallback",
                        "selected",
                        serde_json::json!({
                            "contract": format!("{}.{}", fallback.exchange, fallback.symbol),
                            "reason": e.to_string(),
                        }),
                    );
                }
                Ok(fallback)
            }
            Err(e) => Err(e).context("Rithmic front-month discovery failed"),
        }
    }

    async fn discover_systems(
        gateway_url: &str,
        diagnostic_log: Option<&Arc<DiagnosticLog>>,
    ) -> anyhow::Result<ResponseSystemInfo> {
        let (mut socket, _) = connect_async(gateway_url).await?;
        let request = RequestSystemInfo {
            template_id: SYSTEM_INFO_REQUEST_TEMPLATE_ID,
            ..Default::default()
        };
        Self::send_protobuf(&mut socket, &request).await?;
        let response = Self::receive_expected(&mut socket, SYSTEM_INFO_RESPONSE_TEMPLATE_ID).await?;
        let InboundMessage::SystemInfo(systems) = response else {
            anyhow::bail!("Rithmic system discovery returned an unexpected response")
        };
        if let Some(log) = diagnostic_log {
            log.record(
                "system_discovery",
                if systems.rp_code.first().is_some_and(|code| code == "0") {
                    "success"
                } else {
                    "error"
                },
                serde_json::json!({
                    "rp_code": systems.rp_code,
                    "system_names": systems.system_name,
                    "has_aggregated_quotes": systems.has_aggregated_quotes,
                }),
            );
        }
        Self::ensure_codes_succeed(systems.template_id, &systems.rp_code)?;
        socket.close(None).await?;
        Ok(systems)
    }

    pub(crate) fn available_systems(&self) -> &[String] {
        &self.available_systems
    }

    pub(crate) fn diagnostic_log_path(&self) -> Option<&Path> {
        self.diagnostic_log.as_ref().map(|log| log.path.as_path())
    }

    /// Discovers exchange entitlements on an Order Plant socket, closes it, then optionally
    /// discovers futures instruments on a separate Ticker Plant socket.
    pub(crate) async fn discover_catalog_sequential(
        gateway_url: &str,
        credentials: &LoginCredentials,
        include_instruments: bool,
        diagnostic_log_dir: Option<&str>,
    ) -> anyhow::Result<RithmicDiscoveryCatalog> {
        let diagnostic_log = DiagnosticLog::create(diagnostic_log_dir)?;
        let mut order_session = Self::connect_inner(
            gateway_url,
            credentials,
            diagnostic_log.clone(),
            InfrastructureType::OrderPlant,
        )
        .await?;
        let request = RequestListExchangePermissions {
            template_id: LIST_EXCHANGE_PERMISSIONS_REQUEST_TEMPLATE_ID,
            user_msg: vec!["catalog_exchanges".to_string()],
            user: credentials.user.clone(),
        };
        Self::send_protobuf(&mut order_session.socket, &request).await?;
        let mut catalog = RithmicDiscoveryCatalog::default();
        loop {
            let response = order_session.receive_exchange_permission().await?;
            Self::ensure_handler_codes_succeed(&response.rq_handler_rp_code)?;
            if !response.exchange.is_empty() {
                catalog.exchanges.push(RithmicExchangeInfo {
                    exchange: response.exchange,
                    level_1_market_data: response.level_1_market_data,
                    level_2_market_data: response.level_2_market_data,
                    entitled: response.entitlement_flag == EntitlementFlag::Enabled as i32,
                });
            }
            if !response.rp_code.is_empty() {
                Self::ensure_codes_succeed(response.template_id, &response.rp_code)?;
                break;
            }
        }
        order_session.logout_and_close().await?;

        if include_instruments {
            let mut ticker_session = Self::connect_inner(
                gateway_url,
                credentials,
                diagnostic_log.clone(),
                InfrastructureType::TickerPlant,
            )
            .await?;
            let exchanges = catalog
                .exchanges
                .iter()
                .filter(|exchange| exchange.entitled)
                .map(|exchange| exchange.exchange.clone())
                .collect::<Vec<_>>();
            for exchange in exchanges {
                catalog
                    .instruments
                    .extend(ticker_session.search_futures(&exchange).await?);
            }
            ticker_session.logout_and_close().await?;
            catalog.instruments.sort_by(|a, b| {
                (&a.exchange, &a.symbol).cmp(&(&b.exchange, &b.symbol))
            });
            catalog.instruments.dedup_by(|a, b| {
                a.exchange == b.exchange && a.symbol == b.symbol
            });
        }
        catalog.exchanges.sort_by(|a, b| a.exchange.cmp(&b.exchange));
        if let Some(log) = &diagnostic_log {
            log.record(
                "market_instrument_discovery",
                "success",
                serde_json::json!({
                    "exchanges": catalog.exchanges.len(),
                    "entitled_exchanges": catalog.exchanges.iter().filter(|value| value.entitled).count(),
                    "instruments": catalog.instruments.len(),
                    "instrument_search_enabled": include_instruments,
                }),
            );
        }
        Ok(catalog)
    }

    pub(crate) async fn logout_and_close(&mut self) -> anyhow::Result<()> {
        let logout = RequestLogout {
            template_id: LOGOUT_REQUEST_TEMPLATE_ID,
            ..Default::default()
        };
        Self::send_protobuf(&mut self.socket, &logout).await?;
        self.socket.close(None).await?;
        Ok(())
    }

    pub(crate) async fn connect_history(
        gateway_url: &str,
        credentials: &LoginCredentials,
        diagnostic_log_dir: Option<&str>,
    ) -> anyhow::Result<Self> {
        let diagnostic_log = DiagnosticLog::create(diagnostic_log_dir)?;
        Self::connect_inner(
            gateway_url,
            credentials,
            diagnostic_log,
            InfrastructureType::HistoryPlant,
        )
        .await
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn replay_time_bars(
        &mut self,
        exchange: &str,
        symbol: &str,
        requested_type: RithmicHistoricalBarType,
        period: u32,
        start_seconds: i32,
        finish_seconds: i32,
        max_pages: usize,
        clock: &'static AtomicTime,
    ) -> anyhow::Result<(Vec<nautilus_model::data::Bar>, usize)> {
        let protocol_type = match requested_type {
            RithmicHistoricalBarType::Second => TimeBarType::Second,
            RithmicHistoricalBarType::Minute => TimeBarType::Minute,
            RithmicHistoricalBarType::Daily => TimeBarType::Daily,
            RithmicHistoricalBarType::Weekly => TimeBarType::Weekly,
        };
        let mut bars = Vec::new();
        let mut page_start = start_seconds;
        let mut pages = 0_usize;
        let mut heartbeat = tokio::time::interval(self.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;

        while pages < max_pages && page_start <= finish_seconds {
            pages += 1;
            let request = RequestTimeBarReplay {
                template_id: TIME_BAR_REPLAY_REQUEST_TEMPLATE_ID,
                user_msg: vec![format!("historical:{exchange}:{symbol}:{pages}")],
                symbol: symbol.to_string(),
                exchange: exchange.to_string(),
                bar_type: protocol_type as i32,
                bar_type_period: period as i32,
                start_index: page_start,
                finish_index: finish_seconds,
                user_max_count: 10_000,
                direction: ReplayDirection::First as i32,
                time_order: ReplayTimeOrder::Forwards as i32,
                resume_bars: false,
            };
            Self::send_protobuf(&mut self.socket, &request).await?;
            let page_initial_count = bars.len();
            let mut last_marker = 0_i32;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        anyhow::bail!("Rithmic historical replay stalled for 30 seconds")
                    }
                    _ = heartbeat.tick() => {
                        Self::send_protobuf(&mut self.socket, &heartbeat_request(0, 0)).await?;
                    }
                    message = self.socket.next() => {
                        let Some(message) = message else {
                            anyhow::bail!("Rithmic History Plant stream ended during replay")
                        };
                        match message? {
                            WebSocketMessage::Binary(data) => match decode_inbound(&data)? {
                                InboundMessage::TimeBarReplay(response) => {
                                    Self::ensure_handler_codes_succeed(&response.rq_handler_rp_code)?;
                                    if response.marker > 0 && !response.symbol.is_empty() {
                                        last_marker = last_marker.max(response.marker);
                                        bars.push(parse_time_bar(
                                            &response,
                                            requested_type,
                                            period,
                                            clock.get_time_ns(),
                                        )?);
                                    }
                                    if !response.rp_code.is_empty() {
                                        Self::ensure_codes_succeed(response.template_id, &response.rp_code)?;
                                        break;
                                    }
                                }
                                InboundMessage::Reject(response) => {
                                    Self::ensure_codes_succeed(REJECT_TEMPLATE_ID, &response.rp_code)?;
                                }
                                InboundMessage::ForcedLogout => anyhow::bail!("Rithmic forced logout during historical replay"),
                                _ => {}
                            },
                            WebSocketMessage::Ping(data) => self.socket.send(WebSocketMessage::Pong(data)).await?,
                            WebSocketMessage::Close(frame) => anyhow::bail!("Rithmic History Plant closed during replay: {frame:?}"),
                            WebSocketMessage::Text(_) | WebSocketMessage::Pong(_) | WebSocketMessage::Frame(_) => {}
                        }
                    }
                }
            }
            if bars.len() == page_initial_count || last_marker <= 0 || last_marker >= finish_seconds {
                break;
            }
            page_start = last_marker.saturating_add(1);
        }
        bars.sort_by_key(|bar| bar.ts_event);
        bars.dedup_by_key(|bar| bar.ts_event);
        if let Some(log) = &self.diagnostic_log {
            log.record(
                "historical_time_bar_replay",
                "success",
                serde_json::json!({
                    "instrument": format!("{symbol}.{exchange}"),
                    "bar_type": format!("{requested_type:?}"),
                    "period": period,
                    "start_seconds": start_seconds,
                    "finish_seconds": finish_seconds,
                    "pages": pages,
                    "bars": bars.len(),
                    "first_timestamp": bars.first().map(|bar| bar.ts_event.as_u64()),
                    "last_timestamp": bars.last().map(|bar| bar.ts_event.as_u64()),
                }),
            );
        }
        Ok((bars, pages))
    }

    async fn search_futures(&mut self, exchange: &str) -> anyhow::Result<Vec<RithmicInstrumentInfo>> {
        let request = RequestSearchSymbols {
            template_id: SEARCH_SYMBOLS_REQUEST_TEMPLATE_ID,
            user_msg: vec![format!("catalog_instruments:{exchange}")],
            search_text: String::new(),
            exchange: exchange.to_string(),
            product_code: String::new(),
            instrument_type: SearchInstrumentType::Future as i32,
            pattern: SearchPattern::Contains as i32,
        };
        Self::send_protobuf(&mut self.socket, &request).await?;
        let mut instruments = Vec::new();
        loop {
            let response = self.receive_search_symbol().await?;
            Self::ensure_handler_codes_succeed(&response.rq_handler_rp_code)?;
            if !response.symbol.is_empty() {
                instruments.push(RithmicInstrumentInfo {
                    symbol: response.symbol,
                    exchange: response.exchange,
                    symbol_name: response.symbol_name,
                    product_code: response.product_code,
                    instrument_type: response.instrument_type,
                    expiration_date: response.expiration_date,
                });
            }
            if !response.rp_code.is_empty() {
                // Code 7 means this exchange returned no matching futures, not a session failure.
                if response.rp_code.first().is_some_and(|code| code == "7") {
                    break;
                }
                Self::ensure_codes_succeed(response.template_id, &response.rp_code)?;
                break;
            }
        }
        Ok(instruments)
    }

    async fn receive_exchange_permission(
        &mut self,
    ) -> anyhow::Result<crate::protocol::ResponseListExchangePermissions> {
        loop {
            match self.receive_discovery_message().await? {
                InboundMessage::ExchangePermission(response) => return Ok(response),
                _ => continue,
            }
        }
    }

    async fn receive_search_symbol(
        &mut self,
    ) -> anyhow::Result<crate::protocol::ResponseSearchSymbols> {
        loop {
            match self.receive_discovery_message().await? {
                InboundMessage::SearchSymbol(response) => return Ok(response),
                _ => continue,
            }
        }
    }

    async fn receive_discovery_message(&mut self) -> anyhow::Result<InboundMessage> {
        while let Some(message) = self.socket.next().await {
            match message? {
                WebSocketMessage::Binary(data) => {
                    let message = decode_inbound(&data)?;
                    match &message {
                        InboundMessage::Reject(response) => {
                            Self::ensure_codes_succeed(REJECT_TEMPLATE_ID, &response.rp_code)?;
                        }
                        InboundMessage::ForcedLogout => anyhow::bail!("Rithmic forced logout received"),
                        _ => return Ok(message),
                    }
                }
                WebSocketMessage::Ping(data) => {
                    self.socket.send(WebSocketMessage::Pong(data)).await?;
                }
                WebSocketMessage::Close(frame) => {
                    anyhow::bail!("Rithmic WebSocket closed during catalog discovery: {frame:?}")
                }
                WebSocketMessage::Text(_) | WebSocketMessage::Pong(_) | WebSocketMessage::Frame(_) => {}
            }
        }
        anyhow::bail!("Rithmic WebSocket ended during catalog discovery")
    }

    fn ensure_handler_codes_succeed(rp_code: &[String]) -> anyhow::Result<()> {
        if rp_code.is_empty() {
            return Ok(());
        }
        Self::ensure_codes_succeed(0, rp_code)
    }

    pub(crate) async fn subscribe(
        &mut self,
        subscription: &MarketSubscription,
    ) -> anyhow::Result<()> {
        let request = subscription.request(SubscriptionRequest::Subscribe);
        Self::send_protobuf(&mut self.socket, &request).await
    }

    async fn unsubscribe(
        &mut self,
        subscription: &MarketSubscription,
    ) -> anyhow::Result<()> {
        let request = subscription.request(SubscriptionRequest::Unsubscribe);
        Self::send_protobuf(&mut self.socket, &request).await
    }

    pub(crate) async fn run(
        mut self,
        subscriptions: Vec<MarketSubscription>,
        data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
        clock: &'static AtomicTime,
        cancel: CancellationToken,
        raw_book_metrics: Option<Arc<RawOrderBookMetrics>>,
        probe_depth_by_order: bool,
        depth_by_order_price: Option<f64>,
    ) -> anyhow::Result<()> {
        let mut heartbeat = tokio::time::interval(self.heartbeat_interval);
        let mut book_sequence = 0_u64;
        let mut quote_cache = HashMap::<InstrumentId, QuoteState>::new();
        let mut depth_by_order_requests = HashMap::<InstrumentId, (MarketSubscription, f64)>::new();
        let mut mbo_order_ids = HashMap::<u64, String>::new();
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;

        if probe_depth_by_order {
            if let Some(depth_price) = depth_by_order_price {
                anyhow::ensure!(
                    depth_price.is_finite() && depth_price > 0.0,
                    "Rithmic depth-by-order probe price must be positive"
                );
                for subscription in &subscriptions {
                    self.request_depth_by_order(subscription, depth_price).await?;
                    let instrument_id = InstrumentId::from(
                        format!("{}.{}", subscription.symbol, subscription.exchange).as_str(),
                    );
                    depth_by_order_requests
                        .insert(instrument_id, (subscription.clone(), depth_price));
                }
                if let Some(metrics) = &raw_book_metrics {
                    metrics
                        .mbo_selected_price_bits
                        .store(depth_price.to_bits(), Ordering::Relaxed);
                }
            }
        }

        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = heartbeat.tick() => {
                    Self::send_protobuf(&mut self.socket, &heartbeat_request(0, 0)).await?;
                }
                message = self.socket.next() => {
                    let Some(message) = message else {
                        anyhow::bail!("Rithmic WebSocket stream ended")
                    };
                    self.handle_websocket_message(
                        message?,
                        &data_sender,
                        clock,
                        &mut book_sequence,
                        &mut quote_cache,
                        raw_book_metrics.as_ref(),
                        probe_depth_by_order,
                        depth_by_order_price,
                        &mut depth_by_order_requests,
                        &mut mbo_order_ids,
                    ).await?;
                }
            }
        }

        for subscription in &subscriptions {
            self.unsubscribe(subscription).await?;
        }
        for (_, (subscription, depth_price)) in depth_by_order_requests {
            self.unsubscribe_depth_by_order(&subscription, depth_price)
                .await?;
        }
        let logout = RequestLogout {
            template_id: LOGOUT_REQUEST_TEMPLATE_ID,
            ..Default::default()
        };
        Self::send_protobuf(&mut self.socket, &logout).await?;
        self.socket.close(None).await?;
        Ok(())
    }

    async fn handle_websocket_message(
        &mut self,
        message: WebSocketMessage,
        data_sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
        clock: &AtomicTime,
        book_sequence: &mut u64,
        quote_cache: &mut HashMap<InstrumentId, QuoteState>,
        raw_book_metrics: Option<&Arc<RawOrderBookMetrics>>,
        probe_depth_by_order: bool,
        configured_depth_price: Option<f64>,
        depth_by_order_requests: &mut HashMap<
            InstrumentId,
            (MarketSubscription, f64),
        >,
        mbo_order_ids: &mut HashMap<u64, String>,
    ) -> anyhow::Result<()> {
        match message {
            WebSocketMessage::Binary(data) => match decode_inbound(&data)? {
                InboundMessage::Reject(response) => {
                    if response.user_msg.iter().any(|value| value == "mbo_probe") {
                        if let Some(metrics) = raw_book_metrics {
                            metrics
                                .mbo_subscription_rejected
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        if let Some(log) = &self.diagnostic_log {
                            log.record(
                                "depth_by_order_reject",
                                "rejected",
                                serde_json::json!({"rp_code": response.rp_code}),
                            );
                        }
                        log::warn!(
                            "Rithmic depth-by-order request rejected: {}",
                            response.rp_code.join(": ")
                        );
                    } else {
                        Self::ensure_codes_succeed(REJECT_TEMPLATE_ID, &response.rp_code)?;
                    }
                }
                InboundMessage::ForcedLogout => {
                    anyhow::bail!("Rithmic forced logout received")
                }
                InboundMessage::MarketDataResponse(response) => {
                    if let Some(log) = &self.diagnostic_log {
                        log.record(
                            "market_data_subscription",
                            if response.rp_code.first().is_some_and(|code| code == "0") {
                                "success"
                            } else {
                                "error"
                            },
                            serde_json::json!({"rp_code": response.rp_code}),
                        );
                    }
                    ensure_response_success(&response)?;
                    log::debug!("Rithmic market-data subscription accepted");
                }
                InboundMessage::LastTrade(update) => {
                    match parse_trade(&update, clock.get_time_ns()) {
                        Ok(trade) => Self::send_data(data_sender, Data::Trade(trade)),
                        Err(error) => log::debug!("Ignoring Rithmic trade update: {error}"),
                    }
                }
                InboundMessage::BestBidOffer(update) => {
                    let instrument_id = InstrumentId::from(
                        format!("{}.{}", update.symbol, update.exchange).as_str(),
                    );
                    let state = quote_cache.entry(instrument_id).or_default();
                    match parse_quote(&update, state, clock.get_time_ns()) {
                        Ok(Some(quote)) => Self::send_data(data_sender, Data::Quote(quote)),
                        Ok(None) => {}
                        Err(error) => log::warn!("Ignoring invalid Rithmic BBO update: {error}"),
                    }
                    if probe_depth_by_order && !depth_by_order_requests.contains_key(&instrument_id)
                    {
                        let depth_price = configured_depth_price.or_else(|| {
                            (update.bid_price.is_finite() && update.bid_price > 0.0)
                                .then_some(update.bid_price)
                                .or_else(|| {
                                    (update.ask_price.is_finite() && update.ask_price > 0.0)
                                        .then_some(update.ask_price)
                                })
                        });
                        if let Some(depth_price) = depth_price {
                            let subscription = MarketSubscription::new(
                                update.symbol.clone(),
                                update.exchange.clone(),
                                0,
                            );
                            self.request_depth_by_order(&subscription, depth_price).await?;
                            depth_by_order_requests
                                .insert(instrument_id, (subscription, depth_price));
                            if let Some(metrics) = raw_book_metrics {
                                metrics
                                    .mbo_selected_price_bits
                                    .store(depth_price.to_bits(), Ordering::Relaxed);
                            }
                        }
                    }
                }
                InboundMessage::OrderBook(update) => {
                    if let Some(metrics) = raw_book_metrics {
                        metrics.observe(&update);
                    }
                    *book_sequence = book_sequence.saturating_add(1);
                    match parse_order_book(
                        &update,
                        *book_sequence,
                        clock.get_time_ns(),
                    ) {
                        Ok(deltas) => {
                            Self::send_data(data_sender, Data::Deltas(Box::new(deltas)));
                        }
                        Err(error) => {
                            if let Some(metrics) = raw_book_metrics {
                                metrics.observe_mbp_parse_error(&error);
                            }
                            log::warn!("Ignoring invalid Rithmic order-book update: {error}");
                        }
                    }
                }
                InboundMessage::DepthByOrderSnapshot(response) => {
                    if let Some(metrics) = raw_book_metrics {
                        metrics.observe_mbo_snapshot(&response);
                    }
                    if !response.rp_code.is_empty()
                        && !(response.rp_code.len() == 1 && response.rp_code[0] == "0")
                    {
                        log::warn!(
                            "Rithmic depth-by-order snapshot failed: {}",
                            response.rp_code.join(": ")
                        );
                    }
                    if let Some(log) = &self.diagnostic_log {
                        log.record(
                            "depth_by_order_snapshot",
                            if response.rp_code.is_empty()
                                || (response.rp_code.len() == 1 && response.rp_code[0] == "0")
                            {
                                "received"
                            } else {
                                "error"
                            },
                            serde_json::json!({
                                "rp_code": response.rp_code,
                                "rq_handler_rp_code": response.rq_handler_rp_code,
                                "exchange": response.exchange,
                                "symbol": response.symbol,
                                "depth_price": response.depth_price,
                                "orders": response.exchange_order_id.len(),
                            }),
                        );
                    }
                    Self::validate_mbo_order_ids(&response.exchange_order_id, mbo_order_ids)?;
                    match parse_depth_by_order_snapshot(&response, clock.get_time_ns()) {
                        Ok(deltas) => {
                            Self::send_data(data_sender, Data::Deltas(Box::new(deltas)));
                        }
                        Err(e) if response.exchange_order_id.is_empty() => {
                            log::debug!("Ignoring empty Rithmic MBO snapshot response: {e}");
                        }
                        Err(e) => log::warn!("Ignoring invalid Rithmic MBO snapshot: {e}"),
                    }
                }
                InboundMessage::DepthByOrderResponse(response) => {
                    let accepted = response.rp_code.len() == 1 && response.rp_code[0] == "0";
                    if let Some(metrics) = raw_book_metrics {
                        if accepted {
                            metrics
                                .mbo_subscription_accepted
                                .fetch_add(1, Ordering::Relaxed);
                        } else {
                            metrics
                                .mbo_subscription_rejected
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if accepted {
                        log::info!("Rithmic depth-by-order subscription accepted");
                    } else {
                        log::warn!(
                            "Rithmic depth-by-order subscription rejected: {}",
                            response.rp_code.join(": ")
                        );
                    }
                    if let Some(log) = &self.diagnostic_log {
                        log.record(
                            "depth_by_order_subscription",
                            if accepted { "accepted" } else { "rejected" },
                            serde_json::json!({"rp_code": response.rp_code}),
                        );
                    }
                }
                InboundMessage::DepthByOrder(update) => {
                    if let Some(metrics) = raw_book_metrics {
                        metrics.observe_mbo_update(&update);
                    }
                    Self::validate_mbo_order_ids(&update.exchange_order_id, mbo_order_ids)?;
                    match parse_depth_by_order_update(&update, clock.get_time_ns()) {
                        Ok(deltas) => {
                            Self::send_data(data_sender, Data::Deltas(Box::new(deltas)));
                        }
                        Err(e) => log::warn!("Ignoring invalid Rithmic MBO update: {e}"),
                    }
                    Self::release_deleted_mbo_order_ids(&update, mbo_order_ids);
                }
                InboundMessage::DepthByOrderEnd(_) => {
                    if let Some(metrics) = raw_book_metrics {
                        metrics.mbo_end_events.fetch_add(1, Ordering::Relaxed);
                    }
                }
                InboundMessage::Unsupported(template_id) => {
                    log::debug!("Ignoring unsupported Rithmic template {template_id}");
                }
                _ => {}
            },
            WebSocketMessage::Close(frame) => {
                anyhow::bail!("Rithmic WebSocket closed: {frame:?}")
            }
            WebSocketMessage::Ping(data) => {
                self.socket.send(WebSocketMessage::Pong(data)).await?;
            }
            WebSocketMessage::Text(_)
            | WebSocketMessage::Pong(_)
            | WebSocketMessage::Frame(_) => {}
        }
        Ok(())
    }

    async fn request_depth_by_order(
        &mut self,
        subscription: &MarketSubscription,
        depth_price: f64,
    ) -> anyhow::Result<()> {
        let snapshot = RequestDepthByOrderSnapshot {
            template_id: DEPTH_BY_ORDER_SNAPSHOT_REQUEST_TEMPLATE_ID,
            user_msg: vec!["mbo_probe".to_string()],
            symbol: subscription.symbol.clone(),
            exchange: subscription.exchange.clone(),
            depth_price,
            ..Default::default()
        };
        Self::send_protobuf(&mut self.socket, &snapshot).await?;
        let updates = RequestDepthByOrderUpdates {
            template_id: DEPTH_BY_ORDER_UPDATES_REQUEST_TEMPLATE_ID,
            user_msg: vec!["mbo_probe".to_string()],
            request: SubscriptionRequest::Subscribe as i32,
            symbol: subscription.symbol.clone(),
            exchange: subscription.exchange.clone(),
            depth_price,
            ..Default::default()
        };
        Self::send_protobuf(&mut self.socket, &updates).await?;
        log::info!(
            "Requested Rithmic depth by order for {}.{} at {depth_price}",
            subscription.exchange,
            subscription.symbol
        );
        Ok(())
    }

    fn validate_mbo_order_ids(
        exchange_order_ids: &[String],
        known: &mut HashMap<u64, String>,
    ) -> anyhow::Result<()> {
        for exchange_order_id in exchange_order_ids {
            let order_id = mbo_order_id(exchange_order_id)?;
            if let Some(existing) = known.get(&order_id) {
                anyhow::ensure!(
                    existing == exchange_order_id,
                    "Rithmic MBO order ID hash collision between '{existing}' and '{exchange_order_id}'"
                );
            } else {
                known.insert(order_id, exchange_order_id.clone());
            }
        }
        Ok(())
    }

    fn release_deleted_mbo_order_ids(
        update: &DepthByOrder,
        known: &mut HashMap<u64, String>,
    ) {
        for (update_type, exchange_order_id) in
            update.update_type.iter().zip(&update.exchange_order_id)
        {
            if matches!(
                DepthUpdateType::try_from(*update_type),
                Ok(DepthUpdateType::Delete)
            ) {
                if let Ok(order_id) = mbo_order_id(exchange_order_id) {
                    known.remove(&order_id);
                }
            }
        }
    }

    async fn unsubscribe_depth_by_order(
        &mut self,
        subscription: &MarketSubscription,
        depth_price: f64,
    ) -> anyhow::Result<()> {
        let request = RequestDepthByOrderUpdates {
            template_id: DEPTH_BY_ORDER_UPDATES_REQUEST_TEMPLATE_ID,
            user_msg: vec!["mbo_probe".to_string()],
            request: SubscriptionRequest::Unsubscribe as i32,
            symbol: subscription.symbol.clone(),
            exchange: subscription.exchange.clone(),
            depth_price,
            ..Default::default()
        };
        Self::send_protobuf(&mut self.socket, &request).await
    }

    async fn receive_expected(
        socket: &mut RithmicWebSocket,
        expected_template_id: i32,
    ) -> anyhow::Result<InboundMessage> {
        while let Some(message) = socket.next().await {
            match message? {
                WebSocketMessage::Binary(data) => {
                    let message = decode_inbound(&data)?;
                    match &message {
                        InboundMessage::Reject(response) => {
                            Self::ensure_codes_succeed(REJECT_TEMPLATE_ID, &response.rp_code)?;
                        }
                        InboundMessage::ForcedLogout => {
                            anyhow::bail!("Rithmic forced logout received")
                        }
                        _ if Self::template_id(&message) == Some(expected_template_id) => {
                            return Ok(message);
                        }
                        _ => {}
                    }
                }
                WebSocketMessage::Ping(data) => {
                    socket.send(WebSocketMessage::Pong(data)).await?;
                }
                WebSocketMessage::Close(frame) => {
                    anyhow::bail!("Rithmic WebSocket closed while awaiting response: {frame:?}")
                }
                WebSocketMessage::Text(_)
                | WebSocketMessage::Pong(_)
                | WebSocketMessage::Frame(_) => {}
            }
        }
        anyhow::bail!("Rithmic WebSocket ended while awaiting template {expected_template_id}")
    }

    fn template_id(message: &InboundMessage) -> Option<i32> {
        match message {
            InboundMessage::Login(response) => Some(response.template_id),
            InboundMessage::SystemInfo(response) => Some(response.template_id),
            InboundMessage::Logout(response)
            | InboundMessage::Heartbeat(response)
            | InboundMessage::MarketDataResponse(response)
            | InboundMessage::Reject(response) => Some(response.template_id),
            InboundMessage::FrontMonth(response) => Some(response.template_id),
            InboundMessage::LastTrade(response) => Some(response.template_id),
            InboundMessage::BestBidOffer(response) => Some(response.template_id),
            InboundMessage::OrderBook(response) => Some(response.template_id),
            InboundMessage::DepthByOrderSnapshot(response) => Some(response.template_id),
            InboundMessage::DepthByOrderResponse(response) => Some(response.template_id),
            InboundMessage::DepthByOrder(response) => Some(response.template_id),
            InboundMessage::DepthByOrderEnd(response) => Some(response.template_id),
            InboundMessage::ExchangePermission(response) => Some(response.template_id),
            InboundMessage::SearchSymbol(response) => Some(response.template_id),
            InboundMessage::TimeBarReplay(response) => Some(response.template_id),
            InboundMessage::Unsupported(template_id) => Some(*template_id),
            InboundMessage::ForcedLogout => Some(FORCED_LOGOUT_TEMPLATE_ID),
        }
    }

    async fn send_protobuf<M: ProstMessage>(
        socket: &mut RithmicWebSocket,
        message: &M,
    ) -> anyhow::Result<()> {
        socket
            .send(WebSocketMessage::Binary(encode_frame(message).into()))
            .await?;
        Ok(())
    }

    fn ensure_codes_succeed(template_id: i32, rp_code: &[String]) -> anyhow::Result<()> {
        let response = ResponseCode {
            template_id,
            rp_code: rp_code.to_vec(),
            ..Default::default()
        };
        ensure_response_success(&response)
    }

    fn send_data(
        sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
        data: Data,
    ) {
        if let Err(error) = sender.send(DataEvent::Data(data)) {
            log::error!("Failed to emit Rithmic data event: {error}");
        }
    }
}

fn looks_like_futures_contract(symbol: &str) -> bool {
    futures_contract_root(symbol).is_some()
}

fn futures_contract_root(symbol: &str) -> Option<&str> {
    const MONTH_CODES: &str = "FGHJKMNQUVXZ";

    let year_digits = symbol
        .as_bytes()
        .iter()
        .rev()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if year_digits == 0 || year_digits > 4 || symbol.len() <= year_digits {
        return None;
    }
    let month_index = symbol.len() - year_digits - 1;
    let month = symbol.as_bytes()[month_index] as char;
    (month_index > 0 && MONTH_CODES.contains(month)).then_some(&symbol[..month_index])
}

fn resolved_subscription(
    requested: &MarketSubscription,
    response: &ResponseFrontMonthContract,
) -> anyhow::Result<MarketSubscription> {
    RithmicSession::ensure_codes_succeed(response.template_id, &response.rp_code)?;
    anyhow::ensure!(
        !response.trading_symbol.is_empty(),
        "Rithmic returned no front-month contract for {}.{}",
        requested.exchange,
        requested.symbol
    );
    let exchange = if response.trading_exchange.is_empty() {
        requested.exchange.clone()
    } else {
        response.trading_exchange.clone()
    };
    log::info!(
        "Resolved Rithmic root {}.{} to {}.{}",
        requested.exchange,
        requested.symbol,
        exchange,
        response.trading_symbol
    );
    Ok(MarketSubscription::new(
        response.trading_symbol.clone(),
        exchange,
        requested.update_bits,
    ))
}

fn front_month_unavailable(response: &ResponseFrontMonthContract) -> bool {
    response.rp_code.first().is_some_and(|code| code == "7")
        || (response.rp_code.first().is_some_and(|code| code == "0")
            && response.trading_symbol.is_empty())
}

fn parse_fallback(
    value: Option<&str>,
    requested: &MarketSubscription,
) -> anyhow::Result<MarketSubscription> {
    let value = value.ok_or_else(|| {
        anyhow::anyhow!(
            "Rithmic returned no front-month data for {}.{} and no fallback was configured",
            requested.exchange,
            requested.symbol
        )
    })?;
    let (exchange, symbol) = value.split_once('.').ok_or_else(|| {
        anyhow::anyhow!("Invalid Rithmic front-month fallback '{value}': expected EXCHANGE.SYMBOL")
    })?;
    anyhow::ensure!(
        exchange == requested.exchange
            && futures_contract_root(symbol).is_some_and(|root| root == requested.symbol),
        "Invalid Rithmic front-month fallback '{value}': expected an explicit contract for {}.{}",
        requested.exchange,
        requested.symbol,
    );
    Ok(MarketSubscription::new(
        symbol,
        exchange,
        requested.update_bits,
    ))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn reconnect_backoff_increases_linearly_and_caps() {
        let mut backoff = ReconnectBackoff::new(
            Duration::from_secs(10),
            Duration::from_secs(30),
        )
        .unwrap();

        assert_eq!(backoff.next_delay(), Duration::from_secs(10));
        assert_eq!(backoff.next_delay(), Duration::from_secs(20));
        assert_eq!(backoff.next_delay(), Duration::from_secs(30));
        assert_eq!(backoff.next_delay(), Duration::from_secs(30));
    }

    #[rstest]
    fn reconnect_backoff_resets_after_success() {
        let mut backoff = ReconnectBackoff::new(
            Duration::from_secs(10),
            Duration::from_secs(30),
        )
        .unwrap();
        backoff.next_delay();
        backoff.next_delay();
        backoff.reset();

        assert_eq!(backoff.next_delay(), Duration::from_secs(10));
    }

    #[rstest]
    fn reconnect_backoff_rejects_invalid_bounds() {
        assert!(ReconnectBackoff::new(Duration::ZERO, Duration::from_secs(30)).is_err());
        assert!(
            ReconnectBackoff::new(Duration::from_secs(31), Duration::from_secs(30)).is_err()
        );
    }

    #[rstest]
    #[case("MESU6", true)]
    #[case("ESZ26", true)]
    #[case("NQH2027", true)]
    #[case("MES", false)]
    #[case("6E", false)]
    fn identifies_explicit_futures_contracts(#[case] symbol: &str, #[case] expected: bool) {
        assert_eq!(looks_like_futures_contract(symbol), expected);
    }

    #[rstest]
    fn maps_front_month_response_and_preserves_update_bits() {
        let requested = MarketSubscription::all_market_data("MES", "CME");
        let response = ResponseFrontMonthContract {
            template_id: FRONT_MONTH_RESPONSE_TEMPLATE_ID,
            rp_code: vec!["0".to_string()],
            trading_symbol: "MESU6".to_string(),
            trading_exchange: "CME".to_string(),
            ..Default::default()
        };

        let resolved = resolved_subscription(&requested, &response).unwrap();
        assert_eq!(resolved.symbol, "MESU6");
        assert_eq!(resolved.exchange, "CME");
        assert_eq!(resolved.update_bits, requested.update_bits);
    }

    #[rstest]
    fn rejects_empty_front_month_response() {
        let requested = MarketSubscription::all_market_data("MES", "CME");
        let response = ResponseFrontMonthContract {
            template_id: FRONT_MONTH_RESPONSE_TEMPLATE_ID,
            rp_code: vec!["0".to_string()],
            ..Default::default()
        };

        assert!(resolved_subscription(&requested, &response).is_err());
    }

    #[rstest]
    fn maps_no_data_to_explicit_fallback() {
        let requested = MarketSubscription::all_market_data("MES", "CME");
        let response = ResponseFrontMonthContract {
            template_id: FRONT_MONTH_RESPONSE_TEMPLATE_ID,
            rp_code: vec!["7".to_string(), "no data".to_string()],
            ..Default::default()
        };

        assert!(front_month_unavailable(&response));
        let fallback = parse_fallback(Some("CME.MESU6"), &requested).unwrap();
        assert_eq!(fallback.symbol, "MESU6");
        assert_eq!(fallback.exchange, "CME");
        assert_eq!(fallback.update_bits, requested.update_bits);
    }

    #[rstest]
    fn rejects_invalid_front_month_fallback() {
        let requested = MarketSubscription::all_market_data("MES", "CME");
        assert!(parse_fallback(Some("MESU6"), &requested).is_err());
        assert!(parse_fallback(Some("CME.MES"), &requested).is_err());
        assert!(parse_fallback(Some("CME.NQU6"), &requested).is_err());
        assert!(parse_fallback(Some("CBOT.MESU6"), &requested).is_err());
    }

    #[rstest]
    fn captures_raw_market_by_price_metadata() {
        let metrics = RawOrderBookMetrics::default();
        let update = crate::protocol::OrderBook {
            bid_price: vec![6000.00, 5999.75],
            bid_orders: vec![3, 2],
            implicit_bid_size: vec![0, 4],
            ask_price: vec![6000.25],
            ask_orders: vec![5],
            ..Default::default()
        };

        metrics.observe(&update);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages, 1);
        assert_eq!(snapshot.max_bid_entries, 2);
        assert_eq!(snapshot.max_ask_entries, 1);
        assert_eq!(snapshot.messages_with_order_counts, 1);
        assert_eq!(snapshot.messages_with_implicit_liquidity, 1);
    }

    #[rstest]
    fn captures_depth_by_order_identity_and_cancel_actions() {
        let metrics = RawOrderBookMetrics::default();
        let update = DepthByOrder {
            update_type: vec![
                DepthUpdateType::New as i32,
                DepthUpdateType::Change as i32,
                DepthUpdateType::Delete as i32,
            ],
            exchange_order_id: vec![
                "order-1".to_string(),
                "order-2".to_string(),
                "order-3".to_string(),
            ],
            depth_order_priority: vec![10, 20, 30],
            prev_depth_price_flag: vec![false, true, false],
            ..Default::default()
        };

        metrics.observe_mbo_update(&update);
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.mbo_update_messages, 1);
        assert_eq!(snapshot.mbo_new_orders, 1);
        assert_eq!(snapshot.mbo_changed_orders, 1);
        assert_eq!(snapshot.mbo_deleted_orders, 1);
        assert_eq!(snapshot.mbo_deletes_with_order_ids, 1);
        assert_eq!(snapshot.mbo_entries_with_order_ids, 3);
        assert_eq!(snapshot.mbo_entries_with_priority, 3);
        assert_eq!(snapshot.mbo_entries_with_previous_price, 1);
    }
}
