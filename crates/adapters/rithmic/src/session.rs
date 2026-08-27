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
    flow::{
        LoginCredentials, MarketSubscription, ensure_response_success, heartbeat_interval,
        heartbeat_request,
    },
    parse::{QuoteState, parse_order_book, parse_quote, parse_trade},
    protocol::{
        FORCED_LOGOUT_TEMPLATE_ID, InboundMessage, LOGOUT_REQUEST_TEMPLATE_ID,
        LOGIN_RESPONSE_TEMPLATE_ID, REJECT_TEMPLATE_ID, RequestLogout, RequestSystemInfo,
        RequestFrontMonthContract, ResponseCode, ResponseFrontMonthContract, ResponseSystemInfo,
        SYSTEM_INFO_REQUEST_TEMPLATE_ID, SYSTEM_INFO_RESPONSE_TEMPLATE_ID, SubscriptionRequest,
        FRONT_MONTH_REQUEST_TEMPLATE_ID, FRONT_MONTH_RESPONSE_TEMPLATE_ID, decode_inbound,
        encode_frame,
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

    pub(crate) fn snapshot(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.messages.load(Ordering::Relaxed),
            self.max_bid_entries.load(Ordering::Relaxed),
            self.max_ask_entries.load(Ordering::Relaxed),
            self.messages_with_order_counts.load(Ordering::Relaxed),
            self.messages_with_implicit_liquidity
                .load(Ordering::Relaxed),
        )
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
    async fn connect_inner(
        gateway_url: &str,
        credentials: &LoginCredentials,
        diagnostic_log: Option<Arc<DiagnosticLog>>,
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
        Self::send_protobuf(&mut socket, &credentials.ticker_plant_request()).await?;
        let response = Self::receive_expected(&mut socket, LOGIN_RESPONSE_TEMPLATE_ID).await?;
        let InboundMessage::Login(login) = response else {
            anyhow::bail!("Rithmic login returned an unexpected response")
        };
        if let Some(log) = &diagnostic_log {
            log.record(
                "ticker_plant_login",
                if login.rp_code.first().is_some_and(|code| code == "0") {
                    "success"
                } else {
                    "error"
                },
                serde_json::json!({
                    "system_name": credentials.system_name,
                    "rp_code": login.rp_code,
                    "heartbeat_interval": login.heartbeat_interval,
                }),
            );
        }
        let heartbeat_interval = heartbeat_interval(&login)?;

        log::info!(
            "Connected to Rithmic ticker plant '{}'",
            credentials.system_name
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
            let mut session = Self::connect_inner(gateway_url, credentials, diagnostic_log).await?;
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
    ) -> anyhow::Result<()> {
        let mut heartbeat = tokio::time::interval(self.heartbeat_interval);
        let mut book_sequence = 0_u64;
        let mut quote_cache = HashMap::<InstrumentId, QuoteState>::new();
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;

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
                    ).await?;
                }
            }
        }

        for subscription in &subscriptions {
            self.unsubscribe(subscription).await?;
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
    ) -> anyhow::Result<()> {
        match message {
            WebSocketMessage::Binary(data) => match decode_inbound(&data)? {
                InboundMessage::Reject(response) => {
                    Self::ensure_codes_succeed(REJECT_TEMPLATE_ID, &response.rp_code)?;
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
                            log::warn!("Ignoring invalid Rithmic order-book update: {error}");
                        }
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

        assert_eq!(metrics.snapshot(), (1, 2, 1, 1, 1));
    }
}
