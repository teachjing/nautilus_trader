// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::get_data_event_sender},
    messages::{
        DataEvent,
        data::{
            BarsResponse, DataResponse, RequestBars, SubscribeBookDeltas, SubscribeQuotes,
            SubscribeTrades, UnsubscribeBookDeltas, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    datetime::datetime_to_unix_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    data::Bar,
    enums::{AggregationSource, BarAggregation, BookType, PriceType},
    identifiers::{ClientId, Venue},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    config::{RithmicBookFeed, RithmicDataClientConfig},
    flow::{LoginCredentials, MarketSubscription},
    history::RithmicHistoricalBarType,
    mbo::register_rithmic_custom_data,
    protocol::update_bits,
    session::{
        ReconnectBackoff, RithmicSession, RithmicSessionCommand, RuntimeDataType,
        RuntimeSubscription,
    },
};

const STABLE_SESSION_THRESHOLD: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct HistoryRequest {
    exchange: String,
    symbol: String,
    bar_type: RithmicHistoricalBarType,
    period: u32,
    start_seconds: i32,
    finish_seconds: i32,
    max_pages: usize,
    response: tokio::sync::oneshot::Sender<anyhow::Result<Vec<Bar>>>,
}

/// Native Rithmic market-data client registered with the Nautilus DataEngine.
#[derive(Debug)]
pub struct RithmicDataClient {
    client_id: ClientId,
    venue: Venue,
    config: RithmicDataClientConfig,
    connected: Arc<AtomicBool>,
    cancellation_token: CancellationToken,
    session_task: Option<JoinHandle<()>>,
    history_task: Option<JoinHandle<()>>,
    history_sender: tokio::sync::mpsc::UnboundedSender<HistoryRequest>,
    history_receiver: Option<tokio::sync::mpsc::UnboundedReceiver<HistoryRequest>>,
    runtime_sender: tokio::sync::mpsc::UnboundedSender<RithmicSessionCommand>,
    runtime_receiver: Option<tokio::sync::mpsc::UnboundedReceiver<RithmicSessionCommand>>,
    runtime_subscriptions: Arc<Mutex<HashSet<RuntimeSubscription>>>,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    clock: &'static AtomicTime,
}

impl RithmicDataClient {
    /// Creates a Rithmic data client.
    ///
    /// # Errors
    ///
    /// Returns an error when required credentials are unavailable.
    pub fn new(client_id: ClientId, config: RithmicDataClientConfig) -> anyhow::Result<Self> {
        let has_user = config.username.is_some() || std::env::var_os("RITHMIC_USER").is_some();
        let has_password =
            config.password.is_some() || std::env::var_os("RITHMIC_PASSWORD").is_some();
        anyhow::ensure!(has_user, "Rithmic username missing: set config or RITHMIC_USER");
        anyhow::ensure!(
            has_password,
            "Rithmic password missing: set config or RITHMIC_PASSWORD"
        );
        anyhow::ensure!(
            config.connect_timeout_secs > 0,
            "Rithmic connect timeout must be positive"
        );
        anyhow::ensure!(
            config.reconnect_delay_initial_secs > 0,
            "Rithmic initial reconnect delay must be positive"
        );
        anyhow::ensure!(
            config.reconnect_delay_initial_secs <= config.reconnect_delay_max_secs,
            "Rithmic initial reconnect delay cannot exceed maximum delay"
        );
        anyhow::ensure!(
            config.book_feed.is_some()
                || !(config.subscribe_book_deltas && config.subscribe_mbo),
            "Rithmic L2 market-by-price and L3 market-by-order subscriptions are mutually exclusive"
        );
        anyhow::ensure!(
            !config.publish_mbo_events
                || config.effective_book_feed() == RithmicBookFeed::L3Mbo,
            "Rithmic custom MBO events require the L3 MBO book feed"
        );

        Ok(Self::build(client_id, config, get_data_event_sender()))
    }

    fn build(
        client_id: ClientId,
        config: RithmicDataClientConfig,
        data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    ) -> Self {
        register_rithmic_custom_data();
        let (history_sender, history_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (runtime_sender, runtime_receiver) = tokio::sync::mpsc::unbounded_channel();
        let client_id = config
            .client_id
            .as_deref()
            .map(ClientId::from)
            .unwrap_or(client_id);
        Self {
            client_id,
            venue: Venue::from("CME"),
            config,
            connected: Arc::new(AtomicBool::new(false)),
            cancellation_token: CancellationToken::new(),
            session_task: None,
            history_task: None,
            history_sender,
            history_receiver: Some(history_receiver),
            runtime_sender,
            runtime_receiver: Some(runtime_receiver),
            runtime_subscriptions: Arc::new(Mutex::new(HashSet::new())),
            data_sender,
            clock: get_atomic_clock_realtime(),
        }
    }

    fn credentials(&self) -> anyhow::Result<LoginCredentials> {
        let user = self
            .config
            .username
            .clone()
            .or_else(|| std::env::var("RITHMIC_USER").ok())
            .ok_or_else(|| anyhow::anyhow!("Rithmic username missing"))?;
        let password = self
            .config
            .password
            .clone()
            .or_else(|| std::env::var("RITHMIC_PASSWORD").ok())
            .ok_or_else(|| anyhow::anyhow!("Rithmic password missing"))?;

        Ok(LoginCredentials {
            user,
            password,
            system_name: self.config.system_name.clone(),
            app_name: "NautilusTrader".to_string(),
            app_version: env!("CARGO_PKG_VERSION").to_string(),
            aggregated_quotes: false,
        })
    }

    fn subscriptions(&self) -> anyhow::Result<Vec<MarketSubscription>> {
        let mut bits = 0;
        if self.config.subscribe_trades {
            bits |= update_bits::LAST_TRADE;
        }
        if self.config.subscribe_quotes {
            bits |= update_bits::BBO;
        }
        if self.config.effective_book_feed() == RithmicBookFeed::L2Mbp {
            bits |= update_bits::ORDER_BOOK;
        }
        anyhow::ensure!(
            self.config.market_subscriptions.is_empty()
                || bits != 0
                || self.config.effective_book_feed() == RithmicBookFeed::L3Mbo,
            "At least one Rithmic market-data type must be enabled for configured instruments"
        );

        self.config
            .market_subscriptions
            .iter()
            .map(|value| parse_subscription(value, bits))
            .collect()
    }

    fn reset_history_channel(&mut self) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        self.history_sender = sender;
        self.history_receiver = Some(receiver);
    }

    fn reset_runtime_channel(&mut self) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        self.runtime_sender = sender;
        self.runtime_receiver = Some(receiver);
        self.runtime_subscriptions
            .lock()
            .expect("Rithmic runtime subscription lock poisoned")
            .clear();
    }

    fn send_runtime_subscription(
        &self,
        instrument_id: nautilus_model::identifiers::InstrumentId,
        data_type: RuntimeDataType,
        subscribe: bool,
    ) -> anyhow::Result<()> {
        let subscription = RuntimeSubscription {
            instrument_id,
            data_type,
        };
        let mut desired = self
            .runtime_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("Rithmic runtime subscription lock poisoned"))?;
        let changed = if subscribe {
            desired.insert(subscription.clone())
        } else {
            desired.remove(&subscription)
        };
        if !changed {
            return Ok(());
        }
        let command = if subscribe {
            RithmicSessionCommand::Subscribe(subscription)
        } else {
            RithmicSessionCommand::Unsubscribe(subscription)
        };
        self.runtime_sender
            .send(command)
            .map_err(|_| anyhow::anyhow!("Rithmic runtime subscription worker is unavailable"))
    }
}

fn parse_subscription(value: &str, bits: u32) -> anyhow::Result<MarketSubscription> {
    let (exchange, symbol) = value.split_once('.').ok_or_else(|| {
        anyhow::anyhow!(
            "Invalid Rithmic subscription '{value}': expected EXCHANGE.SYMBOL \
             (for example CME.MESU6)"
        )
    })?;
    anyhow::ensure!(
        !exchange.is_empty() && !symbol.is_empty() && !symbol.contains('.'),
        "Invalid Rithmic subscription '{value}': expected EXCHANGE.SYMBOL"
    );
    Ok(MarketSubscription::new(symbol, exchange, bits))
}

async fn run_history_worker(
    gateway_url: String,
    credentials: LoginCredentials,
    diagnostic_log_dir: Option<String>,
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<HistoryRequest>,
    cancel: CancellationToken,
    clock: &'static AtomicTime,
) {
    let mut session: Option<RithmicSession> = None;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    loop {
        let request = tokio::select! {
            () = cancel.cancelled() => None,
            request = receiver.recv() => request,
            _ = heartbeat.tick() => {
                if let Some(active) = &mut session
                    && let Err(error) = active.send_heartbeat().await
                {
                    log::warn!("Rithmic History Plant heartbeat failed: {error:#}");
                    session = None;
                }
                continue;
            },
        };
        let Some(request) = request else { break };
        if session.is_none() {
            match RithmicSession::connect_history(
                &gateway_url,
                &credentials,
                diagnostic_log_dir.as_deref(),
            )
            .await
            {
                Ok(connected) => session = Some(connected),
                Err(error) => {
                    let _ = request.response.send(Err(error));
                    continue;
                }
            }
        }
        let result = session
            .as_mut()
            .expect("History Plant session initialized")
            .replay_time_bars(
                &request.exchange,
                &request.symbol,
                request.bar_type,
                request.period,
                request.start_seconds,
                request.finish_seconds,
                request.max_pages,
                clock,
            )
            .await
            .map(|(bars, _)| bars);
        if result.is_err() {
            session = None;
        }
        let _ = request.response.send(result);
    }
    if let Some(mut session) = session {
        let _ = session.logout_and_close().await;
    }
}

fn map_historical_bar_type(
    aggregation: BarAggregation,
) -> anyhow::Result<RithmicHistoricalBarType> {
    match aggregation {
        BarAggregation::Second => Ok(RithmicHistoricalBarType::Second),
        BarAggregation::Minute => Ok(RithmicHistoricalBarType::Minute),
        BarAggregation::Day => Ok(RithmicHistoricalBarType::Daily),
        BarAggregation::Week => Ok(RithmicHistoricalBarType::Weekly),
        _ => anyhow::bail!("Rithmic does not support historical {aggregation:?} bars"),
    }
}

fn interval_seconds(bar_type: RithmicHistoricalBarType, period: u32) -> i64 {
    let unit = match bar_type {
        RithmicHistoricalBarType::Second => 1,
        RithmicHistoricalBarType::Minute => 60,
        RithmicHistoricalBarType::Daily => 86_400,
        RithmicHistoricalBarType::Weekly => 604_800,
    };
    i64::from(period) * unit
}

fn rithmic_history_max_pages(params: Option<&nautilus_core::Params>) -> usize {
    params
        .and_then(|params| params.get("options"))
        .and_then(serde_json::Value::as_object)
        .and_then(|options| options.get("max_pages"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(10)
}

#[async_trait::async_trait(?Send)]
impl DataClient for RithmicDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(self.venue)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting {} for {}", self.client_id, self.venue);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        if let Some(handle) = self.session_task.take() {
            handle.abort();
        }
        if let Some(handle) = self.history_task.take() {
            handle.abort();
        }
        self.connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.cancellation_token = CancellationToken::new();
        self.reset_history_channel();
        self.reset_runtime_channel();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }

        let credentials = self.credentials()?;
        let subscriptions = self.subscriptions()?;
        let gateway_url = self.config.gateway_url.clone();
        let front_month_fallback = self.config.front_month_fallback.clone();
        let diagnostic_log_dir = self.config.diagnostic_log_dir.clone();
        let connect_timeout = Duration::from_secs(self.config.connect_timeout_secs);
        let initial_delay = Duration::from_secs(self.config.reconnect_delay_initial_secs);
        let maximum_delay = Duration::from_secs(self.config.reconnect_delay_max_secs);
        let subscribe_mbo = self.config.effective_book_feed() == RithmicBookFeed::L3Mbo;
        let publish_mbo_events = self.config.publish_mbo_events;
        let (session, resolved_subscriptions) = RithmicSession::connect_subscribed(
            &gateway_url,
            &credentials,
            &subscriptions,
            connect_timeout,
            front_month_fallback.as_deref(),
            diagnostic_log_dir.as_deref(),
        )
        .await?;

        if self.cancellation_token.is_cancelled() {
            self.cancellation_token = CancellationToken::new();
        }
        let cancel = self.cancellation_token.clone();
        let connected = Arc::clone(&self.connected);
        let data_sender = self.data_sender.clone();
        let clock = self.clock;
        let mut runtime_receiver = self
            .runtime_receiver
            .take()
            .ok_or_else(|| anyhow::anyhow!("Rithmic runtime subscription worker is unavailable"))?;
        let runtime_sender = self.runtime_sender.clone();
        let runtime_subscriptions = Arc::clone(&self.runtime_subscriptions);
        if self.history_task.is_none() {
            let history_receiver = self
                .history_receiver
                .take()
                .ok_or_else(|| anyhow::anyhow!("Rithmic History Plant worker is unavailable"))?;
            let history_cancel = cancel.clone();
            let history_gateway_url = gateway_url.clone();
            let history_credentials = credentials.clone();
            let history_diagnostic_log_dir = diagnostic_log_dir.clone();
            self.history_task = Some(get_runtime().spawn(run_history_worker(
                history_gateway_url,
                history_credentials,
                history_diagnostic_log_dir,
                history_receiver,
                history_cancel,
                clock,
            )));
        }
        connected.store(true, Ordering::Release);
        self.session_task = Some(get_runtime().spawn(async move {
            let mut active_session = Some((session, resolved_subscriptions));
            let mut backoff = ReconnectBackoff::new(initial_delay, maximum_delay)
                .expect("validated Rithmic reconnect delays");

            loop {
                let (session, resolved_subscriptions) = active_session
                    .take()
                    .expect("Rithmic session available before run");
                let session_started = Instant::now();
                let desired = runtime_subscriptions
                    .lock()
                    .expect("Rithmic runtime subscription lock poisoned")
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();
                for subscription in desired {
                    let _ = runtime_sender.send(RithmicSessionCommand::Subscribe(subscription));
                }
                let result = session
                    .run(
                        resolved_subscriptions,
                        &mut runtime_receiver,
                        data_sender.clone(),
                        clock,
                        cancel.clone(),
                        None,
                        subscribe_mbo,
                        None,
                        publish_mbo_events,
                    )
                    .await;
                if session_started.elapsed() >= STABLE_SESSION_THRESHOLD {
                    backoff.reset();
                }
                connected.store(false, Ordering::Release);
                if cancel.is_cancelled() {
                    break;
                }
                if let Err(error) = result {
                    log::warn!("Rithmic session disconnected: {error:#}");
                }

                loop {
                    let delay = backoff.next_delay_with_jitter();
                    log::info!("Reconnecting Rithmic ticker plant in {delay:?}");
                    tokio::select! {
                        () = cancel.cancelled() => break,
                        () = tokio::time::sleep(delay) => {}
                    }
                    if cancel.is_cancelled() {
                        break;
                    }

                    match RithmicSession::connect_subscribed(
                        &gateway_url,
                        &credentials,
                        &subscriptions,
                        connect_timeout,
                        front_month_fallback.as_deref(),
                        diagnostic_log_dir.as_deref(),
                    )
                    .await
                    {
                        Ok((session, resolved_subscriptions)) => {
                            active_session = Some((session, resolved_subscriptions));
                            connected.store(true, Ordering::Release);
                            log::info!("Rithmic ticker plant reconnected and resubscribed");
                            break;
                        }
                        Err(error) => {
                            log::warn!("Rithmic reconnect attempt failed: {error:#}");
                        }
                    }
                }
                if cancel.is_cancelled() {
                    break;
                }
            }
            connected.store(false, Ordering::Release);
        }));
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        if let Some(handle) = self.session_task.take() {
            handle.await?;
        }
        if let Some(handle) = self.history_task.take() {
            handle.await?;
        }
        self.connected.store(false, Ordering::Release);
        self.cancellation_token = CancellationToken::new();
        self.reset_history_channel();
        self.reset_runtime_channel();
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        let feed = self.config.effective_book_feed();
        anyhow::ensure!(feed != RithmicBookFeed::None, "Rithmic order-book feed is disabled");
        let expected = match feed {
            RithmicBookFeed::L2Mbp => BookType::L2_MBP,
            RithmicBookFeed::L3Mbo => BookType::L3_MBO,
            RithmicBookFeed::None => unreachable!(),
        };
        anyhow::ensure!(
            cmd.book_type == expected,
            "Rithmic client {} is configured for {expected:?}, received {:?}",
            self.client_id,
            cmd.book_type,
        );
        self.send_runtime_subscription(
            cmd.instrument_id,
            RuntimeDataType::Book(feed),
            true,
        )
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        self.send_runtime_subscription(cmd.instrument_id, RuntimeDataType::Quote, true)
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        self.send_runtime_subscription(cmd.instrument_id, RuntimeDataType::Trade, true)
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        self.send_runtime_subscription(
            cmd.instrument_id,
            RuntimeDataType::Book(self.config.effective_book_feed()),
            false,
        )
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        self.send_runtime_subscription(cmd.instrument_id, RuntimeDataType::Quote, false)
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.send_runtime_subscription(cmd.instrument_id, RuntimeDataType::Trade, false)
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let bar_type = request.bar_type;
        anyhow::ensure!(
            bar_type.aggregation_source() == AggregationSource::External,
            "Rithmic historical bars require EXTERNAL aggregation (got {bar_type})"
        );
        anyhow::ensure!(
            bar_type.spec().price_type == PriceType::Last,
            "Rithmic historical bars require LAST price type (got {bar_type})"
        );
        let historical_type = map_historical_bar_type(bar_type.spec().aggregation)?;
        let period = bar_type.spec().step.get();
        let period = u32::try_from(period)
            .map_err(|_| anyhow::anyhow!("Rithmic historical bar period is too large"))?;
        let instrument_id = bar_type.instrument_id();
        let symbol = instrument_id.symbol.to_string();
        let exchange = instrument_id.venue.to_string();
        let now_seconds = (self.clock.get_time_ns().as_u64() / 1_000_000_000)
            .min(i32::MAX as u64) as i32;
        let finish_seconds = request
            .end
            .map_or(i64::from(now_seconds), |value| value.as_second());
        let requested_limit = request.limit.map(|value| value.get());
        let default_count = requested_limit.unwrap_or(1_000);
        let default_span = interval_seconds(historical_type, period)
            .saturating_mul(i64::try_from(default_count).unwrap_or(i64::MAX));
        let start_seconds = request
            .start
            .map_or_else(|| finish_seconds.saturating_sub(default_span), |value| value.as_second());
        let start_seconds = i32::try_from(start_seconds)
            .map_err(|_| anyhow::anyhow!("Rithmic historical start is outside epoch-second range"))?;
        let finish_seconds = i32::try_from(finish_seconds)
            .map_err(|_| anyhow::anyhow!("Rithmic historical end is outside epoch-second range"))?;
        anyhow::ensure!(finish_seconds > start_seconds, "Rithmic historical range is invalid");
        let max_pages = rithmic_history_max_pages(request.params.as_ref());
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
        self.history_sender
            .send(HistoryRequest {
                exchange,
                symbol,
                bar_type: historical_type,
                period,
                start_seconds,
                finish_seconds,
                max_pages,
                response: response_sender,
            })
            .map_err(|_| anyhow::anyhow!("Rithmic History Plant worker is unavailable"))?;

        let sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let params = request.params;
        let start = datetime_to_unix_nanos(request.start);
        let end = datetime_to_unix_nanos(request.end);
        let clock = self.clock;
        get_runtime().spawn(async move {
            match response_receiver.await {
                Ok(Ok(mut bars)) => {
                    if let Some(limit) = requested_limit
                        && bars.len() > limit
                    {
                        let drop_count = bars.len() - limit;
                        bars.drain(..drop_count);
                    }
                    let response = DataResponse::Bars(BarsResponse::new(
                        request_id,
                        client_id,
                        bar_type,
                        bars,
                        start,
                        end,
                        clock.get_time_ns(),
                        params,
                    ));
                    if let Err(error) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send Rithmic bars response: {error}");
                    }
                }
                Ok(Err(error)) => log::error!("Rithmic historical bars request failed: {error:#}"),
                Err(_) => log::error!("Rithmic History Plant response channel closed"),
            }
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn parses_exchange_and_symbol_subscription() {
        let subscription = parse_subscription("CME.MESU6", update_bits::LAST_TRADE).unwrap();

        assert_eq!(subscription.exchange, "CME");
        assert_eq!(subscription.symbol, "MESU6");
        assert_eq!(subscription.update_bits, update_bits::LAST_TRADE);
    }

    #[rstest]
    #[case("")]
    #[case("CME")]
    #[case(".MESU6")]
    #[case("CME.")]
    #[case("CME.MES.U6")]
    fn rejects_invalid_subscription(#[case] value: &str) {
        assert!(parse_subscription(value, update_bits::LAST_TRADE).is_err());
    }

    #[rstest]
    fn rejects_mixed_mbp_and_mbo_configuration() {
        let config = RithmicDataClientConfig {
            username: Some("user".to_string()),
            password: Some("password".to_string()),
            subscribe_book_deltas: true,
            subscribe_mbo: true,
            ..Default::default()
        };

        let error = RithmicDataClient::new(ClientId::from("RITHMIC"), config).unwrap_err();

        assert!(error.to_string().contains("mutually exclusive"));
    }

    #[rstest]
    fn typed_book_feed_controls_identity_and_runtime_book_type() {
        let config = RithmicDataClientConfig {
            username: Some("user".to_string()),
            password: Some("password".to_string()),
            book_feed: Some(RithmicBookFeed::L3Mbo),
            client_id: Some("RITHMIC_MBO".to_string()),
            ..Default::default()
        };
        let (data_sender, _data_receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut client =
            RithmicDataClient::build(ClientId::from("RITHMIC"), config, data_sender);
        let command = SubscribeBookDeltas::new(
            nautilus_model::identifiers::InstrumentId::from("MESU6.CME"),
            BookType::L2_MBP,
            Some(client.client_id()),
            None,
            nautilus_core::UUID4::new(),
            nautilus_core::UnixNanos::default(),
            None,
            true,
            None,
            None,
        );

        let error = client.subscribe_book_deltas(command).unwrap_err();

        assert_eq!(client.client_id(), ClientId::from("RITHMIC_MBO"));
        assert!(error.to_string().contains("L3_MBO"));
    }

    #[rstest]
    fn runtime_subscriptions_are_idempotent() {
        let config = RithmicDataClientConfig {
            username: Some("user".to_string()),
            password: Some("password".to_string()),
            ..Default::default()
        };
        let (data_sender, _data_receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut client =
            RithmicDataClient::build(ClientId::from("RITHMIC_MBP"), config, data_sender);
        let instrument_id = nautilus_model::identifiers::InstrumentId::from("MESU6.CME");

        client
            .send_runtime_subscription(instrument_id, RuntimeDataType::Quote, true)
            .unwrap();
        client
            .send_runtime_subscription(instrument_id, RuntimeDataType::Quote, true)
            .unwrap();

        let receiver = client.runtime_receiver.as_mut().unwrap();
        assert!(matches!(
            receiver.try_recv(),
            Ok(RithmicSessionCommand::Subscribe(_))
        ));
        assert!(receiver.try_recv().is_err());
    }

    #[rstest]
    fn permits_connect_first_configuration_without_seed_subscriptions() {
        let config = RithmicDataClientConfig {
            username: Some("user".to_string()),
            password: Some("password".to_string()),
            market_subscriptions: Vec::new(),
            subscribe_quotes: false,
            subscribe_trades: false,
            subscribe_book_deltas: false,
            book_feed: Some(RithmicBookFeed::None),
            ..Default::default()
        };
        let (data_sender, _data_receiver) = tokio::sync::mpsc::unbounded_channel();
        let client = RithmicDataClient::build(ClientId::from("RITHMIC"), config, data_sender);

        assert!(client.subscriptions().unwrap().is_empty());
    }

    #[rstest]
    #[case(BarAggregation::Second, RithmicHistoricalBarType::Second)]
    #[case(BarAggregation::Minute, RithmicHistoricalBarType::Minute)]
    #[case(BarAggregation::Day, RithmicHistoricalBarType::Daily)]
    #[case(BarAggregation::Week, RithmicHistoricalBarType::Weekly)]
    fn maps_nautilus_historical_bar_aggregation(
        #[case] aggregation: BarAggregation,
        #[case] expected: RithmicHistoricalBarType,
    ) {
        assert_eq!(map_historical_bar_type(aggregation).unwrap(), expected);
    }

    #[rstest]
    fn reads_rithmic_history_options_from_nested_property_bag() {
        let params: nautilus_core::Params = serde_json::from_value(serde_json::json!({
            "options": {
                "max_pages": 25,
                "future_option": true
            }
        }))
        .unwrap();

        assert_eq!(rithmic_history_max_pages(Some(&params)), 25);
        assert_eq!(params["options"]["future_option"], true);
    }
}
