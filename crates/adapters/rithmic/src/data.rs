// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::get_data_event_sender},
    messages::DataEvent,
};
use nautilus_core::time::{AtomicTime, get_atomic_clock_realtime};
use nautilus_model::identifiers::{ClientId, Venue};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    config::RithmicDataClientConfig,
    flow::{LoginCredentials, MarketSubscription},
    protocol::update_bits,
    session::{ReconnectBackoff, RithmicSession},
};

/// Native Rithmic market-data client registered with the Nautilus DataEngine.
#[derive(Debug)]
pub struct RithmicDataClient {
    client_id: ClientId,
    venue: Venue,
    config: RithmicDataClientConfig,
    connected: Arc<AtomicBool>,
    cancellation_token: CancellationToken,
    session_task: Option<JoinHandle<()>>,
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

        Ok(Self {
            client_id,
            venue: Venue::from("CME"),
            config,
            connected: Arc::new(AtomicBool::new(false)),
            cancellation_token: CancellationToken::new(),
            session_task: None,
            data_sender: get_data_event_sender(),
            clock: get_atomic_clock_realtime(),
        })
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
        if self.config.subscribe_book_deltas {
            bits |= update_bits::ORDER_BOOK;
        }
        anyhow::ensure!(bits != 0, "At least one Rithmic market-data type must be enabled");

        self.config
            .market_subscriptions
            .iter()
            .map(|value| parse_subscription(value, bits))
            .collect()
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
        self.connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.cancellation_token = CancellationToken::new();
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
        let connect_timeout = Duration::from_secs(self.config.connect_timeout_secs);
        let initial_delay = Duration::from_secs(self.config.reconnect_delay_initial_secs);
        let maximum_delay = Duration::from_secs(self.config.reconnect_delay_max_secs);
        let session = RithmicSession::connect_subscribed(
            &gateway_url,
            &credentials,
            &subscriptions,
            connect_timeout,
        )
        .await?;

        if self.cancellation_token.is_cancelled() {
            self.cancellation_token = CancellationToken::new();
        }
        let cancel = self.cancellation_token.clone();
        let connected = Arc::clone(&self.connected);
        let data_sender = self.data_sender.clone();
        let clock = self.clock;
        connected.store(true, Ordering::Release);
        self.session_task = Some(get_runtime().spawn(async move {
            let mut active_session = Some(session);
            let mut backoff = ReconnectBackoff::new(initial_delay, maximum_delay)
                .expect("validated Rithmic reconnect delays");

            loop {
                let session = active_session
                    .take()
                    .expect("Rithmic session available before run");
                let result = session
                    .run(
                        subscriptions.clone(),
                        data_sender.clone(),
                        clock,
                        cancel.clone(),
                    )
                    .await;
                connected.store(false, Ordering::Release);
                if cancel.is_cancelled() {
                    break;
                }
                if let Err(error) = result {
                    log::warn!("Rithmic session disconnected: {error:#}");
                }

                loop {
                    let delay = backoff.next_delay();
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
                    )
                    .await
                    {
                        Ok(session) => {
                            active_session = Some(session);
                            backoff.reset();
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
        self.connected.store(false, Ordering::Release);
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
}
