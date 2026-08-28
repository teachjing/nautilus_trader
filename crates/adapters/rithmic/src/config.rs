// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::fmt::{Debug, Formatter};

use serde::{Deserialize, Serialize};

/// Rithmic order-book feed carried by one data-client instance.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        frozen,
        eq,
        module = "nautilus_trader.adapters.rithmic",
        from_py_object,
        rename_all = "SCREAMING_SNAKE_CASE",
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.rithmic")
)]
pub enum RithmicBookFeed {
    /// Do not request an order-book feed.
    None,
    /// Aggregated market-by-price levels from template 156.
    #[default]
    L2Mbp,
    /// Full-depth market-by-order events from templates 115-118 and 160-161.
    L3Mbo,
}

/// Configuration for the native Rithmic live data client.
#[derive(Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
pub struct RithmicDataClientConfig {
    /// Rithmic gateway WebSocket URL.
    pub gateway_url: String,
    /// Rithmic system name selected during login.
    pub system_name: String,
    /// Optional username; falls back to `RITHMIC_USER`.
    pub username: Option<String>,
    /// Optional password; falls back to `RITHMIC_PASSWORD`.
    pub password: Option<String>,
    /// Provider-neutral roots such as `CME.MES`.
    #[builder(default)]
    pub market_subscriptions: Vec<String>,
    /// Explicit contract used when front-month discovery returns no data.
    pub front_month_fallback: Option<String>,
    /// Directory for credential-safe Rithmic discovery and connection logs.
    pub diagnostic_log_dir: Option<String>,
    /// Calendar days before expiration to roll to the next contract.
    #[builder(default = 7)]
    pub rollover_days: u16,
    /// Request market-by-price order-book updates.
    #[builder(default = true)]
    pub subscribe_book_deltas: bool,
    /// Request full-depth market-by-order updates with exchange order IDs.
    ///
    /// This is mutually exclusive with `subscribe_book_deltas` because Nautilus books must not
    /// mix L2 market-by-price and L3 market-by-order deltas.
    #[builder(default)]
    pub subscribe_mbo: bool,
    /// Explicit order-book feed selection for runtime and UI configuration.
    ///
    /// When set, this supersedes the legacy `subscribe_book_deltas` and `subscribe_mbo` flags.
    pub book_feed: Option<RithmicBookFeed>,
    /// Optional stable client identity shown in Nautilus commands, logs, and UI events.
    pub client_id: Option<String>,
    /// Request best-bid/offer updates.
    #[builder(default = true)]
    pub subscribe_quotes: bool,
    /// Request last-trade updates.
    #[builder(default = true)]
    pub subscribe_trades: bool,
    /// Timeout for discovery, login, and subscription setup.
    #[builder(default = 30)]
    pub connect_timeout_secs: u64,
    /// Initial delay before reconnecting a dropped ticker-plant session.
    #[builder(default = 10)]
    pub reconnect_delay_initial_secs: u64,
    /// Maximum delay between ticker-plant reconnect attempts.
    #[builder(default = 120)]
    pub reconnect_delay_max_secs: u64,
}

impl Debug for RithmicDataClientConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RithmicDataClientConfig")
            .field("gateway_url", &self.gateway_url)
            .field("system_name", &self.system_name)
            .field("username", &self.username.as_ref().map(|_| "<redacted>"))
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("market_subscriptions", &self.market_subscriptions)
            .field("front_month_fallback", &self.front_month_fallback)
            .field("diagnostic_log_dir", &self.diagnostic_log_dir)
            .field("rollover_days", &self.rollover_days)
            .field("subscribe_book_deltas", &self.subscribe_book_deltas)
            .field("subscribe_mbo", &self.subscribe_mbo)
            .field("book_feed", &self.book_feed)
            .field("client_id", &self.client_id)
            .field("subscribe_quotes", &self.subscribe_quotes)
            .field("subscribe_trades", &self.subscribe_trades)
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field(
                "reconnect_delay_initial_secs",
                &self.reconnect_delay_initial_secs,
            )
            .field(
                "reconnect_delay_max_secs",
                &self.reconnect_delay_max_secs,
            )
            .finish()
    }
}

impl Default for RithmicDataClientConfig {
    fn default() -> Self {
        Self::builder()
            .gateway_url("wss://rprotocol-mobile.rithmic.com/".to_string())
            .system_name("Rithmic Paper Trading".to_string())
            .build()
    }
}

impl RithmicDataClientConfig {
    /// Returns the effective order-book feed, including legacy configuration compatibility.
    #[must_use]
    pub fn effective_book_feed(&self) -> RithmicBookFeed {
        self.book_feed.unwrap_or_else(|| {
            if self.subscribe_mbo {
                RithmicBookFeed::L3Mbo
            } else if self.subscribe_book_deltas {
                RithmicBookFeed::L2Mbp
            } else {
                RithmicBookFeed::None
            }
        })
    }
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(RithmicDataClientConfig {
    gateway_url: String,
    system_name: String,
    market_subscriptions: Vec<String>,
    front_month_fallback: Option<String>,
    diagnostic_log_dir: Option<String>,
    rollover_days: u16,
    subscribe_book_deltas: bool,
    subscribe_mbo: bool,
    book_feed: Option<RithmicBookFeed>,
    client_id: Option<String>,
    subscribe_quotes: bool,
    subscribe_trades: bool,
    connect_timeout_secs: u64,
    reconnect_delay_initial_secs: u64,
    reconnect_delay_max_secs: u64,
});

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn default_config_uses_provider_neutral_discovery() {
        let config = RithmicDataClientConfig::default();
        assert!(config.market_subscriptions.is_empty());
        assert!(config.front_month_fallback.is_none());
        assert!(config.diagnostic_log_dir.is_none());
        assert_eq!(config.rollover_days, 7);
        assert!(config.subscribe_book_deltas);
        assert!(!config.subscribe_mbo);
        assert_eq!(config.effective_book_feed(), RithmicBookFeed::L2Mbp);
        assert!(config.subscribe_quotes);
        assert!(config.subscribe_trades);
        assert_eq!(config.connect_timeout_secs, 30);
        assert_eq!(config.reconnect_delay_initial_secs, 10);
        assert_eq!(config.reconnect_delay_max_secs, 120);
    }

    #[rstest]
    fn debug_output_redacts_credentials() {
        let config = RithmicDataClientConfig {
            username: Some("rithmic-user".to_string()),
            password: Some("rithmic-secret".to_string()),
            ..Default::default()
        };
        let output = format!("{config:?}");

        assert!(!output.contains("rithmic-user"));
        assert!(!output.contains("rithmic-secret"));
        assert!(output.contains("<redacted>"));
    }
}
