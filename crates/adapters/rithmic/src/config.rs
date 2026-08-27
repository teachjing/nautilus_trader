// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::fmt::{Debug, Formatter};

use serde::{Deserialize, Serialize};

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
    /// Calendar days before expiration to roll to the next contract.
    #[builder(default = 7)]
    pub rollover_days: u16,
    /// Request market-by-price order-book updates.
    #[builder(default = true)]
    pub subscribe_book_deltas: bool,
    /// Request best-bid/offer updates.
    #[builder(default = true)]
    pub subscribe_quotes: bool,
    /// Request last-trade updates.
    #[builder(default = true)]
    pub subscribe_trades: bool,
}

impl Debug for RithmicDataClientConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RithmicDataClientConfig")
            .field("gateway_url", &self.gateway_url)
            .field("system_name", &self.system_name)
            .field("username", &self.username.as_ref().map(|_| "<redacted>"))
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("market_subscriptions", &self.market_subscriptions)
            .field("rollover_days", &self.rollover_days)
            .field("subscribe_book_deltas", &self.subscribe_book_deltas)
            .field("subscribe_quotes", &self.subscribe_quotes)
            .field("subscribe_trades", &self.subscribe_trades)
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

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(RithmicDataClientConfig {
    gateway_url: String,
    system_name: String,
    market_subscriptions: Vec<String>,
    rollover_days: u16,
    subscribe_book_deltas: bool,
    subscribe_quotes: bool,
    subscribe_trades: bool,
});

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn default_config_uses_provider_neutral_discovery() {
        let config = RithmicDataClientConfig::default();
        assert!(config.market_subscriptions.is_empty());
        assert_eq!(config.rollover_days, 7);
        assert!(config.subscribe_book_deltas);
        assert!(config.subscribe_quotes);
        assert!(config.subscribe_trades);
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
