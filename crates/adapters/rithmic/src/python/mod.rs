// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Python bindings for the native Rithmic adapter.

use nautilus_common::factories::{ClientConfig, DataClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    config::{RithmicBookFeed, RithmicDataClientConfig},
    factories::{RITHMIC, RithmicDataClientFactory},
    flow::LoginCredentials,
    mbo::{RithmicMboAction, RithmicMboEvent, register_rithmic_custom_data},
    session::RithmicSession,
};

fn discovery_credentials(config: &RithmicDataClientConfig) -> PyResult<LoginCredentials> {
    let user = config
        .username
        .clone()
        .or_else(|| std::env::var("RITHMIC_USER").ok())
        .ok_or_else(|| to_pyruntime_err("Rithmic username missing"))?;
    let password = config
        .password
        .clone()
        .or_else(|| std::env::var("RITHMIC_PASSWORD").ok())
        .ok_or_else(|| to_pyruntime_err("Rithmic password missing"))?;

    Ok(LoginCredentials {
        user,
        password,
        system_name: config.system_name.clone(),
        app_name: "NautilusTrader".to_string(),
        app_version: env!("CARGO_PKG_VERSION").to_string(),
        aggregated_quotes: false,
    })
}

/// Discovers entitled exchanges and, optionally, their futures instruments.
///
/// The Python caller should invoke this blocking function through
/// `asyncio.to_thread` so provider discovery never blocks the FastAPI event loop.
#[pyfunction]
#[pyo3(signature = (config, include_instruments = false, instrument_exchanges = None))]
fn discover_catalog_json(
    py: Python<'_>,
    config: RithmicDataClientConfig,
    include_instruments: bool,
    instrument_exchanges: Option<Vec<String>>,
) -> PyResult<String> {
    let credentials = discovery_credentials(&config)?;
    let exchanges = instrument_exchanges.unwrap_or_default();
    py.detach(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                to_pyruntime_err(format!(
                    "Failed to create Rithmic discovery runtime: {e}"
                ))
            })?;
        let catalog = runtime
            .block_on(RithmicSession::discover_catalog_sequential(
                &config.gateway_url,
                &credentials,
                include_instruments,
                &exchanges,
                config.diagnostic_log_dir.as_deref(),
            ))
            .map_err(|e| to_pyruntime_err(format!("Rithmic catalog discovery failed: {e:#}")))?;
        serde_json::to_string(&catalog)
            .map_err(|e| to_pyruntime_err(format!("Failed to serialize Rithmic catalog: {e}")))
    })
}

/// Searches futures instruments for one entitled exchange.
///
/// A full-instrument result can be converted directly to Nautilus identity as
/// `SYMBOL.EXCHANGE`.
#[pyfunction]
fn search_instruments_json(
    py: Python<'_>,
    config: RithmicDataClientConfig,
    exchange: String,
    query: String,
) -> PyResult<String> {
    if exchange.trim().is_empty() || query.trim().is_empty() {
        return Err(to_pyruntime_err("Rithmic exchange and query are required"));
    }
    let credentials = discovery_credentials(&config)?;
    py.detach(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                to_pyruntime_err(format!(
                    "Failed to create Rithmic discovery runtime: {e}"
                ))
            })?;
        let instruments = runtime
            .block_on(RithmicSession::search_instruments(
                &config.gateway_url,
                &credentials,
                &exchange,
                &query,
                config.diagnostic_log_dir.as_deref(),
            ))
            .map_err(|e| to_pyruntime_err(format!("Rithmic instrument search failed: {e:#}")))?;
        serde_json::to_string(&instruments)
            .map_err(|e| to_pyruntime_err(format!("Failed to serialize Rithmic instruments: {e}")))
    })
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicDataClientConfig {
    /// Creates a Rithmic data-client configuration.
    #[new]
    #[pyo3(signature = (
        gateway_url = None,
        system_name = None,
        username = None,
        password = None,
        market_subscriptions = None,
        front_month_fallback = None,
        diagnostic_log_dir = None,
        rollover_days = None,
        subscribe_book_deltas = None,
        subscribe_mbo = None,
        book_feed = None,
        publish_mbo_events = None,
        client_id = None,
        subscribe_quotes = None,
        subscribe_trades = None,
        connect_timeout_secs = None,
        reconnect_delay_initial_secs = None,
        reconnect_delay_max_secs = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn py_new(
        gateway_url: Option<String>,
        system_name: Option<String>,
        username: Option<String>,
        password: Option<String>,
        market_subscriptions: Option<Vec<String>>,
        front_month_fallback: Option<String>,
        diagnostic_log_dir: Option<String>,
        rollover_days: Option<u16>,
        subscribe_book_deltas: Option<bool>,
        subscribe_mbo: Option<bool>,
        book_feed: Option<RithmicBookFeed>,
        publish_mbo_events: Option<bool>,
        client_id: Option<String>,
        subscribe_quotes: Option<bool>,
        subscribe_trades: Option<bool>,
        connect_timeout_secs: Option<u64>,
        reconnect_delay_initial_secs: Option<u64>,
        reconnect_delay_max_secs: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            gateway_url: gateway_url.unwrap_or(defaults.gateway_url),
            system_name: system_name.unwrap_or(defaults.system_name),
            username,
            password,
            market_subscriptions: market_subscriptions.unwrap_or_default(),
            front_month_fallback,
            diagnostic_log_dir,
            rollover_days: rollover_days.unwrap_or(defaults.rollover_days),
            subscribe_book_deltas: subscribe_book_deltas
                .unwrap_or(defaults.subscribe_book_deltas),
            subscribe_mbo: subscribe_mbo.unwrap_or(defaults.subscribe_mbo),
            book_feed,
            publish_mbo_events: publish_mbo_events.unwrap_or(defaults.publish_mbo_events),
            client_id,
            subscribe_quotes: subscribe_quotes.unwrap_or(defaults.subscribe_quotes),
            subscribe_trades: subscribe_trades.unwrap_or(defaults.subscribe_trades),
            connect_timeout_secs: connect_timeout_secs.unwrap_or(defaults.connect_timeout_secs),
            reconnect_delay_initial_secs: reconnect_delay_initial_secs
                .unwrap_or(defaults.reconnect_delay_initial_secs),
            reconnect_delay_max_secs: reconnect_delay_max_secs
                .unwrap_or(defaults.reconnect_delay_max_secs),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "RithmicDataClientConfig(gateway_url='{}', system_name='{}', subscriptions={:?})",
            self.gateway_url, self.system_name, self.market_subscriptions
        )
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicDataClientFactory {
    #[new]
    fn py_new() -> Self {
        Self
    }

    #[pyo3(name = "name")]
    fn py_name(&self) -> &str {
        RITHMIC
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_factory(py: Python<'_>, factory: Py<PyAny>) -> PyResult<Box<dyn DataClientFactory>> {
    factory
        .extract::<RithmicDataClientFactory>(py)
        .map(|value| Box::new(value) as Box<dyn DataClientFactory>)
        .map_err(|e| to_pyvalue_err(format!("Failed to extract Rithmic factory: {e}")))
}

#[expect(clippy::needless_pass_by_value)]
fn extract_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    config
        .extract::<RithmicDataClientConfig>(py)
        .map(|value| Box::new(value) as Box<dyn ClientConfig>)
        .map_err(|e| to_pyvalue_err(format!("Failed to extract Rithmic config: {e}")))
}

/// Exposes and registers the native adapter through `nautilus_trader.adapters.rithmic`.
///
/// # Errors
///
/// Returns an error if a Python class or registry extractor cannot be registered.
#[pymodule]
pub fn rithmic(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    register_rithmic_custom_data();
    m.add_class::<RithmicBookFeed>()?;
    m.add_class::<RithmicMboAction>()?;
    m.add_class::<RithmicMboEvent>()?;
    m.add_class::<RithmicDataClientConfig>()?;
    m.add_class::<RithmicDataClientFactory>()?;
    m.add_function(wrap_pyfunction!(discover_catalog_json, m)?)?;
    m.add_function(wrap_pyfunction!(search_instruments_json, m)?)?;

    let registry = get_global_pyo3_registry();
    registry
        .register_factory_extractor(RITHMIC.to_string(), extract_factory)
        .map_err(|e| to_pyruntime_err(format!("Failed to register Rithmic factory: {e}")))?;
    registry
        .register_config_extractor("RithmicDataClientConfig".to_string(), extract_config)
        .map_err(|e| to_pyruntime_err(format!("Failed to register Rithmic config: {e}")))?;
    Ok(())
}
