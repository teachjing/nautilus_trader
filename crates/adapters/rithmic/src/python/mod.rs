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
    config::RithmicDataClientConfig,
    factories::{RITHMIC, RithmicDataClientFactory},
};

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
        rollover_days = None,
        subscribe_book_deltas = None,
        subscribe_quotes = None,
        subscribe_trades = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn py_new(
        gateway_url: Option<String>,
        system_name: Option<String>,
        username: Option<String>,
        password: Option<String>,
        market_subscriptions: Option<Vec<String>>,
        rollover_days: Option<u16>,
        subscribe_book_deltas: Option<bool>,
        subscribe_quotes: Option<bool>,
        subscribe_trades: Option<bool>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            gateway_url: gateway_url.unwrap_or(defaults.gateway_url),
            system_name: system_name.unwrap_or(defaults.system_name),
            username,
            password,
            market_subscriptions: market_subscriptions.unwrap_or_default(),
            rollover_days: rollover_days.unwrap_or(defaults.rollover_days),
            subscribe_book_deltas: subscribe_book_deltas
                .unwrap_or(defaults.subscribe_book_deltas),
            subscribe_quotes: subscribe_quotes.unwrap_or(defaults.subscribe_quotes),
            subscribe_trades: subscribe_trades.unwrap_or(defaults.subscribe_trades),
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
    m.add_class::<RithmicDataClientConfig>()?;
    m.add_class::<RithmicDataClientFactory>()?;

    let registry = get_global_pyo3_registry();
    registry
        .register_factory_extractor(RITHMIC.to_string(), extract_factory)
        .map_err(|e| to_pyruntime_err(format!("Failed to register Rithmic factory: {e}")))?;
    registry
        .register_config_extractor("RithmicDataClientConfig".to_string(), extract_config)
        .map_err(|e| to_pyruntime_err(format!("Failed to register Rithmic config: {e}")))?;
    Ok(())
}
