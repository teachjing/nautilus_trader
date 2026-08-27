// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{config::RithmicDataClientConfig, data::RithmicDataClient};

/// Stable adapter name used by the global factory registry.
pub const RITHMIC: &str = "RITHMIC";

impl ClientConfig for RithmicDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for native Rithmic data clients.
#[derive(Debug, Clone, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
pub struct RithmicDataClientFactory;

impl DataClientFactory for RithmicDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<RithmicDataClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("Expected RithmicDataClientConfig, was {config:?}"))?
            .clone();
        Ok(Box::new(RithmicDataClient::new(
            ClientId::from(name),
            config,
        )?))
    }

    fn name(&self) -> &'static str {
        RITHMIC
    }

    fn config_type(&self) -> &'static str {
        "RithmicDataClientConfig"
    }
}
