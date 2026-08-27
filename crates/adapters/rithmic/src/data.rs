// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use nautilus_common::clients::DataClient;
use nautilus_model::identifiers::{ClientId, Venue};

use crate::config::RithmicDataClientConfig;

/// Native Rithmic market-data client registered with the Nautilus DataEngine.
#[derive(Debug)]
pub struct RithmicDataClient {
    client_id: ClientId,
    venue: Venue,
    config: RithmicDataClientConfig,
    connected: Arc<AtomicBool>,
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

        Ok(Self {
            client_id,
            venue: Venue::from("CME"),
            config,
            connected: Arc::new(AtomicBool::new(false)),
        })
    }
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
        self.connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
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
        anyhow::bail!(
            "Rithmic transport is not enabled yet for gateway {}",
            self.config.gateway_url
        )
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected.store(false, Ordering::Release);
        Ok(())
    }
}
