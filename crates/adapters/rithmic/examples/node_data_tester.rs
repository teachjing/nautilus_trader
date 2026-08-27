// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Runs a bounded live Rithmic ticker-plant connection and native-event probe.

use std::time::Duration;

use nautilus_rithmic::{
    config::RithmicDataClientConfig,
    diagnostics::run_connection_probe,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let system_name = std::env::var("RITHMIC_SYSTEM_NAME")
        .unwrap_or_else(|_| "Rithmic Paper Trading".to_string());
    let gateway_url = std::env::var("RITHMIC_GATEWAY_URL")
        .unwrap_or_else(|_| "wss://rprotocol-mobile.rithmic.com/".to_string());
    let subscriptions = std::env::var("RITHMIC_LIVE_SUBSCRIPTIONS")
        .unwrap_or_else(|_| "CME.MES".to_string())
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect();
    let fallback = std::env::var("RITHMIC_LIVE_FALLBACK_SUBSCRIPTION")
        .unwrap_or_else(|_| "CME.MESU6".to_string());
    let diagnostic_log_dir = std::env::var("RITHMIC_DIAGNOSTIC_LOG_DIR")
        .unwrap_or_else(|_| "target/rithmic-diagnostics".to_string());
    let duration_secs = std::env::var("RITHMIC_LIVE_DURATION_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);
    let config = RithmicDataClientConfig {
        system_name,
        gateway_url,
        market_subscriptions: subscriptions,
        front_month_fallback: Some(fallback),
        diagnostic_log_dir: Some(diagnostic_log_dir),
        ..Default::default()
    };

    let result = run_connection_probe(config, Duration::from_secs(duration_secs)).await?;
    println!("{result:#?}");
    anyhow::ensure!(
        result.total_events() > 0,
        "Connected successfully but received no native market-data events"
    );
    Ok(())
}
