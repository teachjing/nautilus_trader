// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

#![cfg(feature = "live-tests")]

use std::time::Duration;

use nautilus_rithmic::{
    config::RithmicDataClientConfig,
    diagnostics::run_connection_probe,
};

#[tokio::test]
#[ignore = "requires live Rithmic credentials, entitlements, and an active market"]
async fn validates_live_ticker_plant_connection_and_native_events() {
    let system_name = std::env::var("RITHMIC_SYSTEM_NAME")
        .unwrap_or_else(|_| "Rithmic Paper Trading".to_string());
    let gateway_url = std::env::var("RITHMIC_GATEWAY_URL")
        .unwrap_or_else(|_| "wss://rprotocol-mobile.rithmic.com/".to_string());
    let subscription =
        std::env::var("RITHMIC_LIVE_SUBSCRIPTION").unwrap_or_else(|_| "CME.MES".to_string());
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
        market_subscriptions: vec![subscription],
        front_month_fallback: Some(fallback),
        diagnostic_log_dir: Some(diagnostic_log_dir),
        ..Default::default()
    };
    let expected_system_name = config.system_name.clone();

    let result = run_connection_probe(config, Duration::from_secs(duration_secs))
        .await
        .expect("Rithmic live connection probe failed");
    println!("Rithmic live connection result: {result:#?}");

    assert!(!result.resolved_subscriptions.is_empty());
    assert!(result.available_systems.contains(&expected_system_name));
    assert!(result.diagnostic_log_path.is_some());
    assert!(
        result.total_events() > 0,
        "Connected successfully but received no native market-data events; \
         run while the market is active"
    );
}
