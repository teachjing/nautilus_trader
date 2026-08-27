// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

#![cfg(feature = "live-tests")]

use nautilus_rithmic::{
    config::RithmicDataClientConfig,
    diagnostics::run_plant_capacity_probe,
};

#[tokio::test]
#[ignore = "requires live Rithmic credentials"]
async fn reports_concurrent_infrastructure_plant_capacity() {
    let config = RithmicDataClientConfig {
        gateway_url: std::env::var("RITHMIC_GATEWAY_URL")
            .unwrap_or_else(|_| "wss://rprotocol-mobile.rithmic.com/".to_string()),
        system_name: std::env::var("RITHMIC_SYSTEM_NAME")
            .unwrap_or_else(|_| "Rithmic Paper Trading".to_string()),
        diagnostic_log_dir: Some(
            std::env::var("RITHMIC_DIAGNOSTIC_LOG_DIR")
                .unwrap_or_else(|_| "target/rithmic-diagnostics".to_string()),
        ),
        ..Default::default()
    };
    let result = run_plant_capacity_probe(&config)
        .await
        .expect("Rithmic plant-capacity probe failed");
    println!("Rithmic plant connection capacity: {result:#?}");
    assert!(result.ticker_connected);
}
