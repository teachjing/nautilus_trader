// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

#![cfg(feature = "live-tests")]

use std::time::{SystemTime, UNIX_EPOCH};

use nautilus_rithmic::{
    config::RithmicDataClientConfig,
    history::{RithmicHistoricalBarType, run_historical_time_bar_probe},
};

#[tokio::test]
#[ignore = "requires live Rithmic credentials and History Plant entitlement"]
async fn validates_history_plant_time_bar_replay() {
    let subscription = std::env::var("RITHMIC_HISTORICAL_SUBSCRIPTION")
        .unwrap_or_else(|_| "CME.ESU6".to_string());
    let (exchange, symbol) = subscription
        .split_once('.')
        .expect("RITHMIC_HISTORICAL_SUBSCRIPTION must be EXCHANGE.SYMBOL");
    let finish = std::env::var("RITHMIC_HISTORICAL_FINISH_SECONDS")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_secs() as i32
        });
    let lookback_secs = std::env::var("RITHMIC_HISTORICAL_LOOKBACK_SECS")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(86_400);
    let period = std::env::var("RITHMIC_HISTORICAL_BAR_PERIOD")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let bar_type = RithmicHistoricalBarType::parse(
        &std::env::var("RITHMIC_HISTORICAL_BAR_TYPE")
            .unwrap_or_else(|_| "minute".to_string()),
    )
    .expect("RITHMIC_HISTORICAL_BAR_TYPE is invalid");
    let max_pages = std::env::var("RITHMIC_HISTORICAL_MAX_PAGES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
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
    let result = run_historical_time_bar_probe(
        config,
        exchange,
        symbol,
        bar_type,
        period,
        finish.saturating_sub(lookback_secs),
        finish,
        max_pages,
    )
    .await
    .expect("Rithmic historical time-bar probe failed");

    println!(
        "Rithmic historical result: instrument={}, bars={}, pages={}, first={:?}, last={:?}, output={}",
        result.instrument,
        result.bars.len(),
        result.pages,
        result.first_timestamp,
        result.last_timestamp,
        result.output_path,
    );
    assert!(!result.available_systems.is_empty());
    assert!(!result.bars.is_empty(), "History Plant returned no bars for the requested range");
    assert_eq!(result.records.len(), result.bars.len());
    assert!(std::path::Path::new(&result.output_path).exists());
    assert!(result.bars.windows(2).all(|pair| pair[0].ts_event < pair[1].ts_event));
}
