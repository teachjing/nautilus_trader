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
    diagnostics::run_instrument_search,
};

#[tokio::test]
#[ignore = "requires live Rithmic credentials"]
async fn searches_contracts_for_selected_market_and_text() {
    let exchange = std::env::var("RITHMIC_SEARCH_EXCHANGE").unwrap_or_else(|_| "CME".to_string());
    let search_text =
        std::env::var("RITHMIC_SEARCH_TEXT").unwrap_or_else(|_| "MES".to_string());
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
    let result = run_instrument_search(&config, &exchange, &search_text)
        .await
        .expect("Rithmic instrument search failed");
    println!(
        "Rithmic instrument search result:\n{}",
        serde_json::to_string_pretty(&result).expect("instrument search result must serialize")
    );
    assert_eq!(result.exchange, exchange.trim().to_ascii_uppercase());
    assert_eq!(result.search_text, search_text.trim().to_ascii_uppercase());
    assert!(result.match_count > 0);
    assert!(!result.instruments.is_empty());
    assert!(std::path::Path::new(&result.output_path).exists());
}
