// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

#![cfg(feature = "live-tests")]

use std::time::Duration;

use nautilus_model::enums::BookType;
use nautilus_model::identifiers::InstrumentId;
use nautilus_rithmic::{
    config::RithmicDataClientConfig,
    diagnostics::{run_connection_probe, run_dynamic_subscription_probe},
};

#[tokio::test]
#[ignore = "requires live Rithmic credentials and reference-data entitlement"]
async fn connects_idle_then_hydrates_subscribes_and_unsubscribes() {
    let system_name = std::env::var("RITHMIC_SYSTEM_NAME")
        .unwrap_or_else(|_| "Rithmic Paper Trading".to_string());
    let gateway_url = std::env::var("RITHMIC_GATEWAY_URL")
        .unwrap_or_else(|_| "wss://rprotocol-mobile.rithmic.com/".to_string());
    let instrument_id = std::env::var("RITHMIC_DYNAMIC_INSTRUMENT_ID")
        .unwrap_or_else(|_| "MESU6.CME".to_string());
    let instrument_id = InstrumentId::from_as_ref(&instrument_id)
        .expect("RITHMIC_DYNAMIC_INSTRUMENT_ID must use SYMBOL.VENUE format");
    let idle_secs = std::env::var("RITHMIC_DYNAMIC_IDLE_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2);
    let hydration_timeout_secs = std::env::var("RITHMIC_DYNAMIC_HYDRATION_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);
    let config = RithmicDataClientConfig {
        system_name,
        gateway_url,
        market_subscriptions: Vec::new(),
        diagnostic_log_dir: Some("target/rithmic-diagnostics".to_string()),
        ..Default::default()
    };

    let result = run_dynamic_subscription_probe(
        config,
        instrument_id,
        Duration::from_secs(idle_secs),
        Duration::from_secs(hydration_timeout_secs),
    )
    .await
    .expect("Rithmic dynamic subscription probe failed");
    println!("Rithmic dynamic subscription result: {result:#?}");

    assert!(result.connected_idle);
    assert!(result.instrument_hydrated);
    assert!(result.unsubscribed_cleanly);
}

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
    let require_mbo = std::env::var("RITHMIC_REQUIRE_MBO").is_ok_and(|value| {
        matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
    });
    let test_mbo = std::env::var("RITHMIC_TEST_MBO").is_ok_and(|value| {
        matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
    });
    let subscribe_mbo = require_mbo || test_mbo;
    let publish_mbo_events = std::env::var("RITHMIC_PUBLISH_MBO_EVENTS").is_ok_and(|value| {
        matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
    });
    let config = RithmicDataClientConfig {
        system_name,
        gateway_url,
        market_subscriptions: vec![subscription],
        front_month_fallback: Some(fallback),
        diagnostic_log_dir: Some(diagnostic_log_dir),
        subscribe_book_deltas: !subscribe_mbo,
        subscribe_mbo,
        publish_mbo_events,
        ..Default::default()
    };
    let expected_system_name = config.system_name.clone();
    let require_book_data = std::env::var("RITHMIC_REQUIRE_BOOK_DATA").is_ok_and(|value| {
        matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
    });
    let discover_markets = std::env::var("RITHMIC_DISCOVER_MARKETS").is_ok_and(|value| {
        matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
    });
    let discover_instruments = std::env::var("RITHMIC_DISCOVER_INSTRUMENTS").is_ok_and(|value| {
        matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
    });

    let result = run_connection_probe(config, Duration::from_secs(duration_secs))
        .await
        .expect("Rithmic live connection probe failed");
    println!("Rithmic live connection result: {result:#?}");

    assert!(!result.resolved_subscriptions.is_empty());
    assert!(result.available_systems.contains(&expected_system_name));
    assert!(result.diagnostic_log_path.is_some());
    if require_book_data {
        assert!(result.order_book_deltas > 0);
    }
    if require_mbo {
        assert_eq!(result.order_book_type, Some(BookType::L3_MBO));
        assert!(result.mbo_subscription_accepted);
        assert!(result.mbo_capable);
    }
    if publish_mbo_events {
        assert!(result.mbo_custom_events > 0);
    }
    if discover_markets || discover_instruments {
        assert!(result.discovery_catalog_path.is_some());
        assert!(result.discovered_exchanges > 0);
        assert!(result.entitled_exchanges > 0);
    }
    if discover_instruments {
        assert!(result.discovered_instruments > 0);
    }
    assert!(
        result.total_events() > 0,
        "Connected successfully but received no native market-data events; \
         run while the market is active"
    );
}
