// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Rithmic History Plant time-bar replay and native Nautilus bar conversion.

use std::time::Duration;

use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{Bar, BarType},
    types::{Price, Quantity},
};

use crate::{
    config::RithmicDataClientConfig,
    flow::LoginCredentials,
    protocol::ResponseTimeBarReplay,
    session::RithmicSession,
};

/// Rithmic time-bar interval family accepted by replay template 202.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RithmicHistoricalBarType {
    Second,
    Minute,
    Daily,
    Weekly,
}

impl RithmicHistoricalBarType {
    const fn nautilus_name(self) -> &'static str {
        match self {
            Self::Second => "SECOND",
            Self::Minute => "MINUTE",
            Self::Daily => "DAY",
            Self::Weekly => "WEEK",
        }
    }

    /// Parses a UI/config interval family name.
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "second" | "seconds" => Ok(Self::Second),
            "minute" | "minutes" => Ok(Self::Minute),
            "day" | "daily" => Ok(Self::Daily),
            "week" | "weekly" => Ok(Self::Weekly),
            _ => anyhow::bail!(
                "Unsupported Rithmic historical bar type '{value}'; expected second, minute, daily, or weekly"
            ),
        }
    }
}

/// JSON-safe historical bar returned for inspection and UI transport.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct RithmicHistoricalBarRecord {
    pub bar_type: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub ts_event: u64,
    pub ts_init: u64,
}

/// Result of one credential-gated History Plant time-bar replay probe.
#[derive(Debug, Clone)]
pub struct RithmicHistoricalBarProbeResult {
    pub available_systems: Vec<String>,
    pub instrument: String,
    pub bars: Vec<Bar>,
    pub records: Vec<RithmicHistoricalBarRecord>,
    pub output_path: String,
    pub pages: usize,
    pub first_timestamp: Option<UnixNanos>,
    pub last_timestamp: Option<UnixNanos>,
}

/// Requests historical bars through Rithmic templates 202/203 and converts them to native bars.
///
/// Only one WebSocket is active during this function. The History Plant socket is closed before
/// the result is returned.
///
/// # Errors
///
/// Returns an error for credentials, login, entitlement, replay, pagination, or conversion errors.
#[expect(clippy::too_many_arguments)]
pub async fn run_historical_time_bar_probe(
    config: RithmicDataClientConfig,
    exchange: &str,
    symbol: &str,
    bar_type: RithmicHistoricalBarType,
    period: u32,
    start_seconds: i32,
    finish_seconds: i32,
    max_pages: usize,
) -> anyhow::Result<RithmicHistoricalBarProbeResult> {
    anyhow::ensure!(!exchange.is_empty() && !symbol.is_empty(), "Historical instrument is empty");
    anyhow::ensure!(period > 0, "Historical bar period must be positive");
    anyhow::ensure!(period <= i32::MAX as u32, "Historical bar period is too large");
    anyhow::ensure!(start_seconds > 0 && finish_seconds > start_seconds, "Invalid replay range");
    anyhow::ensure!(max_pages > 0, "Historical max pages must be positive");
    let credentials = credentials(&config)?;
    let timeout = Duration::from_secs(config.connect_timeout_secs.max(30));
    let mut session = tokio::time::timeout(
        timeout,
        RithmicSession::connect_history(
            &config.gateway_url,
            &credentials,
            config.diagnostic_log_dir.as_deref(),
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("Rithmic History Plant login timed out after {timeout:?}"))??;
    let available_systems = session.available_systems().to_vec();
    let (bars, pages) = session
        .replay_time_bars(
            exchange,
            symbol,
            bar_type,
            period,
            start_seconds,
            finish_seconds,
            max_pages,
            get_atomic_clock_realtime(),
        )
        .await?;
    session.logout_and_close().await?;
    let first_timestamp = bars.first().map(|bar| bar.ts_event);
    let last_timestamp = bars.last().map(|bar| bar.ts_event);
    let records = bars
        .iter()
        .map(|bar| RithmicHistoricalBarRecord {
            bar_type: bar.bar_type.to_string(),
            open: bar.open.to_string(),
            high: bar.high.to_string(),
            low: bar.low.to_string(),
            close: bar.close.to_string(),
            volume: bar.volume.to_string(),
            ts_event: bar.ts_event.as_u64(),
            ts_init: bar.ts_init.as_u64(),
        })
        .collect::<Vec<_>>();
    let output_dir = config
        .diagnostic_log_dir
        .as_deref()
        .unwrap_or("target/rithmic-diagnostics");
    let output_path = std::path::Path::new(output_dir).join(format!(
        "rithmic-historical-{exchange}-{symbol}-{}-{period}.json",
        bar_type.nautilus_name().to_ascii_lowercase(),
    ));
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&output_path, serde_json::to_vec_pretty(&records)?)?;
    Ok(RithmicHistoricalBarProbeResult {
        available_systems,
        instrument: format!("{symbol}.{exchange}"),
        bars,
        records,
        output_path: output_path.display().to_string(),
        pages,
        first_timestamp,
        last_timestamp,
    })
}

pub(crate) fn parse_time_bar(
    response: &ResponseTimeBarReplay,
    requested_type: RithmicHistoricalBarType,
    period: u32,
    ts_init: UnixNanos,
) -> anyhow::Result<Bar> {
    anyhow::ensure!(response.marker > 0, "Rithmic historical bar marker is invalid");
    anyhow::ensure!(!response.symbol.is_empty() && !response.exchange.is_empty(), "Rithmic historical bar instrument is empty");
    let prices = [
        response.open_price,
        response.high_price,
        response.low_price,
        response.close_price,
    ];
    anyhow::ensure!(prices.iter().all(|value| value.is_finite() && *value > 0.0), "Rithmic historical OHLC is invalid");
    let precision = prices.iter().map(|value| decimal_precision(*value)).max().unwrap_or(0);
    let bar_type = BarType::from(
        format!(
            "{}.{}-{period}-{}-LAST-EXTERNAL",
            response.symbol,
            response.exchange,
            requested_type.nautilus_name(),
        )
        .as_str(),
    );
    let ts_event = UnixNanos::from((response.marker as u64) * 1_000_000_000);
    Bar::new_checked(
        bar_type,
        Price::new(response.open_price, precision),
        Price::new(response.high_price, precision),
        Price::new(response.low_price, precision),
        Price::new(response.close_price, precision),
        Quantity::new(response.volume as f64, 0),
        ts_event,
        ts_init,
    )
}

fn decimal_precision(value: f64) -> u8 {
    value
        .to_string()
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.trim_end_matches('0').len().min(9) as u8)
}

fn credentials(config: &RithmicDataClientConfig) -> anyhow::Result<LoginCredentials> {
    let user = config
        .username
        .clone()
        .or_else(|| std::env::var("RITHMIC_USER").ok())
        .ok_or_else(|| anyhow::anyhow!("Set RITHMIC_USER for the historical probe"))?;
    let password = config
        .password
        .clone()
        .or_else(|| std::env::var("RITHMIC_PASSWORD").ok())
        .ok_or_else(|| anyhow::anyhow!("Set RITHMIC_PASSWORD for the historical probe"))?;
    Ok(LoginCredentials {
        user,
        password,
        system_name: config.system_name.clone(),
        app_name: "NautilusTrader".to_string(),
        app_version: env!("CARGO_PKG_VERSION").to_string(),
        aggregated_quotes: false,
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn converts_replayed_minute_bar() {
        let response = ResponseTimeBarReplay {
            symbol: "ESU6".to_string(),
            exchange: "CME".to_string(),
            marker: 1_700_000_060,
            open_price: 6_000.0,
            high_price: 6_001.25,
            low_price: 5_999.75,
            close_price: 6_000.50,
            volume: 123,
            ..Default::default()
        };
        let bar = parse_time_bar(
            &response,
            RithmicHistoricalBarType::Minute,
            1,
            UnixNanos::from(1_700_000_061_000_000_000),
        )
        .unwrap();
        assert_eq!(bar.bar_type.to_string(), "ESU6.CME-1-MINUTE-LAST-EXTERNAL");
        assert_eq!(bar.volume, Quantity::from(123));
        assert_eq!(bar.open.precision, bar.close.precision);
    }

    #[rstest]
    #[case("second", RithmicHistoricalBarType::Second)]
    #[case("MINUTE", RithmicHistoricalBarType::Minute)]
    #[case("daily", RithmicHistoricalBarType::Daily)]
    #[case("weekly", RithmicHistoricalBarType::Weekly)]
    fn parses_historical_bar_type(
        #[case] value: &str,
        #[case] expected: RithmicHistoricalBarType,
    ) {
        assert_eq!(RithmicHistoricalBarType::parse(value).unwrap(), expected);
    }
}
