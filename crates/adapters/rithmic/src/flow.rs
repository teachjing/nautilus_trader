// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Rithmic ticker-plant session flow.

use std::{
    fmt::{Debug, Formatter},
    time::Duration,
};

use crate::protocol::{
    HEARTBEAT_REQUEST_TEMPLATE_ID, InfrastructureType, LOGIN_REQUEST_TEMPLATE_ID,
    MARKET_DATA_REQUEST_TEMPLATE_ID, PROTOCOL_TEMPLATE_VERSION, RequestHeartbeat, RequestLogin,
    RequestMarketDataUpdate, ResponseCode, ResponseLogin, SubscriptionRequest, update_bits,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketSubscription {
    pub symbol: String,
    pub exchange: String,
    pub update_bits: u32,
}

impl MarketSubscription {
    /// Creates a trades, quotes, and market-by-price subscription.
    #[must_use]
    pub fn all_market_data(symbol: impl Into<String>, exchange: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            exchange: exchange.into(),
            update_bits: update_bits::LAST_TRADE | update_bits::BBO | update_bits::ORDER_BOOK,
        }
    }

    #[must_use]
    pub fn request(&self, action: SubscriptionRequest) -> RequestMarketDataUpdate {
        RequestMarketDataUpdate {
            template_id: MARKET_DATA_REQUEST_TEMPLATE_ID,
            symbol: self.symbol.clone(),
            exchange: self.exchange.clone(),
            request: action as i32,
            update_bits: self.update_bits,
            ..Default::default()
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct LoginCredentials {
    pub user: String,
    pub password: String,
    pub system_name: String,
    pub app_name: String,
    pub app_version: String,
    pub aggregated_quotes: bool,
}

impl Debug for LoginCredentials {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoginCredentials")
            .field("user", &"<redacted>")
            .field("password", &"<redacted>")
            .field("system_name", &self.system_name)
            .field("app_name", &self.app_name)
            .field("app_version", &self.app_version)
            .field("aggregated_quotes", &self.aggregated_quotes)
            .finish()
    }
}

impl LoginCredentials {
    #[must_use]
    pub fn ticker_plant_request(&self) -> RequestLogin {
        RequestLogin {
            template_id: LOGIN_REQUEST_TEMPLATE_ID,
            template_version: PROTOCOL_TEMPLATE_VERSION.to_string(),
            user: self.user.clone(),
            password: self.password.clone(),
            app_name: self.app_name.clone(),
            app_version: self.app_version.clone(),
            system_name: self.system_name.clone(),
            infra_type: InfrastructureType::TickerPlant as i32,
            aggregated_quotes: self.aggregated_quotes,
            ..Default::default()
        }
    }
}

/// Validates a Rithmic response-code array.
///
/// Rithmic represents success as the one-element array `["0"]` and errors as an error code plus
/// human-readable text.
pub fn ensure_response_success(response: &ResponseCode) -> anyhow::Result<()> {
    anyhow::ensure!(
        response.rp_code.len() == 1 && response.rp_code[0] == "0",
        "Rithmic request failed: {}",
        response.rp_code.join(": ")
    );
    Ok(())
}

/// Returns the server heartbeat cadence after validating login.
pub fn heartbeat_interval(response: &ResponseLogin) -> anyhow::Result<Duration> {
    let code = ResponseCode {
        template_id: response.template_id,
        rp_code: response.rp_code.clone(),
        ..Default::default()
    };
    ensure_response_success(&code)?;
    anyhow::ensure!(
        response.heartbeat_interval.is_finite() && response.heartbeat_interval > 0.0,
        "Rithmic returned an invalid heartbeat interval: {}",
        response.heartbeat_interval
    );
    Ok(Duration::from_secs_f64(response.heartbeat_interval))
}

#[must_use]
pub fn heartbeat_request(ssboe: i32, usecs: i32) -> RequestHeartbeat {
    RequestHeartbeat {
        template_id: HEARTBEAT_REQUEST_TEMPLATE_ID,
        ssboe,
        usecs,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn ticker_login_uses_required_protocol_fields() {
        let request = LoginCredentials {
            user: "user".to_string(),
            password: "secret".to_string(),
            system_name: "Rithmic Paper Trading".to_string(),
            app_name: "NautilusTrader".to_string(),
            app_version: "2.0".to_string(),
            aggregated_quotes: false,
        }
        .ticker_plant_request();

        assert_eq!(request.template_id, LOGIN_REQUEST_TEMPLATE_ID);
        assert_eq!(request.template_version, PROTOCOL_TEMPLATE_VERSION);
        assert_eq!(request.infra_type, InfrastructureType::TickerPlant as i32);
    }

    #[rstest]
    fn login_debug_output_redacts_credentials() {
        let credentials = LoginCredentials {
            user: "rithmic-user".to_string(),
            password: "rithmic-secret".to_string(),
            system_name: "Rithmic Paper Trading".to_string(),
            app_name: "NautilusTrader".to_string(),
            app_version: "2.0".to_string(),
            aggregated_quotes: false,
        };
        let output = format!("{credentials:?}");

        assert!(!output.contains("rithmic-user"));
        assert!(!output.contains("rithmic-secret"));
        assert!(output.contains("<redacted>"));
    }

    #[rstest]
    fn subscription_combines_requested_market_data() {
        let request = MarketSubscription::all_market_data("MESU6", "CME")
            .request(SubscriptionRequest::Subscribe);

        assert_eq!(request.template_id, MARKET_DATA_REQUEST_TEMPLATE_ID);
        assert_eq!(request.request, SubscriptionRequest::Subscribe as i32);
        assert_eq!(request.update_bits, 7);
    }

    #[rstest]
    fn login_uses_server_heartbeat_interval() {
        let response = ResponseLogin {
            rp_code: vec!["0".to_string()],
            heartbeat_interval: 20.5,
            ..Default::default()
        };

        assert_eq!(heartbeat_interval(&response).unwrap(), Duration::from_millis(20_500));
    }

    #[rstest]
    fn rejects_rithmic_error_response() {
        let response = ResponseCode {
            rp_code: vec!["13".to_string(), "Permission denied".to_string()],
            ..Default::default()
        };

        assert!(
            ensure_response_success(&response)
                .unwrap_err()
                .to_string()
                .contains("Permission denied")
        );
    }
}