// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Queryable Rithmic exchange and instrument discovery catalog.

use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RithmicExchangeInfo {
    pub exchange: String,
    pub level_1_market_data: String,
    pub level_2_market_data: String,
    pub entitled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RithmicInstrumentInfo {
    pub symbol: String,
    pub exchange: String,
    pub symbol_name: String,
    pub product_code: String,
    pub instrument_type: String,
    pub expiration_date: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RithmicDiscoveryCatalog {
    pub exchanges: Vec<RithmicExchangeInfo>,
    pub instruments: Vec<RithmicInstrumentInfo>,
}

impl RithmicDiscoveryCatalog {
    /// Saves the catalog as formatted JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created or the catalog cannot be serialized.
    pub fn save(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }

    /// Loads a catalog from JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or decoded.
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    }

    #[must_use]
    pub fn find_instruments(&self, query: &str) -> Vec<&RithmicInstrumentInfo> {
        let query = query.to_ascii_lowercase();
        self.instruments
            .iter()
            .filter(|instrument| {
                instrument.symbol.to_ascii_lowercase().contains(&query)
                    || instrument.product_code.to_ascii_lowercase().contains(&query)
                    || instrument.symbol_name.to_ascii_lowercase().contains(&query)
            })
            .collect()
    }

    /// Returns all instruments for one exchange, using a case-insensitive exchange match.
    #[must_use]
    pub fn instruments_for_exchange(&self, exchange: &str) -> Vec<&RithmicInstrumentInfo> {
        self.instruments
            .iter()
            .filter(|instrument| instrument.exchange.eq_ignore_ascii_case(exchange))
            .collect()
    }

    /// Returns only exchanges enabled for the authenticated user.
    #[must_use]
    pub fn entitled_exchanges(&self) -> Vec<&RithmicExchangeInfo> {
        self.exchanges
            .iter()
            .filter(|exchange| exchange.entitled)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn queries_catalog_by_text_exchange_and_entitlement() {
        let catalog = RithmicDiscoveryCatalog {
            exchanges: vec![
                RithmicExchangeInfo {
                    exchange: "CME".to_string(),
                    level_1_market_data: "1".to_string(),
                    level_2_market_data: "1".to_string(),
                    entitled: true,
                },
                RithmicExchangeInfo {
                    exchange: "NYMEX".to_string(),
                    level_1_market_data: "0".to_string(),
                    level_2_market_data: "0".to_string(),
                    entitled: false,
                },
            ],
            instruments: vec![RithmicInstrumentInfo {
                symbol: "MESU6".to_string(),
                exchange: "CME".to_string(),
                symbol_name: "Micro E-mini S&P 500".to_string(),
                product_code: "MES".to_string(),
                instrument_type: "Future".to_string(),
                expiration_date: "20260918".to_string(),
            }],
        };

        assert_eq!(catalog.find_instruments("micro").len(), 1);
        assert_eq!(catalog.instruments_for_exchange("cme").len(), 1);
        assert_eq!(catalog.entitled_exchanges().len(), 1);
    }
}
