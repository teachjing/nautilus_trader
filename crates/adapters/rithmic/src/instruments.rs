// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Conversion of Rithmic reference data into native Nautilus instruments.

use std::str::FromStr;

use anyhow::Context;
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    enums::AssetClass,
    identifiers::{InstrumentId, Symbol},
    instruments::{FuturesContract, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use serde_json::json;
use time::{Date, Month, PrimitiveDateTime, Time, UtcOffset};
use ustr::Ustr;

use crate::protocol::{
    ResponseAuxiliaryReferenceData, ResponseReferenceData, ResponseTickSizeTable,
};

fn parse_date(value: Option<&str>, field: &str) -> anyhow::Result<UnixNanos> {
    let value = value.ok_or_else(|| anyhow::anyhow!("Rithmic {field} is missing"))?;
    anyhow::ensure!(value.len() >= 8, "Invalid Rithmic {field} '{value}'");
    let date = Date::from_calendar_date(
        value[0..4].parse()?,
        Month::try_from(value[4..6].parse::<u8>()?)?,
        value[6..8].parse()?,
    )?;
    let timestamp = PrimitiveDateTime::new(date, Time::MIDNIGHT)
        .assume_offset(UtcOffset::UTC)
        .unix_timestamp_nanos();
    Ok(UnixNanos::from(u64::try_from(timestamp)?))
}

fn asset_class(product: &str) -> AssetClass {
    match product.to_ascii_uppercase().as_str() {
        "ES" | "MES" | "NQ" | "MNQ" | "YM" | "MYM" | "RTY" | "M2K" => {
            AssetClass::Index
        }
        "6A" | "6B" | "6C" | "6E" | "6J" | "6S" | "M6A" | "M6B" | "M6E" => {
            AssetClass::FX
        }
        "ZB" | "ZN" | "ZF" | "ZT" | "UB" | "SR3" => AssetClass::Debt,
        _ => AssetClass::Commodity,
    }
}

/// Builds a native futures contract from Rithmic reference responses.
///
/// # Errors
///
/// Returns an error when required economics or dates are absent or invalid.
pub fn parse_futures_contract(
    reference: &ResponseReferenceData,
    auxiliary: &ResponseAuxiliaryReferenceData,
    tick_table: &[ResponseTickSizeTable],
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    let symbol = reference
        .trading_symbol
        .as_deref()
        .or(reference.symbol.as_deref())
        .context("Rithmic reference symbol is missing")?;
    let exchange = reference
        .trading_exchange
        .as_deref()
        .or(reference.exchange.as_deref())
        .context("Rithmic reference exchange is missing")?;
    let product = reference
        .underlying_symbol
        .as_deref()
        .or(reference.product_code.as_deref())
        .context("Rithmic underlying symbol is missing")?;
    let currency = reference.currency.as_deref().unwrap_or("USD");
    let tick = reference
        .min_qprice_change
        .filter(|value| value.is_finite() && *value > 0.0)
        .or_else(|| {
            tick_table
                .iter()
                .filter_map(|row| row.min_fprice_change)
                .find(|value| value.is_finite() && *value > 0.0)
        })
        .context("Rithmic minimum price increment is missing")?;
    let multiplier = reference
        .single_point_value
        .filter(|value| value.is_finite() && *value > 0.0)
        .context("Rithmic single point value is missing")?;
    let price_increment = Price::from_str(&tick.to_string())
        .map_err(|e| anyhow::anyhow!("Invalid Rithmic price increment {tick}: {e}"))?;
    let multiplier = Quantity::from_str(&multiplier.to_string())
        .map_err(|e| anyhow::anyhow!("Invalid Rithmic contract multiplier {multiplier}: {e}"))?;
    let activation_ns = auxiliary
        .first_trading_date
        .as_deref()
        .map(|value| parse_date(Some(value), "first trading date"))
        .transpose()?
        .unwrap_or_default();
    let expiration_ns = parse_date(
        auxiliary
            .last_trading_date
            .as_deref()
            .or(reference.expiration_date.as_deref()),
        "expiration date",
    )?;
    let instrument_id = InstrumentId::from(format!("{symbol}.{exchange}").as_str());
    let mut info = Params::new();
    info.insert("provider".to_string(), json!("Rithmic"));
    info.insert("symbol_name".to_string(), json!(reference.symbol_name));
    info.insert("instrument_type".to_string(), json!(reference.instrument_type));
    info.insert("tick_size_type".to_string(), json!(reference.tick_size_type));
    info.insert("first_notice_date".to_string(), json!(auxiliary.first_notice_date));
    info.insert("unit_of_measure".to_string(), json!(auxiliary.unit_of_measure));
    info.insert(
        "unit_of_measure_qty".to_string(),
        json!(auxiliary.unit_of_measure_qty),
    );

    let instrument = FuturesContract::builder()
        .instrument_id(instrument_id)
        .raw_symbol(Symbol::from(symbol))
        .asset_class(asset_class(product))
        .exchange(Ustr::from(exchange))
        .underlying(Ustr::from(product))
        .activation_ns(activation_ns)
        .expiration_ns(expiration_ns)
        .currency(Currency::from(currency))
        .price_precision(price_increment.precision)
        .price_increment(price_increment)
        .multiplier(multiplier)
        .lot_size(Quantity::from(1))
        .info(info)
        .ts_event(ts_init)
        .ts_init(ts_init)
        .build()
        .context("Failed to construct Rithmic futures contract")?;
    Ok(InstrumentAny::FuturesContract(instrument))
}

#[cfg(test)]
mod tests {
    use nautilus_model::instruments::Instrument;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn builds_mes_futures_contract_from_reference_data() {
        let reference = ResponseReferenceData {
            trading_symbol: Some("MESU6".to_string()),
            trading_exchange: Some("CME".to_string()),
            product_code: Some("MES".to_string()),
            underlying_symbol: Some("MES".to_string()),
            expiration_date: Some("20260918".to_string()),
            currency: Some("USD".to_string()),
            min_qprice_change: Some(0.25),
            single_point_value: Some(5.0),
            ..Default::default()
        };
        let auxiliary = ResponseAuxiliaryReferenceData {
            first_trading_date: Some("20250623".to_string()),
            last_trading_date: Some("20260918".to_string()),
            ..Default::default()
        };

        let instrument = parse_futures_contract(
            &reference,
            &auxiliary,
            &[],
            UnixNanos::from(1_u64),
        )
        .unwrap();

        assert_eq!(instrument.id(), InstrumentId::from("MESU6.CME"));
        assert_eq!(instrument.asset_class(), AssetClass::Index);
        assert_eq!(instrument.price_increment(), Price::from("0.25"));
        assert_eq!(instrument.multiplier(), Quantity::from(5));
    }
}
