// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Rithmic-specific market-by-order custom data.

use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{CustomData, CustomDataTrait, DataType, HasTsInit},
    enums::OrderSide,
    identifiers::InstrumentId,
    types::{Price, Quantity},
};
use serde::{Deserialize, Serialize};

use crate::{
    parse::mbo_order_id,
    protocol::{DepthByOrder, DepthTransactionType, DepthUpdateType, ResponseDepthByOrderSnapshot},
};

/// Lifecycle action carried by a [`RithmicMboEvent`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        frozen,
        eq,
        module = "nautilus_trader.adapters.rithmic",
        from_py_object,
        rename_all = "SCREAMING_SNAKE_CASE",
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.rithmic")
)]
pub enum RithmicMboAction {
    /// One resting order from the initial full-depth snapshot.
    SnapshotAdd,
    /// Boundary indicating that the multipart snapshot is complete.
    SnapshotComplete,
    /// A new resting order.
    Add,
    /// A changed resting order.
    Change,
    /// A deleted resting order.
    Delete,
}

/// Lossless Rithmic MBO lifecycle event for provider-specific actor analysis.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.rithmic", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.rithmic")
)]
pub struct RithmicMboEvent {
    /// Nautilus instrument identity.
    pub instrument_id: InstrumentId,
    /// Rithmic lifecycle action.
    pub action: RithmicMboAction,
    /// Anonymous exchange order identity, absent only for a snapshot boundary.
    pub exchange_order_id: Option<String>,
    /// Stable Nautilus-compatible hash of `exchange_order_id`.
    pub order_id: Option<u64>,
    /// Exchange queue priority identifier.
    pub priority_id: Option<u64>,
    /// Resting order side.
    pub side: Option<OrderSide>,
    /// Current order price.
    pub price: Option<Price>,
    /// Previous order price when Rithmic marks a price change.
    pub previous_price: Option<Price>,
    /// Current displayed order quantity.
    pub size: Option<Quantity>,
    /// Venue sequence carried by the Rithmic message.
    pub sequence: u64,
    /// Effective event timestamp used by the native adapter.
    pub ts_event: UnixNanos,
    /// Rithmic source timestamp, when supplied.
    pub ts_source: Option<UnixNanos>,
    /// Rithmic gateway timestamp (`ssboe`/`usecs`), when supplied.
    pub ts_gateway: Option<UnixNanos>,
    /// Rithmic JOP timestamp, when supplied.
    pub ts_jop: Option<UnixNanos>,
    /// Timestamp when Nautilus initialized the event.
    pub ts_init: UnixNanos,
}

impl RithmicMboEvent {
    /// Converts the event into instrument-addressable Nautilus custom data.
    #[must_use]
    pub fn into_custom_data(self) -> CustomData {
        let identifier = self.instrument_id.to_string();
        CustomData::new(
            Arc::new(self),
            DataType::new(Self::type_name_static(), None, Some(identifier)),
        )
    }

    pub(crate) fn from_snapshot(
        response: &ResponseDepthByOrderSnapshot,
        instrument_id: InstrumentId,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<Self>> {
        anyhow::ensure!(
            response.depth_size.len() == response.exchange_order_id.len(),
            "Rithmic MBO snapshot size/order ID array lengths differ"
        );
        anyhow::ensure!(
            response.depth_order_priority.is_empty()
                || response.depth_order_priority.len() == response.depth_size.len(),
            "Rithmic MBO snapshot priority array length differs"
        );
        let price = response
            .depth_price
            .map(price)
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("Rithmic MBO snapshot has no depth price"))?;
        let side = side(response.depth_side)?;
        let mut events = Vec::with_capacity(response.depth_size.len());
        for (index, (&size, exchange_order_id)) in response
            .depth_size
            .iter()
            .zip(&response.exchange_order_id)
            .enumerate()
        {
            anyhow::ensure!(size > 0, "Rithmic MBO snapshot size must be positive");
            events.push(Self {
                instrument_id,
                action: RithmicMboAction::SnapshotAdd,
                exchange_order_id: Some(exchange_order_id.clone()),
                order_id: Some(mbo_order_id(exchange_order_id)?),
                priority_id: response.depth_order_priority.get(index).copied(),
                side: Some(side),
                price: Some(price),
                previous_price: None,
                size: Some(Quantity::new(size as f64, 0)),
                sequence: response.sequence_number,
                ts_event: ts_init,
                ts_source: None,
                ts_gateway: None,
                ts_jop: None,
                ts_init,
            });
        }
        Ok(events)
    }

    pub(crate) fn snapshot_complete(
        instrument_id: InstrumentId,
        sequence: u64,
        ts_init: UnixNanos,
    ) -> Self {
        Self {
            instrument_id,
            action: RithmicMboAction::SnapshotComplete,
            exchange_order_id: None,
            order_id: None,
            priority_id: None,
            side: None,
            price: None,
            previous_price: None,
            size: None,
            sequence,
            ts_event: ts_init,
            ts_source: None,
            ts_gateway: None,
            ts_jop: None,
            ts_init,
        }
    }

    pub(crate) fn from_update(
        update: &DepthByOrder,
        instrument_id: InstrumentId,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<Self>> {
        let len = update.update_type.len();
        anyhow::ensure!(
            len == update.transaction_type.len()
                && len == update.depth_price.len()
                && len == update.depth_size.len()
                && len == update.exchange_order_id.len()
                && (update.depth_order_priority.is_empty()
                    || len == update.depth_order_priority.len()),
            "Rithmic MBO update arrays differ"
        );
        let ts_source = source_timestamp(
            update.source_ssboe,
            update.source_usecs,
            update.source_nsecs,
        );
        let ts_gateway = microsecond_timestamp(update.ssboe, update.usecs);
        let ts_jop = nanosecond_timestamp(update.jop_ssboe, update.jop_nsecs);
        let ts_event = ts_source.or(ts_gateway).unwrap_or(ts_init);
        let mut events = Vec::with_capacity(len);
        for index in 0..len {
            let action = match DepthUpdateType::try_from(update.update_type[index]) {
                Ok(DepthUpdateType::New) => RithmicMboAction::Add,
                Ok(DepthUpdateType::Change) => RithmicMboAction::Change,
                Ok(DepthUpdateType::Delete) => RithmicMboAction::Delete,
                _ => anyhow::bail!("Unspecified Rithmic MBO update type"),
            };
            let exchange_order_id = update.exchange_order_id[index].clone();
            let previous_price = update
                .prev_depth_price_flag
                .get(index)
                .copied()
                .unwrap_or(false)
                .then(|| update.prev_depth_price.get(index).copied())
                .flatten()
                .map(price)
                .transpose()?;
            let size = update.depth_size[index];
            anyhow::ensure!(size >= 0, "Rithmic MBO size cannot be negative");
            events.push(Self {
                instrument_id,
                action,
                exchange_order_id: Some(exchange_order_id.clone()),
                order_id: Some(mbo_order_id(&exchange_order_id)?),
                priority_id: update.depth_order_priority.get(index).copied(),
                side: Some(side(update.transaction_type[index])?),
                price: Some(price(update.depth_price[index])?),
                previous_price,
                size: Some(Quantity::new(size as f64, 0)),
                sequence: update.sequence_number,
                ts_event,
                ts_source,
                ts_gateway,
                ts_jop,
                ts_init,
            });
        }
        Ok(events)
    }
}

impl HasTsInit for RithmicMboEvent {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

impl CustomDataTrait for RithmicMboEvent {
    fn type_name(&self) -> &'static str {
        "RithmicMboEvent"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    #[cfg(feature = "python")]
    fn to_pyobject(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        nautilus_model::data::custom::clone_pyclass_to_pyobject(self, py)
    }

    fn type_name_static() -> &'static str {
        "RithmicMboEvent"
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        Ok(Arc::new(serde_json::from_value::<Self>(value)?))
    }
}

/// Registers the Rithmic custom data types with Nautilus serialization.
pub fn register_rithmic_custom_data() {
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<RithmicMboEvent>();
}

fn side(value: i32) -> anyhow::Result<OrderSide> {
    match DepthTransactionType::try_from(value) {
        Ok(DepthTransactionType::Buy) => Ok(OrderSide::Buy),
        Ok(DepthTransactionType::Sell) => Ok(OrderSide::Sell),
        _ => anyhow::bail!("Unspecified Rithmic MBO side"),
    }
}

fn price(value: f64) -> anyhow::Result<Price> {
    anyhow::ensure!(value.is_finite() && value > 0.0, "Invalid Rithmic MBO price {value}");
    let precision = value
        .to_string()
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.trim_end_matches('0').len().min(9) as u8);
    Ok(Price::new(value, precision))
}

fn microsecond_timestamp(seconds: i32, microseconds: i32) -> Option<UnixNanos> {
    if seconds <= 0 || !(0..1_000_000).contains(&microseconds) {
        return None;
    }
    Some(UnixNanos::from(
        (seconds as u64)
            .checked_mul(1_000_000_000)?
            .checked_add((microseconds as u64) * 1_000)?,
    ))
}

fn nanosecond_timestamp(seconds: i32, nanoseconds: i32) -> Option<UnixNanos> {
    if seconds <= 0 || !(0..1_000_000_000).contains(&nanoseconds) {
        return None;
    }
    Some(UnixNanos::from(
        (seconds as u64)
            .checked_mul(1_000_000_000)?
            .checked_add(nanoseconds as u64)?,
    ))
}

fn source_timestamp(seconds: i32, microseconds: i32, nanoseconds: i32) -> Option<UnixNanos> {
    if nanoseconds > 0 {
        nanosecond_timestamp(seconds, nanoseconds)
    } else {
        microsecond_timestamp(seconds, microseconds)
    }
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl RithmicMboEvent {
    #[getter]
    #[pyo3(name = "instrument_id")]
    fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter]
    #[pyo3(name = "action")]
    fn py_action(&self) -> RithmicMboAction {
        self.action
    }

    #[getter]
    #[pyo3(name = "exchange_order_id")]
    fn py_exchange_order_id(&self) -> Option<String> {
        self.exchange_order_id.clone()
    }

    #[getter]
    #[pyo3(name = "order_id")]
    fn py_order_id(&self) -> Option<u64> {
        self.order_id
    }

    #[getter]
    #[pyo3(name = "priority_id")]
    fn py_priority_id(&self) -> Option<u64> {
        self.priority_id
    }

    #[getter]
    #[pyo3(name = "side")]
    fn py_side(&self) -> Option<OrderSide> {
        self.side
    }

    #[getter]
    #[pyo3(name = "price")]
    fn py_price(&self) -> Option<Price> {
        self.price
    }

    #[getter]
    #[pyo3(name = "previous_price")]
    fn py_previous_price(&self) -> Option<Price> {
        self.previous_price
    }

    #[getter]
    #[pyo3(name = "size")]
    fn py_size(&self) -> Option<Quantity> {
        self.size
    }

    #[getter]
    #[pyo3(name = "sequence")]
    fn py_sequence(&self) -> u64 {
        self.sequence
    }

    #[getter]
    #[pyo3(name = "ts_event")]
    fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter]
    #[pyo3(name = "ts_source")]
    fn py_ts_source(&self) -> Option<u64> {
        self.ts_source.map(|ts| ts.as_u64())
    }

    #[getter]
    #[pyo3(name = "ts_gateway")]
    fn py_ts_gateway(&self) -> Option<u64> {
        self.ts_gateway.map(|ts| ts.as_u64())
    }

    #[getter]
    #[pyo3(name = "ts_jop")]
    fn py_ts_jop(&self) -> Option<u64> {
        self.ts_jop.map(|ts| ts.as_u64())
    }

    #[getter]
    #[pyo3(name = "ts_init")]
    fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_rithmic_mbo_update_metadata() {
        let update = DepthByOrder {
            symbol: "MESU6".to_string(),
            exchange: "CME".to_string(),
            sequence_number: 42,
            update_type: vec![DepthUpdateType::Change as i32],
            transaction_type: vec![DepthTransactionType::Buy as i32],
            depth_price: vec![6_000.25],
            prev_depth_price: vec![6_000.00],
            prev_depth_price_flag: vec![true],
            depth_size: vec![7],
            depth_order_priority: vec![123],
            exchange_order_id: vec!["order-1".to_string()],
            source_ssboe: 1_700_000_000,
            source_nsecs: 123,
            jop_ssboe: 1_700_000_000,
            jop_nsecs: 456,
            ..Default::default()
        };

        let event = RithmicMboEvent::from_update(
            &update,
            InstrumentId::from("MESU6.CME"),
            UnixNanos::from(1_700_000_001_000_000_000),
        )
        .unwrap()
        .remove(0);

        assert_eq!(event.action, RithmicMboAction::Change);
        assert_eq!(event.exchange_order_id.as_deref(), Some("order-1"));
        assert_eq!(event.priority_id, Some(123));
        assert_eq!(event.previous_price, Some(Price::from("6000.00")));
        assert_eq!(event.ts_source, Some(UnixNanos::from(1_700_000_000_000_000_123)));
        assert_eq!(event.ts_jop, Some(UnixNanos::from(1_700_000_000_000_000_456)));
    }

    #[test]
    fn snapshot_boundary_has_no_synthetic_order_identity() {
        let event = RithmicMboEvent::snapshot_complete(
            InstrumentId::from("MESU6.CME"),
            42,
            UnixNanos::from(1),
        );

        assert_eq!(event.action, RithmicMboAction::SnapshotComplete);
        assert_eq!(event.exchange_order_id, None);
        assert_eq!(event.order_id, None);
        assert_eq!(event.price, None);
    }

    #[test]
    fn custom_data_is_instrument_addressable_and_json_round_trips() {
        register_rithmic_custom_data();
        let instrument_id = InstrumentId::from("MESU6.CME");
        let event = RithmicMboEvent::snapshot_complete(
            instrument_id,
            42,
            UnixNanos::from(1),
        );
        let custom = event.clone().into_custom_data();

        assert_eq!(custom.data_type.identifier(), Some("MESU6.CME"));
        let decoded = RithmicMboEvent::from_json(
            serde_json::from_str(&event.to_json().unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(decoded.as_any().downcast_ref::<RithmicMboEvent>(), Some(&event));
    }
}
