// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Conversion from Rithmic protobuf market data to native Nautilus data.

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BookOrder, OrderBookDelta, OrderBookDeltas, QuoteTick, TradeTick},
    enums::{AggressorSide, BookAction, OrderSide, RecordFlag},
    identifiers::{InstrumentId, Symbol, TradeId, Venue},
    types::{Price, Quantity},
};

use crate::protocol::{
    BestBidOffer, BookUpdateType, DepthByOrder, DepthTransactionType, DepthUpdateType, LastTrade,
    OrderBook, ResponseDepthByOrderSnapshot, TransactionType, book_presence_bits,
    quote_presence_bits, trade_presence_bits,
};

/// Converts a Rithmic last-trade update into a native [`TradeTick`].
///
/// # Errors
///
/// Returns an error when the update does not contain a valid trade, instrument, or timestamp.
pub fn parse_trade(update: &LastTrade, ts_init: UnixNanos) -> anyhow::Result<TradeTick> {
    anyhow::ensure!(
        update.presence_bits & trade_presence_bits::LAST_TRADE != 0,
        "Rithmic message does not contain a last-trade update"
    );
    anyhow::ensure!(
        update.clear_bits & trade_presence_bits::LAST_TRADE == 0,
        "Rithmic message clears the last-trade state"
    );
    validate_price(update.trade_price, "trade price")?;
    anyhow::ensure!(update.trade_size > 0, "Rithmic trade size must be positive");

    let ts_event = source_timestamp(
        update.source_ssboe,
        update.source_usecs,
        update.source_nsecs,
    )
    .or_else(|| timestamp(update.ssboe, update.usecs))
    .ok_or_else(|| anyhow::anyhow!("Rithmic trade timestamp is invalid"))?;
    let aggressor_side = match TransactionType::try_from(update.aggressor) {
        Ok(TransactionType::Buy) => AggressorSide::Buy,
        Ok(TransactionType::Sell) => AggressorSide::Sell,
        _ => AggressorSide::NoAggressor,
    };
    let trade_id = trade_id(update, ts_event)?;

    TradeTick::new_checked(
        instrument_id(&update.symbol, &update.exchange)?,
        price(update.trade_price),
        Quantity::new(update.trade_size as f64, 0),
        aggressor_side,
        trade_id,
        ts_event,
        ts_init,
    )
}

/// Cached state for Rithmic BBO messages, which may update only one side at a time.
#[derive(Debug, Default, Clone)]
pub struct QuoteState {
    bid: Option<(Price, Quantity)>,
    ask: Option<(Price, Quantity)>,
    price_precision: u8,
}

/// Applies a Rithmic BBO update and returns a quote once both sides are available.
///
/// # Errors
///
/// Returns an error when an included side, instrument, or timestamp is invalid.
pub fn parse_quote(
    update: &BestBidOffer,
    state: &mut QuoteState,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<QuoteTick>> {
    let ts_event = timestamp(update.ssboe, update.usecs)
        .ok_or_else(|| anyhow::anyhow!("Rithmic BBO timestamp is invalid"))?;
    let bid_cleared = update.clear_bits & quote_presence_bits::BID != 0;
    let ask_cleared = update.clear_bits & quote_presence_bits::ASK != 0;
    if bid_cleared {
        state.bid = None;
    }
    if ask_cleared {
        state.ask = None;
    }
    if update.presence_bits & quote_presence_bits::BID != 0 {
        validate_price(update.bid_price, "bid price")?;
        anyhow::ensure!(update.bid_size >= 0, "Rithmic bid size cannot be negative");
        state.price_precision = state
            .price_precision
            .max(decimal_precision(update.bid_price));
        state.bid = Some((
            price(update.bid_price),
            Quantity::new(update.bid_size as f64, 0),
        ));
    }
    if update.presence_bits & quote_presence_bits::ASK != 0 {
        validate_price(update.ask_price, "ask price")?;
        anyhow::ensure!(update.ask_size >= 0, "Rithmic ask size cannot be negative");
        state.price_precision = state
            .price_precision
            .max(decimal_precision(update.ask_price));
        state.ask = Some((
            price(update.ask_price),
            Quantity::new(update.ask_size as f64, 0),
        ));
    }
    let (Some((bid_price, bid_size)), Some((ask_price, ask_size))) = (state.bid, state.ask) else {
        return Ok(None);
    };
    let bid_price = Price::new(bid_price.as_f64(), state.price_precision);
    let ask_price = Price::new(ask_price.as_f64(), state.price_precision);
    state.bid = Some((bid_price, bid_size));
    state.ask = Some((ask_price, ask_size));

    Ok(Some(QuoteTick::new_checked(
        instrument_id(&update.symbol, &update.exchange)?,
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        ts_event,
        ts_init,
    )?))
}

/// Converts a Rithmic market-by-price update into native [`OrderBookDeltas`].
///
/// # Errors
///
/// Returns an error for invalid array lengths, levels, instrument identifiers, or timestamps.
pub fn parse_order_book(
    update: &OrderBook,
    sequence: u64,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    validate_book_arrays(update)?;
    let instrument_id = instrument_id(&update.symbol, &update.exchange)?;
    let update_type = BookUpdateType::try_from(update.update_type)
        .unwrap_or(BookUpdateType::Unspecified);
    let ts_event = timestamp(update.ssboe, update.usecs).or_else(|| {
        matches!(
            update_type,
            BookUpdateType::SnapshotImage
                | BookUpdateType::ClearOrderBook
                | BookUpdateType::NoBook
        )
        .then_some(ts_init)
    })
    .ok_or_else(|| anyhow::anyhow!("Rithmic order-book timestamp is invalid"))?;
    let snapshot = update_type == BookUpdateType::SnapshotImage;
    let terminal = matches!(
        update_type,
        BookUpdateType::SnapshotImage
            | BookUpdateType::End
            | BookUpdateType::Solo
            | BookUpdateType::ClearOrderBook
            | BookUpdateType::NoBook
    );
    let mut deltas = Vec::with_capacity(update.bid_price.len() + update.ask_price.len() + 1);

    if matches!(
        update_type,
        BookUpdateType::SnapshotImage | BookUpdateType::ClearOrderBook | BookUpdateType::NoBook
    ) {
        deltas.push(OrderBookDelta::clear(
            instrument_id,
            sequence,
            ts_event,
            ts_init,
        ));
    }

    append_levels(
        &mut deltas,
        instrument_id,
        OrderSide::Buy,
        &update.bid_price,
        &update.bid_size,
        snapshot,
        sequence,
        ts_event,
        ts_init,
    )?;
    append_levels(
        &mut deltas,
        instrument_id,
        OrderSide::Sell,
        &update.ask_price,
        &update.ask_size,
        snapshot,
        sequence,
        ts_event,
        ts_init,
    )?;

    anyhow::ensure!(!deltas.is_empty(), "Rithmic order-book update contains no levels");
    if let Some(last) = deltas.last_mut() {
        last.flags |= RecordFlag::F_MBP as u8;
        if snapshot {
            last.flags |= RecordFlag::F_SNAPSHOT as u8;
        }
        if terminal {
            last.flags |= RecordFlag::F_LAST as u8;
        }
    }
    OrderBookDeltas::new_checked(instrument_id, deltas)
}

#[allow(clippy::too_many_arguments)]
fn append_levels(
    deltas: &mut Vec<OrderBookDelta>,
    instrument_id: InstrumentId,
    side: OrderSide,
    prices: &[f64],
    sizes: &[i32],
    snapshot: bool,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<()> {
    for (&level_price, &level_size) in prices.iter().zip(sizes) {
        validate_price(level_price, "order-book price")?;
        anyhow::ensure!(level_size >= 0, "Rithmic order-book size cannot be negative");
        let action = if level_size == 0 {
            BookAction::Delete
        } else if snapshot {
            BookAction::Add
        } else {
            BookAction::Update
        };
        let order = BookOrder::new(
            side,
            price(level_price),
            Quantity::new(level_size as f64, 0),
            0,
        );
        deltas.push(OrderBookDelta::new(
            instrument_id,
            action,
            order,
            RecordFlag::F_MBP as u8,
            sequence,
            ts_event,
            ts_init,
        ));
    }
    Ok(())
}

fn validate_book_arrays(update: &OrderBook) -> anyhow::Result<()> {
    anyhow::ensure!(
        update.presence_bits & book_presence_bits::BID == 0
            || update.bid_price.len() == update.bid_size.len(),
        "Rithmic bid price/size array lengths differ"
    );
    anyhow::ensure!(
        update.presence_bits & book_presence_bits::ASK == 0
            || update.ask_price.len() == update.ask_size.len(),
        "Rithmic ask price/size array lengths differ"
    );
    Ok(())
}

/// Converts one Rithmic depth-by-order snapshot response into native L3 deltas.
///
/// # Errors
///
/// Returns an error for invalid array lengths, prices, sizes, sides, or order identifiers.
pub fn parse_depth_by_order_snapshot(
    response: &ResponseDepthByOrderSnapshot,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    anyhow::ensure!(
        response.depth_size.len() == response.exchange_order_id.len(),
        "Rithmic MBO snapshot size/order ID array lengths differ"
    );
    anyhow::ensure!(
        response.depth_order_priority.is_empty()
            || response.depth_order_priority.len() == response.depth_size.len(),
        "Rithmic MBO snapshot priority array length differs"
    );
    let depth_price = response
        .depth_price
        .ok_or_else(|| anyhow::anyhow!("Rithmic MBO snapshot has no depth price"))?;
    validate_price(depth_price, "MBO snapshot price")?;
    let side = depth_side(response.depth_side)?;
    let instrument_id = instrument_id(&response.symbol, &response.exchange)?;
    let mut deltas = Vec::with_capacity(response.depth_size.len());
    for (&size, exchange_order_id) in response.depth_size.iter().zip(&response.exchange_order_id) {
        anyhow::ensure!(size > 0, "Rithmic MBO snapshot size must be positive");
        deltas.push(OrderBookDelta::new(
            instrument_id,
            BookAction::Add,
            BookOrder::new(
                side,
                price(depth_price),
                Quantity::new(size as f64, 0),
                mbo_order_id(exchange_order_id)?,
            ),
            RecordFlag::F_SNAPSHOT as u8,
            response.sequence_number,
            ts_init,
            ts_init,
        ));
    }
    anyhow::ensure!(!deltas.is_empty(), "Rithmic MBO snapshot contains no orders");
    if !response.rp_code.is_empty() {
        if let Some(last) = deltas.last_mut() {
            last.flags |= RecordFlag::F_LAST as u8;
        }
    }
    OrderBookDeltas::new_checked(instrument_id, deltas)
}

/// Converts a Rithmic depth-by-order update into native L3 deltas.
///
/// # Errors
///
/// Returns an error for invalid arrays, prices, sizes, sides, update types, or order identifiers.
pub fn parse_depth_by_order_update(
    update: &DepthByOrder,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
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
    let instrument_id = instrument_id(&update.symbol, &update.exchange)?;
    let ts_event = source_timestamp(update.source_ssboe, update.source_usecs, update.source_nsecs)
        .or_else(|| timestamp(update.ssboe, update.usecs))
        .unwrap_or(ts_init);
    let mut deltas = Vec::with_capacity(len.saturating_mul(2));
    for index in 0..len {
        let side = depth_side(update.transaction_type[index])?;
        let action = DepthUpdateType::try_from(update.update_type[index])
            .map_err(|_| anyhow::anyhow!("Unknown Rithmic MBO update type"))?;
        let order_id = mbo_order_id(&update.exchange_order_id[index])?;
        let current_price = update.depth_price[index];
        validate_price(current_price, "MBO update price")?;
        let size = update.depth_size[index];
        anyhow::ensure!(size >= 0, "Rithmic MBO size cannot be negative");
        if matches!(action, DepthUpdateType::New | DepthUpdateType::Change) {
            anyhow::ensure!(size > 0, "Rithmic MBO add/update size must be positive");
        }
        let price_changed = action == DepthUpdateType::Change
            && update.prev_depth_price_flag.get(index).copied().unwrap_or(false);
        if price_changed {
            let previous_price = *update.prev_depth_price.get(index).ok_or_else(|| {
                anyhow::anyhow!("Rithmic MBO previous-price flag has no matching price")
            })?;
            validate_price(previous_price, "MBO previous price")?;
            deltas.push(OrderBookDelta::new(
                instrument_id,
                BookAction::Delete,
                BookOrder::new(side, price(previous_price), Quantity::new(0.0, 0), order_id),
                0,
                update.sequence_number,
                ts_event,
                ts_init,
            ));
        }
        let book_action = match action {
            DepthUpdateType::New => BookAction::Add,
            DepthUpdateType::Change if price_changed => BookAction::Add,
            DepthUpdateType::Change => BookAction::Update,
            DepthUpdateType::Delete => BookAction::Delete,
            DepthUpdateType::Unspecified => anyhow::bail!("Unspecified Rithmic MBO update type"),
        };
        deltas.push(OrderBookDelta::new(
            instrument_id,
            book_action,
            BookOrder::new(
                side,
                price(current_price),
                Quantity::new(size as f64, 0),
                order_id,
            ),
            0,
            update.sequence_number,
            ts_event,
            ts_init,
        ));
    }
    anyhow::ensure!(!deltas.is_empty(), "Rithmic MBO update contains no orders");
    if let Some(last) = deltas.last_mut() {
        last.flags |= RecordFlag::F_LAST as u8;
    }
    OrderBookDeltas::new_checked(instrument_id, deltas)
}

fn depth_side(value: i32) -> anyhow::Result<OrderSide> {
    match DepthTransactionType::try_from(value) {
        Ok(DepthTransactionType::Buy) => Ok(OrderSide::Buy),
        Ok(DepthTransactionType::Sell) => Ok(OrderSide::Sell),
        _ => anyhow::bail!("Unspecified Rithmic MBO side"),
    }
}

pub(crate) fn mbo_order_id(value: &str) -> anyhow::Result<u64> {
    anyhow::ensure!(!value.is_empty(), "Rithmic MBO exchange order ID is empty");
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    Ok(if hash == 0 { 1 } else { hash })
}

fn instrument_id(symbol: &str, exchange: &str) -> anyhow::Result<InstrumentId> {
    anyhow::ensure!(!symbol.is_empty(), "Rithmic symbol is empty");
    anyhow::ensure!(!exchange.is_empty(), "Rithmic exchange is empty");
    Ok(InstrumentId::new(Symbol::new(symbol), Venue::new(exchange)))
}

fn trade_id(update: &LastTrade, ts_event: UnixNanos) -> anyhow::Result<TradeId> {
    let value = if !update.aggressor_exchange_order_id.is_empty() {
        update.aggressor_exchange_order_id.clone()
    } else if !update.exchange_order_id.is_empty() {
        update.exchange_order_id.clone()
    } else {
        format!("{}-{}", update.ssboe, update.usecs)
    };
    TradeId::new_checked(value.chars().take(36).collect::<String>())
        .map_err(|error| anyhow::anyhow!("Invalid Rithmic trade ID at {ts_event}: {error}"))
}

fn timestamp(seconds: i32, microseconds: i32) -> Option<UnixNanos> {
    if seconds <= 0 || !(0..1_000_000).contains(&microseconds) {
        return None;
    }
    let nanos = (seconds as u64)
        .checked_mul(1_000_000_000)?
        .checked_add((microseconds as u64) * 1_000)?;
    Some(UnixNanos::from(nanos))
}

fn source_timestamp(seconds: i32, microseconds: i32, nanoseconds: i32) -> Option<UnixNanos> {
    if seconds <= 0
        || !(0..1_000_000).contains(&microseconds)
        || !(0..1_000_000_000).contains(&nanoseconds)
    {
        return None;
    }
    let fractional_nanos = if nanoseconds > 0 {
        nanoseconds as u64
    } else {
        (microseconds as u64) * 1_000
    };
    let nanos = (seconds as u64)
        .checked_mul(1_000_000_000)?
        .checked_add(fractional_nanos)?;
    Some(UnixNanos::from(nanos))
}

fn validate_price(value: f64, label: &str) -> anyhow::Result<()> {
    anyhow::ensure!(value.is_finite() && value > 0.0, "Rithmic {label} is invalid: {value}");
    Ok(())
}

fn price(value: f64) -> Price {
    Price::new(value, decimal_precision(value))
}

fn decimal_precision(value: f64) -> u8 {
    let rendered = value.to_string();
    rendered
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.trim_end_matches('0').len().min(9) as u8)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::protocol::{BBO_TEMPLATE_ID, LAST_TRADE_TEMPLATE_ID, ORDER_BOOK_TEMPLATE_ID};

    const TS_INIT: UnixNanos = UnixNanos::new(1_700_000_001_000_000_000);

    #[rstest]
    fn converts_last_trade() {
        let update = LastTrade {
            template_id: LAST_TRADE_TEMPLATE_ID,
            symbol: "MESU6".to_string(),
            exchange: "CME".to_string(),
            trade_price: 6_000.25,
            trade_size: 3,
            presence_bits: trade_presence_bits::LAST_TRADE,
            aggressor: TransactionType::Buy as i32,
            exchange_order_id: "trade-123".to_string(),
            ssboe: 1_700_000_000,
            usecs: 123_456,
            ..Default::default()
        };

        let trade = parse_trade(&update, TS_INIT).unwrap();
        assert_eq!(trade.instrument_id.to_string(), "MESU6.CME");
        assert_eq!(trade.price, Price::from("6000.25"));
        assert_eq!(trade.size, Quantity::from(3));
        assert_eq!(trade.aggressor_side, AggressorSide::Buy);
        assert_eq!(trade.ts_event, UnixNanos::from(1_700_000_000_123_456_000));
    }

    #[rstest]
    fn converts_best_bid_offer() {
        let update = BestBidOffer {
            template_id: BBO_TEMPLATE_ID,
            symbol: "MESU6".to_string(),
            exchange: "CME".to_string(),
            bid_price: 6_000.00,
            bid_size: 12,
            ask_price: 6_000.25,
            ask_size: 9,
            presence_bits: quote_presence_bits::BID | quote_presence_bits::ASK,
            ssboe: 1_700_000_000,
            usecs: 500,
            ..Default::default()
        };

        let quote = parse_quote(&update, &mut QuoteState::default(), TS_INIT)
            .unwrap()
            .unwrap();
        assert_eq!(quote.bid_price, Price::from("6000.00"));
        assert_eq!(quote.ask_price, Price::from("6000.25"));
        assert_eq!(quote.bid_price.precision, 2);
        assert_eq!(quote.ask_price.precision, 2);
        assert_eq!(quote.bid_size, Quantity::from(12));
        assert_eq!(quote.ask_size, Quantity::from(9));
    }

    #[rstest]
    fn converts_snapshot_book_with_clear_and_boundary_flags() {
        let update = OrderBook {
            template_id: ORDER_BOOK_TEMPLATE_ID,
            symbol: "MESU6".to_string(),
            exchange: "CME".to_string(),
            update_type: BookUpdateType::SnapshotImage as i32,
            presence_bits: book_presence_bits::BID | book_presence_bits::ASK,
            bid_price: vec![6_000.00],
            bid_size: vec![12],
            ask_price: vec![6_000.25],
            ask_size: vec![9],
            ssboe: 1_700_000_000,
            usecs: 500,
            ..Default::default()
        };

        let book = parse_order_book(&update, 42, TS_INIT).unwrap();
        assert_eq!(book.deltas.len(), 3);
        assert_eq!(book.deltas[0].action, BookAction::Clear);
        assert_eq!(book.deltas[1].action, BookAction::Add);
        assert!(RecordFlag::F_MBP.matches(book.flags));
        assert!(RecordFlag::F_SNAPSHOT.matches(book.flags));
        assert!(RecordFlag::F_LAST.matches(book.flags));
        assert_eq!(book.sequence, 42);
    }

    #[rstest]
    fn rejects_misaligned_book_arrays() {
        let update = OrderBook {
            symbol: "MESU6".to_string(),
            exchange: "CME".to_string(),
            bid_price: vec![6_000.00],
            bid_size: vec![],
            presence_bits: book_presence_bits::BID,
            ssboe: 1_700_000_000,
            ..Default::default()
        };

        assert!(parse_order_book(&update, 1, TS_INIT).is_err());
    }

    #[rstest]
    fn combines_partial_bbo_with_previous_quote() {
        let initial = BestBidOffer {
            symbol: "MESU6".to_string(),
            exchange: "CME".to_string(),
            presence_bits: quote_presence_bits::BID | quote_presence_bits::ASK,
            bid_price: 6_000.00,
            bid_size: 12,
            ask_price: 6_000.25,
            ask_size: 9,
            ssboe: 1_700_000_000,
            ..Default::default()
        };
        let mut state = QuoteState::default();
        let first = parse_quote(&initial, &mut state, TS_INIT).unwrap().unwrap();
        let bid_only = BestBidOffer {
            symbol: "MESU6".to_string(),
            exchange: "CME".to_string(),
            presence_bits: quote_presence_bits::BID,
            bid_price: 6_000.25,
            bid_size: 15,
            ssboe: 1_700_000_001,
            ..Default::default()
        };

        let updated = parse_quote(&bid_only, &mut state, TS_INIT)
            .unwrap()
            .unwrap();
        assert_eq!(updated.bid_price, Price::from("6000.25"));
        assert_eq!(updated.ask_price, first.ask_price);
        assert_eq!(updated.ask_size, first.ask_size);
        assert_eq!(updated.bid_price.precision, updated.ask_price.precision);
    }

    #[rstest]
    fn converts_mbo_snapshot_with_stable_order_ids() {
        let response = ResponseDepthByOrderSnapshot {
            symbol: "ESU6".to_string(),
            exchange: "CME".to_string(),
            sequence_number: 42,
            depth_side: DepthTransactionType::Buy as i32,
            depth_price: Some(6_000.25),
            depth_size: vec![3, 5],
            exchange_order_id: vec!["order-a".to_string(), "order-b".to_string()],
            rp_code: vec!["0".to_string()],
            ..Default::default()
        };

        let deltas = parse_depth_by_order_snapshot(&response, TS_INIT).unwrap();
        assert_eq!(deltas.deltas.len(), 2);
        assert!(deltas.deltas.iter().all(|delta| delta.action == BookAction::Add));
        assert_eq!(deltas.deltas[0].order.order_id, mbo_order_id("order-a").unwrap());
        assert!(RecordFlag::F_SNAPSHOT.matches(deltas.deltas[0].flags));
        assert!(RecordFlag::F_LAST.matches(deltas.flags));
    }

    #[rstest]
    fn converts_mbo_price_change_to_delete_then_add() {
        let update = DepthByOrder {
            symbol: "ESU6".to_string(),
            exchange: "CME".to_string(),
            sequence_number: 43,
            update_type: vec![DepthUpdateType::Change as i32],
            transaction_type: vec![DepthTransactionType::Sell as i32],
            depth_price: vec![6_001.00],
            prev_depth_price: vec![6_000.75],
            prev_depth_price_flag: vec![true],
            depth_size: vec![4],
            exchange_order_id: vec!["order-a".to_string()],
            ssboe: 1_700_000_000,
            ..Default::default()
        };

        let deltas = parse_depth_by_order_update(&update, TS_INIT).unwrap();
        assert_eq!(deltas.deltas.len(), 2);
        assert_eq!(deltas.deltas[0].action, BookAction::Delete);
        assert_eq!(deltas.deltas[1].action, BookAction::Add);
        assert_eq!(deltas.deltas[0].order.order_id, deltas.deltas[1].order.order_id);
        assert!(RecordFlag::F_LAST.matches(deltas.flags));
    }

    #[rstest]
    fn converts_mbo_cancel_with_exchange_order_identity() {
        let update = DepthByOrder {
            symbol: "ESU6".to_string(),
            exchange: "CME".to_string(),
            sequence_number: 44,
            update_type: vec![DepthUpdateType::Delete as i32],
            transaction_type: vec![DepthTransactionType::Buy as i32],
            depth_price: vec![6_000.25],
            depth_size: vec![0],
            exchange_order_id: vec!["cancelled-order".to_string()],
            ssboe: 1_700_000_000,
            ..Default::default()
        };

        let deltas = parse_depth_by_order_update(&update, TS_INIT).unwrap();
        assert_eq!(deltas.deltas[0].action, BookAction::Delete);
        assert_eq!(
            deltas.deltas[0].order.order_id,
            mbo_order_id("cancelled-order").unwrap()
        );
    }
}
