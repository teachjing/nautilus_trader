// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Rithmic R | Protocol framing and market-data messages.
//!
//! Field numbers and template IDs mirror the public Rithmic protocol buffers distributed by
//! `async-rithmic`. Each binary WebSocket frame contains one protobuf message. The adapter owns
//! this wire projection; it does not call Python from Rust.

use prost::Message;

/// Last-trade update template ID.
pub const LAST_TRADE_TEMPLATE_ID: i32 = 150;
/// Best-bid/offer update template ID.
pub const BBO_TEMPLATE_ID: i32 = 151;
/// Market-by-price order-book update template ID.
pub const ORDER_BOOK_TEMPLATE_ID: i32 = 156;

pub const LOGIN_REQUEST_TEMPLATE_ID: i32 = 10;
pub const LOGIN_RESPONSE_TEMPLATE_ID: i32 = 11;
pub const LOGOUT_REQUEST_TEMPLATE_ID: i32 = 12;
pub const LOGOUT_RESPONSE_TEMPLATE_ID: i32 = 13;
pub const SYSTEM_INFO_REQUEST_TEMPLATE_ID: i32 = 16;
pub const SYSTEM_INFO_RESPONSE_TEMPLATE_ID: i32 = 17;
pub const HEARTBEAT_REQUEST_TEMPLATE_ID: i32 = 18;
pub const HEARTBEAT_RESPONSE_TEMPLATE_ID: i32 = 19;
pub const REJECT_TEMPLATE_ID: i32 = 75;
pub const FORCED_LOGOUT_TEMPLATE_ID: i32 = 77;
pub const MARKET_DATA_REQUEST_TEMPLATE_ID: i32 = 100;
pub const MARKET_DATA_RESPONSE_TEMPLATE_ID: i32 = 101;
pub const FRONT_MONTH_REQUEST_TEMPLATE_ID: i32 = 113;
pub const FRONT_MONTH_RESPONSE_TEMPLATE_ID: i32 = 114;

pub const PROTOCOL_TEMPLATE_VERSION: &str = "3.9";

#[derive(Clone, PartialEq, Message)]
pub struct RequestSystemInfo {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestLogout {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ResponseSystemInfo {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, repeated, tag = "132766")]
    pub rp_code: Vec<String>,
    #[prost(string, repeated, tag = "153628")]
    pub system_name: Vec<String>,
    #[prost(bool, repeated, tag = "153649")]
    pub has_aggregated_quotes: Vec<bool>,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestLogin {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, tag = "153634")]
    pub template_version: String,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, tag = "131003")]
    pub user: String,
    #[prost(string, tag = "130004")]
    pub password: String,
    #[prost(string, tag = "130002")]
    pub app_name: String,
    #[prost(string, tag = "131803")]
    pub app_version: String,
    #[prost(string, tag = "153628")]
    pub system_name: String,
    #[prost(enumeration = "InfrastructureType", tag = "153621")]
    pub infra_type: i32,
    #[prost(bool, tag = "153644")]
    pub aggregated_quotes: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum InfrastructureType {
    Unspecified = 0,
    TickerPlant = 1,
    OrderPlant = 2,
    HistoryPlant = 3,
    PnlPlant = 4,
    RepositoryPlant = 5,
}

#[derive(Clone, PartialEq, Message)]
pub struct ResponseLogin {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, tag = "153634")]
    pub template_version: String,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, repeated, tag = "132766")]
    pub rp_code: Vec<String>,
    #[prost(string, tag = "154013")]
    pub fcm_id: String,
    #[prost(string, tag = "154014")]
    pub ib_id: String,
    #[prost(double, tag = "153633")]
    pub heartbeat_interval: f64,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestHeartbeat {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(int32, tag = "150100")]
    pub ssboe: i32,
    #[prost(int32, tag = "150101")]
    pub usecs: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestMarketDataUpdate {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(enumeration = "SubscriptionRequest", tag = "100000")]
    pub request: i32,
    #[prost(uint32, tag = "154211")]
    pub update_bits: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum SubscriptionRequest {
    Unspecified = 0,
    Subscribe = 1,
    Unsubscribe = 2,
}

pub mod update_bits {
    pub const LAST_TRADE: u32 = 1;
    pub const BBO: u32 = 2;
    pub const ORDER_BOOK: u32 = 4;
}

pub mod trade_presence_bits {
    pub const LAST_TRADE: u32 = 1;
}

pub mod quote_presence_bits {
    pub const BID: u32 = 1;
    pub const ASK: u32 = 2;
}

pub mod book_presence_bits {
    pub const BID: u32 = 1;
    pub const ASK: u32 = 2;
}

#[derive(Clone, PartialEq, Message)]
pub struct ResponseCode {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, repeated, tag = "132766")]
    pub rp_code: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestFrontMonthContract {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(bool, tag = "154352")]
    pub need_updates: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct ResponseFrontMonthContract {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, repeated, tag = "132766")]
    pub rp_code: Vec<String>,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(bool, tag = "149166")]
    pub is_front_month_symbol: bool,
    #[prost(string, tag = "100003")]
    pub symbol_name: String,
    #[prost(string, tag = "157095")]
    pub trading_symbol: String,
    #[prost(string, tag = "157096")]
    pub trading_exchange: String,
}

/// Minimal common projection used to dispatch a protobuf frame by template ID.
#[derive(Clone, PartialEq, Message)]
pub struct Base {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
}

/// Rithmic last-trade market-data update.
#[derive(Clone, PartialEq, Message)]
pub struct LastTrade {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(uint32, tag = "149138")]
    pub presence_bits: u32,
    #[prost(uint32, tag = "154571")]
    pub clear_bits: u32,
    #[prost(bool, tag = "110121")]
    pub is_snapshot: bool,
    #[prost(double, tag = "100006")]
    pub trade_price: f64,
    #[prost(int32, tag = "100178")]
    pub trade_size: i32,
    #[prost(enumeration = "TransactionType", tag = "112003")]
    pub aggressor: i32,
    #[prost(string, tag = "149238")]
    pub exchange_order_id: String,
    #[prost(string, tag = "154641")]
    pub aggressor_exchange_order_id: String,
    #[prost(uint64, tag = "100032")]
    pub volume: u64,
    #[prost(int32, tag = "150100")]
    pub ssboe: i32,
    #[prost(int32, tag = "150101")]
    pub usecs: i32,
    #[prost(int32, tag = "150400")]
    pub source_ssboe: i32,
    #[prost(int32, tag = "150401")]
    pub source_usecs: i32,
    #[prost(int32, tag = "150404")]
    pub source_nsecs: i32,
}

/// Rithmic aggressor-side classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum TransactionType {
    Unspecified = 0,
    Buy = 1,
    Sell = 2,
}

/// Rithmic top-of-book update.
#[derive(Clone, PartialEq, Message)]
pub struct BestBidOffer {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(uint32, tag = "149138")]
    pub presence_bits: u32,
    #[prost(uint32, tag = "154571")]
    pub clear_bits: u32,
    #[prost(bool, tag = "110121")]
    pub is_snapshot: bool,
    #[prost(double, tag = "100022")]
    pub bid_price: f64,
    #[prost(int32, tag = "100030")]
    pub bid_size: i32,
    #[prost(int32, tag = "154403")]
    pub bid_orders: i32,
    #[prost(int32, tag = "154867")]
    pub bid_implicit_size: i32,
    #[prost(double, tag = "100025")]
    pub ask_price: f64,
    #[prost(int32, tag = "100031")]
    pub ask_size: i32,
    #[prost(int32, tag = "154404")]
    pub ask_orders: i32,
    #[prost(int32, tag = "154868")]
    pub ask_implicit_size: i32,
    #[prost(int32, tag = "150100")]
    pub ssboe: i32,
    #[prost(int32, tag = "150101")]
    pub usecs: i32,
}

/// Rithmic market-by-price order-book image/update.
#[derive(Clone, PartialEq, Message)]
pub struct OrderBook {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(uint32, tag = "149138")]
    pub presence_bits: u32,
    #[prost(enumeration = "BookUpdateType", tag = "157608")]
    pub update_type: i32,
    #[prost(double, repeated, tag = "154282")]
    pub bid_price: Vec<f64>,
    #[prost(int32, repeated, tag = "154283")]
    pub bid_size: Vec<i32>,
    #[prost(int32, repeated, tag = "154401")]
    pub bid_orders: Vec<i32>,
    #[prost(int32, repeated, tag = "154412")]
    pub implicit_bid_size: Vec<i32>,
    #[prost(double, repeated, tag = "154284")]
    pub ask_price: Vec<f64>,
    #[prost(int32, repeated, tag = "154285")]
    pub ask_size: Vec<i32>,
    #[prost(int32, repeated, tag = "154402")]
    pub ask_orders: Vec<i32>,
    #[prost(int32, repeated, tag = "154415")]
    pub implicit_ask_size: Vec<i32>,
    #[prost(int32, tag = "150100")]
    pub ssboe: i32,
    #[prost(int32, tag = "150101")]
    pub usecs: i32,
}

/// Rithmic order-book image/update boundary marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum BookUpdateType {
    Unspecified = 0,
    ClearOrderBook = 1,
    NoBook = 2,
    SnapshotImage = 3,
    Begin = 4,
    Middle = 5,
    End = 6,
    Solo = 7,
}

/// Encodes one protobuf message for a binary WebSocket frame.
#[must_use]
pub fn encode_frame<M: Message>(message: &M) -> Vec<u8> {
    message.encode_to_vec()
}

/// Validates and returns a binary WebSocket frame's protobuf payload.
///
/// # Errors
///
/// Returns an error for an empty frame.
pub fn decode_frame(frame: &[u8]) -> anyhow::Result<&[u8]> {
    anyhow::ensure!(!frame.is_empty(), "Rithmic WebSocket frame is empty");
    Ok(frame)
}

/// Reads the template ID without decoding the complete update.
///
/// # Errors
///
/// Returns an error when framing or protobuf decoding fails.
pub fn decode_template_id(frame: &[u8]) -> anyhow::Result<i32> {
    Ok(Base::decode(decode_frame(frame)?)?.template_id)
}

#[derive(Debug)]
pub enum InboundMessage {
    Login(ResponseLogin),
    Logout(ResponseCode),
    SystemInfo(ResponseSystemInfo),
    Heartbeat(ResponseCode),
    MarketDataResponse(ResponseCode),
    FrontMonth(ResponseFrontMonthContract),
    LastTrade(LastTrade),
    BestBidOffer(BestBidOffer),
    OrderBook(OrderBook),
    Reject(ResponseCode),
    ForcedLogout,
    Unsupported(i32),
}

/// Decodes a framed server message into its typed Rithmic projection.
///
/// # Errors
///
/// Returns an error when framing or protobuf decoding fails.
pub fn decode_inbound(frame: &[u8]) -> anyhow::Result<InboundMessage> {
    let payload = decode_frame(frame)?;
    let template_id = Base::decode(payload)?.template_id;
    let message = match template_id {
        LOGIN_RESPONSE_TEMPLATE_ID => InboundMessage::Login(ResponseLogin::decode(payload)?),
        LOGOUT_RESPONSE_TEMPLATE_ID => InboundMessage::Logout(ResponseCode::decode(payload)?),
        SYSTEM_INFO_RESPONSE_TEMPLATE_ID => {
            InboundMessage::SystemInfo(ResponseSystemInfo::decode(payload)?)
        }
        HEARTBEAT_RESPONSE_TEMPLATE_ID => {
            InboundMessage::Heartbeat(ResponseCode::decode(payload)?)
        }
        MARKET_DATA_RESPONSE_TEMPLATE_ID => {
            InboundMessage::MarketDataResponse(ResponseCode::decode(payload)?)
        }
        FRONT_MONTH_RESPONSE_TEMPLATE_ID => {
            InboundMessage::FrontMonth(ResponseFrontMonthContract::decode(payload)?)
        }
        LAST_TRADE_TEMPLATE_ID => InboundMessage::LastTrade(LastTrade::decode(payload)?),
        BBO_TEMPLATE_ID => InboundMessage::BestBidOffer(BestBidOffer::decode(payload)?),
        ORDER_BOOK_TEMPLATE_ID => InboundMessage::OrderBook(OrderBook::decode(payload)?),
        REJECT_TEMPLATE_ID => InboundMessage::Reject(ResponseCode::decode(payload)?),
        FORCED_LOGOUT_TEMPLATE_ID => InboundMessage::ForcedLogout,
        _ => InboundMessage::Unsupported(template_id),
    };
    Ok(message)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn frame_round_trip_dispatches_by_template() {
        let update = LastTrade {
            template_id: LAST_TRADE_TEMPLATE_ID,
            symbol: "MESU6".to_string(),
            exchange: "CME".to_string(),
            trade_price: 6_000.25,
            trade_size: 3,
            aggressor: TransactionType::Buy as i32,
            ..Default::default()
        };
        let frame = encode_frame(&update);
        assert_eq!(decode_template_id(&frame).unwrap(), LAST_TRADE_TEMPLATE_ID);
        assert_eq!(
            LastTrade::decode(decode_frame(&frame).unwrap())
                .unwrap()
                .symbol,
            "MESU6"
        );
    }

    #[rstest]
    fn rejects_empty_frame() {
        let error = decode_frame(&[]).unwrap_err();
        assert!(error.to_string().contains("empty"));
    }

    #[rstest]
    fn decodes_typed_market_data_message() {
        let update = BestBidOffer {
            template_id: BBO_TEMPLATE_ID,
            symbol: "MESU6".to_string(),
            exchange: "CME".to_string(),
            bid_price: 6_000.00,
            ask_price: 6_000.25,
            ..Default::default()
        };

        let InboundMessage::BestBidOffer(decoded) =
            decode_inbound(&encode_frame(&update)).unwrap()
        else {
            panic!("Expected best-bid/offer message")
        };
        assert_eq!(decoded.symbol, "MESU6");
        assert_eq!(decoded.ask_price, 6_000.25);
    }
}
