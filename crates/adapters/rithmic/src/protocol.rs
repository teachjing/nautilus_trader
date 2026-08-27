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
pub const SEARCH_SYMBOLS_REQUEST_TEMPLATE_ID: i32 = 109;
pub const SEARCH_SYMBOLS_RESPONSE_TEMPLATE_ID: i32 = 110;
pub const FRONT_MONTH_REQUEST_TEMPLATE_ID: i32 = 113;
pub const FRONT_MONTH_RESPONSE_TEMPLATE_ID: i32 = 114;
pub const DEPTH_BY_ORDER_SNAPSHOT_REQUEST_TEMPLATE_ID: i32 = 115;
pub const DEPTH_BY_ORDER_SNAPSHOT_RESPONSE_TEMPLATE_ID: i32 = 116;
pub const DEPTH_BY_ORDER_UPDATES_REQUEST_TEMPLATE_ID: i32 = 117;
pub const DEPTH_BY_ORDER_UPDATES_RESPONSE_TEMPLATE_ID: i32 = 118;
pub const DEPTH_BY_ORDER_TEMPLATE_ID: i32 = 160;
pub const DEPTH_BY_ORDER_END_EVENT_TEMPLATE_ID: i32 = 161;
pub const TIME_BAR_REPLAY_REQUEST_TEMPLATE_ID: i32 = 202;
pub const TIME_BAR_REPLAY_RESPONSE_TEMPLATE_ID: i32 = 203;
pub const LIST_EXCHANGE_PERMISSIONS_REQUEST_TEMPLATE_ID: i32 = 342;
pub const LIST_EXCHANGE_PERMISSIONS_RESPONSE_TEMPLATE_ID: i32 = 343;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum SearchPattern {
    Unspecified = 0,
    Equals = 1,
    Contains = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum SearchInstrumentType {
    Unspecified = 0,
    Future = 1,
    Equity = 2,
    Option = 3,
    FutureOption = 4,
    Spread = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum EntitlementFlag {
    Unspecified = 0,
    Enabled = 1,
    Disabled = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum TimeBarType {
    Unspecified = 0,
    Second = 1,
    Minute = 2,
    Daily = 3,
    Weekly = 4,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum ReplayDirection {
    Unspecified = 0,
    First = 1,
    Last = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum ReplayTimeOrder {
    Unspecified = 0,
    Forwards = 1,
    Backwards = 2,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestTimeBarReplay {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(enumeration = "TimeBarType", tag = "119200")]
    pub bar_type: i32,
    #[prost(int32, tag = "119112")]
    pub bar_type_period: i32,
    #[prost(int32, tag = "153002")]
    pub start_index: i32,
    #[prost(int32, tag = "153003")]
    pub finish_index: i32,
    #[prost(int32, tag = "154020")]
    pub user_max_count: i32,
    #[prost(enumeration = "ReplayDirection", tag = "149253")]
    pub direction: i32,
    #[prost(enumeration = "ReplayTimeOrder", tag = "149307")]
    pub time_order: i32,
    #[prost(bool, tag = "153642")]
    pub resume_bars: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct ResponseTimeBarReplay {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, tag = "132758")]
    pub request_key: String,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, repeated, tag = "132764")]
    pub rq_handler_rp_code: Vec<String>,
    #[prost(string, repeated, tag = "132766")]
    pub rp_code: Vec<String>,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(enumeration = "TimeBarType", tag = "119200")]
    pub bar_type: i32,
    #[prost(string, tag = "119112")]
    pub period: String,
    #[prost(int32, tag = "119100")]
    pub marker: i32,
    #[prost(uint64, tag = "119109")]
    pub num_trades: u64,
    #[prost(uint64, tag = "119110")]
    pub volume: u64,
    #[prost(uint64, tag = "119117")]
    pub bid_volume: u64,
    #[prost(uint64, tag = "119118")]
    pub ask_volume: u64,
    #[prost(double, tag = "100019")]
    pub open_price: f64,
    #[prost(double, tag = "100021")]
    pub close_price: f64,
    #[prost(double, tag = "100012")]
    pub high_price: f64,
    #[prost(double, tag = "100013")]
    pub low_price: f64,
    #[prost(double, tag = "100070")]
    pub settlement_price: f64,
    #[prost(bool, tag = "149138")]
    pub has_settlement_price: bool,
    #[prost(bool, tag = "154571")]
    pub must_clear_settlement_price: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestListExchangePermissions {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, tag = "154220")]
    pub user: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ResponseListExchangePermissions {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, repeated, tag = "132764")]
    pub rq_handler_rp_code: Vec<String>,
    #[prost(string, repeated, tag = "132766")]
    pub rp_code: Vec<String>,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(string, tag = "153508")]
    pub level_1_market_data: String,
    #[prost(string, tag = "153509")]
    pub level_2_market_data: String,
    #[prost(enumeration = "EntitlementFlag", tag = "153400")]
    pub entitlement_flag: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestSearchSymbols {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, tag = "120008")]
    pub search_text: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(string, tag = "100749")]
    pub product_code: String,
    #[prost(enumeration = "SearchInstrumentType", tag = "110116")]
    pub instrument_type: i32,
    #[prost(enumeration = "SearchPattern", tag = "155008")]
    pub pattern: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct ResponseSearchSymbols {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, repeated, tag = "132764")]
    pub rq_handler_rp_code: Vec<String>,
    #[prost(string, repeated, tag = "132766")]
    pub rp_code: Vec<String>,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(string, tag = "100003")]
    pub symbol_name: String,
    #[prost(string, tag = "100749")]
    pub product_code: String,
    #[prost(string, tag = "110116")]
    pub instrument_type: String,
    #[prost(string, tag = "100067")]
    pub expiration_date: String,
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

#[derive(Clone, PartialEq, Message)]
pub struct RequestDepthByOrderSnapshot {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(double, tag = "154405")]
    pub depth_price: f64,
}

#[derive(Clone, PartialEq, Message)]
pub struct ResponseDepthByOrderSnapshot {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(string, repeated, tag = "132764")]
    pub rq_handler_rp_code: Vec<String>,
    #[prost(string, repeated, tag = "132766")]
    pub rp_code: Vec<String>,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(uint64, tag = "112002")]
    pub sequence_number: u64,
    #[prost(enumeration = "DepthTransactionType", tag = "153612")]
    pub depth_side: i32,
    #[prost(double, tag = "154405")]
    pub depth_price: f64,
    #[prost(int32, repeated, tag = "154406")]
    pub depth_size: Vec<i32>,
    #[prost(uint64, repeated, tag = "153613")]
    pub depth_order_priority: Vec<u64>,
    #[prost(string, repeated, tag = "149238")]
    pub exchange_order_id: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestDepthByOrderUpdates {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "132760")]
    pub user_msg: Vec<String>,
    #[prost(enumeration = "SubscriptionRequest", tag = "100000")]
    pub request: i32,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(double, tag = "154405")]
    pub depth_price: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum DepthTransactionType {
    Unspecified = 0,
    Buy = 1,
    Sell = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum DepthUpdateType {
    Unspecified = 0,
    New = 1,
    Change = 2,
    Delete = 3,
}

#[derive(Clone, PartialEq, Message)]
pub struct DepthByOrder {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, tag = "110100")]
    pub symbol: String,
    #[prost(string, tag = "110101")]
    pub exchange: String,
    #[prost(uint64, tag = "112002")]
    pub sequence_number: u64,
    #[prost(enumeration = "DepthUpdateType", repeated, tag = "110121")]
    pub update_type: Vec<i32>,
    #[prost(enumeration = "DepthTransactionType", repeated, tag = "153612")]
    pub transaction_type: Vec<i32>,
    #[prost(double, repeated, tag = "154405")]
    pub depth_price: Vec<f64>,
    #[prost(double, repeated, tag = "154906")]
    pub prev_depth_price: Vec<f64>,
    #[prost(bool, repeated, tag = "154930")]
    pub prev_depth_price_flag: Vec<bool>,
    #[prost(int32, repeated, tag = "154406")]
    pub depth_size: Vec<i32>,
    #[prost(uint64, repeated, tag = "153613")]
    pub depth_order_priority: Vec<u64>,
    #[prost(string, repeated, tag = "149238")]
    pub exchange_order_id: Vec<String>,
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
    #[prost(int32, tag = "150600")]
    pub jop_ssboe: i32,
    #[prost(int32, tag = "150604")]
    pub jop_nsecs: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct DepthByOrderEndEvent {
    #[prost(int32, tag = "154467")]
    pub template_id: i32,
    #[prost(string, repeated, tag = "110100")]
    pub symbol: Vec<String>,
    #[prost(string, repeated, tag = "110101")]
    pub exchange: Vec<String>,
    #[prost(uint64, tag = "112002")]
    pub sequence_number: u64,
    #[prost(int32, tag = "150100")]
    pub ssboe: i32,
    #[prost(int32, tag = "150101")]
    pub usecs: i32,
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
    DepthByOrderSnapshot(ResponseDepthByOrderSnapshot),
    DepthByOrderResponse(ResponseCode),
    DepthByOrder(DepthByOrder),
    DepthByOrderEnd(DepthByOrderEndEvent),
    ExchangePermission(ResponseListExchangePermissions),
    SearchSymbol(ResponseSearchSymbols),
    TimeBarReplay(ResponseTimeBarReplay),
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
        DEPTH_BY_ORDER_SNAPSHOT_RESPONSE_TEMPLATE_ID => {
            InboundMessage::DepthByOrderSnapshot(ResponseDepthByOrderSnapshot::decode(payload)?)
        }
        DEPTH_BY_ORDER_UPDATES_RESPONSE_TEMPLATE_ID => {
            InboundMessage::DepthByOrderResponse(ResponseCode::decode(payload)?)
        }
        DEPTH_BY_ORDER_TEMPLATE_ID => {
            InboundMessage::DepthByOrder(DepthByOrder::decode(payload)?)
        }
        DEPTH_BY_ORDER_END_EVENT_TEMPLATE_ID => {
            InboundMessage::DepthByOrderEnd(DepthByOrderEndEvent::decode(payload)?)
        }
        LIST_EXCHANGE_PERMISSIONS_RESPONSE_TEMPLATE_ID => InboundMessage::ExchangePermission(
            ResponseListExchangePermissions::decode(payload)?,
        ),
        SEARCH_SYMBOLS_RESPONSE_TEMPLATE_ID => {
            InboundMessage::SearchSymbol(ResponseSearchSymbols::decode(payload)?)
        }
        TIME_BAR_REPLAY_RESPONSE_TEMPLATE_ID => {
            InboundMessage::TimeBarReplay(ResponseTimeBarReplay::decode(payload)?)
        }
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

    #[rstest]
    fn decodes_depth_by_order_update_with_order_identity() {
        let update = DepthByOrder {
            template_id: DEPTH_BY_ORDER_TEMPLATE_ID,
            symbol: "MESU6".to_string(),
            exchange: "CME".to_string(),
            sequence_number: 42,
            update_type: vec![DepthUpdateType::Delete as i32],
            transaction_type: vec![DepthTransactionType::Buy as i32],
            depth_price: vec![6_000.25],
            depth_size: vec![3],
            depth_order_priority: vec![99],
            exchange_order_id: vec!["exchange-order-1".to_string()],
            ..Default::default()
        };

        let decoded = decode_inbound(&encode_frame(&update)).unwrap();
        let InboundMessage::DepthByOrder(decoded) = decoded else {
            panic!("Expected depth-by-order update")
        };
        assert_eq!(decoded.sequence_number, 42);
        assert_eq!(decoded.update_type, vec![DepthUpdateType::Delete as i32]);
        assert_eq!(decoded.exchange_order_id, vec!["exchange-order-1"]);
    }

    #[rstest]
    fn decodes_exchange_permission_discovery() {
        let response = ResponseListExchangePermissions {
            template_id: LIST_EXCHANGE_PERMISSIONS_RESPONSE_TEMPLATE_ID,
            exchange: "CME".to_string(),
            level_1_market_data: "1".to_string(),
            level_2_market_data: "1".to_string(),
            entitlement_flag: EntitlementFlag::Enabled as i32,
            ..Default::default()
        };

        let InboundMessage::ExchangePermission(decoded) =
            decode_inbound(&encode_frame(&response)).unwrap()
        else {
            panic!("Expected exchange-permission response")
        };
        assert_eq!(decoded.exchange, "CME");
        assert_eq!(decoded.entitlement_flag, EntitlementFlag::Enabled as i32);
    }

    #[rstest]
    fn decodes_symbol_search_discovery() {
        let response = ResponseSearchSymbols {
            template_id: SEARCH_SYMBOLS_RESPONSE_TEMPLATE_ID,
            symbol: "ESU6".to_string(),
            exchange: "CME".to_string(),
            product_code: "ES".to_string(),
            instrument_type: "Future".to_string(),
            ..Default::default()
        };

        let InboundMessage::SearchSymbol(decoded) =
            decode_inbound(&encode_frame(&response)).unwrap()
        else {
            panic!("Expected symbol-search response")
        };
        assert_eq!(decoded.symbol, "ESU6");
        assert_eq!(decoded.product_code, "ES");
    }

    #[rstest]
    fn decodes_historical_time_bar_replay() {
        let response = ResponseTimeBarReplay {
            template_id: TIME_BAR_REPLAY_RESPONSE_TEMPLATE_ID,
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

        let InboundMessage::TimeBarReplay(decoded) =
            decode_inbound(&encode_frame(&response)).unwrap()
        else {
            panic!("Expected historical time-bar response")
        };
        assert_eq!(decoded.marker, 1_700_000_060);
        assert_eq!(decoded.close_price, 6_000.50);
    }
}
