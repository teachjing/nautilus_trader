// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Rithmic ticker-plant WebSocket session.

use std::{fmt::Debug, time::Duration};

use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::Message as WebSocketMessage,
};
use tokio_util::sync::CancellationToken;

use crate::{
    flow::{
        LoginCredentials, MarketSubscription, ensure_response_success, heartbeat_interval,
        heartbeat_request,
    },
    protocol::{
        FORCED_LOGOUT_TEMPLATE_ID, InboundMessage, LOGOUT_REQUEST_TEMPLATE_ID,
        LOGIN_RESPONSE_TEMPLATE_ID, REJECT_TEMPLATE_ID, RequestLogout, RequestSystemInfo,
        ResponseCode, ResponseSystemInfo, SYSTEM_INFO_REQUEST_TEMPLATE_ID,
        SYSTEM_INFO_RESPONSE_TEMPLATE_ID, SubscriptionRequest, decode_inbound, encode_frame,
    },
};

type RithmicWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(crate) struct RithmicSession {
    socket: RithmicWebSocket,
    heartbeat_interval: Duration,
}

impl Debug for RithmicSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RithmicSession))
            .field("heartbeat_interval", &self.heartbeat_interval)
            .finish_non_exhaustive()
    }
}

impl RithmicSession {
    pub(crate) async fn connect(
        gateway_url: &str,
        credentials: &LoginCredentials,
    ) -> anyhow::Result<Self> {
        let systems = Self::discover_systems(gateway_url).await?;
        anyhow::ensure!(
            systems.system_name.iter().any(|name| name == &credentials.system_name),
            "Rithmic system '{}' is unavailable, available systems: {}",
            credentials.system_name,
            systems.system_name.join(", ")
        );

        let (mut socket, _) = connect_async(gateway_url).await?;
        Self::send_protobuf(&mut socket, &credentials.ticker_plant_request()).await?;
        let response = Self::receive_expected(&mut socket, LOGIN_RESPONSE_TEMPLATE_ID).await?;
        let InboundMessage::Login(login) = response else {
            anyhow::bail!("Rithmic login returned an unexpected response")
        };
        let heartbeat_interval = heartbeat_interval(&login)?;

        log::info!(
            "Connected to Rithmic ticker plant '{}'",
            credentials.system_name
        );

        Ok(Self {
            socket,
            heartbeat_interval,
        })
    }

    async fn discover_systems(gateway_url: &str) -> anyhow::Result<ResponseSystemInfo> {
        let (mut socket, _) = connect_async(gateway_url).await?;
        let request = RequestSystemInfo {
            template_id: SYSTEM_INFO_REQUEST_TEMPLATE_ID,
            ..Default::default()
        };
        Self::send_protobuf(&mut socket, &request).await?;
        let response = Self::receive_expected(&mut socket, SYSTEM_INFO_RESPONSE_TEMPLATE_ID).await?;
        let InboundMessage::SystemInfo(systems) = response else {
            anyhow::bail!("Rithmic system discovery returned an unexpected response")
        };
        Self::ensure_codes_succeed(systems.template_id, &systems.rp_code)?;
        socket.close(None).await?;
        Ok(systems)
    }

    pub(crate) async fn subscribe(
        &mut self,
        subscription: &MarketSubscription,
    ) -> anyhow::Result<()> {
        let request = subscription.request(SubscriptionRequest::Subscribe);
        Self::send_protobuf(&mut self.socket, &request).await
    }

    async fn unsubscribe(
        &mut self,
        subscription: &MarketSubscription,
    ) -> anyhow::Result<()> {
        let request = subscription.request(SubscriptionRequest::Unsubscribe);
        Self::send_protobuf(&mut self.socket, &request).await
    }

    pub(crate) async fn run(
        mut self,
        subscriptions: Vec<MarketSubscription>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut heartbeat = tokio::time::interval(self.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;

        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = heartbeat.tick() => {
                    Self::send_protobuf(&mut self.socket, &heartbeat_request(0, 0)).await?;
                }
                message = self.socket.next() => {
                    let Some(message) = message else {
                        anyhow::bail!("Rithmic WebSocket stream ended")
                    };
                    self.handle_websocket_message(message?).await?;
                }
            }
        }

        for subscription in &subscriptions {
            self.unsubscribe(subscription).await?;
        }
        let logout = RequestLogout {
            template_id: LOGOUT_REQUEST_TEMPLATE_ID,
            ..Default::default()
        };
        Self::send_protobuf(&mut self.socket, &logout).await?;
        self.socket.close(None).await?;
        Ok(())
    }

    async fn handle_websocket_message(
        &mut self,
        message: WebSocketMessage,
    ) -> anyhow::Result<()> {
        match message {
            WebSocketMessage::Binary(data) => match decode_inbound(&data)? {
                InboundMessage::Reject(response) => {
                    Self::ensure_codes_succeed(REJECT_TEMPLATE_ID, &response.rp_code)?;
                }
                InboundMessage::ForcedLogout => {
                    anyhow::bail!("Rithmic forced logout received")
                }
                InboundMessage::MarketDataResponse(response) => {
                    ensure_response_success(&response)?;
                    log::debug!("Rithmic market-data subscription accepted");
                }
                InboundMessage::LastTrade(update) => {
                    log::trace!(
                        "Rithmic trade {}.{} {} @ {}",
                        update.symbol,
                        update.exchange,
                        update.trade_size,
                        update.trade_price
                    );
                }
                InboundMessage::BestBidOffer(update) => {
                    log::trace!(
                        "Rithmic BBO {}.{} {} x {}",
                        update.symbol,
                        update.exchange,
                        update.bid_price,
                        update.ask_price
                    );
                }
                InboundMessage::OrderBook(update) => {
                    log::trace!(
                        "Rithmic book {}.{} type={}",
                        update.symbol,
                        update.exchange,
                        update.update_type
                    );
                }
                InboundMessage::Unsupported(template_id) => {
                    log::debug!("Ignoring unsupported Rithmic template {template_id}");
                }
                _ => {}
            },
            WebSocketMessage::Close(frame) => {
                anyhow::bail!("Rithmic WebSocket closed: {frame:?}")
            }
            WebSocketMessage::Ping(data) => {
                self.socket.send(WebSocketMessage::Pong(data)).await?;
            }
            WebSocketMessage::Text(_)
            | WebSocketMessage::Pong(_)
            | WebSocketMessage::Frame(_) => {}
        }
        Ok(())
    }

    async fn receive_expected(
        socket: &mut RithmicWebSocket,
        expected_template_id: i32,
    ) -> anyhow::Result<InboundMessage> {
        while let Some(message) = socket.next().await {
            match message? {
                WebSocketMessage::Binary(data) => {
                    let message = decode_inbound(&data)?;
                    match &message {
                        InboundMessage::Reject(response) => {
                            Self::ensure_codes_succeed(REJECT_TEMPLATE_ID, &response.rp_code)?;
                        }
                        InboundMessage::ForcedLogout => {
                            anyhow::bail!("Rithmic forced logout received")
                        }
                        _ if Self::template_id(&message) == Some(expected_template_id) => {
                            return Ok(message);
                        }
                        _ => {}
                    }
                }
                WebSocketMessage::Ping(data) => {
                    socket.send(WebSocketMessage::Pong(data)).await?;
                }
                WebSocketMessage::Close(frame) => {
                    anyhow::bail!("Rithmic WebSocket closed while awaiting response: {frame:?}")
                }
                WebSocketMessage::Text(_)
                | WebSocketMessage::Pong(_)
                | WebSocketMessage::Frame(_) => {}
            }
        }
        anyhow::bail!("Rithmic WebSocket ended while awaiting template {expected_template_id}")
    }

    fn template_id(message: &InboundMessage) -> Option<i32> {
        match message {
            InboundMessage::Login(response) => Some(response.template_id),
            InboundMessage::SystemInfo(response) => Some(response.template_id),
            InboundMessage::Logout(response)
            | InboundMessage::Heartbeat(response)
            | InboundMessage::MarketDataResponse(response)
            | InboundMessage::Reject(response) => Some(response.template_id),
            InboundMessage::FrontMonth(response) => Some(response.template_id),
            InboundMessage::LastTrade(response) => Some(response.template_id),
            InboundMessage::BestBidOffer(response) => Some(response.template_id),
            InboundMessage::OrderBook(response) => Some(response.template_id),
            InboundMessage::Unsupported(template_id) => Some(*template_id),
            InboundMessage::ForcedLogout => Some(FORCED_LOGOUT_TEMPLATE_ID),
        }
    }

    async fn send_protobuf<M: ProstMessage>(
        socket: &mut RithmicWebSocket,
        message: &M,
    ) -> anyhow::Result<()> {
        socket
            .send(WebSocketMessage::Binary(encode_frame(message).into()))
            .await?;
        Ok(())
    }

    fn ensure_codes_succeed(template_id: i32, rp_code: &[String]) -> anyhow::Result<()> {
        let response = ResponseCode {
            template_id,
            rp_code: rp_code.to_vec(),
            ..Default::default()
        };
        ensure_response_success(&response)
    }
}
