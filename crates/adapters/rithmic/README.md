# Rithmic adapter

Native NautilusTrader v2 live market-data adapter for Rithmic R | Protocol.

The adapter owns Rithmic connectivity, entitlement and instrument discovery, front-month
resolution, subscriptions, and conversion to native Nautilus data. TickerBot feature data is a
separate `CustomData` flow and is intentionally not routed through this client.

The current protocol slice maps the Rithmic ticker-plant flow:

1. Request system information (templates 16/17) on a temporary connection.
2. Reconnect and log in to the ticker plant (templates 10/11, protocol version 3.9).
3. Use the heartbeat interval returned by login (templates 18/19).
4. Resolve front-month contracts when configured by root (templates 113/114).
5. Subscribe to trades, BBO, and market-by-price data (templates 100/101).
6. Dispatch last trade, BBO, and order-book updates (templates 150/151/156).
7. Unsubscribe and log out during an orderly shutdown (templates 100 and 12/13).

The config and factory are registered with the Nautilus PyO3 registry. `protocol` owns the exact
protobuf wire projections used in binary WebSocket frames, while `flow` owns request construction,
response validation, subscription bits, and server-directed heartbeat cadence. The remaining
transport slice will connect this flow to the Nautilus data engine and convert updates to native
`TradeTick`, `QuoteTick`, and `OrderBookDelta` values.


## Testing ##

Install rust
``` 
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Check

```
apt-get update

apt-get install -y \
  build-essential \
  clang \
  cmake \
  curl \
  git \
  libssl-dev \
  pkg-config


cargo metadata --no-deps --format-version 1 >/dev/null && echo "Workspace valid"

cargo check -p nautilus-rithmic
cargo test -p nautilus-rithmic -- --nocapture
```

### Live connection validation

Live validation is excluded from normal tests because it requires credentials, exchange
entitlements, and an active market. The test performs system discovery, ticker-plant login,
front-month resolution, market-data subscriptions, native Nautilus conversion, unsubscribe, and
logout.

```bash
export RITHMIC_USER="your-user"
export RITHMIC_PASSWORD="your-password"
export RITHMIC_SYSTEM_NAME="Rithmic Paper Trading"
export RITHMIC_GATEWAY_URL="wss://rprotocol-mobile.rithmic.com/"
export RITHMIC_LIVE_SUBSCRIPTION="CME.MES"
export RITHMIC_LIVE_FALLBACK_SUBSCRIPTION="CME.MESU6"
export RITHMIC_LIVE_DURATION_SECS="30"
export RITHMIC_DIAGNOSTIC_LOG_DIR="target/rithmic-diagnostics"

cargo test -p nautilus-rithmic \
  --test live_connection \
  --features live-tests \
  -- --ignored --nocapture
```

The probe always validates that `RITHMIC_SYSTEM_NAME` is present in the template 17 system-info
response before logging in. It then attempts template 113/114 front-month discovery. If Rithmic
returns `7: no data`, the probe uses `RITHMIC_LIVE_FALLBACK_SUBSCRIPTION` and continues.

Credential-safe JSONL logs are written under `RITHMIC_DIAGNOSTIC_LOG_DIR`. They contain system
names, response codes, contract-resolution results, fallback selection, and subscription status,
but never the Rithmic username or password. The result printed by the test includes the exact log
path.

The live result also classifies the observed order-book capability. Template 151 is top-of-book
(`L1_MBP`). Template 156 is aggregated full-depth market by price (`L2_MBP`) and can include the
number of orders and implied size at each price. Size-zero template 156 updates become price-level
deletes; they do not identify an individual canceled order. True market by order (`L3_MBO`) requires
the separate depth-by-order request/update flow (templates 115-118 and 160-161), which is not part
of the template 156 subscription.

For a bounded multi-symbol probe, set a comma-separated list and run the example:

```bash
export RITHMIC_LIVE_SUBSCRIPTIONS="CME.MES,CME.NQ"
cargo run -p nautilus-rithmic --example node_data_tester
```
