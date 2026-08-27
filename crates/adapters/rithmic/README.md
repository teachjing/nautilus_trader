# Rithmic adapter

Native NautilusTrader v2 market-data adapter for the Rithmic R | Protocol.

The adapter owns Rithmic connectivity, system and contract discovery, subscriptions, historical
time-bar replay, and conversion to native Nautilus data. TickerBot feature data remains a separate
`CustomData` flow.

## Protocol plants

Rithmic selects one infrastructure plant during login; plant-specific requests cannot be sent over
a socket authenticated to another plant.

| Plant | Implemented use | Templates |
|---|---|---|
| Shared discovery | Available Rithmic systems | 16/17 |
| Ticker Plant | Symbol search, front month, trades, BBO, MBP and price-specific MBO | 109/110, 113/114, 100/101, 115-118, 150/151/156, 160/161 |
| Order Plant | Exchange and market-data entitlement discovery | 342/343 |
| History Plant | Historical time-bar replay | 202/203 |

The safe discovery probe connects to these plants sequentially and closes each socket before
opening the next. Set `RITHMIC_TEST_PLANT_CAPACITY=true` to explicitly test whether the account
permits concurrent Ticker, Order, and History Plant connections.

## Nautilus data objects

Nautilus's built-in data model includes order-book deltas, depth snapshots, quotes, trades, bars,
reference prices, status events, option Greeks, and custom data. The following table records what
the current Rithmic adapter can populate and which Rithmic templates provide the source fields.

| Nautilus object | Current status | Rithmic source and notes |
|---|---|---|
| `TradeTick` | Implemented | Template 150 last trade: price, size, aggressor, exchange trade/order identity and source timestamp. |
| `QuoteTick` | Implemented | Template 151 BBO. Partial bid/ask updates are combined with cached state and normalized to equal precision. |
| `OrderBookDelta` / `OrderBookDeltas` (`L2_MBP`) | Implemented | Template 156 full-depth market-by-price snapshots and incremental price-level updates. |
| `OrderBookDelta` / `OrderBookDeltas` (`L3_MBO`) | Implemented for requested prices | Templates 115-118 and 160/161 expose exchange order IDs, queue priority, new/change/delete events and previous price. Rithmic depth-by-order requests are price-specific; this is not yet a complete all-price L3 book. |
| `OrderBookDepth10` | Derivable, not emitted | The first ten bid/ask levels can be projected from template 156, but the adapter currently emits deltas so Nautilus maintains the book. |
| `Bar` | Implemented for historical time bars | History Plant templates 202/203 map OHLCV and marker time to an external Nautilus `Bar`. Second, minute, daily and weekly families are supported. |
| Nautilus instrument definitions | Partial discovery only | Templates 109/110 produce a queryable symbol catalog. Full `FuturesContract` creation still needs reference data such as tick size, currency, multiplier and expiration semantics. |
| `InstrumentStatus` | Candidate | Template 157 Market Mode is the likely source; conversion is not implemented. |
| `InstrumentClose` | Candidate | Template 155 End Of Day Prices can supply close/settlement events; conversion is not implemented. |
| `MarkPriceUpdate` / `IndexPriceUpdate` | Candidate after field validation | Template 154 Indicator Prices may expose suitable reference prices, but the semantic mapping must be verified before emitting native objects. |
| `FundingRateUpdate` | Not applicable to CME futures | No equivalent funding-rate stream is used by this adapter. |
| `OptionGreeks` | Not implemented | The current Rithmic slice does not decode a venue-provided Greeks template. |
| `CustomData` | Available for future Rithmic metadata | Trade statistics (152), quote statistics (153), open interest (158), order counts, implied liquidity, MBO priority and other feed-specific fields can be preserved without forcing them into an incompatible built-in object. |

## Build and unit tests

```bash
apt-get update
apt-get install -y build-essential clang cmake curl git libssl-dev pkg-config
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"

cargo metadata --no-deps --format-version 1 >/dev/null
cargo fmt --all -- --check
cargo check -p nautilus-rithmic
cargo test -p nautilus-rithmic --lib -- --nocapture
```

## Live ticker, discovery, MBO, and connection-capacity probe

```bash
export RITHMIC_USER="your-user"
export RITHMIC_PASSWORD="your-password"
export RITHMIC_SYSTEM_NAME="Rithmic Paper Trading"
export RITHMIC_GATEWAY_URL="wss://rprotocol-mobile.rithmic.com/"
export RITHMIC_LIVE_SUBSCRIPTION="CME.ES"
export RITHMIC_LIVE_FALLBACK_SUBSCRIPTION="CME.ESU6"
export RITHMIC_LIVE_DURATION_SECS="30"
export RITHMIC_DIAGNOSTIC_LOG_DIR="target/rithmic-diagnostics"

export RITHMIC_DISCOVER_MARKETS="true"
export RITHMIC_DISCOVER_INSTRUMENTS="true"
export RITHMIC_DISCOVERY_EXCHANGES="CME"
export RITHMIC_DISCOVERY_TIMEOUT_SECS="300"

export RITHMIC_TEST_MBO="true"
export RITHMIC_REQUIRE_MBO="true"

# Optional: discover whether this account permits simultaneous plant sockets.
export RITHMIC_TEST_PLANT_CAPACITY="true"

cargo test -p nautilus-rithmic \
  --test live_connection \
  --features live-tests \
  -- --ignored --nocapture
```

The stable discovery catalog is written to
`target/rithmic-diagnostics/rithmic-discovery.json`. Query it with:

Exchange permissions are always collected for every market returned by Rithmic. Instrument
enumeration is limited to `RITHMIC_DISCOVERY_EXCHANGES` (a comma-separated list) so a normal live
probe does not attempt an unbounded global contract crawl. When the variable is unset, the adapter
uses the exchanges from `RITHMIC_LIVE_SUBSCRIPTION(S)`.

```bash
cargo run -p nautilus-rithmic --example catalog_query -- \
  target/rithmic-diagnostics/rithmic-discovery.json ES
```

Run only the connection-capacity test (no active market data is required):

```bash
cargo test -p nautilus-rithmic \
  --test live_plant_capacity \
  --features live-tests \
  -- --ignored --nocapture
```

The result distinguishes Order-with-Ticker, History-with-Ticker-and-Order, and a History retry
with only Ticker held open. This identifies whether the limit is global, per plant, or total socket
count for the account.

Run only the complete market entitlement report (no active market data is required):

```bash
cargo test -p nautilus-rithmic \
  --test live_market_entitlements \
  --features live-tests \
  -- --ignored --nocapture
```

The test prints every market returned by Rithmic with its entitlement flag and reported L1/L2
access. It also writes the same result to
`target/rithmic-diagnostics/rithmic-market-entitlements.json`.

Search contracts interactively for one selected market and search string:

```bash
export RITHMIC_SEARCH_EXCHANGE="CME"
export RITHMIC_SEARCH_TEXT="MES"

cargo test -p nautilus-rithmic \
  --test live_instrument_search \
  --features live-tests \
  -- --ignored --nocapture
```

This uses templates 109/110 directly and writes the typed matches to
`target/rithmic-diagnostics/rithmic-instrument-search-CME-MES.json`. The public Rust function
`run_instrument_search` provides the same focused operation for a future UI/API endpoint.

## Historical time-bar replay

The Rithmic reference guide assigns templates 202/203 to the History Plant. Replay responses carry
OHLC, volume, trade count, bid/ask volume, settlement metadata and an epoch-second marker. The
current adapter converts OHLCV into native external Nautilus `Bar` objects. The remaining Rithmic
fields stay available in the typed wire response for a future metadata/custom-data extension.

Large replay requests may be truncated by Rithmic. The probe therefore paginates by advancing the
next request to `last_marker + 1`, caps the number of pages, sorts the result, and removes duplicate
timestamps.

Use an explicit contract for the historical probe:

```bash
export RITHMIC_HISTORICAL_SUBSCRIPTION="CME.ESU6"
export RITHMIC_HISTORICAL_LOOKBACK_SECS="86400"
export RITHMIC_HISTORICAL_BAR_PERIOD="1"
export RITHMIC_HISTORICAL_MAX_PAGES="10"

cargo test -p nautilus-rithmic \
  --test live_history \
  --features live-tests \
  -- --ignored --nocapture
```

The test validates system discovery, History Plant login, replay response codes, pagination,
strictly increasing timestamps, OHLC correctness, native `Bar` conversion, and orderly logout.

## Remaining adapter work

- Convert the discovery catalog into full Nautilus `FuturesContract` definitions.
- Route Nautilus historical `request_bars` commands through the History Plant client.
- Add tick replay templates 206/207 and conversion to historical `TradeTick` data.
- Validate and map Market Mode (157), End Of Day Prices (155), and Indicator Prices (154).
- Decide which Rithmic-only statistics should become registered `CustomData` schemas.
