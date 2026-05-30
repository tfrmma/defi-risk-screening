-- tick storage schema
-- ClickHouse is the right tool here: columnar, ridiculous insert throughput,
-- native time-series aggregations. Don't try to do this in Postgres.

CREATE DATABASE IF NOT EXISTS hft;

-- raw protocol events — append-only, never update
CREATE TABLE IF NOT EXISTS hft.protocol_events (
    timestamp        DateTime64(3) CODEC(Delta, ZSTD),
    block_number     UInt64,
    tx_hash          FixedString(66),
    protocol         LowCardinality(String),
    event_name       LowCardinality(String),
    user_address     String,
    asset            LowCardinality(String),
    amount_raw       UInt256,
    amount_usd       Float64,
    health_factor    Float64  -- 0 if not applicable
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (protocol, user_address, timestamp)
TTL timestamp + INTERVAL 90 DAY;

-- account risk snapshots — one row per re-evaluation
CREATE TABLE IF NOT EXISTS hft.account_risk_snapshots (
    timestamp        DateTime64(3) CODEC(Delta, ZSTD),
    protocol         LowCardinality(String),
    address          String,
    collateral_usd   Float64,
    debt_usd         Float64,
    health_factor    Float64,
    emode_id         UInt8,
    liq_price_est    Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (address, timestamp)
TTL timestamp + INTERVAL 30 DAY;

-- CEX order book L2 snapshots (sampled, not every update)
-- storing full ticks would be terabytes/day — sample every 1s instead
CREATE TABLE IF NOT EXISTS hft.orderbook_snapshots (
    timestamp        DateTime64(3) CODEC(Delta, ZSTD),
    exchange         LowCardinality(String),
    symbol           LowCardinality(String),
    side             Enum8('bid' = 0, 'ask' = 1),
    price            Float64,
    qty              Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYDD(timestamp)
ORDER BY (exchange, symbol, timestamp, price)
TTL timestamp + INTERVAL 7 DAY;  -- short TTL — this fills up fast

-- funding rate history — small table, keep forever
CREATE TABLE IF NOT EXISTS hft.funding_rates (
    timestamp        DateTime64(3) CODEC(Delta, ZSTD),
    venue            LowCardinality(String),
    symbol           LowCardinality(String),
    rate_8h          Float64,
    apy              Float64,
    oi_usd           Float64
) ENGINE = MergeTree()
ORDER BY (venue, symbol, timestamp);

-- arb opportunities — for backtest and slippage attribution
CREATE TABLE IF NOT EXISTS hft.arb_opportunities (
    timestamp        DateTime64(3),
    long_asset       LowCardinality(String),
    short_venue      LowCardinality(String),
    borrow_apy       Float64,
    funding_apy      Float64,
    net_apy          Float64,
    oi_usd           Float64,
    squeeze_risk     UInt8  -- bool
) ENGINE = MergeTree()
ORDER BY (long_asset, short_venue, timestamp);

-- cascade simulation results — useful for post-event attribution
CREATE TABLE IF NOT EXISTS hft.cascade_simulations (
    timestamp        DateTime64(3),
    initial_price    Float64,
    initial_drop_pct Float64,
    max_drawdown     Float64,
    total_liq_usd    Float64,
    final_price      Float64,
    n_steps          UInt16,
    alert_level      LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY timestamp
TTL timestamp + INTERVAL 14 DAY;

-- useful mat view: liquidation volume by hour
-- saves recomputing this on every dashboard query
CREATE MATERIALIZED VIEW IF NOT EXISTS hft.hourly_liquidations
ENGINE = SummingMergeTree()
ORDER BY (hour, protocol)
AS
SELECT
    toStartOfHour(timestamp) AS hour,
    protocol,
    count()                  AS event_count,
    sum(amount_usd)          AS total_usd
FROM hft.protocol_events
WHERE event_name = 'Liquidate'
GROUP BY hour, protocol;
