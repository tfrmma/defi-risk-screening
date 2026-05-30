# defi-risk-screening

Institutional liquidation monitor + yield arbitrage screener for Aave V3, Morpho Blue, and Spark.
Built for a directional + delta-neutral desk. Zero third-party RPC dependencies in production.

## Prerequisites

- Reth node (or Erigon) with IPC/WebSocket enabled, fully synced
- Redis 7+
- ClickHouse 23+
- Go 1.22+
- Python 3.11+
- ZeroMQ 4.x (`apt install libzmq3-dev`)

## Setup

```bash
# go deps
go mod download

# python deps
pip install -r requirements.txt

# clickhouse schema
clickhouse-client --queries-file scripts/schema.sql

# build go binaries
./scripts/ops.sh build
```

## Running

```bash
# start everything
./scripts/ops.sh start

# check what's alive
./scripts/ops.sh status

# tail a specific service
./scripts/ops.sh logs risk
```

## Architecture

```
reth node (IPC)
    │
    ▼
[indexer] ──── dirty:accounts ──→ [risk_engine] ──── risk:* ──→ Redis
    │                                   │
    │                                   └── ZMQ PUB :5556 (risk events)
    │
[aggregator] ─── local L2 books ──→ [cascade_sim] ─→ ZMQ PUB :5555
    │ binance/bybit/okx ws
    │
[arb_engine] ─── Aave rates + funding rates ──→ ZMQ PUB :5557

[frontend] ─── subscribes to all ZMQ topics, polls Redis for heatmap
```

## ZMQ Topics

| Addr  | Topic     | Producer    | Description                  |
|-------|-----------|-------------|------------------------------|
| :5555 | `cascade` | cascade_sim | liquidation cascade alerts   |
| :5556 | `risk`    | risk_engine | account HF updates           |
| :5557 | `arb`     | arb_engine  | carry trade opportunities    |

## Config

Edit `config/config.yaml`. Key settings:

- `rpc.ipc_path` — point this at your reth IPC socket
- `thresholds.hf_warn` / `hf_critical` — HF alert levels
- `thresholds.arb_min_spread` — minimum net APY for carry alerts (default 4%)
- `thresholds.cascade_threshold` — price drop % that triggers cascade sim

## Notes

- During high-vol events (CPI, flash crashes), the oracle lag on ETH/USD can reach 10-15 minutes.
  The `OracleLagBar` in the frontend shows this visually. Wide lag = bigger window to front-run liq.
- CVD squeeze filter in arb_engine: if spot CVD Z-score > 0.65, carry opportunities are suppressed.
  Institutions are absorbing — fighting that with a short hedge is expensive.
- The cascade sim uses a 75% MEV fill efficiency factor. Adjust `MEV_FILL_EFFICIENCY` in cascade_sim.py
  based on observed bot behavior post-liquidation events.
