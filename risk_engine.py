"""
risk_engine.py — HF calculator + whale tracker

Zero RPC calls in the hot path. State is built entirely from Redis,
which the Go indexer keeps current via on-chain events.
The only time we touch the node is for oracle lag checks (low frequency).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

import redis.asyncio as aioredis
import zmq
import zmq.asyncio
from web3 import AsyncWeb3, WebSocketProvider

log = logging.getLogger(__name__)

WAD = Decimal(10**18)
RAY = Decimal(10**27)

# eMode liquidation thresholds — pull from protocol config in prod
# these are the Aave V3 mainnet defaults
EMODE_LT = {
    0: Decimal("0.825"),   # standard
    1: Decimal("0.930"),   # ETH correlated
    2: Decimal("0.970"),   # stablecoin
}


@dataclass
class AccountState:
    """Reconstructed from Redis — no RPC involved."""
    address:  str
    protocol: str
    # per-asset positions keyed by token address
    collateral: dict[str, Decimal]  # token -> amount in base units
    debt:       dict[str, Decimal]
    emode_id:   int
    updated_at: float


@dataclass
class AccountRisk:
    address:        str
    collateral_usd: Decimal
    debt_usd:       Decimal
    health_factor:  Decimal
    emode_id:       int
    protocol:       str
    liq_price_est:  Optional[Decimal] = None
    updated_at:     float = field(default_factory=time.time)

    @property
    def is_at_risk(self) -> bool:
        return self.health_factor < Decimal("1.2")

    @property
    def is_critical(self) -> bool:
        return self.health_factor < Decimal("1.05")


class LocalStateReader:
    """
    Reads account positions from Redis as maintained by the Go indexer.
    Format written by indexer: hash at acct:{protocol}:{address}:{token}
    keys: collateral_raw, debt_raw, decimals, price_usd (updated by oracle tracker)
    """

    def __init__(self, rdb: aioredis.Redis):
        self.rdb = rdb

    async def load_account(self, protocol: str, address: str) -> Optional[AccountRisk]:
        # grab all token positions for this account in one pipeline
        pattern = f"acct:{protocol}:{address}:*"
        keys = await self.rdb.keys(pattern)
        if not keys:
            return None

        pipe = self.rdb.pipeline(transaction=False)
        for k in keys:
            pipe.hgetall(k)
        raw_positions = await pipe.execute()

        emode_raw = await self.rdb.get(f"acct:{protocol}:{address}:emode")
        emode_id = int(emode_raw or 0)
        lt = EMODE_LT.get(emode_id, EMODE_LT[0])

        total_collateral_usd = Decimal("0")
        weighted_col_usd     = Decimal("0")  # weighted by liquidation threshold
        total_debt_usd       = Decimal("0")

        for pos in raw_positions:
            if not pos:
                continue
            col_raw  = Decimal(pos.get("collateral_raw", "0"))
            debt_raw = Decimal(pos.get("debt_raw", "0"))
            decimals = int(pos.get("decimals", "18"))
            price    = Decimal(pos.get("price_usd", "0"))

            if price == 0:
                continue  # oracle hasn't updated this token yet — skip rather than miscount

            scale = Decimal(10**decimals)
            col_usd  = col_raw  / scale * price
            debt_usd = debt_raw / scale * price

            total_collateral_usd += col_usd
            weighted_col_usd     += col_usd * lt
            total_debt_usd       += debt_usd

        if total_debt_usd == 0:
            return None  # no debt = not interesting

        hf = weighted_col_usd / total_debt_usd

        return AccountRisk(
            address=address,
            protocol=protocol,
            collateral_usd=total_collateral_usd,
            debt_usd=total_debt_usd,
            health_factor=hf,
            emode_id=emode_id,
            liq_price_est=self._estimate_liq_price(total_collateral_usd, total_debt_usd, lt),
        )

    def _estimate_liq_price(
        self,
        col_usd: Decimal,
        debt_usd: Decimal,
        lt: Decimal,
    ) -> Decimal:
        # ratio at which HF = 1.0 relative to current collateral price
        # single-asset approximation — good enough for whale clustering
        if col_usd == 0 or lt == 0:
            return Decimal("0")
        return debt_usd / (col_usd * lt)


class OraclePriceUpdater:
    """
    Periodically fetches oracle prices and writes them into Redis so
    LocalStateReader can do HF math without touching the node.
    This runs at low frequency (every 10s) — it's not on the hot path.
    """

    FEEDS = {
        # token_addr -> chainlink_feed_addr
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",  # WETH
        "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599": "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c",  # WBTC
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6",  # USDC
    }

    FEED_ABI = [{"name": "latestRoundData", "type": "function", "inputs": [], "outputs": [
        {"name": "roundId",         "type": "uint80"},
        {"name": "answer",          "type": "int256"},
        {"name": "startedAt",       "type": "uint256"},
        {"name": "updatedAt",       "type": "uint256"},
        {"name": "answeredInRound", "type": "uint80"},
    ]}]

    def __init__(self, w3: AsyncWeb3, rdb: aioredis.Redis):
        self.w3  = w3
        self.rdb = rdb
        self._feeds = {
            addr: w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(feed),
                abi=self.FEED_ABI,
            )
            for addr, feed in self.FEEDS.items()
        }

    async def run(self):
        while True:
            await self._refresh_all()
            await asyncio.sleep(10)

    async def _refresh_all(self):
        tasks = [self._refresh_token(token, feed) for token, feed in self._feeds.items()]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _refresh_token(self, token_addr: str, feed):
        try:
            data = await feed.functions.latestRoundData().call()
            price_usd = Decimal(data[1]) / Decimal(10**8)   # chainlink answers in 8 decimals
            updated_at = data[3]

            lag = time.time() - updated_at
            await self.rdb.set(f"oracle:price:{token_addr}", str(price_usd))
            await self.rdb.set(f"oracle:lag:{token_addr}", str(lag))

            # propagate price into all account positions for this token
            # Go indexer writes position keys with token_addr embedded
            # we just need to update the price field
            pattern = f"acct:*:*:{token_addr}"
            keys = await self.rdb.keys(pattern)
            if keys:
                pipe = self.rdb.pipeline(transaction=False)
                for k in keys:
                    pipe.hset(k, "price_usd", str(price_usd))
                await pipe.execute()

        except Exception as e:
            log.debug("oracle refresh failed for %s: %s", token_addr[:10], e)


class RiskEngine:
    def __init__(self, cfg: dict):
        self.cfg      = cfg
        self.rdb: aioredis.Redis       = None
        self.zmq_ctx  = zmq.asyncio.Context()
        self.pub: zmq.asyncio.Socket   = None
        self.reader: LocalStateReader  = None
        self.oracle_updater: OraclePriceUpdater = None

    async def setup(self):
        self.rdb = aioredis.from_url(
            f"redis://{self.cfg['redis']['addr']}",
            decode_responses=True,
        )
        self.pub = self.zmq_ctx.socket(zmq.PUB)
        self.pub.bind(self.cfg["zmq"]["alert_pub"])

        self.reader = LocalStateReader(self.rdb)

        # oracle updater needs the node — but only for price fetches, not account state
        w3 = AsyncWeb3(WebSocketProvider(self.cfg["rpc"]["ws_url"]))
        self.oracle_updater = OraclePriceUpdater(w3, self.rdb)

    async def run(self):
        await self.setup()
        log.info("risk engine running — no RPC on hot path")

        # oracle runs in background at low freq
        asyncio.create_task(self.oracle_updater.run())

        while True:
            await self._process_dirty_batch()
            await asyncio.sleep(0.1)  # tighter tick now that we're not waiting on RPC

    async def _process_dirty_batch(self):
        # pop 100 per tick — we can afford more now that eval is just Redis reads + math
        accounts = await self.rdb.spop("dirty:accounts", 100)
        if not accounts:
            return

        tasks = [self._evaluate_account(entry) for entry in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                log.warning("account eval error: %s", r)

    async def _evaluate_account(self, entry: str):
        # entry format: "protocol:address" — written by Go indexer
        parts = entry.split(":", 1)
        if len(parts) != 2:
            log.debug("malformed dirty entry: %s", entry)
            return

        protocol, address = parts
        risk = await self.reader.load_account(protocol, address)
        if risk is None:
            return

        await self._cache_risk(risk)

        if risk.is_at_risk:
            await self._publish_risk_event(risk)
            log.info("at-risk %s proto=%s HF=%.4f", address[:10], protocol, risk.health_factor)

    async def _cache_risk(self, risk: AccountRisk):
        key = f"risk:{risk.protocol}:{risk.address}"
        pipe = self.rdb.pipeline(transaction=False)
        pipe.hset(key, mapping={
            "collateral_usd": str(risk.collateral_usd),
            "debt_usd":       str(risk.debt_usd),
            "health_factor":  str(risk.health_factor),
            "emode_id":       risk.emode_id,
            "liq_price_est":  str(risk.liq_price_est or 0),
            "updated_at":     risk.updated_at,
        })
        pipe.expire(key, 300)
        if risk.is_at_risk:
            pipe.zadd("at_risk:accounts", {f"{risk.protocol}:{risk.address}": float(risk.health_factor)})
        await pipe.execute()

    async def _publish_risk_event(self, risk: AccountRisk):
        payload = {
            "type":           "RISK_UPDATE",
            "address":        risk.address,
            "protocol":       risk.protocol,
            "health_factor":  str(risk.health_factor),
            "collateral_usd": str(risk.collateral_usd),
            "debt_usd":       str(risk.debt_usd),
            "liq_price_est":  str(risk.liq_price_est or 0),
            "critical":       risk.is_critical,
            "ts":             risk.updated_at,
        }
        await self.pub.send_multipart([b"risk", json.dumps(payload).encode()])


async def main():
    import yaml
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with open("config/config.yaml") as f:
        cfg = yaml.safe_load(f)

    await RiskEngine(cfg).run()


if __name__ == "__main__":
    asyncio.run(main())
