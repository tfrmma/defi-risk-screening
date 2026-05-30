"""
risk_engine.py — HF calculator + whale tracker
Reads dirty accounts from Redis, pulls on-chain state, recomputes HF.
Publishes risk events to ZMQ for downstream consumers.
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

# --- ABI stubs (trimmed to what we actually call) ---

AAVE_POOL_ABI = [
    {
        "name": "getUserAccountData",
        "type": "function",
        "inputs": [{"name": "user", "type": "address"}],
        "outputs": [
            {"name": "totalCollateralBase", "type": "uint256"},
            {"name": "totalDebtBase", "type": "uint256"},
            {"name": "availableBorrowsBase", "type": "uint256"},
            {"name": "currentLiquidationThreshold", "type": "uint256"},
            {"name": "ltv", "type": "uint256"},
            {"name": "healthFactor", "type": "uint256"},
        ],
    },
    {
        "name": "getUserEMode",
        "type": "function",
        "inputs": [{"name": "user", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]

MORPHO_HUB_ABI = [
    {
        "name": "position",
        "type": "function",
        "inputs": [
            {"name": "id", "type": "bytes32"},
            {"name": "user", "type": "address"},
        ],
        "outputs": [
            {"name": "supplyShares", "type": "uint128"},
            {"name": "borrowShares", "type": "uint128"},
            {"name": "collateral", "type": "uint128"},
        ],
    }
]

RAY = Decimal(10**27)
WAD = Decimal(10**18)


@dataclass
class AccountRisk:
    address: str
    collateral_usd: Decimal
    debt_usd: Decimal
    health_factor: Decimal
    emode_id: int
    protocol: str
    liq_price_usd: Optional[Decimal] = None  # estimated liquidation trigger price
    updated_at: float = field(default_factory=time.time)

    @property
    def is_at_risk(self) -> bool:
        return self.health_factor < Decimal("1.2")

    @property
    def is_critical(self) -> bool:
        return self.health_factor < Decimal("1.05")


class AaveRiskCalculator:
    """
    Wraps getUserAccountData but also does local HF simulation for oracle-lag estimation.
    We call the contract for ground truth, then use local math for delta projections.
    """

    def __init__(self, w3: AsyncWeb3, pool_addr: str, oracle_addr: str):
        self.pool = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(pool_addr),
            abi=AAVE_POOL_ABI,
        )
        self.oracle_addr = oracle_addr

    async def fetch_account(self, user: str) -> AccountRisk:
        addr = AsyncWeb3.to_checksum_address(user)
        data, emode = await asyncio.gather(
            self.pool.functions.getUserAccountData(addr).call(),
            self.pool.functions.getUserEMode(addr).call(),
        )

        collateral_usd = Decimal(data[0]) / Decimal(10**8)
        debt_usd = Decimal(data[1]) / Decimal(10**8)
        hf_raw = Decimal(data[5]) / WAD

        # emode changes liquidation thresholds — need to account for this
        # in the liq_price estimation below. TODO: pull emode category config
        return AccountRisk(
            address=user,
            collateral_usd=collateral_usd,
            debt_usd=debt_usd,
            health_factor=hf_raw,
            emode_id=emode,
            protocol="aave_v3",
        )

    def estimate_liq_price(self, acct: AccountRisk, liq_threshold: Decimal) -> Decimal:
        """
        Reverse-engineer the price at which HF hits 1.0.
        Assumes single collateral asset — multi-asset accounts need separate handling.
        Close enough for whale tracking purposes.
        """
        if acct.debt_usd == 0:
            return Decimal("0")
        # HF = (col * price * lt) / debt = 1  =>  price = debt / (col_qty * lt)
        # but we only have USD values, so: liq_price = debt / (col_usd/current_price * lt)
        # simplified: liq_price ≈ current_col_price * (debt / (col_usd * lt))
        return acct.debt_usd / (acct.collateral_usd * liq_threshold)


class MorphoRiskCalculator:
    """Morpho Blue — market-scoped positions, simpler than Aave."""

    def __init__(self, w3: AsyncWeb3, hub_addr: str):
        self.hub = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(hub_addr),
            abi=MORPHO_HUB_ABI,
        )

    async def fetch_position(self, market_id: str, user: str) -> dict:
        mid = bytes.fromhex(market_id.lstrip("0x"))
        addr = AsyncWeb3.to_checksum_address(user)
        pos = await self.hub.functions.position(mid, addr).call()
        return {
            "supply_shares": pos[0],
            "borrow_shares": pos[1],
            "collateral": pos[2],
        }


class OracleLagEstimator:
    """
    Chainlink heartbeat vs actual price feed timing.
    The lag between spot move and on-chain oracle update is the alpha window.
    """

    CHAINLINK_HEARTBEATS = {
        "ETH/USD": 3600,   # 1hr max, but ~20min in practice
        "BTC/USD": 3600,
        "USDC/USD": 86400,
    }

    def __init__(self, w3: AsyncWeb3):
        self.w3 = w3
        self._last_updates: dict[str, int] = {}

    async def get_lag_seconds(self, pair: str, feed_addr: str) -> float:
        """Returns seconds since last oracle update. High lag = bigger alpha window."""
        # minimal ABI — just latestRoundData
        abi = [{"name": "latestRoundData", "type": "function", "inputs": [],
                "outputs": [
                    {"name": "roundId", "type": "uint80"},
                    {"name": "answer", "type": "int256"},
                    {"name": "startedAt", "type": "uint256"},
                    {"name": "updatedAt", "type": "uint256"},
                    {"name": "answeredInRound", "type": "uint80"},
                ]}]
        feed = self.w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(feed_addr), abi=abi
        )
        data = await feed.functions.latestRoundData().call()
        updated_at = data[3]
        lag = time.time() - updated_at
        self._last_updates[pair] = updated_at
        return lag


class RiskEngine:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.rdb: aioredis.Redis = None
        self.zmq_ctx = zmq.asyncio.Context()
        self.pub: zmq.asyncio.Socket = None
        self.aave_calc: AaveRiskCalculator = None

    async def setup(self):
        self.rdb = aioredis.from_url(
            f"redis://{self.cfg['redis']['addr']}",
            decode_responses=True,
        )
        self.pub = self.zmq_ctx.socket(zmq.PUB)
        self.pub.bind(self.cfg["zmq"]["alert_pub"])

        w3 = AsyncWeb3(WebSocketProvider(self.cfg["rpc"]["ws_url"]))
        self.aave_calc = AaveRiskCalculator(
            w3,
            self.cfg["protocols"]["aave_v3"]["pool"],
            self.cfg["protocols"]["aave_v3"]["oracle"],
        )

    async def run(self):
        await self.setup()
        log.info("risk engine running")
        while True:
            await self._process_dirty_batch()
            await asyncio.sleep(0.5)  # 500ms tick — fast enough, not stupid

    async def _process_dirty_batch(self):
        # pop up to 50 accounts per tick — don't try to drain the whole set at once
        accounts = await self.rdb.spop("dirty:accounts", 50)
        if not accounts:
            return

        tasks = [self._evaluate_account(addr) for addr in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                log.warning("account eval failed: %s", r)

    async def _evaluate_account(self, address: str):
        try:
            acct = await self.aave_calc.fetch_account(address)
        except Exception as e:
            log.debug("fetch failed for %s: %s", address, e)
            # re-queue so we don't silently drop it
            await self.rdb.sadd("dirty:accounts", address)
            return

        # cache the risk snapshot
        await self._cache_account_risk(acct)

        if acct.is_at_risk:
            await self._publish_risk_event(acct)
            log.info("at-risk account %s HF=%.4f", address[:10], acct.health_factor)

    async def _cache_account_risk(self, acct: AccountRisk):
        key = f"risk:{acct.protocol}:{acct.address}"
        await self.rdb.hset(key, mapping={
            "collateral_usd": str(acct.collateral_usd),
            "debt_usd":       str(acct.debt_usd),
            "health_factor":  str(acct.health_factor),
            "emode_id":       acct.emode_id,
            "updated_at":     acct.updated_at,
        })
        await self.rdb.expire(key, 300)  # 5min — will get refreshed anyway

        if acct.is_at_risk:
            # also add to sorted set keyed by HF for fast range queries
            await self.rdb.zadd(
                "at_risk:accounts",
                {acct.address: float(acct.health_factor)},
            )

    async def _publish_risk_event(self, acct: AccountRisk):
        payload = {
            "type":           "RISK_UPDATE",
            "address":        acct.address,
            "protocol":       acct.protocol,
            "health_factor":  str(acct.health_factor),
            "collateral_usd": str(acct.collateral_usd),
            "debt_usd":       str(acct.debt_usd),
            "critical":       acct.is_critical,
            "ts":             acct.updated_at,
        }
        await self.pub.send_multipart([
            b"risk",
            json.dumps(payload).encode(),
        ])


async def main():
    import yaml
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with open("config/config.yaml") as f:
        cfg = yaml.safe_load(f)

    engine = RiskEngine(cfg)
    await engine.run()


if __name__ == "__main__":
    asyncio.run(main())
