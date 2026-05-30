"""
arb_engine.py — yield arbitrage screener

Funding rates come via WebSocket now, not REST. The old REST approach had
~500ms staleness and rate limit exposure. Not acceptable for carry sizing.

CVD filter is also smarter: high CVD only flags squeeze risk if price is
actually moving up. CVD spike + flat price = absorption, which is a
distribution signal — exactly when you want to be short.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

import aiohttp
import redis.asyncio as aioredis
import websockets
import zmq.asyncio
from web3 import AsyncWeb3, WebSocketProvider

log = logging.getLogger(__name__)

MIN_NET_APY    = Decimal("0.04")
EXEC_COST_BPS  = Decimal("0.005")   # ~50bps round-trip

# CVD window for displacement ratio calculation
CVD_WINDOW_SECS = 300

# if CVD Z-score is above this BUT price hasn't moved proportionally → absorption
CVD_HIGH_THRESHOLD   = 0.65
PRICE_MOVE_MIN_RATIO = 0.30   # price should move at least 30% as much as CVD implies


@dataclass
class FundingRate:
    venue:     str
    symbol:    str
    rate_8h:   Decimal
    apy:       Decimal     # rate_8h * 3 * 365
    oi_usd:    Decimal
    timestamp: float


@dataclass
class BorrowRate:
    protocol: str
    asset:    str
    apy:      Decimal
    timestamp: float


@dataclass
class CarryOpportunity:
    long_asset:   str
    short_venue:  str
    borrow_apy:   Decimal
    funding_apy:  Decimal
    net_apy:      Decimal
    oi_usd:       Decimal
    squeeze_risk: bool
    absorption:   bool      # CVD high but price not moving = distribution in progress


AAVE_RESERVE_ABI = [{"name": "getReserveData", "type": "function",
    "inputs": [{"name": "asset", "type": "address"}],
    "outputs": [
        {"name": "currentLiquidityRate",    "type": "uint128"},
        {"name": "currentVariableBorrowRate","type": "uint128"},
    ]}]

ASSETS = {
    "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
}

PERP_TO_ASSET = {"ETH": "WETH", "BTC": "WBTC", "USDC": "USDC", "SOL": "SOL"}


class AaveBorrowRates:
    """Still using RPC here — borrow rates move on block cadence, not tick cadence.
    30s polling is fine. Doesn't belong on a WebSocket."""

    def __init__(self, w3: AsyncWeb3, pool_addr: str):
        self.pool = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(pool_addr),
            abi=AAVE_RESERVE_ABI,
        )

    async def fetch_all(self) -> list[BorrowRate]:
        tasks = [self._fetch(sym, addr) for sym, addr in ASSETS.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, BorrowRate)]

    async def _fetch(self, symbol: str, asset_addr: str) -> BorrowRate:
        data = await self.pool.functions.getReserveData(
            AsyncWeb3.to_checksum_address(asset_addr)
        ).call()
        ray = Decimal(10**27)
        return BorrowRate(
            protocol="aave_v3",
            asset=symbol,
            apy=Decimal(data[1]) / ray,
            timestamp=time.time(),
        )


class BinanceFundingWS:
    """
    Binance markPrice stream — broadcasts funding rate + mark price in real time.
    Much better than polling premiumIndex REST.
    """

    WS_URL = "wss://fstream.binance.com/stream?streams=ethusdt@markPrice/btcusdt@markPrice/solusdt@markPrice"

    def __init__(self, store: dict[str, FundingRate]):
        self._store = store  # shared with ArbScreener

    async def run(self):
        while True:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20) as ws:
                    log.info("binance funding WS connected")
                    async for raw in ws:
                        self._handle(raw)
            except Exception as e:
                log.warning("binance funding WS error: %s — reconnecting", e)
                await asyncio.sleep(2)

    def _handle(self, raw: str):
        try:
            outer = json.loads(raw)
            data  = outer.get("data", outer)
            sym   = data.get("s", "")
            rate  = Decimal(str(data.get("r", "0")))  # "r" = funding rate
            self._store[f"binance:{sym}"] = FundingRate(
                venue="binance",
                symbol=sym,
                rate_8h=rate,
                apy=rate * 3 * 365,
                oi_usd=Decimal("0"),  # not in markPrice stream; grab from REST on startup
                timestamp=time.time(),
            )
        except Exception:
            pass  # malformed frame — ignore and move on


class BybitFundingWS:
    """
    Bybit tickers stream — includes fundingRate and openInterestValue.
    """

    WS_URL    = "wss://stream.bybit.com/v5/public/linear"
    SYMBOLS   = ["ETHUSDT", "BTCUSDT"]

    def __init__(self, store: dict[str, FundingRate]):
        self._store = store

    async def run(self):
        while True:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20) as ws:
                    sub = {"op": "subscribe", "args": [f"tickers.{s}" for s in self.SYMBOLS]}
                    await ws.send(json.dumps(sub))
                    log.info("bybit funding WS connected")
                    async for raw in ws:
                        self._handle(raw)
            except Exception as e:
                log.warning("bybit funding WS error: %s — reconnecting", e)
                await asyncio.sleep(2)

    def _handle(self, raw: str):
        try:
            msg  = json.loads(raw)
            data = msg.get("data", {})
            sym  = data.get("symbol", "")
            if not sym:
                return
            rate = Decimal(str(data.get("fundingRate", "0")))
            oi   = Decimal(str(data.get("openInterestValue", "0")))
            self._store[f"bybit:{sym}"] = FundingRate(
                venue="bybit",
                symbol=sym,
                rate_8h=rate,
                apy=rate * 3 * 365,
                oi_usd=oi,
                timestamp=time.time(),
            )
        except Exception:
            pass


class HyperliquidFundingPoller:
    """
    Hyperliquid doesn't have a real-time WS for funding yet (as of mid-2025).
    Polling their /info endpoint every 30s is still better than Binance REST
    because HL settlements are more frequent.
    TODO: check if they've shipped a WS funding channel.
    """

    URL = "https://api.hyperliquid.xyz/info"
    TARGET = {"ETH", "BTC", "SOL"}

    def __init__(self, store: dict[str, FundingRate]):
        self._store = store

    async def run(self):
        while True:
            try:
                await self._poll()
            except Exception as e:
                log.warning("hyperliquid poll error: %s", e)
            await asyncio.sleep(30)

    async def _poll(self):
        async with aiohttp.ClientSession() as sess:
            async with sess.post(self.URL, json={"type": "metaAndAssetCtxs"}) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()

        for asset, ctx in zip(data[0].get("universe", []), data[1]):
            if asset["name"] not in self.TARGET:
                continue
            rate = Decimal(str(ctx.get("funding", "0")))
            oi   = Decimal(str(ctx.get("openInterest", "0"))) * Decimal(str(ctx.get("markPx", "1")))
            sym  = asset["name"] + "USDT"
            self._store[f"hyperliquid:{sym}"] = FundingRate(
                venue="hyperliquid",
                symbol=sym,
                rate_8h=rate,
                apy=rate * 3 * 365,
                oi_usd=oi,
                timestamp=time.time(),
            )


class CVDDisplacementMonitor:
    """
    Smarter CVD filter: compares cumulative delta against actual price displacement.

    High CVD + proportional price move = real demand → squeeze risk, skip carry.
    High CVD + flat price = absorption/distribution → good carry entry, proceed.

    Both CVD and price samples are written into Redis by the order book feed handler.
    We read a rolling window and compute the ratio here.
    """

    def __init__(self, rdb: aioredis.Redis, window_secs: int = CVD_WINDOW_SECS):
        self.rdb    = rdb
        self.window = window_secs

    async def classify(self, symbol: str) -> tuple[bool, bool]:
        """
        Returns (squeeze_risk, absorption).
        squeeze_risk: true price move matching CVD — real buying pressure
        absorption:   CVD spike but price not moving — smart money distributing
        """
        cvd_z    = await self._get_float(f"cvd:{symbol}:zscore")
        price_z  = await self._get_float(f"price:{symbol}:zscore_5m")

        if cvd_z is None or abs(cvd_z) < CVD_HIGH_THRESHOLD:
            return False, False  # nothing interesting

        # how much of the CVD move is showing up in price?
        displacement_ratio = abs(price_z) / abs(cvd_z) if cvd_z != 0 else 0

        if displacement_ratio >= PRICE_MOVE_MIN_RATIO:
            # price moving with CVD — real directional pressure
            squeeze = cvd_z > 0   # positive CVD = buying = squeeze risk for shorts
            return squeeze, False
        else:
            # CVD high but price not following — absorption/distribution
            # don't flag as squeeze; flag as absorption so desk knows context
            return False, True

    async def _get_float(self, key: str) -> Optional[float]:
        val = await self.rdb.get(key)
        return float(val) if val is not None else None


class ArbScreener:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.rdb: aioredis.Redis     = None
        self.zmq_ctx = zmq.asyncio.Context()
        self.pub: zmq.asyncio.Socket = None
        self.borrow_rates: AaveBorrowRates = None
        self.cvd: CVDDisplacementMonitor   = None

        # shared in-memory funding rate store — WS handlers write, screener reads
        self._funding_store: dict[str, FundingRate] = {}

    async def setup(self):
        self.rdb = aioredis.from_url(f"redis://{self.cfg['redis']['addr']}", decode_responses=True)
        self.pub = self.zmq_ctx.socket(zmq.PUB)
        self.pub.bind(self.cfg["zmq"]["arb_pub"])

        w3 = AsyncWeb3(WebSocketProvider(self.cfg["rpc"]["ws_url"]))
        self.borrow_rates = AaveBorrowRates(w3, self.cfg["protocols"]["aave_v3"]["pool"])
        self.cvd = CVDDisplacementMonitor(self.rdb)

    async def run(self):
        await self.setup()

        # launch all feed tasks concurrently
        asyncio.create_task(BinanceFundingWS(self._funding_store).run())
        asyncio.create_task(BybitFundingWS(self._funding_store).run())
        asyncio.create_task(HyperliquidFundingPoller(self._funding_store).run())

        # give WS feeds a moment to populate before first scan
        await asyncio.sleep(3)
        log.info("arb screener running — funding via WebSocket")

        while True:
            await self._scan()
            await asyncio.sleep(5)  # tighter now — funding data is live, not 30s stale

    async def _scan(self):
        borrow = await self.borrow_rates.fetch_all()
        borrow_map = {r.asset: r for r in borrow}
        opps = self._find_carries(borrow_map)
        for opp in opps:
            await self._evaluate_and_publish(opp)

    def _find_carries(self, borrow_map: dict[str, BorrowRate]) -> list[CarryOpportunity]:
        opps = []
        for key, fr in list(self._funding_store.items()):
            if time.time() - fr.timestamp > 120:
                continue  # stale — WS probably dropped

            base  = fr.symbol.replace("USDT", "").replace("PERP", "")
            asset = PERP_TO_ASSET.get(base)
            if asset is None or asset not in borrow_map:
                continue

            br      = borrow_map[asset]
            net_apy = fr.apy - br.apy - EXEC_COST_BPS

            if net_apy < MIN_NET_APY:
                continue

            opps.append(CarryOpportunity(
                long_asset=asset,
                short_venue=fr.venue,
                borrow_apy=br.apy,
                funding_apy=fr.apy,
                net_apy=net_apy,
                oi_usd=fr.oi_usd,
                squeeze_risk=False,
                absorption=False,
            ))
        return opps

    async def _evaluate_and_publish(self, opp: CarryOpportunity):
        asset_to_sym = {"WETH": "ETHUSDT", "WBTC": "BTCUSDT"}
        sym = asset_to_sym.get(opp.long_asset, opp.long_asset + "USDT")

        squeeze, absorption = await self.cvd.classify(sym)
        opp.squeeze_risk = squeeze
        opp.absorption   = absorption

        if squeeze:
            log.info("skipping carry %s/%s — CVD-driven price move, squeeze risk",
                     opp.long_asset, opp.short_venue)
            return

        payload = {
            "type":        "CARRY_OPPORTUNITY",
            "long_asset":  opp.long_asset,
            "short_venue": opp.short_venue,
            "borrow_apy":  str(opp.borrow_apy),
            "funding_apy": str(opp.funding_apy),
            "net_apy":     str(opp.net_apy),
            "oi_usd":      str(opp.oi_usd),
            "squeeze_risk": squeeze,
            "absorption":   absorption,
            "ts":          time.time(),
        }

        level = "INFO"
        if absorption:
            # good context for the desk: CVD is hot but price isn't moving
            # means smart money is selling into retail excitement
            level = "ABSORPTION"
            log.info("carry %s/%s net=%.1f%% — ABSORPTION context (distribution likely)",
                     opp.long_asset, opp.short_venue, float(opp.net_apy * 100))
        else:
            log.info("carry %s/%s net=%.1f%%",
                     opp.long_asset, opp.short_venue, float(opp.net_apy * 100))

        await self.pub.send_multipart([b"arb", json.dumps(payload).encode()])
        await self.rdb.setex(
            f"arb:latest:{opp.long_asset}:{opp.short_venue}",
            300,
            json.dumps(payload),
        )


async def main():
    import yaml
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with open("config/config.yaml") as f:
        cfg = yaml.safe_load(f)

    await ArbScreener(cfg).run()


if __name__ == "__main__":
    asyncio.run(main())
