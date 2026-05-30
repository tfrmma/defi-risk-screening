"""
arb_engine.py — yield arbitrage screener (cash & carry, funding vs borrow)
Scans for spread between on-chain borrow rates and perpetual funding rates.
Filters on CVD to avoid entering delta-neutral carries into a squeeze.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import aiohttp
import redis.asyncio as aioredis
import zmq.asyncio
from web3 import AsyncWeb3, WebSocketProvider

log = logging.getLogger(__name__)

# annualization factor — everything normalized to APY
SECONDS_PER_YEAR = 365 * 24 * 3600

# minimum net spread to bother alerting on
MIN_NET_APY = Decimal("0.04")  # 4%

# CVD threshold: if spot cumulative delta exceeds this, we flag squeeze risk
# crude but effective — gets you out before the carry explodes in your face
CVD_SQUEEZE_THRESHOLD = 0.65


@dataclass
class BorrowRate:
    protocol: str
    asset:    str
    apy:      Decimal
    utilization: Decimal
    supply_apy:  Decimal
    timestamp:   float


@dataclass
class FundingRate:
    venue:     str
    symbol:    str
    rate_8h:   Decimal     # raw 8h rate
    apy:       Decimal     # annualized: rate_8h * 3 * 365
    oi_usd:    Decimal
    timestamp: float


@dataclass
class CarryOpportunity:
    long_asset:    str        # borrow this on-chain
    short_venue:   str        # short perpetual here
    borrow_apy:    Decimal
    funding_apy:   Decimal
    net_apy:       Decimal
    oi_usd:        Decimal
    squeeze_risk:  bool       # CVD says someone's loading up on spot


AAVE_POOL_ABI = [
    {
        "name": "getReserveData",
        "type": "function",
        "inputs": [{"name": "asset", "type": "address"}],
        "outputs": [
            # trimmed — only pulling what we need
            {"name": "currentLiquidityRate", "type": "uint128"},
            {"name": "currentVariableBorrowRate", "type": "uint128"},
        ],
    }
]

# mainnet token addresses
ASSETS = {
    "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
}


class AaveBorrowRates:
    def __init__(self, w3: AsyncWeb3, pool_addr: str):
        self.pool = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(pool_addr),
            abi=AAVE_POOL_ABI,
        )

    async def fetch_all(self) -> list[BorrowRate]:
        tasks = [self._fetch_asset(sym, addr) for sym, addr in ASSETS.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, BorrowRate)]

    async def _fetch_asset(self, symbol: str, asset_addr: str) -> BorrowRate:
        data = await self.pool.functions.getReserveData(
            AsyncWeb3.to_checksum_address(asset_addr)
        ).call()

        ray = Decimal(10**27)
        # Aave rates are in RAY (27 decimals), already annualized
        supply_apy  = Decimal(data[0]) / ray
        borrow_apy  = Decimal(data[1]) / ray

        # derive utilization from the kink model — not exposed directly
        # this is an approximation; exact calc requires reserveData struct
        utilization = borrow_apy / (supply_apy + borrow_apy) if supply_apy > 0 else Decimal("0")

        return BorrowRate(
            protocol="aave_v3",
            asset=symbol,
            apy=borrow_apy,
            utilization=utilization,
            supply_apy=supply_apy,
            timestamp=time.time(),
        )


class FundingRateAggregator:
    """Pulls funding rates from Binance, Bybit, Hyperliquid."""

    async def fetch_all(self) -> list[FundingRate]:
        results = await asyncio.gather(
            self._binance_funding(),
            self._bybit_funding(),
            self._hyperliquid_funding(),
            return_exceptions=True,
        )
        rates = []
        for r in results:
            if isinstance(r, list):
                rates.extend(r)
            elif isinstance(r, Exception):
                log.warning("funding fetch error: %s", r)
        return rates

    async def _binance_funding(self) -> list[FundingRate]:
        url = "https://fapi.binance.com/fapi/v1/premiumIndex"
        symbols = ["ETHUSDT", "BTCUSDT", "SOLUSDT"]
        rates = []

        async with aiohttp.ClientSession() as sess:
            for sym in symbols:
                async with sess.get(url, params={"symbol": sym}) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    rate_8h = Decimal(str(data.get("lastFundingRate", "0")))
                    rates.append(FundingRate(
                        venue="binance",
                        symbol=sym,
                        rate_8h=rate_8h,
                        apy=rate_8h * 3 * 365,   # 3 settlements/day
                        oi_usd=Decimal("0"),      # TODO: pull OI separately
                        timestamp=time.time(),
                    ))
        return rates

    async def _bybit_funding(self) -> list[FundingRate]:
        url = "https://api.bybit.com/v5/market/tickers"
        symbols = ["ETHUSDT", "BTCUSDT"]
        rates = []

        async with aiohttp.ClientSession() as sess:
            for sym in symbols:
                async with sess.get(url, params={"category": "linear", "symbol": sym}) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    items = data.get("result", {}).get("list", [])
                    if not items:
                        continue
                    item = items[0]
                    rate_8h = Decimal(str(item.get("fundingRate", "0")))
                    oi = Decimal(str(item.get("openInterestValue", "0")))
                    rates.append(FundingRate(
                        venue="bybit",
                        symbol=sym,
                        rate_8h=rate_8h,
                        apy=rate_8h * 3 * 365,
                        oi_usd=oi,
                        timestamp=time.time(),
                    ))
        return rates

    async def _hyperliquid_funding(self) -> list[FundingRate]:
        # Hyperliquid has non-standard API — this is their meta endpoint
        url = "https://api.hyperliquid.xyz/info"
        rates = []

        async with aiohttp.ClientSession() as sess:
            async with sess.post(url, json={"type": "metaAndAssetCtxs"}) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()

        meta_assets = data[0].get("universe", [])
        asset_ctxs  = data[1]

        target_symbols = {"ETH", "BTC", "SOL"}
        for i, asset in enumerate(meta_assets):
            if asset["name"] not in target_symbols:
                continue
            ctx = asset_ctxs[i]
            rate_8h = Decimal(str(ctx.get("funding", "0")))
            oi = Decimal(str(ctx.get("openInterest", "0"))) * Decimal(str(ctx.get("markPx", "0")))
            rates.append(FundingRate(
                venue="hyperliquid",
                symbol=asset["name"] + "USDT",
                rate_8h=rate_8h,
                apy=rate_8h * 3 * 365,
                oi_usd=oi,
                timestamp=time.time(),
            ))
        return rates


class CVDMonitor:
    """
    Tracks cumulative spot delta (CVD) from trade tape.
    High positive CVD + stable price = absorption (institutional buy).
    In that case, don't lean short even if there's a liq cluster below.
    """

    def __init__(self, rdb: aioredis.Redis):
        self.rdb = rdb

    async def get_cvd_zscore(self, symbol: str, window: int = 300) -> float:
        """
        Returns Z-score of CVD over the last `window` seconds.
        Stored in Redis by the CEX feed handler.
        Positive Z > CVD_SQUEEZE_THRESHOLD = absorbing, don't short.
        """
        key = f"cvd:{symbol}:zscore"
        val = await self.rdb.get(key)
        if val is None:
            return 0.0
        return float(val)

    async def is_squeeze_risk(self, symbol: str) -> bool:
        z = await self.get_cvd_zscore(symbol)
        return z > CVD_SQUEEZE_THRESHOLD


class ArbScreener:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.rdb: aioredis.Redis = None
        self.zmq_ctx = zmq.asyncio.Context()
        self.pub: zmq.asyncio.Socket = None
        self.borrow_fetcher: AaveBorrowRates = None
        self.funding_fetcher = FundingRateAggregator()
        self.cvd_monitor: CVDMonitor = None

    async def setup(self):
        self.rdb = aioredis.from_url(f"redis://{self.cfg['redis']['addr']}", decode_responses=True)
        self.pub = self.zmq_ctx.socket(zmq.PUB)
        self.pub.bind(self.cfg["zmq"]["arb_pub"])

        w3 = AsyncWeb3(WebSocketProvider(self.cfg["rpc"]["ws_url"]))
        self.borrow_fetcher = AaveBorrowRates(w3, self.cfg["protocols"]["aave_v3"]["pool"])
        self.cvd_monitor = CVDMonitor(self.rdb)

    async def run(self):
        await self.setup()
        log.info("arb screener running")

        while True:
            await self._scan()
            await asyncio.sleep(30)  # 30s is fine — rates don't move faster than this

    async def _scan(self):
        borrow_rates, funding_rates = await asyncio.gather(
            self.borrow_fetcher.fetch_all(),
            self.funding_fetcher.fetch_all(),
        )

        opps = self._find_carries(borrow_rates, funding_rates)
        for opp in opps:
            await self._evaluate_and_publish(opp)

    def _find_carries(
        self,
        borrow_rates: list[BorrowRate],
        funding_rates: list[FundingRate],
    ) -> list[CarryOpportunity]:
        """
        Match borrow rates to funding rates by underlying asset.
        Net spread = funding_apy - borrow_apy - execution_costs (est 0.5%).
        """
        opps = []
        borrow_map = {r.asset: r for r in borrow_rates}

        for fr in funding_rates:
            base = fr.symbol.replace("USDT", "").replace("PERP", "")

            # map perp symbols to on-chain assets
            asset_map = {"ETH": "WETH", "BTC": "WBTC", "USDC": "USDC"}
            asset = asset_map.get(base)
            if asset is None or asset not in borrow_map:
                continue

            br = borrow_map[asset]
            net_apy = fr.apy - br.apy - Decimal("0.005")  # ~50bps execution cost

            if net_apy < MIN_NET_APY:
                continue

            opps.append(CarryOpportunity(
                long_asset=asset,
                short_venue=fr.venue,
                borrow_apy=br.apy,
                funding_apy=fr.apy,
                net_apy=net_apy,
                oi_usd=fr.oi_usd,
                squeeze_risk=False,  # filled in below
            ))

        return opps

    async def _evaluate_and_publish(self, opp: CarryOpportunity):
        # check CVD before publishing — high positive delta = squeeze risk = skip
        symbol_map = {"WETH": "ETH", "WBTC": "BTC", "USDC": "USDC"}
        base = symbol_map.get(opp.long_asset, opp.long_asset)
        squeeze = await self.cvd_monitor.is_squeeze_risk(f"{base}USDT")

        if squeeze:
            log.info(
                "skipping carry %s/%s net_apy=%.1f%% — CVD shows squeeze risk",
                opp.long_asset, opp.short_venue, float(opp.net_apy * 100),
            )
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
            "ts":          time.time(),
        }

        log.info(
            "carry opp: %s/%s net_apy=%.1f%%",
            opp.long_asset, opp.short_venue, float(opp.net_apy * 100),
        )
        await self.pub.send_multipart([b"arb", json.dumps(payload).encode()])

        # cache latest opportunities for frontend polling
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

    screener = ArbScreener(cfg)
    await screener.run()


if __name__ == "__main__":
    asyncio.run(main())
