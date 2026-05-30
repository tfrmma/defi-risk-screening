"""
whale_matrix.py — builds the liquidation cluster map used by the cascade simulator.
Output: sorted list of (price_level, total_usd_at_risk) tuples.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterator

import redis.asyncio as aioredis
import numpy as np

log = logging.getLogger(__name__)

# minimum debt to qualify as a whale — below this it's noise
WHALE_THRESHOLD_USD = Decimal("500_000")

# price grid resolution for the heatmap (0.5% buckets)
BUCKET_SIZE = Decimal("0.005")


@dataclass
class LiqCluster:
    price: Decimal          # USD price that triggers this cluster
    total_usd: Decimal      # total collateral at risk
    account_count: int
    protocol: str


def bucket_price(price: Decimal, bucket_size: Decimal = BUCKET_SIZE) -> Decimal:
    """Snap a price to the nearest bucket. Keeps the matrix manageable."""
    return (price / bucket_size).to_integral_value() * bucket_size


class WhaleMatrix:
    """
    Maintains a live map of liquidation pressure by price level.
    Risk engine feeds us account snapshots; we build the cluster view.
    """

    def __init__(self, rdb: aioredis.Redis):
        self.rdb = rdb
        # nested dict: protocol -> price_bucket -> LiqCluster
        self._clusters: dict[str, dict[Decimal, LiqCluster]] = defaultdict(dict)
        self._last_rebuild = 0.0

    async def rebuild(self, liq_threshold: Decimal = Decimal("0.825")):
        """
        Full rebuild from Redis state. Run this on startup and every ~60s.
        Incremental updates happen via update_account().
        """
        t0 = time.time()
        self._clusters.clear()

        # get all at-risk accounts sorted by HF ascending
        raw = await self.rdb.zrangebyscore("at_risk:accounts", 0, 1.2, withscores=True)

        accounts_processed = 0
        for addr, hf in raw:
            snap = await self._load_snapshot(addr)
            if snap is None:
                continue
            if snap["debt_usd"] < WHALE_THRESHOLD_USD:
                continue
            self._insert_into_cluster(addr, snap, liq_threshold)
            accounts_processed += 1

        self._last_rebuild = time.time()
        log.info(
            "matrix rebuilt: %d whales across %d protocols in %.0fms",
            accounts_processed,
            len(self._clusters),
            (time.time() - t0) * 1000,
        )

    async def _load_snapshot(self, address: str) -> dict | None:
        # try all protocols — a bit ugly but avoids storing protocol tag separately
        for proto in ("aave_v3", "spark", "morpho_blue"):
            key = f"risk:{proto}:{address}"
            data = await self.rdb.hgetall(key)
            if data:
                return {
                    "protocol":       proto,
                    "debt_usd":       Decimal(data["debt_usd"]),
                    "collateral_usd": Decimal(data["collateral_usd"]),
                    "health_factor":  Decimal(data["health_factor"]),
                }
        return None

    def _insert_into_cluster(self, addr: str, snap: dict, liq_threshold: Decimal):
        debt = snap["debt_usd"]
        col  = snap["collateral_usd"]
        proto = snap["protocol"]

        if debt == 0 or col == 0:
            return

        # liq price = current_col_price * (debt / (col * lt))
        # we don't have current price here, so express as a ratio
        # actual price = this_ratio * spot_price (applied downstream)
        liq_ratio = debt / (col * liq_threshold)

        # bucket it
        bucket = bucket_price(liq_ratio)

        existing = self._clusters[proto].get(bucket)
        if existing:
            self._clusters[proto][bucket] = LiqCluster(
                price=bucket,
                total_usd=existing.total_usd + col,
                account_count=existing.account_count + 1,
                protocol=proto,
            )
        else:
            self._clusters[proto][bucket] = LiqCluster(
                price=bucket,
                total_usd=col,
                account_count=1,
                protocol=proto,
            )

    def get_clusters_near_price(
        self,
        current_price: Decimal,
        window_pct: Decimal = Decimal("0.05"),
    ) -> list[LiqCluster]:
        """
        Returns clusters within ±5% of current price, sorted by total_usd desc.
        This is what the frontend heatmap and cascade sim consume.
        """
        low  = Decimal("1") - window_pct
        high = Decimal("1") + window_pct

        results = []
        for proto_clusters in self._clusters.values():
            for ratio, cluster in proto_clusters.items():
                price_level = ratio * current_price
                if low * current_price <= price_level <= high * current_price:
                    # clone with absolute price
                    results.append(LiqCluster(
                        price=price_level,
                        total_usd=cluster.total_usd,
                        account_count=cluster.account_count,
                        protocol=cluster.protocol,
                    ))

        results.sort(key=lambda c: c.total_usd, reverse=True)
        return results

    def total_at_risk_usd(self) -> Decimal:
        total = Decimal("0")
        for proto_clusters in self._clusters.values():
            for c in proto_clusters.values():
                total += c.total_usd
        return total

    def to_numpy(self, current_price: Decimal, n_buckets: int = 200) -> np.ndarray:
        """
        Flatten clusters into a (n_buckets, 3) array [price, total_usd, count].
        Used by the cascade sim for vectorized impact calculations.
        """
        clusters = self.get_clusters_near_price(current_price, window_pct=Decimal("0.10"))
        if not clusters:
            return np.zeros((0, 3))

        arr = np.array(
            [(float(c.price), float(c.total_usd), c.account_count) for c in clusters],
            dtype=np.float64,
        )
        return arr

    def iter_sorted(self) -> Iterator[LiqCluster]:
        """All clusters sorted by price asc — used for cascade simulation."""
        all_clusters: list[LiqCluster] = []
        for proto_clusters in self._clusters.values():
            all_clusters.extend(proto_clusters.values())
        return iter(sorted(all_clusters, key=lambda c: c.price))
