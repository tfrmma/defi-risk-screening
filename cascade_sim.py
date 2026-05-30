"""
cascade_sim.py — simulates liquidation cascades and estimates structural volatility risk.
Consumes the whale matrix + order book depth to model second-order effects.

This is the most important module. A lot of people build liquidation heatmaps
and stop there. The cascade logic is what actually matters for sizing the trade.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import numpy as np
import zmq.asyncio

from whale_matrix import WhaleMatrix, LiqCluster

log = logging.getLogger(__name__)

# MEV bots don't dump 100% at once — they drip to minimize impact
# empirically closer to 0.65-0.80 depending on size; 0.75 is a reasonable center
MEV_FILL_EFFICIENCY = 0.75

# slippage buffer above our model estimate — real books have hidden size
SLIPPAGE_HAIRCUT = 1.15


@dataclass
class CascadeStep:
    trigger_price: float
    liq_usd:       float     # USD value being liquidated at this step
    price_impact:  float     # estimated % price move from selling this size
    new_price:     float     # price after impact
    accounts:      int


@dataclass
class CascadeResult:
    triggered:    bool
    steps:        list[CascadeStep]
    total_liq_usd: float
    max_drawdown:  float      # % from initial price
    final_price:   float
    alert_level:   str        # "none" | "warning" | "critical"

    @property
    def is_cascade(self) -> bool:
        return len(self.steps) > 1


class MarketDepthProxy:
    """
    Thin wrapper around the Go aggregator's HTTP endpoint.
    Returns estimated slippage for a given sell size.
    """

    def __init__(self, base_url: str = "http://localhost:8080"):
        self.base_url = base_url

    async def get_slippage(self, notional_usd: float) -> float:
        """
        Real implementation should call the Go aggregator.
        For now returns a model estimate. TODO: wire this up properly.
        """
        # Rough model: 0.1% per $10M in normal conditions
        # This is a placeholder — replace with actual book query
        base_slip = (notional_usd / 10_000_000) * 0.001
        return min(base_slip * SLIPPAGE_HAIRCUT, 0.15)  # cap at 15%


class CascadeSimulator:
    """
    Simulates a price dropping through the whale matrix and tracks knock-on liquidations.
    
    The key insight: a liquidation causes selling which moves price which triggers
    more liquidations. The sim walks down the cluster list and models this propagation.
    """

    def __init__(self, matrix: WhaleMatrix, depth_proxy: MarketDepthProxy):
        self.matrix = matrix
        self.depth  = depth_proxy

    async def simulate(
        self,
        initial_price: float,
        initial_drop_pct: float,
        max_steps: int = 20,
    ) -> CascadeResult:
        """
        Run a cascade simulation starting at initial_price * (1 - initial_drop_pct).
        Walks through the cluster map and propagates impact step by step.
        """
        clusters_arr = self.matrix.to_numpy(Decimal(str(initial_price)))
        if len(clusters_arr) == 0:
            return CascadeResult(
                triggered=False, steps=[], total_liq_usd=0.0,
                max_drawdown=0.0, final_price=initial_price, alert_level="none",
            )

        # sort by price descending — we're walking down
        clusters_arr = clusters_arr[clusters_arr[:, 0].argsort()[::-1]]

        trigger_price = initial_price * (1 - initial_drop_pct)
        current_price = trigger_price
        steps: list[CascadeStep] = []
        total_liq = 0.0

        for _ in range(max_steps):
            # find clusters at or below current price
            active_mask = clusters_arr[:, 0] >= current_price
            if not active_mask.any():
                break

            # take the highest cluster at or below current price
            idx = np.where(active_mask)[0][-1]
            cluster_price, cluster_usd, cluster_count = clusters_arr[idx]

            if cluster_price > current_price:
                break

            # effective sell pressure: MEV bots don't dump everything
            effective_sell = cluster_usd * MEV_FILL_EFFICIENCY

            # price impact from selling this notional
            slippage = await self.depth.get_slippage(effective_sell)
            new_price = current_price * (1 - slippage)

            step = CascadeStep(
                trigger_price=cluster_price,
                liq_usd=cluster_usd,
                price_impact=slippage,
                new_price=new_price,
                accounts=int(cluster_count),
            )
            steps.append(step)
            total_liq += cluster_usd

            # mark this cluster as consumed
            clusters_arr = np.delete(clusters_arr, idx, axis=0)
            current_price = new_price

            if not clusters_arr.any():
                break

        max_drawdown = (initial_price - current_price) / initial_price
        alert_level  = self._classify_severity(max_drawdown, total_liq, len(steps))

        return CascadeResult(
            triggered=len(steps) > 0,
            steps=steps,
            total_liq_usd=total_liq,
            max_drawdown=max_drawdown,
            final_price=current_price,
            alert_level=alert_level,
        )

    def _classify_severity(self, drawdown: float, total_liq: float, n_steps: int) -> str:
        if drawdown > 0.08 or total_liq > 500_000_000:
            return "critical"
        if drawdown > 0.04 or total_liq > 100_000_000 or n_steps > 3:
            return "warning"
        return "none"

    async def scan_price_range(
        self,
        current_price: float,
        drop_range: tuple[float, float] = (0.01, 0.10),
        n_scenarios: int = 20,
    ) -> list[CascadeResult]:
        """
        Run N scenarios across a range of initial drops.
        Useful for building the risk surface displayed on the frontend heatmap.
        """
        drops = np.linspace(drop_range[0], drop_range[1], n_scenarios)
        tasks = [self.simulate(current_price, float(d)) for d in drops]
        return await asyncio.gather(*tasks)


class CascadePublisher:
    """Publishes cascade alerts to ZMQ. Frontend and arb engine both subscribe."""

    def __init__(self, zmq_ctx: zmq.asyncio.Context, bind_addr: str):
        self.sock = zmq_ctx.socket(zmq.PUB)
        self.sock.bind(bind_addr)

    async def publish(self, result: CascadeResult, current_price: float):
        if result.alert_level == "none":
            return

        payload = {
            "type":           "CASCADE_ALERT",
            "alert_level":    result.alert_level,
            "max_drawdown":   result.max_drawdown,
            "total_liq_usd":  result.total_liq_usd,
            "final_price":    result.final_price,
            "current_price":  current_price,
            "n_steps":        len(result.steps),
            "steps": [
                {
                    "trigger_price": s.trigger_price,
                    "liq_usd":       s.liq_usd,
                    "price_impact":  s.price_impact,
                    "new_price":     s.new_price,
                }
                for s in result.steps
            ],
        }

        log.warning(
            "cascade alert=%s drawdown=%.2f%% liq=$%.0fM",
            result.alert_level,
            result.max_drawdown * 100,
            result.total_liq_usd / 1e6,
        )

        await self.sock.send_multipart([
            b"cascade",
            json.dumps(payload).encode(),
        ])
