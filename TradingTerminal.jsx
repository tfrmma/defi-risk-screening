import { useState, useEffect, useRef, useCallback } from "react";

// --- static mock data so the UI is reviewable without a live backend ---
const MOCK_CLUSTERS = [
  { price: 3180, totalUsd: 48_200_000, protocol: "aave_v3",    accounts: 12 },
  { price: 3120, totalUsd: 127_500_000, protocol: "aave_v3",   accounts: 34 },
  { price: 3080, totalUsd: 62_000_000,  protocol: "morpho_blue", accounts: 8 },
  { price: 3040, totalUsd: 215_800_000, protocol: "aave_v3",   accounts: 61 },
  { price: 2990, totalUsd: 38_400_000,  protocol: "spark",     accounts: 6  },
  { price: 2950, totalUsd: 89_700_000,  protocol: "aave_v3",   accounts: 23 },
  { price: 2900, totalUsd: 310_200_000, protocol: "aave_v3",   accounts: 87 },
  { price: 2840, totalUsd: 55_000_000,  protocol: "morpho_blue", accounts: 14 },
];

const MOCK_ARB = [
  { long_asset: "WETH", short_venue: "hyperliquid", borrow_apy: 0.062, funding_apy: 0.194, net_apy: 0.127, oi_usd: 1_820_000_000, squeeze_risk: false },
  { long_asset: "WBTC", short_venue: "bybit",       borrow_apy: 0.048, funding_apy: 0.112, net_apy: 0.059, oi_usd: 4_210_000_000, squeeze_risk: false },
  { long_asset: "USDC", short_venue: "binance",     borrow_apy: 0.071, funding_apy: 0.085, net_apy: 0.009, oi_usd: 920_000_000,  squeeze_risk: true  },
];

const MOCK_RISK_EVENTS = [
  { ts: Date.now() - 4000,  address: "0x4a3b…d91f", hf: "1.04", protocol: "aave_v3",     critical: true  },
  { ts: Date.now() - 12000, address: "0x7e82…aa01", hf: "1.11", protocol: "morpho_blue", critical: false },
  { ts: Date.now() - 28000, address: "0x1f40…c33e", hf: "1.18", protocol: "spark",       critical: false },
  { ts: Date.now() - 45000, address: "0xb99c…778d", hf: "1.07", protocol: "aave_v3",     critical: true  },
];

const CURRENT_PRICE = 3247.80;

// --- formatting helpers ---
const fmtUsd = (v) => {
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(0)}M`;
  return `$${(v / 1e3).toFixed(0)}K`;
};
const fmtPct = (v) => `${(v * 100).toFixed(1)}%`;
const fmtAge = (ts) => {
  const s = Math.floor((Date.now() - ts) / 1000);
  if (s < 60) return `${s}s ago`;
  return `${Math.floor(s / 60)}m ago`;
};

// --- sub-components ---
function StatusBar({ price, nodeOk, redisOk }) {
  const [tick, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 1000);
    return () => clearInterval(id);
  }, []);

  return (
    <div style={{ display: "flex", alignItems: "center", gap: "24px", padding: "10px 20px",
                  borderBottom: "1px solid #1a2a1a", background: "#070d07", fontSize: "11px",
                  fontFamily: "'IBM Plex Mono', monospace", color: "#3d7a3d", letterSpacing: "0.05em" }}>
      <span style={{ color: "#00ff41", fontWeight: 700, fontSize: "13px" }}>
        ETH/USD <span style={{ color: "#e8ff8e" }}>{CURRENT_PRICE.toFixed(2)}</span>
      </span>
      <div style={{ display: "flex", gap: "6px", alignItems: "center" }}>
        <span style={{ width: 7, height: 7, borderRadius: "50%", background: nodeOk ? "#00ff41" : "#ff4444",
                       display: "inline-block", boxShadow: nodeOk ? "0 0 6px #00ff41" : "none" }} />
        <span>RETH</span>
      </div>
      <div style={{ display: "flex", gap: "6px", alignItems: "center" }}>
        <span style={{ width: 7, height: 7, borderRadius: "50%", background: redisOk ? "#00ff41" : "#ff4444",
                       display: "inline-block", boxShadow: redisOk ? "0 0 6px #00ff41" : "none" }} />
        <span>REDIS</span>
      </div>
      <span style={{ marginLeft: "auto", color: "#2a4a2a" }}>
        {new Date().toISOString().replace("T", " ").slice(0, 19)} UTC
      </span>
    </div>
  );
}

function LiqHeatmap({ clusters, currentPrice }) {
  const maxUsd = Math.max(...clusters.map(c => c.totalUsd));

  const sorted = [...clusters].sort((a, b) => b.price - a.price);
  const priceRange = { min: sorted[sorted.length - 1].price * 0.98, max: sorted[0].price * 1.02 };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "2px" }}>
      {sorted.map((c, i) => {
        const barPct = (c.totalUsd / maxUsd) * 100;
        const priceDelta = ((currentPrice - c.price) / currentPrice) * 100;
        const isCritical = c.totalUsd > 150_000_000;
        const isNearby = Math.abs(priceDelta) < 3;

        const barColor = isCritical ? "#ff4444"
                       : isNearby  ? "#ffaa00"
                       :             "#1a6b1a";

        return (
          <div key={i} style={{ display: "flex", alignItems: "center", gap: "8px",
                                padding: "3px 0", opacity: isNearby ? 1 : 0.7,
                                transition: "opacity 0.2s" }}>
            <span style={{ width: 72, textAlign: "right", fontSize: "11px",
                           color: isNearby ? "#e8ff8e" : "#4a7a4a",
                           fontFamily: "'IBM Plex Mono', monospace" }}>
              ${c.price.toLocaleString()}
            </span>
            <div style={{ flex: 1, position: "relative", height: 16 }}>
              <div style={{ position: "absolute", left: 0, top: 2, height: 12,
                            width: `${barPct}%`, background: barColor,
                            opacity: 0.85, transition: "width 0.3s ease",
                            boxShadow: isCritical ? `0 0 8px ${barColor}40` : "none" }} />
            </div>
            <span style={{ width: 60, fontSize: "11px", color: isCritical ? "#ff6666" : "#3d7a3d",
                           fontFamily: "'IBM Plex Mono', monospace" }}>
              {fmtUsd(c.totalUsd)}
            </span>
            <span style={{ width: 48, fontSize: "10px", color: "#2a4a2a",
                           fontFamily: "'IBM Plex Mono', monospace" }}>
              {c.accounts}ac
            </span>
            <span style={{ width: 36, fontSize: "10px",
                           color: priceDelta > 0 ? "#3d7a3d" : "#ff6666",
                           fontFamily: "'IBM Plex Mono', monospace" }}>
              {priceDelta > 0 ? "+" : ""}{priceDelta.toFixed(1)}%
            </span>
          </div>
        );
      })}
    </div>
  );
}

function ArbTable({ opportunities }) {
  return (
    <div>
      <div style={{ display: "grid", gridTemplateColumns: "80px 100px 80px 80px 80px 90px 60px",
                    gap: "0 12px", padding: "0 0 6px 0", borderBottom: "1px solid #1a2a1a",
                    fontSize: "9px", color: "#2a4a2a", fontFamily: "'IBM Plex Mono', monospace",
                    letterSpacing: "0.08em" }}>
        <span>ASSET</span><span>VENUE</span><span>BORROW</span>
        <span>FUNDING</span><span>NET APY</span><span>OI</span><span>RISK</span>
      </div>
      {opportunities.map((opp, i) => (
        <div key={i} style={{ display: "grid",
                              gridTemplateColumns: "80px 100px 80px 80px 80px 90px 60px",
                              gap: "0 12px", padding: "7px 0",
                              borderBottom: "1px solid #0d1a0d",
                              fontSize: "11px", fontFamily: "'IBM Plex Mono', monospace",
                              opacity: opp.squeeze_risk ? 0.45 : 1 }}>
          <span style={{ color: "#e8ff8e" }}>{opp.long_asset}</span>
          <span style={{ color: "#6aaa6a" }}>{opp.short_venue}</span>
          <span style={{ color: "#4a7a4a" }}>{fmtPct(opp.borrow_apy)}</span>
          <span style={{ color: "#6aaa6a" }}>{fmtPct(opp.funding_apy)}</span>
          <span style={{ color: opp.net_apy > 0.08 ? "#00ff41" : "#6aaa6a",
                         fontWeight: opp.net_apy > 0.08 ? 700 : 400 }}>
            {fmtPct(opp.net_apy)}
          </span>
          <span style={{ color: "#3d6a3d" }}>{fmtUsd(opp.oi_usd)}</span>
          <span style={{ color: opp.squeeze_risk ? "#ff6666" : "#2a4a2a",
                         fontSize: "10px" }}>
            {opp.squeeze_risk ? "⚠ SQZ" : "OK"}
          </span>
        </div>
      ))}
    </div>
  );
}

function RiskFeed({ events }) {
  const [items, setItems] = useState(events);
  const feedRef = useRef(null);

  // simulate live feed
  useEffect(() => {
    const id = setInterval(() => {
      const protocols = ["aave_v3", "morpho_blue", "spark"];
      const hf = (1.02 + Math.random() * 0.18).toFixed(2);
      const critical = parseFloat(hf) < 1.07;
      setItems(prev => [{
        ts: Date.now(),
        address: `0x${Math.random().toString(16).slice(2, 6)}…${Math.random().toString(16).slice(2, 6)}`,
        hf,
        protocol: protocols[Math.floor(Math.random() * 3)],
        critical,
      }, ...prev].slice(0, 40));
    }, 4500 + Math.random() * 3000);
    return () => clearInterval(id);
  }, []);

  return (
    <div ref={feedRef} style={{ overflowY: "auto", maxHeight: "100%", display: "flex",
                                flexDirection: "column", gap: "0" }}>
      {items.map((ev, i) => (
        <div key={i} style={{ display: "flex", alignItems: "center", gap: "10px",
                              padding: "5px 0", borderBottom: "1px solid #0a150a",
                              fontSize: "10px", fontFamily: "'IBM Plex Mono', monospace",
                              animation: i === 0 ? "fadeIn 0.3s ease" : "none" }}>
          <span style={{ color: "#2a4a2a", minWidth: 50 }}>{fmtAge(ev.ts)}</span>
          <span style={{ color: "#3d6a3d", minWidth: 90 }}>{ev.address}</span>
          <span style={{ minWidth: 60,
                         color: parseFloat(ev.hf) < 1.05 ? "#ff4444"
                              : parseFloat(ev.hf) < 1.10 ? "#ffaa00"
                              :                            "#6aaa6a" }}>
            HF {ev.hf}
          </span>
          <span style={{ color: "#2a4a2a", fontSize: "9px" }}>{ev.protocol}</span>
          {ev.critical && (
            <span style={{ color: "#ff4444", fontSize: "9px", marginLeft: "auto",
                           animation: "pulse 1s infinite" }}>
              ⚡ CRIT
            </span>
          )}
        </div>
      ))}
    </div>
  );
}

function SlippagePanel() {
  const buckets = [1e6, 5e6, 10e6];
  // rough model values — in prod these come from the Go aggregator
  const slippages = {
    1e6:  { binance: 0.0008, bybit: 0.0010, combined: 0.0009 },
    5e6:  { binance: 0.0041, bybit: 0.0052, combined: 0.0046 },
    10e6: { binance: 0.0098, bybit: 0.0124, combined: 0.0110 },
  };

  return (
    <div>
      <div style={{ display: "grid", gridTemplateColumns: "60px 70px 70px 70px",
                    gap: "0 8px", padding: "0 0 5px 0", borderBottom: "1px solid #1a2a1a",
                    fontSize: "9px", color: "#2a4a2a", fontFamily: "'IBM Plex Mono', monospace",
                    letterSpacing: "0.08em" }}>
        <span>SIZE</span><span>BINANCE</span><span>BYBIT</span><span>COMBINED</span>
      </div>
      {buckets.map((b, i) => {
        const s = slippages[b];
        return (
          <div key={i} style={{ display: "grid", gridTemplateColumns: "60px 70px 70px 70px",
                                gap: "0 8px", padding: "6px 0", borderBottom: "1px solid #0a150a",
                                fontSize: "11px", fontFamily: "'IBM Plex Mono', monospace" }}>
            <span style={{ color: "#e8ff8e" }}>{fmtUsd(b)}</span>
            <span style={{ color: "#4a7a4a" }}>{fmtPct(s.binance)}</span>
            <span style={{ color: "#4a7a4a" }}>{fmtPct(s.bybit)}</span>
            <span style={{ color: s.combined > 0.01 ? "#ffaa00" : "#6aaa6a" }}>
              {fmtPct(s.combined)}
            </span>
          </div>
        );
      })}
    </div>
  );
}

function OracleLagBar({ pair, lagSeconds, heartbeat }) {
  const pct = Math.min((lagSeconds / heartbeat) * 100, 100);
  const color = pct > 80 ? "#ff4444" : pct > 50 ? "#ffaa00" : "#1a6b1a";
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "10px", padding: "4px 0" }}>
      <span style={{ width: 70, fontSize: "10px", color: "#3d6a3d",
                     fontFamily: "'IBM Plex Mono', monospace" }}>
        {pair}
      </span>
      <div style={{ flex: 1, height: 6, background: "#0d1a0d", borderRadius: 2, overflow: "hidden" }}>
        <div style={{ width: `${pct}%`, height: "100%", background: color,
                      boxShadow: pct > 80 ? `0 0 8px ${color}60` : "none",
                      transition: "width 1s ease" }} />
      </div>
      <span style={{ width: 40, fontSize: "10px", color, textAlign: "right",
                     fontFamily: "'IBM Plex Mono', monospace" }}>
        {lagSeconds}s
      </span>
    </div>
  );
}

// --- main terminal component ---
export default function TradingTerminal() {
  const [nodeOk] = useState(true);
  const [redisOk] = useState(true);
  const [oracleLags, setOracleLags] = useState([
    { pair: "ETH/USD", lag: 412,  heartbeat: 3600 },
    { pair: "BTC/USD", lag: 1840, heartbeat: 3600 },
    { pair: "USDC/USD", lag: 14200, heartbeat: 86400 },
  ]);

  // drift the oracle lags to simulate live data
  useEffect(() => {
    const id = setInterval(() => {
      setOracleLags(prev => prev.map(o => ({
        ...o,
        lag: Math.min(o.lag + Math.floor(Math.random() * 15) - 3, o.heartbeat),
      })));
    }, 1200);
    return () => clearInterval(id);
  }, []);

  const totalAtRisk = MOCK_CLUSTERS.reduce((s, c) => s + c.totalUsd, 0);

  return (
    <>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;700&family=Share+Tech+Mono&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { background: #020702; }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: #0a150a; }
        ::-webkit-scrollbar-thumb { background: #1a3a1a; border-radius: 2px; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: none; } }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
        @keyframes scanline {
          0% { transform: translateY(-100%); }
          100% { transform: translateY(100vh); }
        }
      `}</style>

      <div style={{ background: "#020702", minHeight: "100vh", color: "#3d7a3d",
                    fontFamily: "'IBM Plex Mono', monospace", position: "relative",
                    overflow: "hidden" }}>

        {/* scanline effect */}
        <div style={{ position: "fixed", top: 0, left: 0, right: 0, height: "2px",
                      background: "linear-gradient(transparent, #00ff4108, transparent)",
                      animation: "scanline 8s linear infinite", pointerEvents: "none",
                      zIndex: 1000 }} />

        {/* CRT vignette */}
        <div style={{ position: "fixed", inset: 0, pointerEvents: "none", zIndex: 999,
                      background: "radial-gradient(ellipse at center, transparent 60%, #00080040 100%)" }} />

        <StatusBar price={CURRENT_PRICE} nodeOk={nodeOk} redisOk={redisOk} />

        {/* page title */}
        <div style={{ padding: "16px 20px 12px", display: "flex", alignItems: "baseline", gap: "16px" }}>
          <span style={{ fontSize: "14px", color: "#00ff41", letterSpacing: "0.15em",
                         fontFamily: "'Share Tech Mono', monospace", fontWeight: 700 }}>
            DEFI-HFT
          </span>
          <span style={{ fontSize: "10px", color: "#2a4a2a", letterSpacing: "0.1em" }}>
            LIQUIDATION & CARRY MONITOR
          </span>
          <span style={{ marginLeft: "auto", fontSize: "10px", color: "#1a3a1a" }}>
            total at risk: <span style={{ color: "#ffaa00" }}>{fmtUsd(totalAtRisk)}</span>
          </span>
        </div>

        {/* main grid */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 360px",
                      gridTemplateRows: "auto auto",
                      gap: "1px", background: "#1a2a1a",
                      margin: "0 0 1px 0" }}>

          {/* col 1: liquidation heatmap */}
          <div style={{ background: "#020702", padding: "14px 16px", gridRow: "1 / 3" }}>
            <div style={{ fontSize: "9px", color: "#2a4a2a", letterSpacing: "0.12em",
                          marginBottom: "10px", display: "flex", justifyContent: "space-between" }}>
              <span>LIQUIDATION HEATMAP</span>
              <span style={{ color: "#1a3a1a" }}>±5% window</span>
            </div>
            {/* current price indicator */}
            <div style={{ display: "flex", alignItems: "center", gap: "8px",
                          marginBottom: "8px", padding: "4px 0",
                          borderBottom: "1px dashed #1a3a1a" }}>
              <span style={{ width: 72, textAlign: "right", fontSize: "11px", color: "#00ff41",
                             fontFamily: "'IBM Plex Mono', monospace" }}>
                ${CURRENT_PRICE.toFixed(0)}
              </span>
              <span style={{ fontSize: "9px", color: "#00ff4180", marginLeft: 4 }}>
                ◄ SPOT
              </span>
            </div>
            <LiqHeatmap clusters={MOCK_CLUSTERS} currentPrice={CURRENT_PRICE} />
          </div>

          {/* col 2: carry opportunities */}
          <div style={{ background: "#020702", padding: "14px 16px" }}>
            <div style={{ fontSize: "9px", color: "#2a4a2a", letterSpacing: "0.12em",
                          marginBottom: "10px", display: "flex", justifyContent: "space-between" }}>
              <span>CARRY OPPORTUNITIES</span>
              <span style={{ color: "#1a3a1a" }}>min 4% net</span>
            </div>
            <ArbTable opportunities={MOCK_ARB} />
          </div>

          {/* col 2: slippage */}
          <div style={{ background: "#020702", padding: "14px 16px", borderTop: "1px solid #1a2a1a" }}>
            <div style={{ fontSize: "9px", color: "#2a4a2a", letterSpacing: "0.12em",
                          marginBottom: "10px" }}>
              MARKET IMPACT (ETH — SELL)
            </div>
            <SlippagePanel />
          </div>

          {/* col 3: risk event feed + oracle lag */}
          <div style={{ background: "#020702", padding: "14px 16px", gridRow: "1 / 3",
                        display: "flex", flexDirection: "column", gap: "16px", overflow: "hidden" }}>

            <div style={{ flex: "0 0 auto" }}>
              <div style={{ fontSize: "9px", color: "#2a4a2a", letterSpacing: "0.12em",
                            marginBottom: "10px" }}>
                ORACLE LAG
              </div>
              {oracleLags.map((o, i) => (
                <OracleLagBar key={i} pair={o.pair} lagSeconds={o.lag} heartbeat={o.heartbeat} />
              ))}
            </div>

            <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
              <div style={{ fontSize: "9px", color: "#2a4a2a", letterSpacing: "0.12em",
                            marginBottom: "10px", display: "flex", justifyContent: "space-between" }}>
                <span>RISK EVENTS</span>
                <span style={{ color: "#1a3a1a", animation: "pulse 2s infinite" }}>● LIVE</span>
              </div>
              <div style={{ flex: 1, overflow: "hidden" }}>
                <RiskFeed events={MOCK_RISK_EVENTS} />
              </div>
            </div>

          </div>
        </div>

        {/* bottom footer */}
        <div style={{ padding: "6px 20px", borderTop: "1px solid #0d1a0d",
                      fontSize: "9px", color: "#1a3a1a", letterSpacing: "0.08em",
                      display: "flex", gap: "24px" }}>
          <span>RETH node: ipc://localhost</span>
          <span>Redis: localhost:6379</span>
          <span>ZMQ pub: :5555/:5556/:5557</span>
          <span style={{ marginLeft: "auto" }}>
            ClickHouse: tick storage active
          </span>
        </div>
      </div>
    </>
  );
}
