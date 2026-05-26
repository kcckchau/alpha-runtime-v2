"use client";

import React, { useEffect, useMemo, useState } from "react";
import { LineStyle, SeriesMarker, Time } from "lightweight-charts";
import { CandlesChart, EmaConfig } from "@/components/candles-chart";

// ─── Types ────────────────────────────────────────────────────────────────────

type EngineStatus = {
  name: string;
  state: string;
  health: string;
  details: Record<string, unknown>;
};

type RuntimeStatus = {
  mode: string;
  symbols: string[];
  engines: EngineStatus[];
  runtime_state: string;
  updated_at: string | null;
  runtime_available: boolean;
};

type QuoteRow = {
  symbol: string;
  bid_price: string | null;
  ask_price: string | null;
  bid_size: number | null;
  ask_size: number | null;
  timestamp: string;
};

type BarHistoryRow = {
  symbol: string;
  timeframe: string;
  timestamp: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: number;
  vwap?: string | null;
};

type SymbolContext = {
  symbol: string;
  bar_counts?: Record<string, number>;
  ema_levels?: Record<string, Record<string, string | null>>;
  levels?: Record<string, string | null>;
  // Feature snapshot
  relative_volume?: number | null;
  atr_14?: number | null;
  vwap_deviation_pct?: number | null;
  rs_vs_spy?: number | null;
  vwap?: number | null;
  // Market state
  trend?: string | null;
  trend_strength?: number | null;
  vwap_regime?: string | null;
  orb_state?: string | null;
  structure_score?: number | null;
  confidence?: number | null;
  session_phase?: string | null;
};

type SetupRow = {
  setup_id: string;
  symbol: string;
  setup_type: string;
  state: string;
  grade: string | null;
  score?: number | null;
  entry_trigger: string | null;
  stop_reference: string | null;
  target_reference: string | null;
  detected_at: string;
  updated_at: string;
  conditions_met: string[];
  conditions_missing: string[];
};

type RiskState = {
  realized_pnl?: number | null;
  unrealized_pnl?: number | null;
  risk_consumed_pct?: number | null;
  max_drawdown?: number | null;
  is_halted?: boolean | null;
};

type SetupHistoryEntry = {
  setup_id: string;
  setup_type: string;
  state: string;
  detected_at: string;
  updated_at: string;
  resolved_at?: string | null;
  side: string;
  level_tag: string;
  entry_trigger?: string | null;
  stop_reference?: string | null;
  target_reference?: string | null;
  grade?: string | null;
  score?: number | null;
  session_phase: string;
  invalidation_reason?: string | null;
};

type SetupSessionContext = {
  symbol: string;
  session_key: string;
  session_date: string;
  session_open: string;
  session_close: string;
  session_timezone: string;
  last_setup?: SetupHistoryEntry | null;
  setups: SetupHistoryEntry[];
  counts: Record<string, number>;
  counts_by_type: Record<string, Record<string, number>>;
  counts_by_level: Record<string, number>;
};

// ─── Constants ────────────────────────────────────────────────────────────────

const API_BASE =
  process.env.NEXT_PUBLIC_ALPHA_API_BASE_URL?.replace(/\/$/, "") ?? "http://127.0.0.1:8000";

const TIMEFRAMES = ["1s", "1m", "5m", "15m", "1h"] as const;
type Timeframe = (typeof TIMEFRAMES)[number];

const ET_FMT = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

// ─── Shared style constants ───────────────────────────────────────────────────

const S = {
  panel: {
    background: "#111111",
    border: "0.5px solid rgba(255,255,255,0.06)",
    borderRadius: 6,
  } as React.CSSProperties,
  panelHd: {
    padding: "7px 12px",
    borderBottom: "0.5px solid rgba(255,255,255,0.06)",
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
  } as React.CSSProperties,
  panelLbl: {
    fontFamily: "'IBM Plex Mono', monospace",
    fontSize: 10,
    fontWeight: 500,
    letterSpacing: "0.12em",
    textTransform: "uppercase" as const,
    color: "rgba(255,255,255,0.4)",
  } as React.CSSProperties,
  mono: { fontFamily: "'IBM Plex Mono', monospace" } as React.CSSProperties,
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!response.ok) throw new Error(`${path}: ${response.status}`);
  return (await response.json()) as T;
}

function formatPrice(value: string | number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  const price = Number(value);
  if (!Number.isFinite(price)) return "—";
  return price.toLocaleString(undefined, {
    minimumFractionDigits: price >= 1000 ? 1 : 2,
    maximumFractionDigits: 2,
  });
}

function formatPnl(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return (value >= 0 ? "+" : "−") + "$" + Math.abs(value).toFixed(0);
}

function historyStartDate(symbol: string): string {
  const lookbackDays = symbol.includes("MNQ") ? 7 : 5;
  const start = new Date(Date.now() - lookbackDays * 864e5);
  return start.toISOString().slice(0, 10);
}

function todayDate(): string {
  return new Date().toISOString().slice(0, 10);
}

function toETChartTime(timestamp: string): Time {
  const raw = new Date(timestamp).getTime();
  const parts = ET_FMT.formatToParts(new Date(raw));
  const g = (t: string) => parts.find((p) => p.type === t)?.value ?? "00";
  const iso = `${g("year")}-${g("month")}-${g("day")}T${g("hour")}:${g("minute")}:${g("second")}Z`;
  return Math.floor(new Date(iso).getTime() / 1000) as Time;
}

function gradeColor(grade: string | null): string {
  if (grade === "SSS") return "#fbbf24";
  if (grade === "A+") return "#22c55e";
  if (grade === "A") return "#60a5fa";
  return "rgba(255,255,255,0.4)";
}

function gradeBg(grade: string | null): string {
  if (grade === "SSS") return "rgba(251,191,36,0.15)";
  if (grade === "A+") return "rgba(34,197,94,0.15)";
  if (grade === "A") return "rgba(96,165,250,0.15)";
  return "rgba(255,255,255,0.08)";
}

function stateColor(state: string): string {
  if (state === "confirmed") return "#22c55e";
  if (state === "forming") return "#fbbf24";
  if (state === "triggered") return "#60a5fa";
  if (state === "failed" || state === "invalidated" || state === "expired") return "#ef4444";
  return "rgba(255,255,255,0.4)";
}

function trendColor(trend: string | null | undefined): string {
  if (!trend) return "rgba(255,255,255,0.88)";
  if (trend.includes("up")) return "#22c55e";
  if (trend.includes("down")) return "#ef4444";
  return "#fbbf24";
}

function regimeColor(regime: string | null | undefined): string {
  if (!regime) return "rgba(255,255,255,0.88)";
  if (regime === "above" || regime === "reclaiming") return "#fbbf24";
  if (regime === "rejecting") return "#ef4444";
  return "rgba(255,255,255,0.88)";
}

function levelColor(levelTag: string): string {
  if (levelTag === "hod") return "#60a5fa";
  if (levelTag === "vwap") return "#fbbf24";
  if (levelTag === "orb") return "#22c55e";
  if (levelTag === "sweep") return "#ef4444";
  return "rgba(255,255,255,0.55)";
}

function setupMarkerShape(entry: SetupHistoryEntry): "circle" | "square" | "arrowUp" | "arrowDown" {
  if (entry.state === "triggered") return entry.side === "buy" ? "arrowUp" : "arrowDown";
  if (entry.state === "failed" || entry.state === "invalidated" || entry.state === "expired") return "square";
  return "circle";
}

function setupMarkerPosition(entry: SetupHistoryEntry): "aboveBar" | "belowBar" {
  return entry.side === "buy" ? "belowBar" : "aboveBar";
}

// ─── Primitive components ─────────────────────────────────────────────────────

function Dot({ color }: { color: string }) {
  return (
    <span
      style={{ display: "inline-block", width: 6, height: 6, borderRadius: "50%", background: color }}
    />
  );
}

type PillColor = "green" | "amber" | "red" | "blue" | "gray";

function Pill({ color, children }: { color: PillColor; children: React.ReactNode }) {
  const map: Record<PillColor, { bg: string; border: string; color: string }> = {
    green:  { bg: "rgba(34,197,94,0.1)",   border: "rgba(34,197,94,0.4)",   color: "#22c55e" },
    amber:  { bg: "rgba(251,191,36,0.1)",  border: "rgba(251,191,36,0.4)",  color: "#fbbf24" },
    red:    { bg: "rgba(239,68,68,0.1)",   border: "rgba(239,68,68,0.4)",   color: "#ef4444" },
    blue:   { bg: "rgba(96,165,250,0.1)",  border: "rgba(96,165,250,0.4)",  color: "#60a5fa" },
    gray:   { bg: "rgba(255,255,255,0.06)", border: "rgba(255,255,255,0.12)", color: "rgba(255,255,255,0.4)" },
  };
  const t = map[color];
  return (
    <span
      style={{
        display: "inline-flex", alignItems: "center", gap: 4,
        padding: "2px 8px", borderRadius: 100,
        fontSize: 10, ...S.mono, fontWeight: 500, letterSpacing: "0.05em",
        border: `0.5px solid ${t.border}`,
        background: t.bg, color: t.color,
      }}
    >
      {children}
    </span>
  );
}

function MsRow({
  label, value, valueColor, last,
}: {
  label: string; value: string; valueColor?: string; last?: boolean;
}) {
  return (
    <div
      style={{
        display: "flex", justifyContent: "space-between", alignItems: "center",
        padding: "5px 0",
        borderBottom: last ? "none" : "0.5px solid rgba(255,255,255,0.06)",
      }}
    >
      <span style={{ fontSize: 11, color: "rgba(255,255,255,0.4)" }}>{label}</span>
      <span style={{ ...S.mono, fontSize: 11, fontWeight: 500, color: valueColor ?? "rgba(255,255,255,0.88)" }}>
        {value}
      </span>
    </div>
  );
}

function LegendItem({ color, label, dashed }: { color: string; label: string; dashed?: boolean }) {
  return (
    <span style={{ display: "flex", alignItems: "center", gap: 4, ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.4)" }}>
      <span
        style={{
          width: 16, height: 2, borderRadius: 1,
          background: dashed ? "transparent" : color,
          borderTop: dashed ? `2px dashed ${color}` : "none",
        }}
      />
      {label}
    </span>
  );
}

// ─── Sidebar panels ───────────────────────────────────────────────────────────

function SetupItem({ setup, past }: { setup: SetupRow; past?: boolean }) {
  const borderColor = past ? "rgba(255,255,255,0.1)" : gradeColor(setup.grade);
  const stateCol = stateColor(setup.state);
  const time = new Date(setup.detected_at).toLocaleTimeString("en-US", {
    hour: "2-digit", minute: "2-digit", hour12: false,
  });

  return (
    <div
      style={{
        borderLeft: `2px solid ${borderColor}`,
        padding: "7px 9px", marginBottom: 5,
        borderRadius: "0 4px 4px 0",
        background: "rgba(255,255,255,0.03)",
        opacity: past ? 0.7 : 1,
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 3 }}>
        <span style={{ ...S.mono, fontSize: 10, fontWeight: 500 }}>
          {setup.setup_type.toUpperCase()}
        </span>
        {setup.grade && (
          <span
            style={{
              ...S.mono, fontSize: 9, fontWeight: 500,
              padding: "1px 5px", borderRadius: 3,
              background: gradeBg(setup.grade), color: gradeColor(setup.grade),
            }}
          >
            {setup.grade}{setup.score != null ? `·${setup.score}` : ""}
          </span>
        )}
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 3 }}>
        <span
          style={{
            display: "inline-flex", alignItems: "center",
            padding: "1px 5px", borderRadius: 100,
            fontSize: 9, ...S.mono, fontWeight: 500,
            border: `0.5px solid ${stateCol}55`,
            background: `${stateCol}18`, color: stateCol,
          }}
        >
          {setup.state.toUpperCase()}
        </span>
        <span style={{ fontSize: 9, color: "rgba(255,255,255,0.4)", ...S.mono }}>{time}</span>
      </div>
      {!past && setup.entry_trigger && (
        <div style={{ display: "flex", gap: 8 }}>
          <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.4)" }}>
            E <span style={{ color: "rgba(255,255,255,0.88)" }}>{formatPrice(setup.entry_trigger)}</span>
          </span>
          <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.4)" }}>
            SL <span style={{ color: "#ef4444" }}>{formatPrice(setup.stop_reference)}</span>
          </span>
          <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.4)" }}>
            TP <span style={{ color: "#22c55e" }}>{formatPrice(setup.target_reference)}</span>
          </span>
        </div>
      )}
    </div>
  );
}

function SetupsPanel({ setups }: { setups: SetupRow[] }) {
  const active = setups.filter((s) => !["failed", "invalidated", "expired"].includes(s.state));
  const past = setups
    .filter((s) => ["failed", "invalidated", "expired"].includes(s.state))
    .slice(0, 3);

  return (
    <div style={S.panel}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Active Setups</span>
        {active.length > 0 && <Pill color="green">{active.length}</Pill>}
      </div>
      <div style={{ padding: 8 }}>
        {active.length === 0 ? (
          <div style={{ fontSize: 11, color: "rgba(255,255,255,0.4)", ...S.mono, padding: "6px 0" }}>
            No active setups
          </div>
        ) : (
          active.map((s) => <SetupItem key={s.setup_id} setup={s} />)
        )}
        {past.length > 0 && (
          <>
            <div style={{ height: 0.5, background: "rgba(255,255,255,0.06)", margin: "6px 0" }} />
            <span style={{ ...S.panelLbl, display: "block", marginBottom: 6 }}>Past</span>
            {past.map((s) => <SetupItem key={s.setup_id} setup={s} past />)}
          </>
        )}
      </div>
    </div>
  );
}

function SetupHistoryPanel({ context }: { context: SetupSessionContext | null }) {
  const recent = [...(context?.setups ?? [])]
    .sort((a, b) => new Date(b.detected_at).getTime() - new Date(a.detected_at).getTime())
    .slice(0, 6);

  return (
    <div style={S.panel}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Today&apos;s Setup History</span>
        {context && <Pill color="blue">{context.session_key}</Pill>}
      </div>
      <div style={{ padding: 8 }}>
        {!context || recent.length === 0 ? (
          <div style={{ fontSize: 11, color: "rgba(255,255,255,0.4)", ...S.mono, padding: "6px 0" }}>
            No setups recorded this session
          </div>
        ) : (
          <>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6, marginBottom: 8 }}>
              <div style={{ background: "rgba(255,255,255,0.03)", borderRadius: 5, padding: "6px 8px" }}>
                <div style={{ fontSize: 10, color: "rgba(255,255,255,0.4)", ...S.mono, marginBottom: 2 }}>Detected</div>
                <div style={{ ...S.mono, fontSize: 13, fontWeight: 500 }}>{context.counts.detected_total ?? 0}</div>
              </div>
              <div style={{ background: "rgba(255,255,255,0.03)", borderRadius: 5, padding: "6px 8px" }}>
                <div style={{ fontSize: 10, color: "rgba(255,255,255,0.4)", ...S.mono, marginBottom: 2 }}>Triggered</div>
                <div style={{ ...S.mono, fontSize: 13, fontWeight: 500, color: "#22c55e" }}>{context.counts.triggered_total ?? 0}</div>
              </div>
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 8 }}>
              {Object.entries(context.counts_by_level).map(([level, count]) => (
                <span
                  key={level}
                  style={{
                    ...S.mono,
                    fontSize: 10,
                    padding: "2px 6px",
                    borderRadius: 100,
                    background: "rgba(255,255,255,0.05)",
                    border: `0.5px solid ${levelColor(level)}55`,
                    color: levelColor(level),
                  }}
                >
                  {level.toUpperCase()} {count}
                </span>
              ))}
            </div>
            {recent.map((entry) => (
              <div
                key={entry.setup_id}
                style={{
                  borderLeft: `2px solid ${levelColor(entry.level_tag)}`,
                  padding: "7px 9px",
                  marginBottom: 5,
                  borderRadius: "0 4px 4px 0",
                  background: "rgba(255,255,255,0.03)",
                }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8, marginBottom: 3 }}>
                  <span style={{ ...S.mono, fontSize: 10, fontWeight: 500 }}>
                    {entry.setup_type.toUpperCase()}
                  </span>
                  <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.4)" }}>
                    {new Date(entry.detected_at).toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false })}
                  </span>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap", marginBottom: 3 }}>
                  <span style={{ ...S.mono, fontSize: 9, color: levelColor(entry.level_tag) }}>{entry.level_tag.toUpperCase()}</span>
                  <span style={{ ...S.mono, fontSize: 9, color: entry.side === "buy" ? "#22c55e" : "#ef4444" }}>{entry.side.toUpperCase()}</span>
                  <span style={{ ...S.mono, fontSize: 9, color: stateColor(entry.state) }}>{entry.state.toUpperCase()}</span>
                  {entry.grade && <span style={{ ...S.mono, fontSize: 9, color: gradeColor(entry.grade) }}>{entry.grade}</span>}
                </div>
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.4)" }}>
                    E <span style={{ color: "rgba(255,255,255,0.88)" }}>{formatPrice(entry.entry_trigger)}</span>
                  </span>
                  <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.4)" }}>
                    SL <span style={{ color: "#ef4444" }}>{formatPrice(entry.stop_reference)}</span>
                  </span>
                </div>
              </div>
            ))}
          </>
        )}
      </div>
    </div>
  );
}

function FeaturesPanel({ context }: { context: SymbolContext | null }) {
  const rvol = context?.relative_volume;
  const atr = context?.atr_14;
  const vwapDev = context?.vwap_deviation_pct;
  const rs = context?.rs_vs_spy;

  const items = [
    {
      label: "RVOL",
      value: rvol != null ? `${rvol.toFixed(2)}×` : "—",
      color: rvol != null && rvol > 1.5 ? "#22c55e" : "rgba(255,255,255,0.88)",
    },
    {
      label: "ATR(14)",
      value: atr != null ? atr.toFixed(2) : "—",
      color: "rgba(255,255,255,0.88)",
    },
    {
      label: "VWAP DEV",
      value: vwapDev != null ? ((vwapDev >= 0 ? "+" : "") + vwapDev.toFixed(2) + "%") : "—",
      color: vwapDev != null ? (vwapDev >= 0 ? "#fbbf24" : "#ef4444") : "rgba(255,255,255,0.88)",
    },
    {
      label: "RS vs SPY",
      value: rs != null ? ((rs >= 0 ? "+" : "") + rs.toFixed(2)) : "—",
      color: rs != null ? (rs >= 0 ? "#22c55e" : "#ef4444") : "rgba(255,255,255,0.88)",
    },
  ];

  return (
    <div style={S.panel}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Features</span>
      </div>
      <div style={{ padding: 8, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
        {items.map(({ label, value, color }) => (
          <div key={label} style={{ background: "rgba(255,255,255,0.03)", borderRadius: 5, padding: "6px 8px" }}>
            <div style={{ fontSize: 10, color: "rgba(255,255,255,0.4)", ...S.mono, marginBottom: 2 }}>{label}</div>
            <div style={{ ...S.mono, fontSize: 13, fontWeight: 500, color }}>{value}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function MarketStatePanel({ context }: { context: SymbolContext | null }) {
  const trend = context?.trend;
  const regime = context?.vwap_regime;
  const orb = context?.orb_state;
  const structure = context?.structure_score;
  const confidence = context?.confidence;

  return (
    <div style={S.panel}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Market State</span>
      </div>
      <div style={{ padding: "4px 12px 8px" }}>
        <MsRow label="Trend" value={(trend ?? "—").toUpperCase()} valueColor={trendColor(trend)} />
        <MsRow label="VWAP regime" value={(regime ?? "—").toUpperCase()} valueColor={regimeColor(regime)} />
        <MsRow
          label="ORB state"
          value={(orb ?? "—").toUpperCase()}
          valueColor={orb?.includes("breakout") ? "#22c55e" : undefined}
        />
        <MsRow
          label="Structure"
          value={structure != null ? structure.toFixed(2) : "—"}
          valueColor={structure != null && structure > 0.7 ? "#22c55e" : undefined}
        />
        <MsRow
          label="Confidence"
          value={confidence != null ? confidence.toFixed(2) : "—"}
          last
        />
      </div>
    </div>
  );
}

function RiskPanel({ risk }: { risk: RiskState | null }) {
  const isHalted = risk?.is_halted ?? false;
  const pnl = risk?.realized_pnl;
  const unreal = risk?.unrealized_pnl;
  const riskUsed = risk?.risk_consumed_pct;
  const drawdown = risk?.max_drawdown;

  return (
    <div style={S.panel}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Risk</span>
      </div>
      <div style={{ padding: "4px 12px 8px" }}>
        <MsRow
          label="Real P&L"
          value={pnl != null ? formatPnl(pnl) : "—"}
          valueColor={pnl != null ? (pnl >= 0 ? "#22c55e" : "#ef4444") : undefined}
        />
        <MsRow
          label="Unreal"
          value={unreal != null ? formatPnl(unreal) : "—"}
          valueColor={unreal != null ? (unreal >= 0 ? "#22c55e" : "#fbbf24") : undefined}
        />
        <MsRow
          label="Risk used"
          value={riskUsed != null ? `${(riskUsed * 100).toFixed(0)}%` : "—"}
          valueColor={riskUsed != null && riskUsed > 0.7 ? "#ef4444" : "#fbbf24"}
        />
        <MsRow
          label="Drawdown"
          value={drawdown != null ? formatPnl(drawdown) : "—"}
          valueColor="#ef4444"
          last
        />
        <div
          style={{
            marginTop: 8, display: "flex", alignItems: "center", gap: 5, padding: "5px 8px",
            background: isHalted ? "rgba(239,68,68,0.07)" : "rgba(34,197,94,0.07)",
            border: `0.5px solid ${isHalted ? "rgba(239,68,68,0.25)" : "rgba(34,197,94,0.25)"}`,
            borderRadius: 5,
          }}
        >
          <span
            style={{
              ...S.mono, fontSize: 10, fontWeight: 500, letterSpacing: "0.05em",
              color: isHalted ? "#ef4444" : "#22c55e",
            }}
          >
            {isHalted ? "RISK HALTED" : "TRADING ACTIVE"}
          </span>
        </div>
      </div>
    </div>
  );
}

// ─── Main Dashboard ───────────────────────────────────────────────────────────

export function Dashboard() {
  const [status, setStatus] = useState<RuntimeStatus | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState("MNQ");
  const [selectedTimeframe, setSelectedTimeframe] = useState<Timeframe>("1m");
  const [contexts, setContexts] = useState<Record<string, SymbolContext | null>>({});
  const [quotes, setQuotes] = useState<Record<string, QuoteRow | null>>({});
  const [bars, setBars] = useState<BarHistoryRow[]>([]);
  const [setups, setSetups] = useState<SetupRow[]>([]);
  const [setupContexts, setSetupContexts] = useState<Record<string, SetupSessionContext | null>>({});
  const [riskState, setRiskState] = useState<RiskState | null>(null);
  const [clock, setClock] = useState("--:--:-- ET");
  const [error, setError] = useState<string | null>(null);

  // ET clock — ticks every second
  useEffect(() => {
    function tick() {
      const et = new Date(new Date().toLocaleString("en-US", { timeZone: "America/New_York" }));
      setClock(
        `${String(et.getHours()).padStart(2, "0")}:${String(et.getMinutes()).padStart(2, "0")}:${String(et.getSeconds()).padStart(2, "0")} ET`
      );
    }
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, []);

  // Data polling
  useEffect(() => {
    let cancelled = false;

    async function loadAll() {
      try {
        const statusData = await fetchJson<RuntimeStatus>("/runtime/status");
        const symbol = statusData.symbols.includes(selectedSymbol)
          ? selectedSymbol
          : (statusData.symbols[0] ?? "MNQ");

        const [contextData, quoteData, barData, setupData, setupContextData] = await Promise.all([
          fetchJson<Record<string, SymbolContext | null>>(`/runtime/contexts?symbol=${symbol}`),
          fetchJson<Record<string, QuoteRow | null>>(`/runtime/quotes?symbol=${symbol}`),
          fetchJson<BarHistoryRow[]>(
            `/runtime/bars/history?symbol=${symbol}&timeframe=${selectedTimeframe}&start=${historyStartDate(symbol)}&end=${todayDate()}`
          ),
          fetchJson<SetupRow[]>(`/runtime/setups?symbol=${symbol}`),
          fetchJson<Record<string, SetupSessionContext | null>>(`/runtime/setup-contexts?symbol=${symbol}`),
        ]);

        // Optional endpoint — silently ignore if not yet implemented
        const riskData = await fetchJson<RiskState>(`/runtime/risk?symbol=${symbol}`).catch(() => null);

        if (cancelled) return;

        setStatus(statusData);
        setSelectedSymbol(symbol);
        setContexts((prev) => ({ ...prev, ...contextData }));
        setQuotes((prev) => ({ ...prev, ...quoteData }));
        setBars(barData);
        setSetups(setupData);
        setSetupContexts((prev) => ({ ...prev, ...setupContextData }));
        setRiskState(riskData);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Unknown error");
      }
    }

    void loadAll();
    const interval = setInterval(() => void loadAll(), 15000);
    return () => { cancelled = true; clearInterval(interval); };
  }, [selectedSymbol, selectedTimeframe]);

  const currentContext = contexts[selectedSymbol] ?? null;
  const currentQuote = quotes[selectedSymbol] ?? null;
  const currentSetupContext = setupContexts[selectedSymbol] ?? null;

  // Price / change from last two bars
  const lastBar = bars[bars.length - 1];
  const prevBar = bars[bars.length - 2];
  const lastPrice = lastBar ? Number(lastBar.close) : null;
  const prevClose = prevBar ? Number(prevBar.close) : null;
  const pctChange =
    lastPrice !== null && prevClose !== null && prevClose !== 0
      ? ((lastPrice - prevClose) / prevClose) * 100
      : null;

  const activeSetupCount = setups.filter(
    (s) => !["failed", "invalidated", "expired"].includes(s.state)
  ).length;

  const sessionPhase =
    currentContext?.session_phase?.toUpperCase() ??
    status?.runtime_state?.toUpperCase() ??
    "—";

  // Topbar pill colors
  const modeStr = (status?.mode ?? "PAPER").toUpperCase();
  const modePillColor: PillColor =
    modeStr === "LIVE" ? "red" : modeStr === "PAPER" ? "green" : "blue";
  const modeDotColor =
    modePillColor === "green" ? "#22c55e" : modePillColor === "red" ? "#ef4444" : "#60a5fa";
  const sessionPillColor: PillColor =
    ["OPENING_RANGE", "EARLY", "POWER_HOUR"].some((p) => sessionPhase.includes(p.replace("_", "")))
      ? "amber"
      : "gray";
  const sessionDotColor = sessionPillColor === "amber" ? "#fbbf24" : "rgba(255,255,255,0.4)";

  // Chart overlay lines
  const overlayLines = useMemo(() => {
    const overlays: Array<{ label: string; price: number; color: string; style?: number }> = [];


    if (currentContext?.levels) {
      for (const [label, value] of Object.entries(currentContext.levels)) {
        if (!value) continue;
        overlays.push({
          label: label.replaceAll("_", " "),
          price: Number(value),
          color: label.includes("high") ? "#ef4444" : "#60a5fa",
          style: LineStyle.LargeDashed,
        });
      }
    }

    for (const s of setups) {
      if (s.state !== "confirmed" && s.state !== "triggered") continue;
      if (s.entry_trigger) {
        overlays.push({
          label: `${s.setup_type.replace(/_/g, " ")} E`,
          price: Number(s.entry_trigger),
          color: gradeColor(s.grade),
        });
      }
      if (s.stop_reference) {
        overlays.push({ label: "SL", price: Number(s.stop_reference), color: "#ef4444", style: LineStyle.Dashed });
      }
      if (s.target_reference) {
        overlays.push({ label: "TP", price: Number(s.target_reference), color: "#22c55e", style: LineStyle.Dashed });
      }
    }

    for (const entry of currentSetupContext?.setups ?? []) {
      if (!entry.entry_trigger) continue;
      if (setups.some((active) => active.setup_id === entry.setup_id)) continue;
      overlays.push({
        label: `${entry.level_tag} ${entry.state}`,
        price: Number(entry.entry_trigger),
        color: `${levelColor(entry.level_tag)}cc`,
        style: LineStyle.Dotted,
      });
    }

    return overlays;
  }, [currentContext, setups, currentSetupContext]);

  const historyMarkers = useMemo((): SeriesMarker<Time>[] => {
    return (currentSetupContext?.setups ?? [])
      .filter((entry) => !!entry.detected_at)
      .map((entry) => ({
        time: toETChartTime(entry.detected_at),
        position: setupMarkerPosition(entry),
        color: levelColor(entry.level_tag),
        shape: setupMarkerShape(entry),
        text: `${entry.level_tag.toUpperCase()} ${entry.state.toUpperCase()}`,
      }));
  }, [currentSetupContext]);

  // EMA indicator configs — computed from bar data as line series in the chart
  const emas = useMemo((): EmaConfig[] => {
    if (selectedTimeframe === "1m" || selectedTimeframe === "5m") {
      return [
        { period: 9, color: "#60a5fa" },   // blue
        { period: 21, color: "#fbbf24" },  // amber/yellow
      ];
    }
    return [];
  }, [selectedTimeframe]);

  return (
    <div
      style={{
        fontFamily: "'IBM Plex Sans', sans-serif",
        fontSize: 13,
        color: "rgba(255,255,255,0.88)",
        background: "#141414",
        minHeight: "100vh",
        padding: 8,
      }}
    >
      {/* ── Topbar ── */}
      <div
        style={{
          display: "flex", alignItems: "center", justifyContent: "space-between",
          padding: "7px 12px", background: "#111111",
          border: "0.5px solid rgba(255,255,255,0.06)", borderRadius: 6, marginBottom: 8,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={{ ...S.mono, fontSize: 13, fontWeight: 500, letterSpacing: "0.06em" }}>
            ALPHA<span style={{ color: "#22c55e" }}>▸</span>RUNTIME
          </span>
          <Pill color={modePillColor}>
            <Dot color={modeDotColor} />
            {modeStr}
          </Pill>
          <Pill color={sessionPillColor}>
            <Dot color={sessionDotColor} />
            {sessionPhase}
          </Pill>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <select
            value={selectedSymbol}
            onChange={(e) => setSelectedSymbol(e.target.value)}
            style={{
              background: "rgba(255,255,255,0.06)",
              border: "0.5px solid rgba(255,255,255,0.12)",
              color: "rgba(255,255,255,0.88)",
              borderRadius: 4, padding: "2px 8px",
              ...S.mono, fontSize: 10, letterSpacing: "0.05em",
              cursor: "pointer",
            }}
          >
            {(status?.symbols ?? ["MNQ"]).map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <Pill color="green">{activeSetupCount} setups</Pill>
          <Pill color="gray">{clock}</Pill>
        </div>
      </div>

      {/* ── Error banner ── */}
      {error && (
        <div
          style={{
            background: "rgba(239,68,68,0.1)", border: "0.5px solid rgba(239,68,68,0.4)",
            color: "#ef4444", borderRadius: 6, padding: "8px 12px", marginBottom: 8,
            ...S.mono, fontSize: 11,
          }}
        >
          {error}
        </div>
      )}

      {/* ── Main layout: chart + sidebar ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 220px", gap: 8 }}>

        {/* Chart panel */}
        <div style={{ ...S.panel, overflow: "hidden" }}>
          {/* Chart header */}
          <div
            style={{
              display: "flex", alignItems: "center", gap: 8,
              padding: "8px 12px",
              borderBottom: "0.5px solid rgba(255,255,255,0.06)",
              flexWrap: "wrap",
            }}
          >
            <span style={{ ...S.mono, fontSize: 15, fontWeight: 500 }}>{selectedSymbol}</span>
            <span style={{ ...S.mono, fontSize: 15 }}>{formatPrice(lastPrice)}</span>
            {pctChange !== null && (
              <span style={{ ...S.mono, fontSize: 12, color: pctChange >= 0 ? "#22c55e" : "#ef4444" }}>
                {pctChange >= 0 ? "+" : ""}{pctChange.toFixed(2)}%
              </span>
            )}
            <div style={{ width: 0.5, height: 14, background: "rgba(255,255,255,0.06)", margin: "0 4px" }} />
            {TIMEFRAMES.map((tf) => (
              <button
                key={tf}
                onClick={() => setSelectedTimeframe(tf)}
                style={{
                  ...S.mono, fontSize: 11, fontWeight: 500,
                  padding: "3px 8px", borderRadius: 4, cursor: "pointer",
                  border: "0.5px solid rgba(255,255,255,0.06)",
                  background: tf === selectedTimeframe ? "rgba(255,255,255,0.08)" : "transparent",
                  color: tf === selectedTimeframe ? "rgba(255,255,255,0.88)" : "rgba(255,255,255,0.4)",
                }}
              >
                {tf}
              </button>
            ))}
            <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 10 }}>
              <LegendItem color="rgba(255,255,255,0.85)" label="VWAP" />
              {(selectedTimeframe === "1m" || selectedTimeframe === "5m") ? (
                <>
                  <LegendItem color="#60a5fa" label="EMA9" />
                  <LegendItem color="#fbbf24" label="EMA21" />
                </>
              ) : null}
            </div>
          </div>

          {/* Candlestick chart */}
          <CandlesChart bars={bars} overlays={overlayLines} emas={emas} markers={historyMarkers} />

          {/* Quote bar */}
          {currentQuote && (
            <div
              style={{
                display: "flex", gap: 16, padding: "6px 12px",
                borderTop: "0.5px solid rgba(255,255,255,0.06)",
                ...S.mono, fontSize: 11,
              }}
            >
              <span style={{ color: "rgba(255,255,255,0.4)" }}>
                BID{" "}
                <span style={{ color: "#22c55e" }}>{formatPrice(currentQuote.bid_price)}</span>
              </span>
              <span style={{ color: "rgba(255,255,255,0.4)" }}>
                ASK{" "}
                <span style={{ color: "#ef4444" }}>{formatPrice(currentQuote.ask_price)}</span>
              </span>
              <span style={{ color: "rgba(255,255,255,0.4)" }}>
                SIZE {currentQuote.bid_size ?? "—"} × {currentQuote.ask_size ?? "—"}
              </span>
            </div>
          )}
        </div>

        {/* Sidebar */}
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          <SetupHistoryPanel context={currentSetupContext} />
          <SetupsPanel setups={setups} />
          <FeaturesPanel context={currentContext} />
          <MarketStatePanel context={currentContext} />
          <RiskPanel risk={riskState} />
        </div>
      </div>

      {/* ── Engine health strip ── */}
      {(status?.engines ?? []).length > 0 && (
        <div style={{ ...S.panel, marginTop: 8 }}>
          <div style={S.panelHd}>
            <span style={S.panelLbl}>Engine Health</span>
            <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.4)" }}>
              {status?.updated_at ? new Date(status.updated_at).toLocaleTimeString() : ""}
            </span>
          </div>
          <div style={{ display: "flex", flexWrap: "wrap" }}>
            {status!.engines.map((engine, i) => (
              <div
                key={engine.name}
                style={{
                  display: "flex", alignItems: "center", justifyContent: "space-between",
                  padding: "6px 12px", flex: "1 1 160px",
                  borderRight:
                    i < status!.engines.length - 1
                      ? "0.5px solid rgba(255,255,255,0.06)"
                      : "none",
                }}
              >
                <span style={{ ...S.mono, fontSize: 11, color: "rgba(255,255,255,0.6)" }}>
                  {engine.name}
                </span>
                <Pill
                  color={
                    engine.health === "healthy"
                      ? "green"
                      : engine.health === "degraded"
                      ? "amber"
                      : "red"
                  }
                >
                  {engine.health}
                </Pill>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
