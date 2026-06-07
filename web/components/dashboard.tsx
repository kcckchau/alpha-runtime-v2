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
  last_price: string | null;
  last_size: number | null;
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

function normalizeApiBaseUrl(rawUrl: string): string {
  const trimmed = rawUrl.replace(/\/$/, "");
  try {
    const parsed = new URL(trimmed);
    if (typeof window !== "undefined" && parsed.hostname === "127.0.0.1") {
      parsed.hostname = "localhost";
      return parsed.toString().replace(/\/$/, "");
    }
  } catch {
    return trimmed;
  }
  return trimmed;
}

const API_BASE = normalizeApiBaseUrl(
  process.env.NEXT_PUBLIC_ALPHA_API_BASE_URL?.replace(/\/$/, "") ?? "http://127.0.0.1:8000"
);

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

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, { cache: "no-store", ...init });
  if (!response.ok) throw new Error(`${path}: ${response.status}`);
  return (await response.json()) as T;
}

function websocketBaseUrl(): string {
  if (API_BASE.startsWith("https://")) return `wss://${API_BASE.slice("https://".length)}`;
  if (API_BASE.startsWith("http://")) return `ws://${API_BASE.slice("http://".length)}`;
  return `ws://${API_BASE}`;
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

function bucketTimestamp(timestamp: string, timeframe: Timeframe): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return timestamp;

  if (timeframe === "1m") {
    date.setUTCSeconds(0, 0);
    return date.toISOString();
  }

  if (timeframe === "5m" || timeframe === "15m") {
    const minutes = timeframe === "5m" ? 5 : 15;
    date.setUTCMinutes(Math.floor(date.getUTCMinutes() / minutes) * minutes, 0, 0);
    return date.toISOString();
  }

  if (timeframe === "1h") {
    date.setUTCMinutes(0, 0, 0);
    return date.toISOString();
  }

  return timestamp;
}

function mergeLiveBar(
  existingBars: BarHistoryRow[],
  incomingBar: BarHistoryRow,
  timeframe: Timeframe
): BarHistoryRow[] {
  if (timeframe === "1s") return existingBars;

  const bucket = bucketTimestamp(incomingBar.timestamp, timeframe);
  const nextBar: BarHistoryRow = {
    ...incomingBar,
    timeframe,
    timestamp: bucket,
  };

  const merged = [...existingBars];
  const lastIndex = merged.length - 1;
  const lastBar = merged[lastIndex];

  if (!lastBar) return [nextBar];

  if (lastBar.timestamp === bucket) {
    merged[lastIndex] = {
      ...lastBar,
      ...nextBar,
      open: lastBar.open,
      high: String(Math.max(Number(lastBar.high), Number(nextBar.high))),
      low: String(Math.min(Number(lastBar.low), Number(nextBar.low))),
    };
    return merged;
  }

  if (new Date(lastBar.timestamp).getTime() > new Date(bucket).getTime()) {
    const byTimestamp = new Map(merged.map((bar) => [bar.timestamp, bar]));
    const existing = byTimestamp.get(bucket);
    byTimestamp.set(
      bucket,
      existing
        ? {
            ...existing,
            ...nextBar,
            open: existing.open,
            high: String(Math.max(Number(existing.high), Number(nextBar.high))),
            low: String(Math.min(Number(existing.low), Number(nextBar.low))),
          }
        : nextBar
    );
    return [...byTimestamp.values()].sort(
      (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );
  }

  return [...merged, nextBar];
}

type RuntimeWsMessage = {
  type: "runtime_update";
  symbol: string;
  updated_at: string | null;
  runtime_state: string;
  mode: string;
  runtime_available: boolean;
  quote: QuoteRow | null;
  bar: BarHistoryRow | null;
  context: SymbolContext | null;
  setup_context: SetupSessionContext | null;
  prev_setup_context: SetupSessionContext | null;
  setups: SetupRow[];
};

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

// ─── Setup detection rules (mirrors engine logic in readable form) ────────────

type SetupRule = {
  description: string;
  forming: string[];
  confirmation: string[];
  invalidation: string[];
};

const SETUP_RULES: Record<string, SetupRule> = {
  fake_breakdown: {
    description: "Price dips briefly below VWAP but recovers — shallow wick, not a true breakdown.",
    forming: [
      "≥1 bar closed below VWAP",
      "Bar's low is NOT >0.1% below VWAP (shallow dip only)",
    ],
    confirmation: [
      "VWAP cross up occurs",
      "RVOL ≥ 1.2 (volume confirmation)",
      "Close ≥ 50% of the bar's range",
      "Close > Opening Range midpoint",
    ],
    invalidation: [
      "Price stays below VWAP for >15 bars",
    ],
  },
  hod_breakout: {
    description: "Price breaks above session high with momentum after ORB is established.",
    forming: [
      "ORB (Opening Range Breakout) established",
      "Intraday high already above ORB high",
      "Price above VWAP",
      "Making higher highs (is_higher_high)",
      "Close within 0.2% of intraday HOD",
    ],
    confirmation: [
      "New HOD printed on next bar",
      "RVOL ≥ 1.2 (volume confirmation)",
    ],
    invalidation: [
      "Stop hit (bar's low ≤ stop_reference)",
    ],
  },
  trend_pullback: {
    description: "Price in uptrend pulls back toward VWAP — buy the dip.",
    forming: [
      "≥5 consecutive bars above VWAP",
      "VWAP deviation is shrinking (pullback in progress)",
      "VWAP deviation between 0% and 0.5% (close to VWAP)",
    ],
    confirmation: [
      "VWAP deviation ≤ 0.25% (very close to VWAP)",
      "Still above VWAP",
      "RVOL ≥ 0.8",
      "No lower low (downtrend not resuming)",
    ],
    invalidation: [
      "VWAP cross down (trend broken)",
    ],
  },
  vwap_reclaim: {
    description: "Price reclaims VWAP from below with conviction.",
    forming: [
      "VWAP cross up (crossed from below)",
      "≥2 bars were below VWAP before the cross",
      "Close >0.05% above VWAP (conviction)",
      "Close in upper half of the bar's range",
    ],
    confirmation: [
      "Hold above VWAP on next bar",
      "RVOL ≥ 1.0",
      "No lower low",
    ],
    invalidation: [
      "VWAP cross down after reclaim",
    ],
  },
  vwap_rejection: {
    description: "Price fails at VWAP from above — rejection with conviction.",
    forming: [
      "VWAP cross down (crossed from above)",
      "≥2 bars were above VWAP before the cross",
      "Close >0.05% below VWAP (conviction)",
      "Close in lower half of the bar's range",
    ],
    confirmation: [
      "Hold below VWAP on next bar",
      "RVOL ≥ 1.0",
      "No higher high",
    ],
    invalidation: [
      "VWAP cross up after rejection",
    ],
  },
  orb_breakout: {
    description: "Price breaks above Opening Range high. (Not yet implemented)",
    forming: [],
    confirmation: [],
    invalidation: [],
  },
  orb_breakdown: {
    description: "Price breaks below Opening Range low with momentum.",
    forming: [
      "ORB state = BREAKOUT_DOWN (below ORB low)",
      "Price below VWAP",
      "New LOD (is_new_lod)",
    ],
    confirmation: [
      "Hold below ORB low on next bar",
      "RVOL ≥ 1.0",
    ],
    invalidation: [
      "Price reclaims ORB low",
      "Price reclaims VWAP",
    ],
  },
  sweep_reclaim: {
    description: "Price sweeps a key level then reclaims it. (Not yet implemented)",
    forming: [],
    confirmation: [],
    invalidation: [],
  },
};

function computeRR(entry: SetupHistoryEntry): number | null {
  if (!entry.entry_trigger || !entry.stop_reference || !entry.target_reference) return null;
  const e = Number(entry.entry_trigger);
  const sl = Number(entry.stop_reference);
  const tp = Number(entry.target_reference);
  if (!isFinite(e) || !isFinite(sl) || !isFinite(tp)) return null;
  const risk = Math.abs(e - sl);
  const reward = Math.abs(tp - e);
  if (risk === 0) return null;
  return reward / risk;
}

function SetupHistoryPanel({
  context,
  selectedDate,
}: {
  context: SetupSessionContext | null;
  selectedDate: string | null;
}) {
  const [expanded, setExpanded] = React.useState<string | null>(null);

  const sorted = [...(context?.setups ?? [])].sort(
    (a, b) => new Date(b.detected_at).getTime() - new Date(a.detected_at).getTime()
  );

  const title = selectedDate ? `Setups — ${selectedDate}` : "Today's Setup History";

  const statChip = (label: string, value: number, color: string) =>
    value > 0 ? (
      <span
        key={label}
        style={{
          ...S.mono, fontSize: 9, padding: "2px 6px", borderRadius: 4,
          background: `${color}18`, border: `0.5px solid ${color}55`, color,
        }}
      >
        {label} {value}
      </span>
    ) : null;

  return (
    <div style={S.panel}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>{title}</span>
        {context && <Pill color="blue">{context.session_key}</Pill>}
      </div>

      {/* Summary stat chips */}
      {context && (
        <div
          style={{
            display: "flex", gap: 4, padding: "5px 8px", flexWrap: "wrap",
            borderBottom: "0.5px solid rgba(255,255,255,0.06)",
          }}
        >
          {statChip("DET", context.counts.detected_total ?? 0, "rgba(255,255,255,0.5)")}
          {statChip("CFM", context.counts.confirmed_total ?? 0, "#fbbf24")}
          {statChip("TRG", context.counts.triggered_total ?? 0, "#22c55e")}
          {statChip("FAI", context.counts.failed_total ?? 0, "#ef4444")}
          {statChip("INV", context.counts.invalidated_total ?? 0, "rgba(255,255,255,0.3)")}
        </div>
      )}

      <div style={{ padding: 8, maxHeight: 480, overflowY: "auto" }}>
        {sorted.length === 0 ? (
          <div style={{ ...S.mono, fontSize: 11, color: "rgba(255,255,255,0.4)", padding: "6px 0" }}>
            No setups recorded this session
          </div>
        ) : (
          sorted.map((entry) => {
            const isOpen = expanded === entry.setup_id;
            const rules = SETUP_RULES[entry.setup_type.toLowerCase()];
            const rr = computeRR(entry);
            const entryColor = entry.side === "buy" ? "#22c55e" : "#ef4444";

            return (
              <div key={entry.setup_id} style={{ marginBottom: 4 }}>
                <div
                  style={{
                    borderLeft: `2px solid ${levelColor(entry.level_tag)}`,
                    padding: "6px 9px",
                    borderRadius: "0 4px 4px 0",
                    background: isOpen ? "rgba(255,255,255,0.05)" : "rgba(255,255,255,0.02)",
                    cursor: "pointer",
                    userSelect: "none",
                  }}
                  onClick={() => setExpanded(isOpen ? null : entry.setup_id)}
                >
                  {/* Top row: type + state */}
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 3 }}>
                    <span style={{ ...S.mono, fontSize: 10, fontWeight: 500 }}>
                      {entry.setup_type.replaceAll("_", " ").toUpperCase()}
                    </span>
                    <span
                      style={{
                        ...S.mono, fontSize: 9, padding: "1px 5px", borderRadius: 3,
                        background: `${stateColor(entry.state)}18`,
                        border: `0.5px solid ${stateColor(entry.state)}55`,
                        color: stateColor(entry.state),
                      }}
                    >
                      {entry.state.toUpperCase()}
                    </span>
                  </div>

                  {/* Meta row: time, side, level, grade, phase */}
                  <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap", marginBottom: 3 }}>
                    <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.3)" }}>
                      {new Date(entry.detected_at).toLocaleTimeString("en-US", {
                        hour: "2-digit", minute: "2-digit", hour12: false,
                        timeZone: "America/New_York",
                      })}{" "}ET
                    </span>
                    <span style={{ ...S.mono, fontSize: 9, color: entryColor }}>{entry.side.toUpperCase()}</span>
                    <span style={{ ...S.mono, fontSize: 9, color: levelColor(entry.level_tag) }}>
                      {entry.level_tag.toUpperCase()}
                    </span>
                    {entry.grade && (
                      <span style={{ ...S.mono, fontSize: 9, padding: "1px 4px", borderRadius: 3, background: gradeBg(entry.grade), color: gradeColor(entry.grade) }}>
                        {entry.grade}
                      </span>
                    )}
                    {entry.score != null && (
                      <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.4)" }}>
                        score {entry.score}
                      </span>
                    )}
                    <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.2)" }}>
                      {entry.session_phase.toUpperCase()}
                    </span>
                  </div>

                  {/* Price levels + R:R */}
                  {(entry.entry_trigger || entry.stop_reference) && (
                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                      <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.35)" }}>
                        E <span style={{ color: "rgba(255,255,255,0.85)" }}>{formatPrice(entry.entry_trigger)}</span>
                      </span>
                      <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.35)" }}>
                        SL <span style={{ color: "#ef4444" }}>{formatPrice(entry.stop_reference)}</span>
                      </span>
                      <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.35)" }}>
                        TP <span style={{ color: "#22c55e" }}>{formatPrice(entry.target_reference)}</span>
                      </span>
                      {rr !== null && (
                        <span
                          style={{
                            ...S.mono, fontSize: 10,
                            color: rr >= 2.5 ? "#22c55e" : rr >= 1.5 ? "#fbbf24" : "#ef4444",
                          }}
                        >
                          {rr.toFixed(1)}R
                        </span>
                      )}
                    </div>
                  )}

                  {/* Invalidation reason */}
                  {entry.invalidation_reason && (
                    <div style={{ ...S.mono, fontSize: 9, color: "#ef4444", marginTop: 3, opacity: 0.7 }}>
                      ✕ {entry.invalidation_reason}
                    </div>
                  )}
                </div>

                {/* Expanded: detection rules */}
                {isOpen && rules && (
                  <div
                    style={{
                      background: "rgba(255,255,255,0.015)",
                      borderLeft: "2px solid rgba(255,255,255,0.08)",
                      marginLeft: 2,
                      padding: "8px 10px",
                      fontSize: 10, ...S.mono,
                    }}
                  >
                    <div style={{ color: "rgba(255,255,255,0.35)", marginBottom: 4, fontSize: 9 }}>
                      {rules.description}
                    </div>
                    {rules.forming.length > 0 && (
                      <>
                        <div style={{ color: "rgba(255,255,255,0.25)", fontSize: 9, marginBottom: 3, letterSpacing: "0.1em" }}>
                          FORMING CONDITIONS
                        </div>
                        {rules.forming.map((c, i) => (
                          <div key={i} style={{ display: "flex", gap: 5, marginBottom: 2, color: "rgba(255,255,255,0.5)" }}>
                            <span style={{ color: "#fbbf24", flexShrink: 0 }}>◆</span>{c}
                          </div>
                        ))}
                      </>
                    )}
                    {rules.confirmation.length > 0 && (
                      <>
                        <div style={{ color: "rgba(255,255,255,0.25)", fontSize: 9, margin: "6px 0 3px", letterSpacing: "0.1em" }}>
                          CONFIRMATION
                        </div>
                        {rules.confirmation.map((c, i) => (
                          <div key={i} style={{ display: "flex", gap: 5, marginBottom: 2, color: "rgba(255,255,255,0.5)" }}>
                            <span style={{ color: "#22c55e", flexShrink: 0 }}>✓</span>{c}
                          </div>
                        ))}
                      </>
                    )}
                    {rules.invalidation.length > 0 && (
                      <>
                        <div style={{ color: "rgba(255,255,255,0.25)", fontSize: 9, margin: "6px 0 3px", letterSpacing: "0.1em" }}>
                          INVALIDATION
                        </div>
                        {rules.invalidation.map((c, i) => (
                          <div key={i} style={{ display: "flex", gap: 5, marginBottom: 2, color: "rgba(255,255,255,0.5)" }}>
                            <span style={{ color: "#ef4444", flexShrink: 0 }}>✗</span>{c}
                          </div>
                        ))}
                      </>
                    )}
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>

      {/* By-type breakdown */}
      {context && Object.keys(context.counts_by_type).length > 0 && (
        <div style={{ borderTop: "0.5px solid rgba(255,255,255,0.06)", padding: "6px 8px" }}>
          <div style={{ ...S.panelLbl, marginBottom: 5 }}>By Type</div>
          {Object.entries(context.counts_by_type).map(([type, counts]) => (
            <div key={type} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
              <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.35)" }}>
                {type.replaceAll("_", " ").toUpperCase()}
              </span>
              <div style={{ display: "flex", gap: 5 }}>
                {(counts.triggered ?? 0) > 0 && <span style={{ ...S.mono, fontSize: 9, color: "#22c55e" }}>▲{counts.triggered}</span>}
                {(counts.failed ?? 0) > 0 && <span style={{ ...S.mono, fontSize: 9, color: "#ef4444" }}>✗{counts.failed}</span>}
                {(counts.invalidated ?? 0) > 0 && <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.3)" }}>⊘{counts.invalidated}</span>}
                {(counts.forming ?? 0) > 0 && <span style={{ ...S.mono, fontSize: 9, color: "#fbbf24" }}>◆{counts.forming}</span>}
                {(counts.confirmed ?? 0) > 0 && <span style={{ ...S.mono, fontSize: 9, color: "#60a5fa" }}>●{counts.confirmed}</span>}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function FeaturesPanel({ context, isHistorical }: { context: SymbolContext | null; isHistorical?: boolean }) {
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
        {isHistorical && (
          <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.3)", fontStyle: "italic" }}>live only</span>
        )}
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

function MarketStatePanel({ context, isHistorical }: { context: SymbolContext | null; isHistorical?: boolean }) {
  const trend = context?.trend;
  const regime = context?.vwap_regime;
  const orb = context?.orb_state;
  const structure = context?.structure_score;
  const confidence = context?.confidence;

  return (
    <div style={S.panel}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Market State</span>
        {isHistorical && (
          <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.3)", fontStyle: "italic" }}>live only</span>
        )}
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

// ─── Backfill types ───────────────────────────────────────────────────────────

type BackfillJob = {
  job_id: string;
  symbol: string;
  date: string;
  status: "pending" | "fetching_bars" | "detecting_setups" | "done" | "error";
  bars_fetched: number;
  setups_detected: number;
  error: string | null;
  started_at: string | null;
  finished_at: string | null;
};

type AvailableDates = {
  symbol: string;
  bar_dates: string[];
  context_dates: string[];
};

// ─── Main Dashboard ───────────────────────────────────────────────────────────

export function Dashboard() {
  const [status, setStatus] = useState<RuntimeStatus | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState("MNQ");
  const [selectedTimeframe, setSelectedTimeframe] = useState<Timeframe>("1m");
  // null = live mode; string = historical date "YYYY-MM-DD"
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [contexts, setContexts] = useState<Record<string, SymbolContext | null>>({});
  const [quotes, setQuotes] = useState<Record<string, QuoteRow | null>>({});
  const [bars, setBars] = useState<BarHistoryRow[]>([]);
  const [setups, setSetups] = useState<SetupRow[]>([]);
  const [setupContexts, setSetupContexts] = useState<Record<string, SetupSessionContext | null>>({});
  const [prevSetupContexts, setPrevSetupContexts] = useState<Record<string, SetupSessionContext | null>>({});
  const [riskState, setRiskState] = useState<RiskState | null>(null);
  const [clock, setClock] = useState("--:--:-- ET");
  const [error, setError] = useState<string | null>(null);
  // Backfill state
  const [backfillJob, setBackfillJob] = useState<BackfillJob | null>(null);
  const [availableDates, setAvailableDates] = useState<AvailableDates | null>(null);
  // Incremented after a backfill completes to force a data reload
  const [refreshKey, setRefreshKey] = useState(0);

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

  // Available dates — fetched once when symbol changes, enables date picker
  useEffect(() => {
    fetchJson<AvailableDates>(`/runtime/available-dates?symbol=${selectedSymbol}`)
      .then(setAvailableDates)
      .catch(() => setAvailableDates(null));
  }, [selectedSymbol]);

  // Data polling — paused for historical dates (WS handles live updates instead)
  useEffect(() => {
    let cancelled = false;
    const isHistorical = selectedDate !== null;

    async function loadAll() {
      try {
        const statusData = await fetchJson<RuntimeStatus>("/runtime/status");
        const symbol = statusData.symbols.includes(selectedSymbol)
          ? selectedSymbol
          : (statusData.symbols[0] ?? "MNQ");

        // Date bounds: historical uses the selected date, live uses rolling lookback
        const histStart = isHistorical ? selectedDate : historyStartDate(symbol);
        const histEnd   = isHistorical ? selectedDate : todayDate();
        const dateSuffix = isHistorical ? `&date=${selectedDate}` : "";

        const requests: Promise<unknown>[] = [
          // [0] contexts
          isHistorical
            ? Promise.resolve({})
            : fetchJson<Record<string, SymbolContext | null>>(`/runtime/contexts?symbol=${symbol}`),
          // [1] quotes
          isHistorical
            ? Promise.resolve({})
            : fetchJson<Record<string, QuoteRow | null>>(`/runtime/quotes?symbol=${symbol}`),
          // [2] bar history
          fetchJson<BarHistoryRow[]>(
            `/runtime/bars/history?symbol=${symbol}&timeframe=${selectedTimeframe}&start=${histStart}&end=${histEnd}`
          ),
          // [3] latest live bar (only useful in live mode)
          isHistorical
            ? Promise.resolve({})
            : fetchJson<Record<string, BarHistoryRow | null>>(`/runtime/bars?symbol=${symbol}`),
          // [4] setups (live only — no historical setups in the active list)
          isHistorical
            ? Promise.resolve([])
            : fetchJson<SetupRow[]>(`/runtime/setups?symbol=${symbol}`),
          // [5] setup context
          fetchJson<Record<string, SetupSessionContext | null>>(
            `/runtime/setup-contexts?symbol=${symbol}${dateSuffix}`
          ),
          // [6] prev setup context
          fetchJson<Record<string, SetupSessionContext | null>>(
            `/runtime/prev-setup-contexts?symbol=${symbol}${dateSuffix}`
          ),
        ];

        const [
          contextData,
          quoteData,
          barData,
          latestBarData,
          setupData,
          setupContextData,
          prevSetupContextData,
        ] = await Promise.all(requests) as [
          Record<string, SymbolContext | null>,
          Record<string, QuoteRow | null>,
          BarHistoryRow[],
          Record<string, BarHistoryRow | null>,
          SetupRow[],
          Record<string, SetupSessionContext | null>,
          Record<string, SetupSessionContext | null>,
        ];

        const riskData = isHistorical
          ? null
          : await fetchJson<RiskState>(`/runtime/risk?symbol=${symbol}`).catch(() => null);

        if (cancelled) return;

        setStatus(statusData);
        setSelectedSymbol(symbol);

        if (!isHistorical) {
          setContexts((prev) => ({ ...prev, ...contextData }));
          setQuotes((prev) => ({ ...prev, ...quoteData }));
        }

        const latestLiveBar = !isHistorical ? (latestBarData[symbol] ?? null) : null;
        setBars(latestLiveBar ? mergeLiveBar(barData, latestLiveBar, selectedTimeframe) : barData);
        setSetups(setupData);
        setSetupContexts((prev) => ({ ...prev, ...setupContextData }));
        setPrevSetupContexts((prev) => ({ ...prev, ...prevSetupContextData }));

        if (!isHistorical) setRiskState(riskData);

        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Unknown error");
      }
    }

    void loadAll();
    // In historical mode, poll less aggressively (data is static)
    const interval = setInterval(() => void loadAll(), isHistorical ? 60000 : 15000);
    return () => { cancelled = true; clearInterval(interval); };
  }, [selectedSymbol, selectedTimeframe, selectedDate, refreshKey]);

  useEffect(() => {
    // In historical mode the WebSocket is kept alive only for runtime status
    // updates (mode, health). Bar, quote, and setup updates are suppressed so
    // they don't overwrite the historical view.
    const isHistorical = selectedDate !== null;

    const ws = new WebSocket(
      `${websocketBaseUrl()}/runtime/ws?symbol=${encodeURIComponent(selectedSymbol)}`
    );

    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data) as RuntimeWsMessage;
        if (payload.type !== "runtime_update" || payload.symbol !== selectedSymbol) return;

        // Always update runtime status (mode / health)
        setStatus((prev) =>
          prev
            ? {
                ...prev,
                mode: payload.mode,
                runtime_state: payload.runtime_state,
                runtime_available: payload.runtime_available,
                updated_at: payload.updated_at,
              }
            : prev
        );

        // All live data updates are suppressed when viewing a historical date
        if (isHistorical) return;

        if (payload.context) {
          setContexts((prev) => ({ ...prev, [selectedSymbol]: payload.context }));
        }
        if (payload.quote) {
          setQuotes((prev) => ({ ...prev, [selectedSymbol]: payload.quote }));

          // Use last trade price to keep the live candle close current between
          // partial bar updates (which arrive every few seconds). The partial
          // bar will correct the full OHLCV when it arrives.
          const { last_price, timestamp } = payload.quote;
          if (last_price != null) {
            const price = Number(last_price);
            if (isFinite(price) && price > 0) {
              setBars((prev) => {
                if (prev.length === 0) return prev;
                const lastBar = prev[prev.length - 1];
                if (
                  bucketTimestamp(timestamp, selectedTimeframe) !==
                  bucketTimestamp(lastBar.timestamp, selectedTimeframe)
                ) {
                  return prev;
                }
                return [
                  ...prev.slice(0, -1),
                  {
                    ...lastBar,
                    close: String(price),
                    high: String(Math.max(Number(lastBar.high), price)),
                    low: String(Math.min(Number(lastBar.low), price)),
                  },
                ];
              });
            }
          }
        }
        if (payload.setup_context) {
          setSetupContexts((prev) => ({ ...prev, [selectedSymbol]: payload.setup_context }));
        }
        if (payload.prev_setup_context) {
          setPrevSetupContexts((prev) => ({ ...prev, [selectedSymbol]: payload.prev_setup_context }));
        }
        if (payload.setups) {
          setSetups(payload.setups);
        }
        const liveBar = payload.bar;
        if (liveBar) {
          setBars((prev) => mergeLiveBar(prev, liveBar, selectedTimeframe));
        }
        setError(null);
      } catch {
        // Ignore malformed websocket payloads; polling remains the fallback path.
      }
    };

    ws.onerror = () => {
      // Polling remains active, so websocket failures should stay non-fatal.
    };

    return () => {
      ws.close();
    };
  }, [selectedSymbol, selectedTimeframe, selectedDate]);

  // Backfill job polling — only runs while a job is active
  useEffect(() => {
    if (!backfillJob || backfillJob.status === "done" || backfillJob.status === "error") return;

    const id = setInterval(async () => {
      try {
        const updated = await fetchJson<BackfillJob>(
          `/runtime/backfill-jobs/${encodeURIComponent(backfillJob.job_id)}`
        );
        setBackfillJob(updated);
        // When done, reload data for the selected date
        if (updated.status === "done") {
          clearInterval(id);
          setRefreshKey((k) => k + 1);
        }
      } catch {
        // non-fatal
      }
    }, 2000);
    return () => clearInterval(id);
  }, [backfillJob]);

  async function triggerBackfill(symbol: string, date: string) {
    try {
      const job = await fetchJson<BackfillJob>(`/runtime/backfill-date`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol, date }),
      } as Parameters<typeof fetchJson>[1]);
      setBackfillJob(job as BackfillJob);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Backfill request failed");
    }
  }

  const currentContext = contexts[selectedSymbol] ?? null;
  const currentQuote = quotes[selectedSymbol] ?? null;
  const _rawSetupContext = setupContexts[selectedSymbol] ?? null;
  const _prevSetupContext = prevSetupContexts[selectedSymbol] ?? null;
  // Fall back to previous session's history when the current session has no setups yet
  // (e.g. after 6 PM ET when CME session rolls but today's setups should remain visible)
  const currentSetupContext =
    (_rawSetupContext?.setups?.length ?? 0) > 0 ? _rawSetupContext : (_prevSetupContext ?? _rawSetupContext);

  // Price / change
  // Preference order: last trade price (tick-level) → bar close (few-second partial bar) → stale
  const lastBar = bars[bars.length - 1];
  const prevBar = bars[bars.length - 2];
  const lastTradePrice =
    currentQuote?.last_price != null ? Number(currentQuote.last_price) : null;
  const lastPrice = lastTradePrice ?? (lastBar ? Number(lastBar.close) : null);
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

    // Historical setup price lines intentionally omitted — too noisy with many setups.

    return overlays;
  }, [currentContext, setups, currentSetupContext]);

  const historyMarkers = useMemo((): SeriesMarker<Time>[] => {
    return (currentSetupContext?.setups ?? [])
      .filter((entry) => !!entry.detected_at && ["triggered", "failed", "invalidated"].includes(entry.state))
      .map((entry) => ({
        time: toETChartTime(entry.detected_at),
        position: setupMarkerPosition(entry),
        color: levelColor(entry.level_tag),
        shape: setupMarkerShape(entry),
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
          flexWrap: "wrap", gap: 6,
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
          {selectedDate === null && (
            <Pill color={sessionPillColor}>
              <Dot color={sessionDotColor} />
              {sessionPhase}
            </Pill>
          )}
          {selectedDate !== null && (
            <Pill color="blue">
              <Dot color="#60a5fa" />
              HISTORY · {selectedDate}
            </Pill>
          )}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
          <select
            value={selectedSymbol}
            onChange={(e) => {
              setSelectedSymbol(e.target.value);
              setBackfillJob(null);
            }}
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

          {/* Date picker */}
          <input
            type="date"
            value={selectedDate ?? ""}
            max={todayDate()}
            onChange={(e) => {
              const val = e.target.value;
              setSelectedDate(val || null);
              setBackfillJob(null);
              setBars([]);
              setSetups([]);
            }}
            style={{
              background: "rgba(255,255,255,0.06)",
              border: `0.5px solid ${selectedDate ? "rgba(96,165,250,0.4)" : "rgba(255,255,255,0.12)"}`,
              color: "rgba(255,255,255,0.88)",
              borderRadius: 4, padding: "2px 8px",
              ...S.mono, fontSize: 10,
              cursor: "pointer",
              colorScheme: "dark",
            }}
          />
          {selectedDate !== null && (
            <button
              onClick={() => {
                setSelectedDate(null);
                setBackfillJob(null);
                setBars([]);
              }}
              style={{
                ...S.mono, fontSize: 10, fontWeight: 500,
                padding: "3px 8px", borderRadius: 4, cursor: "pointer",
                border: "0.5px solid rgba(255,255,255,0.12)",
                background: "rgba(255,255,255,0.06)",
                color: "rgba(255,255,255,0.6)",
              }}
            >
              Live ▸
            </button>
          )}

          {selectedDate === null && <Pill color="green">{activeSetupCount} setups</Pill>}
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

      {/* ── Historical data banner ── */}
      {selectedDate !== null && (() => {
        const hasBarData = bars.length > 0;
        const jobRunning = backfillJob && (
          backfillJob.status === "pending" ||
          backfillJob.status === "fetching_bars" ||
          backfillJob.status === "detecting_setups"
        );
        const jobDone    = backfillJob?.status === "done";
        const jobError   = backfillJob?.status === "error";

        const statusLabel: Record<string, string> = {
          pending: "Queued…",
          fetching_bars: `Fetching bars from IBKR…`,
          detecting_setups: `Detecting setups… (${backfillJob?.bars_fetched ?? 0} bars)`,
          done: `Done — ${backfillJob?.setups_detected ?? 0} setups detected`,
          error: `Error: ${backfillJob?.error ?? "unknown"}`,
        };

        if (jobRunning || jobDone || jobError) {
          return (
            <div
              style={{
                display: "flex", alignItems: "center", gap: 10,
                background: jobError ? "rgba(239,68,68,0.07)" : "rgba(96,165,250,0.07)",
                border: `0.5px solid ${jobError ? "rgba(239,68,68,0.3)" : "rgba(96,165,250,0.3)"}`,
                color: jobError ? "#ef4444" : "#60a5fa",
                borderRadius: 6, padding: "8px 12px", marginBottom: 8,
                ...S.mono, fontSize: 11,
              }}
            >
              {jobRunning && (
                <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: "#60a5fa", animation: "pulse 1.5s ease-in-out infinite" }} />
              )}
              {statusLabel[backfillJob!.status]}
            </div>
          );
        }

        if (!hasBarData) {
          return (
            <div
              style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                background: "rgba(251,191,36,0.07)", border: "0.5px solid rgba(251,191,36,0.3)",
                borderRadius: 6, padding: "8px 12px", marginBottom: 8,
              }}
            >
              <span style={{ ...S.mono, fontSize: 11, color: "#fbbf24" }}>
                No data for {selectedDate}. Fetch 1m bars and detect setups?
              </span>
              <button
                onClick={() => void triggerBackfill(selectedSymbol, selectedDate)}
                style={{
                  ...S.mono, fontSize: 10, fontWeight: 600,
                  padding: "4px 12px", borderRadius: 4, cursor: "pointer",
                  border: "0.5px solid rgba(251,191,36,0.5)",
                  background: "rgba(251,191,36,0.12)", color: "#fbbf24",
                }}
              >
                Backfill
              </button>
            </div>
          );
        }

        const hasContext = (setupContexts[selectedSymbol]?.setups?.length ?? 0) > 0;

        // Bars loaded but no setups context yet — first-run detection prompt
        if (!hasContext) {
          return (
            <div
              style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                background: "rgba(96,165,250,0.07)", border: "0.5px solid rgba(96,165,250,0.3)",
                borderRadius: 6, padding: "8px 12px", marginBottom: 8,
              }}
            >
              <span style={{ ...S.mono, fontSize: 11, color: "#60a5fa" }}>
                Bars loaded — no setup context for {selectedDate}. Run detection with warmup?
              </span>
              <button
                onClick={() => void triggerBackfill(selectedSymbol, selectedDate)}
                style={{
                  ...S.mono, fontSize: 10, fontWeight: 600,
                  padding: "4px 12px", borderRadius: 4, cursor: "pointer",
                  border: "0.5px solid rgba(96,165,250,0.5)",
                  background: "rgba(96,165,250,0.12)", color: "#60a5fa",
                }}
              >
                Detect Setups
              </button>
            </div>
          );
        }

        // Bars + context exist — show a subtle re-detect option for when
        // the user wants to re-run detection (e.g. after a logic update)
        return (
          <div
            style={{
              display: "flex", alignItems: "center", justifyContent: "flex-end",
              gap: 8, marginBottom: 8,
            }}
          >
            <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.25)" }}>
              {setupContexts[selectedSymbol]?.counts?.detected_total ?? 0} setups on record
            </span>
            <button
              onClick={() => void triggerBackfill(selectedSymbol, selectedDate)}
              style={{
                ...S.mono, fontSize: 10,
                padding: "3px 10px", borderRadius: 4, cursor: "pointer",
                border: "0.5px solid rgba(255,255,255,0.12)",
                background: "rgba(255,255,255,0.04)", color: "rgba(255,255,255,0.4)",
              }}
            >
              ↺ Re-detect
            </button>
          </div>
        );
      })()}

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
          <CandlesChart
            bars={bars}
            overlays={overlayLines}
            emas={emas}
            markers={historyMarkers}
            viewportKey={`${selectedSymbol}:${selectedTimeframe}`}
          />

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
          <SetupHistoryPanel context={currentSetupContext} selectedDate={selectedDate} />
          <SetupsPanel setups={setups} />
          <FeaturesPanel context={currentContext} isHistorical={!!selectedDate} />
          <MarketStatePanel context={currentContext} isHistorical={!!selectedDate} />
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
