"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { LineStyle, SeriesMarker, Time } from "lightweight-charts";
import { CandlesChart, EmaConfig } from "@/components/candles-chart";

// ─── Types ────────────────────────────────────────────────────────────────────

type EngineStatus = {
  name: string;
  state: string;
  health: string;
  details: Record<string, unknown>;
};

type FeedQualityEntry = {
  quality: "clean" | "degraded" | "recovering" | "failed";
  degraded_reason: string | null;
  signals_allowed: boolean;
  last_record_at: string | null;
  last_bar_at: string | null;
};

type RuntimeStatus = {
  mode: string;
  symbols: string[];
  engines: EngineStatus[];
  runtime_state: string;
  updated_at: string | null;
  runtime_available: boolean;
  feed_quality: Record<string, FeedQualityEntry> | null;
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
  // Legacy market state fields (not populated in current backend — use MarketStateData instead)
  trend?: string | null;
  trend_strength?: number | null;
  vwap_regime?: string | null;
  orb_state?: string | null;
  structure_score?: number | null;
  confidence?: number | null;
  session_phase?: string | null;
};

type MarketStateData = {
  symbol: string;
  timestamp: string;
  // Trend
  trend: string;
  trend_strength: number;
  trend_bars: number;
  // VWAP
  vwap_state: string;
  // ORB
  orb_state: string;
  // Session
  session_phase: string;
  is_extended: boolean;
  // Quality
  structure_score: number;
  confidence: number;
  // Day type — locked once per session after ORB + 30 bars + 3-bar confirmation
  day_type: string;
  day_type_status: string;   // forming | locked_healthy | stressed | invalidated
  // Live bias — per-bar, never locks
  live_bias: string;         // bullish | bearish | transitioning_bullish | transitioning_bearish | neutral | unknown
  // Trade permission derived from day_type_status + live_bias
  trade_long_allowed: boolean;
  trade_short_allowed: boolean;
  trade_permission_reason: string;
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

type AccountRiskState = {
  account_id: string;
  account_type: string;           // "day" | "swing"
  account_size: number;
  realized_pnl: number;
  unrealized_pnl: number;
  session_high_pnl: number;
  daily_loss_limit: number;
  risk_consumed_pct: number;
  max_drawdown: number;
  trades_taken: number;
  open_positions: number;
  // Account metrics (populated after first broker sync)
  net_liquidation: number;
  cash_balance: number;
  gross_position_value: number;
  leverage_ratio: number;
  is_halted: boolean;
  halt_reason: string | null;
  halt_time: string | null;
  profit_protect_activation: number;
  profit_protect_giveback_pct: number;
  kill_switch_flatten: boolean;
};

// Keyed by account_id
type RiskData = Record<string, AccountRiskState>;

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

type ThesisCandidate = {
  thesis_id: string;
  thesis_type: string;
  state: string;
  confidence: number;
  bars_alive: number;
  entry: string | null;
  stop: string | null;
  target: string | null;
  key_level: string | null;
  sweep_low: string | null;
  rejection_high: string | null;
  evidence_positive: string[];
  evidence_negative: string[];
  commit_conditions: string[];
  invalidation_conditions: string[];
  possible_flip: string | null;
  invalidation_reason: string | null;
};

type ThesisData = {
  symbol: string;
  dominant: ThesisCandidate | null;
  flip: ThesisCandidate | null;
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
  // Per-bar MarketState snapshots, keyed by ISO timestamp.
  // Populated during backfill replay; absent on live session contexts.
  bar_market_states?: Record<string, MarketStateData>;
};

type PipelineDebug = {
  pipeline_ts: string;        // when BarPipeline finished processing this bar
  bar_ts: string;             // bar close timestamp (M1)
  market_state_ts: string | null;
  thesis_type: string | null;
  flow_available: boolean;
  active_setup_count: number;
  scored_setup_count: number;
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

function historyStartDate(symbol: string, timeframe: Timeframe): string {
  // Limit rows returned per timeframe so the initial load stays fast.
  // 1m = 2 days (~2880 bars for 24h futures), 5m/15m = 5 days, 1h/1d = 30 days.
  const lookbackDays =
    timeframe === "1m" ? 2
    : timeframe === "5m" || timeframe === "15m" ? 5
    : 30;
  void symbol; // symbol-specific overrides can go here in the future
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

  // When the incoming bar's native timeframe matches the chart timeframe, the
  // backend has already accumulated the correct OHLCV for that period. Trust it
  // directly — do NOT take Math.max/Math.min, which would lock in any stale or
  // bad value from a previous update and prevent it from ever being corrected.
  //
  // Math.max/Math.min is only correct when aggregating sub-timeframe bars into a
  // higher timeframe (e.g. 1m bars into a 5m bucket), where the frontend must
  // accumulate highs/lows across several incoming bars.
  const nativeTf = String(incomingBar.timeframe ?? "").toLowerCase();
  const sameTimeframe = nativeTf === timeframe || nativeTf === "";

  function mergeInto(existing: BarHistoryRow): BarHistoryRow {
    if (sameTimeframe) {
      // Replace: backend is authoritative, bad values from prior updates get corrected.
      return { ...nextBar, open: existing.open };
    }
    // Cross-timeframe: accumulate across sub-bars.
    return {
      ...existing,
      ...nextBar,
      open: existing.open,
      high: String(Math.max(Number(existing.high), Number(nextBar.high))),
      low: String(Math.min(Number(existing.low), Number(nextBar.low))),
    };
  }

  const merged = [...existingBars];
  const lastIndex = merged.length - 1;
  const lastBar = merged[lastIndex];

  if (!lastBar) return [nextBar];

  if (lastBar.timestamp === bucket) {
    merged[lastIndex] = mergeInto(lastBar);
    return merged;
  }

  if (new Date(lastBar.timestamp).getTime() > new Date(bucket).getTime()) {
    const byTimestamp = new Map(merged.map((bar) => [bar.timestamp, bar]));
    const existing = byTimestamp.get(bucket);
    byTimestamp.set(bucket, existing ? mergeInto(existing) : nextBar);
    return [...byTimestamp.values()].sort(
      (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );
  }

  return [...merged, nextBar];
}

function symbolsMatch(a: string, b: string): boolean {
  const au = a.toUpperCase();
  const bu = b.toUpperCase();
  if (au === bu) return true;
  return au.split("-")[0] === bu.split("-")[0];
}

/** Keep WS-appended bars when a history poll returns stale parquet data. */
function mergeHistoryWithLiveTail(
  barData: BarHistoryRow[],
  latestLiveBar: BarHistoryRow | null,
  prevBars: BarHistoryRow[],
  timeframe: Timeframe,
): BarHistoryRow[] {
  let merged = latestLiveBar
    ? mergeLiveBar(barData, latestLiveBar, timeframe)
    : barData;
  if (prevBars.length === 0 || barData.length === 0) return merged;

  const lastHistTs = new Date(barData[barData.length - 1].timestamp).getTime();
  const liveTail = prevBars.filter(
    (b) => new Date(b.timestamp).getTime() > lastHistTs
  );
  for (const bar of liveTail) {
    merged = mergeLiveBar(merged, bar, timeframe);
  }
  return merged;
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
  market_state: MarketStateData | null;
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

function dayTypeColor(dayType: string | null | undefined): string {
  if (!dayType || dayType === "unknown") return "rgba(255,255,255,0.4)";
  if (dayType === "trend_up") return "#22c55e";
  if (dayType === "trend_down") return "#ef4444";
  if (dayType === "range") return "#fbbf24";
  if (dayType === "balanced") return "#60a5fa";
  return "rgba(255,255,255,0.4)";
}

function dayTypePillColor(dayType: string | null | undefined): PillColor {
  if (!dayType || dayType === "unknown") return "gray";
  if (dayType === "trend_up") return "green";
  if (dayType === "trend_down") return "red";
  if (dayType === "range") return "amber";
  if (dayType === "balanced") return "blue";
  return "gray";
}

function dayTypeLabel(dayType: string | null | undefined): string {
  if (!dayType || dayType === "unknown") return "UNKNOWN";
  return dayType.replaceAll("_", " ").toUpperCase();
}

function dayTypeStatusColor(status: string | null | undefined): string {
  if (!status || status === "forming") return "rgba(255,255,255,0.4)";
  if (status === "locked_healthy") return "#22c55e";
  if (status === "stressed") return "#fbbf24";
  if (status === "invalidated") return "#ef4444";
  return "rgba(255,255,255,0.4)";
}

function dayTypeStatusLabel(status: string | null | undefined): string {
  if (!status) return "—";
  return status.replaceAll("_", " ").toUpperCase();
}

function liveBiasColor(bias: string | null | undefined): string {
  if (!bias || bias === "unknown") return "rgba(255,255,255,0.4)";
  if (bias === "bullish") return "#22c55e";
  if (bias === "bearish") return "#ef4444";
  if (bias === "transitioning_bullish") return "#86efac";  // light green
  if (bias === "transitioning_bearish") return "#fca5a5";  // light red
  if (bias === "neutral") return "#fbbf24";
  return "rgba(255,255,255,0.4)";
}

function liveBiasLabel(bias: string | null | undefined): string {
  if (!bias || bias === "unknown") return "—";
  return bias.replaceAll("_", " ").toUpperCase();
}

function tradePermissionSummary(ms: MarketStateData | null): { label: string; color: string; detail: string } {
  if (!ms) return { label: "—", color: "rgba(255,255,255,0.4)", detail: "" };
  const { trade_long_allowed, trade_short_allowed, trade_permission_reason } = ms;
  if (!trade_long_allowed && !trade_short_allowed)
    return { label: "ALL BLOCKED", color: "#ef4444", detail: trade_permission_reason };
  if (!trade_long_allowed)
    return { label: "LONG BLOCKED", color: "#ef4444", detail: trade_permission_reason };
  if (!trade_short_allowed)
    return { label: "SHORT BLOCKED", color: "#fbbf24", detail: trade_permission_reason };
  return { label: "BOTH ALLOWED", color: "#22c55e", detail: "" };
}

function regimeColor(regime: string | null | undefined): string {
  if (!regime) return "rgba(255,255,255,0.88)";
  if (regime === "above" || regime === "reclaiming") return "#fbbf24";
  if (regime === "rejecting") return "#ef4444";
  return "rgba(255,255,255,0.88)";
}

const SHORT_SETUP_TYPES = new Set(["vwap_rejection", "orb_breakdown"]);
function setupTypeColor(setupType: string): string {
  return SHORT_SETUP_TYPES.has(setupType.toLowerCase()) ? "#ef4444" : "#22c55e";
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
        <span style={{ ...S.mono, fontSize: 10, fontWeight: 500, color: setupTypeColor(setup.setup_type) }}>
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
          {(setup.state ?? "").toUpperCase()}
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

function SetupsPanel({
  setups,
  thesis,
}: {
  setups: SetupRow[];
  thesis: ThesisData | null;
}) {
  const active = setups.filter((s) => !["failed", "invalidated", "expired"].includes(s.state));
  const confirmed = active.filter((s) => s.state === "confirmed");
  const past = setups
    .filter((s) => ["failed", "invalidated", "expired"].includes(s.state))
    .slice(0, 3);

  // "Why no trade?" — only shown when thesis is active but no confirmed setup exists
  const thesisActive = thesis?.dominant && !["invalidated", "expired"].includes(thesis.dominant.state);
  const showWhyNoTrade = thesisActive && confirmed.length === 0;

  return (
    <div style={S.panel}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Setup</span>
        {confirmed.length > 0 && <Pill color="green">{confirmed.length} confirmed</Pill>}
        {active.length > 0 && confirmed.length === 0 && <Pill color="amber">{active.length} forming</Pill>}
      </div>
      <div style={{ padding: 8 }}>
        {active.length === 0 ? (
          <div style={{ fontSize: 11, color: "rgba(255,255,255,0.4)", ...S.mono, padding: "6px 0" }}>
            No active setups
          </div>
        ) : (
          active.map((s) => <SetupItem key={s.setup_id} setup={s} />)
        )}

        {/* "Why no trade?" — visible when thesis is watching/building but no confirmed setup */}
        {showWhyNoTrade && (
          <div style={{
            marginTop: 6,
            padding: "7px 9px",
            borderRadius: 5,
            background: "rgba(251,191,36,0.06)",
            border: "0.5px solid rgba(251,191,36,0.2)",
          }}>
            <div style={{ ...S.mono, fontSize: 9, color: "#fbbf24", letterSpacing: "0.08em", marginBottom: 4 }}>
              WHY NO TRADE?
            </div>
            <div style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.5)", lineHeight: 1.5 }}>
              {thesis?.dominant?.state === "watching"
                ? "Thesis watching — no confirmation signal yet"
                : thesis?.dominant?.state === "building"
                ? "Thesis building — waiting for setup confirmation"
                : "Thesis not ready for entry"}
              {active.length > 0 && (
                <span style={{ display: "block", marginTop: 2 }}>
                  {active.length} setup{active.length > 1 ? "s" : ""} forming, none confirmed yet
                </span>
              )}
            </div>
          </div>
        )}

        {past.length > 0 && (
          <>
            <div style={{ height: 0.5, background: "rgba(255,255,255,0.06)", margin: "6px 0" }} />
            <span style={{ ...S.panelLbl, display: "block", marginBottom: 6 }}>Past (TTL)</span>
            {past.map((s) => <SetupItem key={s.setup_id} setup={s} past />)}
          </>
        )}
      </div>
    </div>
  );
}

// ─── Flow panel ───────────────────────────────────────────────────────────────

type PositionData = {
  signal_type: "would_enter" | "would_hold" | "would_exit";
  setup_type: string;
  direction: "buy" | "sell";
  entry_price: string;
  stop: string;
  target: string;
  current_price: string;
  grade: string;
  // Entry
  intrabar_delta?: number | null;
  bid_ask_imbalance?: number | null;
  // Exit
  exit_reason?: string | null;
  pnl_pts?: string | null;
  bars_held?: number | null;
  // Hold
  bars_held_so_far?: number | null;
  mfe?: string | null;
  mae?: string | null;
};

function PositionPanel({ position }: { position: PositionData | null }) {
  if (!position) return null;

  const isLong = position.direction === "buy";
  const entryPrice = parseFloat(position.entry_price);
  const currentPrice = parseFloat(position.current_price);
  const stop = parseFloat(position.stop);
  const target = parseFloat(position.target);
  const unrealized = isLong ? currentPrice - entryPrice : entryPrice - currentPrice;
  const distToStop = isLong ? currentPrice - stop : stop - currentPrice;
  const distToTarget = isLong ? target - currentPrice : currentPrice - target;
  const rr = Math.abs(target - entryPrice) / Math.abs(stop - entryPrice);

  const dirColor = isLong ? "#26a69a" : "#ef5350";
  const pnlColor = unrealized >= 0 ? "#26a69a" : "#ef5350";

  const signalLabel =
    position.signal_type === "would_enter" ? "ENTRY SIGNAL"
    : position.signal_type === "would_exit" ? "EXITED"
    : "IN TRADE";

  const signalColor =
    position.signal_type === "would_enter" ? "#f0b429"
    : position.signal_type === "would_exit" ? "rgba(255,255,255,0.4)"
    : "#26a69a";

  return (
    <div style={{ ...S.panel, borderColor: signalColor + "55" }}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Shadow Position</span>
        <span style={{ ...S.mono, fontSize: 10, color: signalColor, fontWeight: 700 }}>{signalLabel}</span>
      </div>

      {/* Direction + setup type */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
        <span style={{ ...S.mono, fontSize: 11, color: dirColor, fontWeight: 700 }}>
          {isLong ? "▲ LONG" : "▼ SHORT"}
        </span>
        <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.5)" }}>
          {position.setup_type.replace(/_/g, " ")}
        </span>
        <span style={{
          fontSize: 9, fontWeight: 700, padding: "1px 5px",
          borderRadius: 3, background: "rgba(255,255,255,0.1)",
          color: "rgba(255,255,255,0.8)", marginLeft: "auto"
        }}>{position.grade}</span>
      </div>

      {/* Price levels */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 4, marginBottom: 6 }}>
        {[
          { label: "Entry", value: entryPrice.toFixed(2), color: "rgba(255,255,255,0.7)" },
          { label: "Stop", value: stop.toFixed(2), color: "#ef5350" },
          { label: "Target", value: target.toFixed(2), color: "#26a69a" },
        ].map(({ label, value, color }) => (
          <div key={label} style={{ textAlign: "center" }}>
            <div style={{ fontSize: 9, color: "rgba(255,255,255,0.4)", marginBottom: 1 }}>{label}</div>
            <div style={{ ...S.mono, fontSize: 11, color }}>{value}</div>
          </div>
        ))}
      </div>

      {/* Exit result */}
      {position.signal_type === "would_exit" && (
        <div style={{ padding: "4px 8px", borderRadius: 4, background: "rgba(255,255,255,0.06)", marginBottom: 6, textAlign: "center" }}>
          <span style={{ fontSize: 9, color: "rgba(255,255,255,0.5)" }}>
            {(position.exit_reason ?? "").replace(/_/g, " ").toUpperCase()} ·{" "}
          </span>
          <span style={{ ...S.mono, fontSize: 12, color: parseFloat(position.pnl_pts ?? "0") >= 0 ? "#26a69a" : "#ef5350", fontWeight: 700 }}>
            {parseFloat(position.pnl_pts ?? "0") >= 0 ? "+" : ""}{parseFloat(position.pnl_pts ?? "0").toFixed(2)} pts
          </span>
          {position.bars_held != null && (
            <span style={{ fontSize: 9, color: "rgba(255,255,255,0.4)" }}> · {position.bars_held}s held</span>
          )}
        </div>
      )}

      {/* In-trade metrics */}
      {position.signal_type === "would_hold" && (
        <>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
            <span style={{ fontSize: 10, color: "rgba(255,255,255,0.4)" }}>Unrealized</span>
            <span style={{ ...S.mono, fontSize: 11, color: pnlColor, fontWeight: 700 }}>
              {unrealized >= 0 ? "+" : ""}{unrealized.toFixed(2)} pts
            </span>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 4, marginBottom: 4 }}>
            {[
              { label: "→ Stop", value: distToStop.toFixed(1), color: "#ef5350" },
              { label: "→ Tgt", value: distToTarget.toFixed(1), color: "#26a69a" },
              { label: "MFE", value: position.mfe ? parseFloat(position.mfe).toFixed(1) : "—", color: "#26a69a" },
              { label: "MAE", value: position.mae ? parseFloat(position.mae).toFixed(1) : "—", color: "#ef5350" },
            ].map(({ label, value, color }) => (
              <div key={label} style={{ textAlign: "center" }}>
                <div style={{ fontSize: 9, color: "rgba(255,255,255,0.4)" }}>{label}</div>
                <div style={{ ...S.mono, fontSize: 10, color }}>{value}</div>
              </div>
            ))}
          </div>
          {position.bars_held_so_far != null && (
            <div style={{ fontSize: 9, color: "rgba(255,255,255,0.3)", textAlign: "right" }}>
              {position.bars_held_so_far}s elapsed
            </div>
          )}
        </>
      )}

      {/* Entry confirmation signals */}
      {position.signal_type === "would_enter" && (
        <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
          <div style={{ flex: 1, textAlign: "center" }}>
            <div style={{ fontSize: 9, color: "rgba(255,255,255,0.4)" }}>R:R</div>
            <div style={{ ...S.mono, fontSize: 11, color: "rgba(255,255,255,0.8)" }}>{rr.toFixed(2)}</div>
          </div>
          {position.intrabar_delta != null && (
            <div style={{ flex: 1, textAlign: "center" }}>
              <div style={{ fontSize: 9, color: "rgba(255,255,255,0.4)" }}>Δ Delta</div>
              <div style={{ ...S.mono, fontSize: 11, color: position.intrabar_delta > 0 ? "#26a69a" : "#ef5350" }}>
                {position.intrabar_delta > 0 ? "+" : ""}{position.intrabar_delta}
              </div>
            </div>
          )}
          {position.bid_ask_imbalance != null && (
            <div style={{ flex: 1, textAlign: "center" }}>
              <div style={{ fontSize: 9, color: "rgba(255,255,255,0.4)" }}>BAI</div>
              <div style={{ ...S.mono, fontSize: 11, color: position.bid_ask_imbalance > 0.5 ? "#26a69a" : "#ef5350" }}>
                {position.bid_ask_imbalance.toFixed(3)}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

type FlowData = {
  available: boolean;
  bar_ts?: string;
  total_volume?: number;
  buy_volume?: number;
  sell_volume?: number;
  delta?: number;
  delta_pct?: number | null;
  large_buy_count?: number;
  large_sell_count?: number;
  large_trade_threshold?: number;
  bid_ask_imbalance?: number | null;
  twap_bid_size?: number | null;
  twap_ask_size?: number | null;
  trade_count?: number;
  trade_velocity?: number | null;
  avg_trade_size?: number | null;
  is_genuine_sweep_reversal?: boolean;
  is_v_reversal?: boolean;
  absorption?: { detected: boolean; confidence: number; sell_volume_at_low: number };
  split?: { sweep_volume: number; sweep_delta: number; recovery_volume: number; recovery_delta: number; recovery_ratio: number | null } | null;
  has_trade_data?: boolean;
  has_quote_data?: boolean;
};

function DeltaSparkline({ history }: { history: number[] }) {
  if (history.length < 2) return null;
  const w = 196, h = 36;
  const min = Math.min(...history);
  const max = Math.max(...history);
  const range = max - min || 1;
  const zero = max >= 0 && min <= 0 ? h - ((0 - min) / range) * h : (min >= 0 ? h : 0);

  const pts = history.map((v, i) => {
    const x = (i / (history.length - 1)) * w;
    const y = h - ((v - min) / range) * h;
    return `${x},${y}`;
  }).join(" ");

  const lastVal = history[history.length - 1];
  const prevVal = history[history.length - 2];
  const slope = lastVal - prevVal;
  const lineColor = lastVal > 0 ? "#26a69a" : lastVal < 0 ? "#ef5350" : "rgba(255,255,255,0.3)";
  const slopeLabel = slope > 0 ? "↑" : slope < 0 ? "↓" : "→";
  const slopeColor = slope > 2 ? "#26a69a" : slope < -2 ? "#ef5350" : "rgba(255,255,255,0.4)";

  return (
    <div style={{ position: "relative" }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 2 }}>
        <span style={{ ...S.mono, fontSize: 8, color: "rgba(255,255,255,0.3)" }}>Δ slope</span>
        <span style={{ ...S.mono, fontSize: 8, color: slopeColor }}>{slopeLabel} {slope > 0 ? "+" : ""}{slope}</span>
      </div>
      <svg width={w} height={h} style={{ display: "block", overflow: "visible" }}>
        {/* Zero line */}
        <line x1={0} y1={zero} x2={w} y2={zero}
          stroke="rgba(255,255,255,0.1)" strokeWidth={0.5} strokeDasharray="2,2" />
        {/* Delta curve */}
        <polyline points={pts} fill="none" stroke={lineColor} strokeWidth={1.2} />
        {/* Last point dot */}
        <circle
          cx={(history.length - 1) / (history.length - 1) * w}
          cy={h - ((lastVal - min) / range) * h}
          r={2} fill={lineColor}
        />
      </svg>
    </div>
  );
}

function FlowPanel({ flow, live, deltaHistory }: { flow: FlowData | null; live?: boolean; deltaHistory?: number[] }) {
  if (!flow || !flow.available) {
    return (
      <div style={{ ...S.panel, borderColor: "rgba(255,255,255,0.05)" }}>
        <div style={S.panelHd}><span style={S.panelLbl}>Order Flow</span></div>
        <div style={{ padding: "8px 10px", ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.2)" }}>
          {flow ? "No flow data for this bar" : "Waiting…"}
        </div>
      </div>
    );
  }

  const total = flow.total_volume ?? 0;
  const buyVol = flow.buy_volume ?? 0;
  const sellVol = flow.sell_volume ?? 0;
  const buyPct = total > 0 ? (buyVol / total) * 100 : 50;
  const sellPct = total > 0 ? (sellVol / total) * 100 : 50;
  const delta = flow.delta ?? 0;
  const deltaPct = flow.delta_pct ?? null;
  const deltaColor = delta > 0 ? "#22c55e" : delta < 0 ? "#ef4444" : "rgba(255,255,255,0.4)";
  const imbalance = flow.bid_ask_imbalance ?? null;
  // bid_ask_imbalance = twap_bid / (bid+ask); >0.5 = bid-heavy (buyers); <0.5 = ask-heavy (sellers)
  const imbalancePct = imbalance !== null ? Math.round(imbalance * 100) : null;
  const imbalanceColor = imbalance !== null
    ? imbalance > 0.55 ? "#22c55e" : imbalance < 0.45 ? "#ef4444" : "rgba(255,255,255,0.5)"
    : "rgba(255,255,255,0.3)";

  return (
    <div style={{ ...S.panel, borderColor: "rgba(255,255,255,0.05)" }}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Order Flow</span>
        <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
          {live && <span style={{ ...S.mono, fontSize: 8, color: "#22c55e" }}>● LIVE</span>}
          {!flow.has_trade_data && (
            <span style={{ ...S.mono, fontSize: 8, color: "#fbbf24" }}>no tape</span>
          )}
        </div>
      </div>
      <div style={{ padding: "6px 10px", display: "flex", flexDirection: "column", gap: 6 }}>

        {/* Buy / Sell volume bar */}
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
            <span style={{ ...S.mono, fontSize: 9, color: "#22c55e" }}>B {buyVol.toLocaleString()}</span>
            <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.3)" }}>vol {total.toLocaleString()}</span>
            <span style={{ ...S.mono, fontSize: 9, color: "#ef4444" }}>{sellVol.toLocaleString()} S</span>
          </div>
          <div style={{ height: 6, borderRadius: 3, overflow: "hidden", display: "flex", background: "rgba(255,255,255,0.06)" }}>
            <div style={{ width: `${buyPct}%`, background: "rgba(34,197,94,0.6)", transition: "width 0.3s" }} />
            <div style={{ width: `${sellPct}%`, background: "rgba(239,68,68,0.6)", transition: "width 0.3s" }} />
          </div>
        </div>

        {/* Delta */}
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.35)" }}>Delta</span>
          <span style={{ ...S.mono, fontSize: 11, fontWeight: 600, color: deltaColor }}>
            {delta > 0 ? "+" : ""}{delta.toLocaleString()}
            {deltaPct !== null && (
              <span style={{ fontSize: 9, fontWeight: 400, marginLeft: 4, color: "rgba(255,255,255,0.35)" }}>
                ({deltaPct > 0 ? "+" : ""}{deltaPct}%)
              </span>
            )}
          </span>
        </div>

        {/* Delta slope sparkline */}
        {deltaHistory && deltaHistory.length >= 2 && (
          <DeltaSparkline history={deltaHistory} />
        )}

        {/* Bid/Ask imbalance */}
        {imbalancePct !== null && (
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
              <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.35)" }}>Quote pressure</span>
              <span style={{ ...S.mono, fontSize: 9, color: imbalanceColor }}>
                {imbalancePct > 50 ? `${imbalancePct}% bid` : `${100 - imbalancePct}% ask`}
              </span>
            </div>
            <div style={{ height: 4, borderRadius: 2, overflow: "hidden", display: "flex", background: "rgba(255,255,255,0.06)" }}>
              <div style={{ width: `${imbalancePct}%`, background: "rgba(34,197,94,0.5)", transition: "width 0.3s" }} />
              <div style={{ width: `${100 - imbalancePct}%`, background: "rgba(239,68,68,0.5)", transition: "width 0.3s" }} />
            </div>
          </div>
        )}

        {/* Large trades */}
        {((flow.large_buy_count ?? 0) > 0 || (flow.large_sell_count ?? 0) > 0) && (
          <div style={{ display: "flex", justifyContent: "space-between" }}>
            <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.35)" }}>
              Large (≥{flow.large_trade_threshold})
            </span>
            <span style={{ ...S.mono, fontSize: 9 }}>
              <span style={{ color: "#22c55e" }}>▲{flow.large_buy_count ?? 0}</span>
              <span style={{ color: "rgba(255,255,255,0.2)", margin: "0 4px" }}>|</span>
              <span style={{ color: "#ef4444" }}>▼{flow.large_sell_count ?? 0}</span>
            </span>
          </div>
        )}

        {/* Signals */}
        {(flow.is_genuine_sweep_reversal || flow.is_v_reversal || flow.absorption?.detected) && (
          <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
            {flow.is_genuine_sweep_reversal && (
              <span style={{ ...S.mono, fontSize: 8, padding: "1px 5px", borderRadius: 3, background: "rgba(34,197,94,0.15)", color: "#22c55e", border: "0.5px solid rgba(34,197,94,0.3)" }}>
                SWEEP REVERSAL
              </span>
            )}
            {flow.absorption?.detected && (
              <span style={{ ...S.mono, fontSize: 8, padding: "1px 5px", borderRadius: 3, background: "rgba(96,165,250,0.15)", color: "#60a5fa", border: "0.5px solid rgba(96,165,250,0.3)" }}>
                ABSORPTION {Math.round((flow.absorption.confidence ?? 0) * 100)}%
              </span>
            )}
            {flow.is_v_reversal && !flow.is_genuine_sweep_reversal && (
              <span style={{ ...S.mono, fontSize: 8, padding: "1px 5px", borderRadius: 3, background: "rgba(251,191,36,0.12)", color: "#fbbf24", border: "0.5px solid rgba(251,191,36,0.3)" }}>
                V-SNAP
              </span>
            )}
          </div>
        )}

        {/* Split bar */}
        {flow.split && (
          <div style={{ borderTop: "0.5px solid rgba(255,255,255,0.06)", paddingTop: 5 }}>
            <div style={{ ...S.mono, fontSize: 8, color: "rgba(255,255,255,0.25)", marginBottom: 3 }}>Intrabar split</div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ ...S.mono, fontSize: 9, color: "#ef4444" }}>
                Sweep Δ {flow.split.sweep_delta.toLocaleString()}
              </span>
              <span style={{ ...S.mono, fontSize: 9, color: "#22c55e" }}>
                Recovery Δ +{flow.split.recovery_delta.toLocaleString()}
                {flow.split.recovery_ratio !== null && (
                  <span style={{ color: "rgba(255,255,255,0.35)", marginLeft: 4 }}>
                    ({Math.round(flow.split.recovery_ratio * 100)}%)
                  </span>
                )}
              </span>
            </div>
          </div>
        )}

        <div style={{ ...S.mono, fontSize: 8, color: "rgba(255,255,255,0.15)", marginTop: 1 }}>
          {flow.trade_count ?? 0} trades · {flow.trade_velocity?.toFixed(1) ?? "—"}/s · avg {flow.avg_trade_size?.toFixed(1) ?? "—"} lots
        </div>
      </div>
    </div>
  );
}

// ─── Freshness / debug card ────────────────────────────────────────────────────

function FreshnessCard({ debug }: { debug: PipelineDebug | null }) {
  function fmtTs(iso: string | null): string {
    if (!iso) return "—";
    try {
      const d = new Date(iso);
      return `${String(d.getUTCHours()).padStart(2,"0")}:${String(d.getUTCMinutes()).padStart(2,"0")}:${String(d.getUTCSeconds()).padStart(2,"0")}.${String(d.getUTCMilliseconds()).padStart(3,"0")}`;
    } catch { return "—"; }
  }

  const rows: Array<{ label: string; value: string; dim?: boolean; color?: string }> = debug
    ? [
        { label: "Last M1 bar",    value: fmtTs(debug.bar_ts) },
        { label: "Pipeline seal",  value: fmtTs(debug.pipeline_ts) },
        { label: "MarketState ts", value: fmtTs(debug.market_state_ts) },
        { label: "Flow data",      value: debug.flow_available ? "yes" : "no", color: debug.flow_available ? "#22c55e" : "#ef4444" },
        { label: "Active setups",  value: String(debug.active_setup_count) },
        { label: "Scored setups",  value: String(debug.scored_setup_count) },
      ]
    : [{ label: "Waiting for pipeline…", value: "", dim: true }];

  return (
    <div style={{ ...S.panel, borderColor: "rgba(255,255,255,0.05)" }}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Data Freshness</span>
        <span style={{ ...S.mono, fontSize: 9, color: debug ? "#22c55e" : "rgba(255,255,255,0.25)" }}>
          {debug ? "live" : "waiting"}
        </span>
      </div>
      <div style={{ padding: "6px 10px", display: "flex", flexDirection: "column", gap: 3 }}>
        {rows.map(({ label, value, dim, color }) => (
          <div key={label} style={{ display: "flex", justifyContent: "space-between", gap: 4 }}>
            <span style={{ ...S.mono, fontSize: 9, color: dim ? "rgba(255,255,255,0.2)" : "rgba(255,255,255,0.35)" }}>
              {label}
            </span>
            <span style={{ ...S.mono, fontSize: 9, color: color ?? (dim ? "rgba(255,255,255,0.15)" : "rgba(255,255,255,0.7)") }}>
              {value}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Thesis panel ─────────────────────────────────────────────────────────────

const THESIS_LABELS: Record<string, string> = {
  fake_breakdown_reclaim_long: "FAKE BREAKDOWN RECLAIM",
  vwap_failed_reclaim_short:   "VWAP FAILED RECLAIM",
};

const THESIS_STATE_COLOR: Record<string, string> = {
  watching:      "rgba(255,255,255,0.35)",
  building:      "#fbbf24",
  ready:         "#22c55e",
  order_working: "#60a5fa",
  triggered:     "#22c55e",
  invalidated:   "#ef4444",
  flipped:       "#a78bfa",
  expired:       "rgba(255,255,255,0.25)",
};

function ThesisPanel({ thesis }: { thesis: ThesisData | null }) {
  const dominant = thesis?.dominant ?? null;

  if (!dominant) {
    return (
      <div style={S.panel}>
        <div style={S.panelHd}>
          <span style={S.panelLbl}>CURRENT THESIS</span>
        </div>
        <div style={{ padding: "10px 12px", ...S.mono, fontSize: 11, color: "rgba(255,255,255,0.3)" }}>
          Watching for setup…
        </div>
      </div>
    );
  }

  const stateColor = THESIS_STATE_COLOR[dominant.state] ?? "rgba(255,255,255,0.4)";
  const label = THESIS_LABELS[dominant.thesis_type] ?? dominant.thesis_type.replace(/_/g, " ").toUpperCase();
  const isLong = dominant.thesis_type.includes("long");
  const dirColor = isLong ? "#22c55e" : "#ef4444";
  const confPct = Math.round(dominant.confidence * 100);
  const confBarFill = Math.round(dominant.confidence * 14); // 14 segments

  // Risk/reward
  const entry = dominant.entry ? Number(dominant.entry) : null;
  const stop  = dominant.stop  ? Number(dominant.stop)  : null;
  const tgt   = dominant.target ? Number(dominant.target) : null;
  const riskPts   = entry !== null && stop  !== null ? Math.abs(entry - stop)  : null;
  const rewardPts = entry !== null && tgt   !== null ? Math.abs(tgt  - entry)  : null;
  const rr = riskPts && rewardPts && riskPts > 0 ? (rewardPts / riskPts) : null;

  const flipLabel = dominant.possible_flip
    ? THESIS_LABELS[dominant.possible_flip] ?? dominant.possible_flip
    : null;

  return (
    <div style={S.panel}>
      {/* Header */}
      <div style={{ ...S.panelHd, borderBottom: "0.5px solid rgba(255,255,255,0.06)" }}>
        <span style={S.panelLbl}>CURRENT THESIS</span>
        <span style={{ ...S.mono, fontSize: 9, color: `${stateColor}`, textTransform: "uppercase", letterSpacing: 1 }}>
          {dominant.state}
        </span>
      </div>

      <div style={{ padding: "9px 12px", display: "flex", flexDirection: "column", gap: 7 }}>

        {/* Thesis type + direction */}
        <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 4 }}>
          <span style={{ ...S.mono, fontSize: 10, fontWeight: 600, color: dirColor, lineHeight: 1.3 }}>
            {label}
          </span>
          <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.4)", whiteSpace: "nowrap" }}>
            {dominant.bars_alive}b
          </span>
        </div>

        {/* Confidence bar */}
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
            <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.4)" }}>confidence</span>
            <span style={{ ...S.mono, fontSize: 9, color: stateColor, fontWeight: 600 }}>{confPct}%</span>
          </div>
          <div style={{ display: "flex", gap: 1.5 }}>
            {Array.from({ length: 14 }).map((_, i) => (
              <div
                key={i}
                style={{
                  flex: 1, height: 4, borderRadius: 1,
                  background: i < confBarFill ? stateColor : "rgba(255,255,255,0.08)",
                }}
              />
            ))}
          </div>
        </div>

        {/* Key level */}
        {dominant.key_level && (
          <div style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.45)" }}>
            VWAP <span style={{ color: "rgba(255,255,255,0.75)" }}>{formatPrice(dominant.key_level)}</span>
            {dominant.sweep_low && (
              <span>  sweep <span style={{ color: "#ef444490" }}>{formatPrice(dominant.sweep_low)}</span></span>
            )}
            {dominant.rejection_high && (
              <span>  rej <span style={{ color: "#ef444490" }}>{formatPrice(dominant.rejection_high)}</span></span>
            )}
          </div>
        )}

        {/* Evidence */}
        {(dominant.evidence_positive.length > 0 || dominant.evidence_negative.length > 0) && (
          <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
            {dominant.evidence_positive.slice(0, 3).map((e, i) => (
              <div key={i} style={{ ...S.mono, fontSize: 9, color: "#22c55e", lineHeight: 1.4 }}>
                + {e}
              </div>
            ))}
            {dominant.evidence_negative.slice(0, 2).map((e, i) => (
              <div key={i} style={{ ...S.mono, fontSize: 9, color: "#ef4444", lineHeight: 1.4 }}>
                − {e}
              </div>
            ))}
          </div>
        )}

        {/* Commit conditions — only show in WATCHING/BUILDING */}
        {["watching", "building"].includes(dominant.state) && dominant.commit_conditions.length > 0 && (
          <div>
            <div style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.3)", marginBottom: 2, textTransform: "uppercase", letterSpacing: 0.5 }}>commit if</div>
            {dominant.commit_conditions.slice(0, 2).map((c, i) => (
              <div key={i} style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.5)", lineHeight: 1.4 }}>
                • {c}
              </div>
            ))}
          </div>
        )}

        {/* Invalidation reason — only when invalidated */}
        {dominant.state === "invalidated" && dominant.invalidation_reason && (
          <div style={{ ...S.mono, fontSize: 9, color: "#ef4444", lineHeight: 1.4 }}>
            ✕ {dominant.invalidation_reason}
          </div>
        )}

        {/* Entry plan — show when READY or ORDER_WORKING */}
        {["ready", "order_working", "triggered"].includes(dominant.state) && entry !== null && stop !== null && (
          <div
            style={{
              background: "rgba(255,255,255,0.04)",
              border: "0.5px solid rgba(255,255,255,0.08)",
              borderRadius: 4, padding: "6px 8px",
              display: "flex", flexDirection: "column", gap: 3,
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.4)" }}>ENTRY</span>
              <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.85)" }}>{formatPrice(dominant.entry)}</span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.4)" }}>STOP</span>
              <span style={{ ...S.mono, fontSize: 9, color: "#ef4444" }}>{formatPrice(dominant.stop)}</span>
            </div>
            {tgt !== null && (
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.4)" }}>TP</span>
                <span style={{ ...S.mono, fontSize: 9, color: "#22c55e" }}>{formatPrice(dominant.target)}</span>
              </div>
            )}
            {riskPts !== null && (
              <div
                style={{
                  display: "flex", justifyContent: "space-between",
                  borderTop: "0.5px solid rgba(255,255,255,0.06)", paddingTop: 3, marginTop: 1,
                }}
              >
                <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.4)" }}>
                  RISK {riskPts.toFixed(2)} pts
                </span>
                {rr !== null && (
                  <span style={{ ...S.mono, fontSize: 9, color: stateColor, fontWeight: 600 }}>
                    {rr.toFixed(1)}R
                  </span>
                )}
              </div>
            )}
          </div>
        )}

        {/* Flip indicator */}
        {flipLabel && dominant.state !== "invalidated" && (
          <div style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.25)", lineHeight: 1.4 }}>
            ↓ flip → {flipLabel.toLowerCase()}
          </div>
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
  selectedSetupId,
  onSelectSetup,
  flashSetupId,
}: {
  context: SetupSessionContext | null;
  selectedDate: string | null;
  selectedSetupId?: string | null;
  onSelectSetup?: (id: string) => void;
  flashSetupId?: string | null;
}) {
  const [expanded, setExpanded] = React.useState<string | null>(null);
  const itemRefs = React.useRef<Map<string, HTMLDivElement>>(new Map());

  React.useEffect(() => {
    if (!selectedSetupId) return;
    const el = itemRefs.current.get(selectedSetupId);
    el?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }, [selectedSetupId]);

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
            const isSelected = selectedSetupId === entry.setup_id;
            const isFlashing = flashSetupId === entry.setup_id;
            const rules = SETUP_RULES[entry.setup_type.toLowerCase()];
            const rr = computeRR(entry);
            const entryColor = entry.side === "buy" ? "#22c55e" : "#ef4444";

            return (
              <div
                key={entry.setup_id}
                style={{ marginBottom: 4 }}
                ref={(el) => {
                  if (el) itemRefs.current.set(entry.setup_id, el);
                  else itemRefs.current.delete(entry.setup_id);
                }}
              >
                <div
                  style={{
                    borderLeft: `2px solid ${isSelected || isFlashing ? entryColor : levelColor(entry.level_tag)}`,
                    padding: "6px 9px",
                    borderRadius: "0 4px 4px 0",
                    background: isFlashing
                      ? `${entryColor}28`
                      : isSelected
                      ? `${entryColor}14`
                      : isOpen
                      ? "rgba(255,255,255,0.05)"
                      : "rgba(255,255,255,0.02)",
                    cursor: "pointer",
                    userSelect: "none",
                    transition: "background 0.3s ease, border-color 0.3s ease",
                  }}
                  onClick={() => {
                    setExpanded(isOpen ? null : entry.setup_id);
                    onSelectSetup?.(entry.setup_id);
                  }}
                >
                  {/* Top row: type + state */}
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 3 }}>
                    <span style={{ ...S.mono, fontSize: 10, fontWeight: 500, color: entry.side === "buy" ? "#22c55e" : "#ef4444" }}>
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

function MarketStatePanel({
  marketState,
  isHistorical,
}: {
  marketState: MarketStateData | null;
  isHistorical?: boolean;
}) {
  const dayType = marketState?.day_type;
  const dayTypeStatus = marketState?.day_type_status;
  const liveBias = marketState?.live_bias;
  const trend = marketState?.trend;
  const vwapState = marketState?.vwap_state;
  const orb = marketState?.orb_state;
  const structure = marketState?.structure_score;
  const confidence = marketState?.confidence;
  const trendBars = marketState?.trend_bars;
  const perm = tradePermissionSummary(marketState ?? null);

  const isLocked = dayType && dayType !== "unknown";
  const hasData = marketState != null;

  return (
    <div style={S.panel}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Market State</span>
        {isHistorical && !hasData && (
          <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.3)", fontStyle: "italic" }}>
            scrub to see
          </span>
        )}
      </div>

      {/* ── Layer 1: Day type + status ── */}
      <div
        style={{
          margin: "6px 8px 0",
          padding: "8px 10px",
          borderRadius: 5,
          background: isLocked ? `${dayTypeColor(dayType)}14` : "rgba(255,255,255,0.03)",
          border: `0.5px solid ${isLocked ? `${dayTypeColor(dayType)}40` : "rgba(255,255,255,0.08)"}`,
        }}
      >
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
          <div>
            <div style={{ fontSize: 9, ...S.mono, color: "rgba(255,255,255,0.3)", letterSpacing: "0.1em", marginBottom: 3 }}>
              DAY TYPE
            </div>
            <div style={{ ...S.mono, fontSize: 13, fontWeight: 600, color: dayTypeColor(dayType), letterSpacing: "0.04em" }}>
              {dayTypeLabel(dayType)}
            </div>
          </div>
          <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 4 }}>
            <Pill color={dayTypePillColor(dayType)}>
              {isLocked ? "LOCKED" : "FORMING"}
            </Pill>
          </div>
        </div>
        {/* Status + confidence on same line */}
        {isLocked && dayTypeStatus && (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: 5 }}>
            <span style={{
              ...S.mono, fontSize: 9, fontWeight: 500, letterSpacing: "0.05em",
              color: dayTypeStatusColor(dayTypeStatus),
            }}>
              {dayTypeStatusLabel(dayTypeStatus)}
            </span>
            {confidence != null && (
              <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.35)" }}>
                conf {(confidence * 100).toFixed(0)}%
              </span>
            )}
          </div>
        )}
      </div>

      {/* ── Layer 2: Live bias ── */}
      <div
        style={{
          margin: "4px 8px 0",
          padding: "6px 10px",
          borderRadius: 5,
          background: "rgba(255,255,255,0.02)",
          border: "0.5px solid rgba(255,255,255,0.06)",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <div>
          <div style={{ fontSize: 9, ...S.mono, color: "rgba(255,255,255,0.3)", letterSpacing: "0.1em", marginBottom: 3 }}>
            LIVE BIAS
          </div>
          <div style={{ ...S.mono, fontSize: 11, fontWeight: 500, color: liveBiasColor(liveBias) }}>
            {liveBiasLabel(liveBias)}
          </div>
        </div>
        <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.2)", fontStyle: "italic" }}>
          UPDATING
        </span>
      </div>

      {/* ── Layer 3: Trade permission ── */}
      <div
        style={{
          margin: "4px 8px 6px",
          padding: "6px 10px",
          borderRadius: 5,
          background: `${perm.color}0d`,
          border: `0.5px solid ${perm.color}40`,
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <div>
          <div style={{ fontSize: 9, ...S.mono, color: "rgba(255,255,255,0.3)", letterSpacing: "0.1em", marginBottom: 3 }}>
            PERMISSION
          </div>
          <div style={{ ...S.mono, fontSize: 11, fontWeight: 600, color: perm.color }}>
            {perm.label}
          </div>
        </div>
      </div>

      {/* ── Supporting rows ── */}
      <div style={{ padding: "0 12px 8px" }}>
        <MsRow
          label="Trend"
          value={trend ? `${trend.replace(/_/g, " ").toUpperCase()}${trendBars ? ` · ${trendBars}b` : ""}` : "—"}
          valueColor={trendColor(trend)}
        />
        <MsRow
          label="VWAP"
          value={(vwapState ?? "—").replace(/_/g, " ").toUpperCase()}
          valueColor={regimeColor(vwapState)}
        />
        <MsRow
          label="ORB"
          value={(orb ?? "—").replace(/_/g, " ").toUpperCase()}
          valueColor={orb?.includes("breakout") ? "#22c55e" : orb?.includes("breakdown") ? "#ef4444" : undefined}
        />
        <MsRow
          label="Structure"
          value={structure != null ? structure.toFixed(2) : "—"}
          valueColor={structure != null && structure > 0.7 ? "#22c55e" : undefined}
          last
        />
      </div>
    </div>
  );
}

function RiskPanel({ risk }: { risk: RiskData | null }) {
  // Derive a single summary across all accounts for the sidebar
  // Guard against old flat-format snapshots which produce primitive values
  const accounts = Object.values(risk ?? {}).filter(isAccountRiskState);
  const anyHalted = accounts.some((a) => a.is_halted);
  const totalPnl = accounts.length > 0
    ? accounts.reduce((sum, a) => sum + a.realized_pnl, 0)
    : null;
  const totalUnreal = accounts.length > 0
    ? accounts.reduce((sum, a) => sum + a.unrealized_pnl, 0)
    : null;
  const worstRiskUsed = accounts.length > 0
    ? Math.max(...accounts.map((a) => a.risk_consumed_pct))
    : null;
  const haltedAccounts = accounts.filter((a) => a.is_halted).map((a) => a.account_id.toUpperCase());

  return (
    <div style={S.panel}>
      <div style={S.panelHd}>
        <span style={S.panelLbl}>Risk</span>
        {accounts.length > 1 && (
          <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.3)" }}>
            {accounts.length} accounts
          </span>
        )}
      </div>
      <div style={{ padding: "4px 12px 8px" }}>
        <MsRow
          label="Total P&L"
          value={totalPnl != null ? formatPnl(totalPnl) : "—"}
          valueColor={totalPnl != null ? (totalPnl >= 0 ? "#22c55e" : "#ef4444") : undefined}
        />
        <MsRow
          label="Unreal"
          value={totalUnreal != null ? formatPnl(totalUnreal) : "—"}
          valueColor={totalUnreal != null ? (totalUnreal >= 0 ? "#22c55e" : "#fbbf24") : undefined}
        />
        <MsRow
          label="Risk used"
          value={worstRiskUsed != null ? `${(worstRiskUsed * 100).toFixed(0)}%` : "—"}
          valueColor={worstRiskUsed != null && worstRiskUsed > 0.7 ? "#ef4444" : "#fbbf24"}
          last
        />
        <div
          style={{
            marginTop: 8, display: "flex", alignItems: "center", gap: 5, padding: "5px 8px",
            background: anyHalted ? "rgba(239,68,68,0.07)" : "rgba(34,197,94,0.07)",
            border: `0.5px solid ${anyHalted ? "rgba(239,68,68,0.25)" : "rgba(34,197,94,0.25)"}`,
            borderRadius: 5,
          }}
        >
          <span style={{ ...S.mono, fontSize: 10, fontWeight: 500, letterSpacing: "0.05em", color: anyHalted ? "#ef4444" : "#22c55e" }}>
            {anyHalted ? `HALTED · ${haltedAccounts.join(", ")}` : "TRADING ACTIVE"}
          </span>
        </div>
      </div>
    </div>
  );
}

// ─── Account P&L bar (below chart) ───────────────────────────────────────────

function haltReasonLabel(reason: string | null): string {
  if (!reason) return "—";
  if (reason === "daily_loss_limit") return "LOSS LIMIT";
  if (reason === "profit_protection") return "PROFIT PROTECT";
  if (reason === "manual") return "MANUAL";
  return reason.toUpperCase();
}

function isAccountRiskState(v: unknown): v is AccountRiskState {
  return typeof v === "object" && v !== null && "account_id" in v && "is_halted" in v;
}

function AccountPnlBar({ risk }: { risk: RiskData | null }) {
  // Guard against old flat-format snapshots (pre-migration) which produce primitive values
  const accounts = Object.values(risk ?? {}).filter(isAccountRiskState);
  if (accounts.length === 0) return null;

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: `repeat(${accounts.length}, 1fr)`,
        borderTop: "0.5px solid rgba(255,255,255,0.06)",
      }}
    >
      {accounts.map((acct, idx) => {
        const isHalted = acct.is_halted;
        const pnl = acct.realized_pnl;
        const high = acct.session_high_pnl;
        const limit = acct.daily_loss_limit;
        const pctUsed = acct.risk_consumed_pct;          // 0–1 fraction of limit consumed
        const activation = acct.profit_protect_activation;
        const givebackPct = acct.profit_protect_giveback_pct;
        const openPos = acct.open_positions;
        const trades = acct.trades_taken;

        const nlv = acct.net_liquidation ?? 0;
        const cash = acct.cash_balance ?? 0;
        const gpv = acct.gross_position_value ?? 0;
        const lev = acct.leverage_ratio ?? 0;

        // Leverage risk colouring: >4x = red, >2x = amber, else green
        const levColor = lev > 4 ? "#ef4444" : lev > 2 ? "#fbbf24" : lev > 0.5 ? "#22c55e" : "rgba(255,255,255,0.35)";
        const levLabel = lev >= 10 ? lev.toFixed(1) + "x" : lev > 0 ? lev.toFixed(2) + "x" : "—";
        const levRisk = lev > 4 ? "HIGH" : lev > 2 ? "MED" : lev > 0.5 ? "LOW" : null;

        const pnlColor = isHalted ? "#ef4444" : pnl > 0 ? "#22c55e" : pnl < 0 ? "#ef4444" : "rgba(255,255,255,0.5)";
        const typeLabel = (acct.account_type ?? "unknown").toUpperCase();

        // Profit protection progress: how far we are toward triggering
        // protection activates when session_high >= activation
        // inside that region, giveback allowed = high * givebackPct
        // current giveback = high - pnl
        const protectionActive = high >= activation && high > 0;
        const maxGiveback = high * givebackPct;
        const currentGiveback = Math.max(0, high - pnl);
        const givebackFraction = protectionActive && maxGiveback > 0
          ? Math.min(1, currentGiveback / maxGiveback)
          : 0;
        const givebackColor = givebackFraction > 0.8 ? "#ef4444" : givebackFraction > 0.5 ? "#fbbf24" : "#22c55e";

        // Daily loss bar — fills red as you approach the limit
        const lossBarFraction = Math.min(1, pctUsed);
        const lossBarColor = lossBarFraction > 0.7 ? "#ef4444" : lossBarFraction > 0.4 ? "#fbbf24" : "#22c55e";

        return (
          <div
            key={acct.account_id}
            style={{
              padding: "10px 14px",
              borderRight: idx < accounts.length - 1 ? "0.5px solid rgba(255,255,255,0.06)" : "none",
              background: isHalted ? "rgba(239,68,68,0.04)" : "transparent",
            }}
          >
            {/* Header row: account label + halt badge */}
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <span
                  style={{
                    ...S.mono, fontSize: 9, fontWeight: 600, letterSpacing: "0.1em",
                    padding: "2px 6px", borderRadius: 3,
                    background: acct.account_type === "day" ? "rgba(96,165,250,0.12)" : "rgba(251,191,36,0.12)",
                    color: acct.account_type === "day" ? "#60a5fa" : "#fbbf24",
                    border: `0.5px solid ${acct.account_type === "day" ? "rgba(96,165,250,0.3)" : "rgba(251,191,36,0.3)"}`,
                  }}
                >
                  {typeLabel}
                </span>
                <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.35)" }}>
                  {acct.account_id}
                </span>
              </div>
              <div
                style={{
                  display: "flex", alignItems: "center", gap: 4,
                  padding: "2px 7px", borderRadius: 100,
                  background: isHalted ? "rgba(239,68,68,0.12)" : "rgba(34,197,94,0.08)",
                  border: `0.5px solid ${isHalted ? "rgba(239,68,68,0.35)" : "rgba(34,197,94,0.25)"}`,
                }}
              >
                <span style={{ width: 5, height: 5, borderRadius: "50%", background: isHalted ? "#ef4444" : "#22c55e", display: "inline-block" }} />
                <span style={{ ...S.mono, fontSize: 9, fontWeight: 600, letterSpacing: "0.06em", color: isHalted ? "#ef4444" : "#22c55e" }}>
                  {isHalted ? haltReasonLabel(acct.halt_reason) : "ACTIVE"}
                </span>
              </div>
            </div>

            {/* P&L + session high row */}
            <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 6 }}>
              <span style={{ ...S.mono, fontSize: 22, fontWeight: 600, color: pnlColor, letterSpacing: "-0.01em" }}>
                {formatPnl(pnl)}
              </span>
              {high > 0 && (
                <span style={{ ...S.mono, fontSize: 11, color: "rgba(255,255,255,0.3)" }}>
                  HIGH <span style={{ color: "#22c55e" }}>{formatPnl(high)}</span>
                </span>
              )}
            </div>

            {/* Stats row */}
            <div style={{ display: "flex", gap: 12, marginBottom: 6, flexWrap: "wrap" }}>
              <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.35)" }}>
                LIMIT <span style={{ color: "#ef4444" }}>−{formatPnl(limit)}</span>
              </span>
              <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.35)" }}>
                POS <span style={{ color: "rgba(255,255,255,0.7)" }}>{openPos}</span>
              </span>
              <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.35)" }}>
                TRADES <span style={{ color: "rgba(255,255,255,0.7)" }}>{trades}</span>
              </span>
            </div>

            {/* Account metrics row — NLV / Cash / Leverage */}
            {nlv > 0 && (
              <div
                style={{
                  display: "flex", gap: 0, marginBottom: 8,
                  background: "rgba(255,255,255,0.03)",
                  border: "0.5px solid rgba(255,255,255,0.07)",
                  borderRadius: 4, overflow: "hidden",
                }}
              >
                {/* Net Liquidation */}
                <div style={{ flex: 1, padding: "5px 8px", borderRight: "0.5px solid rgba(255,255,255,0.07)" }}>
                  <div style={{ ...S.mono, fontSize: 8, color: "rgba(255,255,255,0.3)", letterSpacing: "0.08em", marginBottom: 2 }}>NLV</div>
                  <div style={{ ...S.mono, fontSize: 11, fontWeight: 600, color: "rgba(255,255,255,0.8)" }}>
                    {nlv >= 1000 ? `$${(nlv / 1000).toFixed(1)}k` : `$${nlv.toFixed(0)}`}
                  </div>
                </div>
                {/* Cash */}
                <div style={{ flex: 1, padding: "5px 8px", borderRight: "0.5px solid rgba(255,255,255,0.07)" }}>
                  <div style={{ ...S.mono, fontSize: 8, color: "rgba(255,255,255,0.3)", letterSpacing: "0.08em", marginBottom: 2 }}>CASH</div>
                  <div style={{ ...S.mono, fontSize: 11, fontWeight: 600, color: cash < 0 ? "#ef4444" : "rgba(255,255,255,0.8)" }}>
                    {cash >= 1000 ? `$${(cash / 1000).toFixed(1)}k` : cash < -1000 ? `−$${(Math.abs(cash) / 1000).toFixed(1)}k` : `$${cash.toFixed(0)}`}
                  </div>
                </div>
                {/* Leverage */}
                <div style={{ flex: 1, padding: "5px 8px" }}>
                  <div style={{ ...S.mono, fontSize: 8, color: "rgba(255,255,255,0.3)", letterSpacing: "0.08em", marginBottom: 2 }}>
                    LEVERAGE{levRisk && <span style={{ marginLeft: 4, color: levColor }}>{levRisk}</span>}
                  </div>
                  <div style={{ ...S.mono, fontSize: 11, fontWeight: 600, color: levColor }}>
                    {levLabel}
                  </div>
                </div>
              </div>
            )}

            {/* Daily loss bar */}
            <div style={{ marginBottom: protectionActive ? 6 : 0 }}>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
                <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.3)", letterSpacing: "0.08em" }}>
                  DAILY LOSS
                </span>
                <span style={{ ...S.mono, fontSize: 9, color: lossBarFraction > 0.5 ? lossBarColor : "rgba(255,255,255,0.3)" }}>
                  {(lossBarFraction * 100).toFixed(0)}%
                </span>
              </div>
              <div style={{ height: 4, background: "rgba(255,255,255,0.06)", borderRadius: 2, overflow: "hidden" }}>
                <div
                  style={{
                    height: "100%",
                    width: `${lossBarFraction * 100}%`,
                    background: lossBarColor,
                    borderRadius: 2,
                    transition: "width 0.4s ease",
                  }}
                />
              </div>
            </div>

            {/* Profit protection bar — only shown once high-water mark passes activation */}
            {protectionActive && (
              <div>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3, marginTop: 6 }}>
                  <span style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.3)", letterSpacing: "0.08em" }}>
                    PROFIT PROTECT
                  </span>
                  <span style={{ ...S.mono, fontSize: 9, color: givebackFraction > 0.5 ? givebackColor : "rgba(255,255,255,0.3)" }}>
                    {(givebackFraction * 100).toFixed(0)}% giveback
                  </span>
                </div>
                <div style={{ height: 4, background: "rgba(255,255,255,0.06)", borderRadius: 2, overflow: "hidden" }}>
                  <div
                    style={{
                      height: "100%",
                      width: `${givebackFraction * 100}%`,
                      background: givebackColor,
                      borderRadius: 2,
                      transition: "width 0.4s ease",
                    }}
                  />
                </div>
                <div style={{ ...S.mono, fontSize: 9, color: "rgba(255,255,255,0.2)", marginTop: 3 }}>
                  max giveback {formatPnl(maxGiveback)} of {formatPnl(high)}
                </div>
              </div>
            )}
          </div>
        );
      })}
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
  const [marketStates, setMarketStates] = useState<Record<string, MarketStateData | null>>({});
  const [quotes, setQuotes] = useState<Record<string, QuoteRow | null>>({});
  const [bars, setBars] = useState<BarHistoryRow[]>([]);
  const [setups, setSetups] = useState<SetupRow[]>([]);
  const [thesis, setThesis] = useState<ThesisData | null>(null);
  const [setupContexts, setSetupContexts] = useState<Record<string, SetupSessionContext | null>>({});
  const [prevSetupContexts, setPrevSetupContexts] = useState<Record<string, SetupSessionContext | null>>({});
  const [riskState, setRiskState] = useState<RiskData | null>(null);
  const [clock, setClock] = useState("--:--:-- ET");
  const [error, setError] = useState<string | null>(null);
  // Backfill state
  const [backfillJob, setBackfillJob] = useState<BackfillJob | null>(null);
  const [availableDates, setAvailableDates] = useState<AvailableDates | null>(null);
  // Incremented after a backfill completes to force a data reload
  const [refreshKey, setRefreshKey] = useState(0);
  const [selectedSetupId, setSelectedSetupId] = useState<string | null>(null);
  // Replay mode — only active in historical (selectedDate !== null)
  const [replayMode, setReplayMode] = useState(false);
  const [replayIndex, setReplayIndex] = useState(0);   // 0 = empty, bars.length = full
  const [replayPlaying, setReplayPlaying] = useState(false);
  const [replaySpeed, setReplaySpeed] = useState(5);   // bars per second
  const [replayEpoch, setReplayEpoch] = useState(0);   // incremented to trigger chart fitContent
  const flashRef = useRef<Set<string>>(new Set());
  const [flashSetupId, setFlashSetupId] = useState<string | null>(null);
  const [pipelineDebug, setPipelineDebug] = useState<PipelineDebug | null>(null);
  const [flow, setFlow] = useState<FlowData | null>(null);
  const [intrabarLive, setIntrabarLive] = useState(false);
  const deltaHistoryRef = useRef<number[]>([]);  // rolling 60s delta slope
  const [position, setPosition] = useState<PositionData | null>(null);
  const [sidebarTab, setSidebarTab] = useState<"signal" | "market">("signal");
  // Tracks whether we've done the one-time bar history re-fetch after bootstrap.
  // Bootstrap fills a gap (up to ~20 min) in Parquet that the initial load missed;
  // the first pipeline_complete event signals bars are ready.
  const barsRefreshedRef = useRef(false);

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

        // Date bounds: historical uses the selected date, live uses rolling lookback.
        // For 24h futures (MNQ, NQ, ES, …) the CME session starts at 18:00 ET the
        // *prior* calendar day, whose UTC bars sit in the previous day's partition.
        // Pull histStart back one day so overnight bars are included in the chart.
        const is24hSymbol = /^(MNQ|NQ|ES|MES|RTY|M2K|YM|MYM|CL|GC|SI|NKD|EMD)/.test(symbol);
        const rawHistStart = isHistorical ? selectedDate : historyStartDate(symbol, selectedTimeframe);
        const histStart = (isHistorical && is24hSymbol)
          ? (() => { const d = new Date(rawHistStart + "T00:00:00Z"); d.setUTCDate(d.getUTCDate() - 1); return d.toISOString().slice(0, 10); })()
          : rawHistStart;
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
          // [7] market states (live only — locked per session)
          isHistorical
            ? Promise.resolve({})
            : fetchJson<Record<string, MarketStateData | null>>(`/runtime/market-states?symbol=${symbol}`).catch(() => ({})),
          // [8] thesis (live only — ThesisEngine is a real-time engine)
          isHistorical
            ? Promise.resolve(null)
            : fetchJson<ThesisData>(`/runtime/thesis/${symbol}`).catch(() => null),
        ];

        const [
          contextData,
          quoteData,
          barData,
          latestBarData,
          setupData,
          setupContextData,
          prevSetupContextData,
          marketStateData,
          thesisData,
        ] = await Promise.all(requests) as [
          Record<string, SymbolContext | null>,
          Record<string, QuoteRow | null>,
          BarHistoryRow[],
          Record<string, BarHistoryRow | null>,
          SetupRow[],
          Record<string, SetupSessionContext | null>,
          Record<string, SetupSessionContext | null>,
          Record<string, MarketStateData | null>,
          ThesisData | null,
        ];

        const riskData = isHistorical
          ? null
          : await fetchJson<RiskData>(`/runtime/risk?symbol=${symbol}`).catch(() => null);

        if (cancelled) return;

        setStatus(statusData);
        setSelectedSymbol(symbol);

        if (!isHistorical) {
          setContexts((prev) => ({ ...prev, ...contextData }));
          setMarketStates((prev) => ({ ...prev, ...marketStateData }));
          setQuotes((prev) => ({ ...prev, ...quoteData }));
        }

        const latestLiveBar = !isHistorical ? (latestBarData[symbol] ?? null) : null;
        setBars((prev) =>
          mergeHistoryWithLiveTail(barData, latestLiveBar, prev, selectedTimeframe)
        );
        setSetups(setupData);
        if (!isHistorical) setThesis(thesisData);
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
        if (payload.type !== "runtime_update" || !symbolsMatch(payload.symbol, selectedSymbol)) return;

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
        if (payload.market_state) {
          setMarketStates((prev) => ({ ...prev, [selectedSymbol]: payload.market_state }));
        }
        if (payload.quote) {
          setQuotes((prev) => ({ ...prev, [selectedSymbol]: payload.quote }));

          // Use last trade price to keep the live candle close current between
          // partial bar updates (which arrive every few seconds). The partial
          // bar will correct the full OHLCV when it arrives.
          const { last_price, bid_price, ask_price, timestamp } = payload.quote;
          if (last_price != null) {
            const price = Number(last_price);
            const bid = Number(bid_price);
            const ask = Number(ask_price);
            // Validate last_price is near the current spread to reject off-market prints.
            const mid = isFinite(bid) && isFinite(ask) && ask > bid ? (bid + ask) / 2 : 0;
            const inRange = mid === 0 || Math.abs(price - mid) / mid < 0.005;
            if (isFinite(price) && price > 0 && inRange) {
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

    // Push-based live WS — receives bar and quote events immediately from the EventBus.
    // Only active when alpha run embeds the API (requires injected EventBus).
    const liveWs = new WebSocket(
      `${websocketBaseUrl()}/runtime/ws/live?symbol=${encodeURIComponent(selectedSymbol)}`
    );

    liveWs.onopen = () => {
      // Reset so the first pipeline_complete after this connection triggers a bar refresh.
      barsRefreshedRef.current = false;
    };

    liveWs.onmessage = (ev) => {
      if (isHistorical) return;
      try {
        const msg = JSON.parse(ev.data);
        if (!symbolsMatch(msg.symbol, selectedSymbol)) return;

        if (msg.type === "bar") {
          const tf = String(msg.timeframe).toLowerCase();
          if (tf !== selectedTimeframe) return;
          const barRow: BarHistoryRow = {
            symbol: msg.symbol,
            timeframe: msg.timeframe,
            timestamp: msg.timestamp,
            open: msg.open,
            high: msg.high,
            low: msg.low,
            close: msg.close,
            volume: Number(msg.volume),
            vwap: msg.vwap ?? null,
          };
          setBars((prev) => mergeLiveBar(prev, barRow, selectedTimeframe));
        }

        if (msg.type === "pipeline_complete") {
          // One authoritative packet per M1 bar — update debug state and
          // trigger targeted re-fetch of all panels from fresh REST data.
          setPipelineDebug({
            pipeline_ts: msg.pipeline_ts,
            bar_ts: msg.timestamp,
            market_state_ts: msg.market_state_ts ?? null,
            thesis_type: msg.thesis_type ?? null,
            flow_available: !!msg.flow_available,
            active_setup_count: Number(msg.active_setup_count ?? 0),
            scored_setup_count: Number(msg.scored_setup_count ?? 0),
          });
          // Re-fetch all panels in parallel — each is fast and uses fresh data
          fetchJson<ThesisData>(`/runtime/thesis/${selectedSymbol}`)
            .then((d) => d && setThesis(d)).catch(() => {});
          fetchJson<SetupRow[]>(`/runtime/setups/${selectedSymbol}`)
            .then((d) => d && setSetups(d)).catch(() => {});
          fetchJson<MarketStateData>(`/runtime/market-state/${selectedSymbol}`)
            .then((d) => d && setMarketStates((prev) => ({ ...prev, [selectedSymbol]: d }))).catch(() => {});
          fetchJson<SymbolContext>(`/runtime/context/${selectedSymbol}`)
            .then((d) => d && setContexts((prev) => ({ ...prev, [selectedSymbol]: d }))).catch(() => {});
          // Flow data is embedded in pipeline_complete — set it directly from msg.flow
          if (msg.flow) setFlow(msg.flow as FlowData);
          deltaHistoryRef.current = [];  // new bar — reset sparkline
          // One-time bar history refresh after bootstrap: fills the gap between the
          // initial REST fetch (now-3min) and bars stored during the bootstrap period.
          if (!barsRefreshedRef.current) {
            barsRefreshedRef.current = true;
            const start = historyStartDate(selectedSymbol, selectedTimeframe);
            fetchJson<BarHistoryRow[]>(
              `/runtime/bars/history?symbol=${selectedSymbol}&timeframe=${selectedTimeframe}&start=${start}&end=${todayDate()}`
            ).then((d) => {
              if (d && d.length > 0) setBars((prev) => mergeHistoryWithLiveTail(d, null, prev, selectedTimeframe));
            }).catch(() => {});
          }
        }

        if (msg.type === "position_signal") {
          setPosition(msg as PositionData);
          setSidebarTab("signal");
        }

        if (msg.type === "intrabar_flow") {
          // Accumulate delta history for sparkline (reset each bar via pipeline_complete)
          const h = deltaHistoryRef.current;
          h.push(msg.delta);
          if (h.length > 60) h.shift();
          // Merge live 1s intrabar fields into flow — keep sealed-bar fields from pipeline_complete
          setIntrabarLive(true);
          setFlow((prev) => prev ? {
            ...prev,
            delta: msg.delta,
            buy_volume: msg.buy_volume,
            sell_volume: msg.sell_volume,
            bid_ask_imbalance: msg.bid_ask_imbalance ?? prev.bid_ask_imbalance,
            has_trade_data: (msg.trade_count ?? 0) > 0,
          } : prev);
        }

        if (msg.type === "thesis") {
          // Thesis state transition — re-fetch immediately rather than waiting for next poll
          fetchJson<ThesisData>(`/runtime/thesis/${selectedSymbol}`)
            .then((d) => d && setThesis(d)).catch(() => {});
        }

        if (msg.type === "setup") {
          // Setup state transition — re-fetch immediately
          fetchJson<SetupRow[]>(`/runtime/setups/${selectedSymbol}`)
            .then((d) => d && setSetups(d)).catch(() => {});
        }

        if (msg.type === "quote" && msg.last_price != null) {
          const price = Number(msg.last_price);
          const bid = Number(msg.bid_price);
          const ask = Number(msg.ask_price);
          const mid = isFinite(bid) && isFinite(ask) && ask > bid ? (bid + ask) / 2 : 0;
          const inRange = mid === 0 || Math.abs(price - mid) / mid < 0.005;
          if (isFinite(price) && price > 0) {
            setQuotes((prev) => ({
              ...prev,
              [selectedSymbol]: {
                symbol: msg.symbol,
                bid_price: msg.bid_price,
                ask_price: msg.ask_price,
                bid_size: msg.bid_size,
                ask_size: msg.ask_size,
                last_price: msg.last_price,
                last_size: msg.last_size ?? null,
                timestamp: msg.timestamp,
              },
            }));
            if (inRange) {
              setBars((prev) => {
                if (prev.length === 0) return prev;
                const lastBar = prev[prev.length - 1];
                if (
                  bucketTimestamp(msg.timestamp, selectedTimeframe) !==
                  bucketTimestamp(lastBar.timestamp, selectedTimeframe)
                ) {
                  return prev;
                }
                return [
                  ...prev.slice(0, -1),
                  {
                    ...lastBar,
                    close: String(price),
                  },
                ];
              });
            }
          }
        }
      } catch {
        // Ignore malformed messages
      }
    };

    liveWs.onerror = () => {
      // Push WS is best-effort; polling WS remains the fallback.
    };

    return () => {
      ws.close();
      liveWs.close();
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

  // Exit replay when switching to live or changing date
  useEffect(() => {
    if (selectedDate === null) {
      setReplayMode(false);
      setReplayPlaying(false);
    }
  }, [selectedDate]);

  // Reset position when entering replay or data reloads.
  // For historical dates, skip prior-day overnight bars (loaded for chart context)
  // and start replay at the first bar on the selected calendar date, so they are
  // already visible as static background rather than being replayed from scratch.
  useEffect(() => {
    if (replayMode) {
      let startIdx = 0;
      if (selectedDate && bars.length > 0) {
        const targetPrefix = selectedDate; // "YYYY-MM-DD"
        const idx = bars.findIndex((b) => b.timestamp >= targetPrefix + "T00:00:00");
        if (idx > 0) startIdx = idx;
      }
      setReplayIndex(startIdx);
      setReplayPlaying(false);
      flashRef.current.clear();
      setReplayEpoch((e) => e + 1);
    }
  }, [replayMode, selectedDate, bars]);

  // Replay timer
  useEffect(() => {
    if (!replayPlaying || !replayMode) return;
    const ms = Math.round(1000 / replaySpeed);
    const id = setInterval(() => {
      setReplayIndex((prev) => {
        if (prev >= bars.length) { setReplayPlaying(false); return prev; }
        return prev + 1;
      });
    }, ms);
    return () => clearInterval(id);
  }, [replayPlaying, replayMode, replaySpeed, bars.length]);

  const currentContext = contexts[selectedSymbol] ?? null;
  const currentMarketState = marketStates[selectedSymbol] ?? null;
  const currentQuote = quotes[selectedSymbol] ?? null;
  const _rawSetupContext = setupContexts[selectedSymbol] ?? null;
  const _prevSetupContext = prevSetupContexts[selectedSymbol] ?? null;
  // Fall back to previous session's history when the current session has no setups yet
  // (e.g. after 6 PM ET when CME session rolls but today's setups should remain visible)
  const currentSetupContext =
    (_rawSetupContext?.setups?.length ?? 0) > 0 ? _rawSetupContext : (_prevSetupContext ?? _rawSetupContext);

  // ── Replay derived data ──────────────────────────────────────────────────────
  const displayBars = replayMode ? bars.slice(0, replayIndex) : bars;
  const replayBarTime = replayMode && replayIndex > 0 ? bars[replayIndex - 1]?.timestamp : null;

  const replaySetupContext = useMemo(() => {
    if (!replayMode || !replayBarTime || !currentSetupContext) return currentSetupContext;
    const filtered = currentSetupContext.setups.filter((s) => s.detected_at <= replayBarTime);
    return { ...currentSetupContext, setups: filtered };
  }, [replayMode, replayBarTime, currentSetupContext]);

  const activeSetupCtx = replayMode ? replaySetupContext : currentSetupContext;

  // Per-bar market state for historical / replay mode.
  // In replay: find the nearest prior timestamp in bar_market_states.
  // In static historical: show the last bar's state.
  // In live: use currentMarketState from WS.
  const displayMarketState = useMemo((): MarketStateData | null => {
    const bms = currentSetupContext?.bar_market_states;
    if (selectedDate === null) {
      // Live mode — use WS/REST state
      return currentMarketState;
    }
    if (!bms || Object.keys(bms).length === 0) return null;
    const targetTime = replayMode && replayBarTime ? replayBarTime : null;
    const timestamps = Object.keys(bms).sort();
    if (!targetTime) {
      // Static historical view — show last available bar state
      return bms[timestamps[timestamps.length - 1]] ?? null;
    }
    // Replay scrubbing — find the nearest prior or exact timestamp
    let nearest: MarketStateData | null = null;
    for (const ts of timestamps) {
      if (ts <= targetTime) nearest = bms[ts] as MarketStateData;
      else break;
    }
    return nearest;
  }, [selectedDate, replayMode, replayBarTime, currentMarketState, currentSetupContext]);

  // Flash animation when a new setup appears during replay
  useEffect(() => {
    if (!replayMode || !replaySetupContext) return;
    const ids = new Set(replaySetupContext.setups.map((s) => s.setup_id));
    for (const id of ids) {
      if (!flashRef.current.has(id)) {
        flashRef.current.add(id);
        setFlashSetupId(id);
        setTimeout(() => setFlashSetupId((cur) => (cur === id ? null : cur)), 1800);
      }
    }
  }, [replayMode, replaySetupContext]);

  // Price / change
  // Preference order: last trade price (tick-level) → bar close (few-second partial bar) → stale
  const lastBar = displayBars[displayBars.length - 1];
  const prevBar = displayBars[displayBars.length - 2];
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
        if (label.startsWith("ema")) continue;
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

    // Show E/SL/TP for the selected historical setup
    if (selectedSetupId) {
      const sel = activeSetupCtx?.setups.find((s) => s.setup_id === selectedSetupId);
      if (sel) {
        const sideColor = sel.side === "buy" ? "#22c55e" : "#ef4444";
        if (sel.entry_trigger)   overlays.push({ label: "E",  price: Number(sel.entry_trigger),   color: sideColor,  style: LineStyle.Solid });
        if (sel.stop_reference)  overlays.push({ label: "SL", price: Number(sel.stop_reference),  color: "#ef4444",  style: LineStyle.Dashed });
        if (sel.target_reference) overlays.push({ label: "TP", price: Number(sel.target_reference), color: "#22c55e", style: LineStyle.Dashed });
      }
    }

    return overlays;
  }, [currentContext, setups, currentSetupContext, selectedSetupId, activeSetupCtx]);

  const historyMarkers = useMemo((): SeriesMarker<Time>[] => {
    return (activeSetupCtx?.setups ?? [])
      .filter((entry) => !!entry.detected_at && ["triggered", "failed", "invalidated", "expired"].includes(entry.state))
      .map((entry) => {
        const isSelected = entry.setup_id === selectedSetupId;
        const sideColor = entry.side === "buy" ? "#22c55e" : "#ef4444";
        return {
          id: entry.setup_id,
          time: toETChartTime(entry.detected_at),
          position: setupMarkerPosition(entry),
          color: isSelected ? "#ffffff" : sideColor,
          shape: setupMarkerShape(entry),
          size: isSelected ? 2 : 1,
          borderColor: isSelected ? sideColor : undefined,
          borderWidth: isSelected ? 2 : undefined,
        };
      });
  }, [activeSetupCtx, selectedSetupId]);

  const focusTime = useMemo((): Time | undefined => {
    if (!selectedSetupId) return undefined;
    const entry = activeSetupCtx?.setups.find((e) => e.setup_id === selectedSetupId);
    return entry ? toETChartTime(entry.detected_at) : undefined;
  }, [selectedSetupId, activeSetupCtx]);

  // 24-hour instruments (futures) — session boundaries differ from equities
  const is24h = /^(MNQ|NQ|ES|MES|RTY|M2K|YM|MYM|CL|GC|SI|NKD|EMD)/.test(selectedSymbol);

  // For historical 24h futures, bars from the prior calendar day are loaded as
  // overnight context. Pass that date so the chart can tint them distinctly.
  const prevDayDate = (selectedDate && is24h)
    ? (() => { const d = new Date(selectedDate + "T00:00:00Z"); d.setUTCDate(d.getUTCDate() - 1); return d.toISOString().slice(0, 10); })()
    : undefined;

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
              setFlow(null);
              setIntrabarLive(false);
              deltaHistoryRef.current = [];
              setPosition(null);
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
            <>
              <button
                onClick={() => setReplayMode((m) => !m)}
                style={{
                  ...S.mono, fontSize: 10, fontWeight: 500,
                  padding: "3px 8px", borderRadius: 4, cursor: "pointer",
                  border: `0.5px solid ${replayMode ? "rgba(96,165,250,0.5)" : "rgba(255,255,255,0.12)"}`,
                  background: replayMode ? "rgba(96,165,250,0.15)" : "rgba(255,255,255,0.06)",
                  color: replayMode ? "#60a5fa" : "rgba(255,255,255,0.6)",
                }}
              >
                {replayMode ? "▶ Replay" : "▶ Replay"}
              </button>
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
            </>
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
        // For 24h futures a full session is ~1300 bars; equities ~390.
        // If we have bars but well below the expected minimum, treat the
        // data as incomplete and offer a re-fetch via the Backfill button.
        const minBars = is24h ? 1000 : 350;
        const hasBarData = bars.length > 0 && bars.length >= minBars;
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

          {/* Replay controls */}
          {replayMode && (
            <div
              style={{
                display: "flex", alignItems: "center", gap: 8,
                padding: "6px 12px",
                borderBottom: "0.5px solid rgba(255,255,255,0.06)",
                background: "rgba(96,165,250,0.04)",
              }}
            >
              <button
                onClick={() => setReplayIndex((i) => Math.max(0, i - 1))}
                style={{ ...S.mono, fontSize: 11, padding: "2px 7px", borderRadius: 4, cursor: "pointer", border: "0.5px solid rgba(255,255,255,0.12)", background: "rgba(255,255,255,0.05)", color: "rgba(255,255,255,0.6)" }}
              >◀</button>
              <button
                onClick={() => setReplayPlaying((p) => !p)}
                style={{ ...S.mono, fontSize: 11, padding: "2px 10px", borderRadius: 4, cursor: "pointer", border: "0.5px solid rgba(96,165,250,0.4)", background: "rgba(96,165,250,0.12)", color: "#60a5fa", fontWeight: 600 }}
              >{replayPlaying ? "⏸" : "▶"}</button>
              <button
                onClick={() => setReplayIndex((i) => Math.min(bars.length, i + 1))}
                style={{ ...S.mono, fontSize: 11, padding: "2px 7px", borderRadius: 4, cursor: "pointer", border: "0.5px solid rgba(255,255,255,0.12)", background: "rgba(255,255,255,0.05)", color: "rgba(255,255,255,0.6)" }}
              >▶</button>
              <div style={{ width: 0.5, height: 12, background: "rgba(255,255,255,0.1)" }} />
              {([1, 5, 15, 30] as const).map((spd) => (
                <button
                  key={spd}
                  onClick={() => setReplaySpeed(spd)}
                  style={{ ...S.mono, fontSize: 10, padding: "2px 6px", borderRadius: 4, cursor: "pointer", border: `0.5px solid ${replaySpeed === spd ? "rgba(96,165,250,0.5)" : "rgba(255,255,255,0.08)"}`, background: replaySpeed === spd ? "rgba(96,165,250,0.15)" : "transparent", color: replaySpeed === spd ? "#60a5fa" : "rgba(255,255,255,0.4)" }}
                >{spd}x</button>
              ))}
              <div style={{ flex: 1, height: 3, background: "rgba(255,255,255,0.08)", borderRadius: 2, cursor: "pointer", position: "relative" }}
                onClick={(e) => {
                  const rect = e.currentTarget.getBoundingClientRect();
                  const pct = (e.clientX - rect.left) / rect.width;
                  setReplayIndex(Math.round(pct * bars.length));
                  setReplayPlaying(false);
                }}
              >
                <div style={{ height: "100%", width: `${bars.length > 0 ? (replayIndex / bars.length) * 100 : 0}%`, background: "#60a5fa", borderRadius: 2 }} />
              </div>
              <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.4)", whiteSpace: "nowrap" }}>
                {replayIndex}/{bars.length}
              </span>
              <button
                onClick={() => { setReplayMode(false); setReplayPlaying(false); }}
                style={{ ...S.mono, fontSize: 10, padding: "2px 7px", borderRadius: 4, cursor: "pointer", border: "0.5px solid rgba(255,255,255,0.12)", background: "rgba(255,255,255,0.04)", color: "rgba(255,255,255,0.35)" }}
              >✕</button>
            </div>
          )}

          {/* Candlestick chart */}
          <CandlesChart
            bars={displayBars}
            overlays={overlayLines}
            emas={emas}
            markers={historyMarkers}
            viewportKey={replayMode ? `${selectedSymbol}:${selectedTimeframe}:replay:${replayEpoch}` : `${selectedSymbol}:${selectedTimeframe}`}
            onMarkerClick={setSelectedSetupId}
            focusTime={focusTime}
            is24h={is24h}
            prevDayDate={prevDayDate}
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

          {/* Per-account P&L — sits inside the chart panel, always visible */}
          <AccountPnlBar risk={riskState} />
        </div>

        {/* Sidebar */}
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>

          {/* Tab bar */}
          <div style={{ display: "flex", gap: 2 }}>
            {(["signal", "market"] as const).map((tab) => (
              <button
                key={tab}
                onClick={() => setSidebarTab(tab)}
                style={{
                  flex: 1, ...S.mono, fontSize: 10, fontWeight: 600,
                  padding: "5px 0", borderRadius: 4, cursor: "pointer",
                  border: "0.5px solid rgba(255,255,255,0.08)",
                  background: sidebarTab === tab ? "rgba(255,255,255,0.08)" : "transparent",
                  color: sidebarTab === tab ? "rgba(255,255,255,0.88)" : "rgba(255,255,255,0.35)",
                  textTransform: "uppercase",
                  letterSpacing: "0.05em",
                }}
              >
                {tab === "signal" ? (position ? "⬤ Signal" : "Signal") : "Market"}
              </button>
            ))}
          </div>

          {/* Signal tab — live trading view */}
          {sidebarTab === "signal" && (
            <>
              {selectedDate === null && <ThesisPanel thesis={thesis} />}
              <SetupsPanel setups={setups} thesis={thesis} />
              {selectedDate === null && <PositionPanel position={position} />}
              {selectedDate === null && <FlowPanel flow={flow} live={intrabarLive} deltaHistory={deltaHistoryRef.current} />}
            </>
          )}

          {/* Market tab — reference / analysis */}
          {sidebarTab === "market" && (
            <>
              <SetupHistoryPanel
                context={activeSetupCtx}
                selectedDate={selectedDate}
                selectedSetupId={selectedSetupId}
                onSelectSetup={setSelectedSetupId}
                flashSetupId={flashSetupId}
              />
              <FeaturesPanel context={currentContext} isHistorical={!!selectedDate} />
              <MarketStatePanel marketState={displayMarketState} isHistorical={!!selectedDate} />
              <RiskPanel risk={riskState} />
              {selectedDate === null && <FreshnessCard debug={pipelineDebug} />}
            </>
          )}
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

          {/* ── Feed quality per symbol ── */}
          {status?.feed_quality && Object.keys(status.feed_quality).length > 0 && (
            <div style={{
              borderTop: "0.5px solid rgba(255,255,255,0.06)",
              paddingTop: 6, marginTop: 2,
              display: "flex", flexWrap: "wrap", gap: 6, padding: "6px 12px",
            }}>
              <span style={{ ...S.mono, fontSize: 10, color: "rgba(255,255,255,0.35)", marginRight: 4 }}>
                Feed Quality
              </span>
              {Object.entries(status.feed_quality).map(([sym, fq]) => {
                const color =
                  fq.quality === "clean" ? "green"
                  : fq.quality === "recovering" ? "amber"
                  : "red";
                return (
                  <div
                    key={sym}
                    title={fq.degraded_reason ?? `${sym} — ${fq.quality}`}
                    style={{ display: "flex", alignItems: "center", gap: 4 }}
                  >
                    <span style={{ ...S.mono, fontSize: 11, color: "rgba(255,255,255,0.6)" }}>
                      {sym}
                    </span>
                    <Pill color={color}>{fq.quality}</Pill>
                    {!fq.signals_allowed && (
                      <span style={{ ...S.mono, fontSize: 10, color: "#ef4444" }}>
                        signals blocked
                      </span>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
