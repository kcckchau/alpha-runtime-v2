"use client";

import React, { useEffect, useMemo, useState } from "react";
import { LineStyle, SeriesMarker, Time } from "lightweight-charts";
import { CandlesChart, EmaConfig, ExtraLineSeries, toETEpoch } from "@/components/candles-chart";

// ─── API base (same resolution logic as dashboard.tsx) ────────────────────────

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

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${path}: ${res.status} ${body}`);
  }
  return (await res.json()) as T;
}

// ─── Types (mirror src/alpha/research/query.py payload shape) ─────────────────

type ResearchBar = {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  vwap: number;
  rth_vwap: number | null;
  proximity_ticks: number;
  separation_ticks: number;
  tick_size: number;
  session_phase: string;
  orb_state: string;
};

type EpisodeBarRow = {
  bar_seq: number;
  bar_timestamp: string;
  open_side: string;
  close_side: string;
  range_spans_level: boolean;
  high_distance_ticks: number;
  low_distance_ticks: number;
  close_distance_ticks: number;
  level_value_at_timestamp: number;
  atr_14: number | null;
  proximity_ticks: number;
  separation_ticks: number;
};

type Episode = {
  episode_id: string;
  prior_episode_id: string | null;
  level_type: string;
  interaction_index: number;
  started_at: string;
  ended_at: string | null;
  approach_side: string;
  bar_count: number;
  range_span_count: number;
  max_above_ticks: number;
  max_below_ticks: number;
  end_side: string | null;
  end_reason: string;
  close_side_flip_count: number;
  max_consecutive_closes_above: number;
  max_consecutive_closes_below: number;
  is_valid_for_research: boolean;
  session_scope: string;
  start_session_phase: string;
  end_session_phase: string | null;
  bars: EpisodeBarRow[];
};

type VpProfile = {
  poc: number;
  vah: number;
  val: number;
  hvn_levels: number[];
  lvn_levels: number[];
  source: string;
};

type ChartPayload = {
  symbol: string;
  session_date: string;
  bars: ResearchBar[];
  orb_high: number | null;
  orb_low: number | null;
  episodes: Episode[];
  vp_rth: VpProfile | null;
  vp_globex: { poc: number; vah: number; val: number; hvn_levels: number[]; lvn_levels: number[]; source: string } | null;
};

// ─── Level type styling ─────────────────────────────────────────────────────────
//
// Phase 1 (LevelInteractionEngine) tracks *four* levels, not three: the same
// level_type="vwap" is emitted twice — once per session_scope ("full_session"
// and "rth") — so level_type alone is ambiguous. Always key by the pair.

function levelKey(levelType: string, sessionScope: string): string {
  return `${levelType}:${sessionScope}`;
}

const LEVEL_STYLE: Record<string, { color: string; label: string; short: string }> = {
  "vwap:full_session": { color: "rgba(255, 255, 255, 0.9)", label: "VWAP (full session)", short: "V" },
  "vwap:rth":           { color: "#a855f7", label: "VWAP (RTH)", short: "R" },
  "orh:rth":            { color: "#f59e0b", label: "ORH", short: "H" },
  "orl:rth":            { color: "#3b82f6", label: "ORL", short: "L" },
};

const VP_COLORS = {
  poc:        "#f97316",  // orange
  vah:        "#22c55e",  // green
  val:        "#ef4444",  // red
  hvn:        "rgba(249,115,22,0.45)",  // dim orange
  lvn:        "rgba(20,184,166,0.45)",  // dim teal
  globex_poc: "rgba(139,92,246,0.7)",   // dim purple
};

// Keep the research view visually consistent with the live M1/5M chart.
const RESEARCH_EMAS: EmaConfig[] = [
  { period: 9, color: "#60a5fa" },   // blue
  { period: 21, color: "#fbbf24" },  // amber/yellow
];

function styleFor(levelType: string, sessionScope: string) {
  return (
    LEVEL_STYLE[levelKey(levelType, sessionScope)] ??
    { color: "#a1a1aa", label: levelType.toUpperCase(), short: levelType.slice(0, 1).toUpperCase() }
  );
}

function toEpoch(iso: string): number {
  // CandlesChart re-expresses all bar timestamps as ET wall-clock time to
  // compensate for lightweight-charts lacking timezone support. Markers and
  // pre-computed research lines must use the identical epoch or they drift by
  // the UTC offset (four hours in EDT) from their source bars.
  return toETEpoch(iso);
}

function cmeSessionStartEpoch(sessionDate: string): number {
  // CandlesChart's time axis uses ET wall-clock timestamps re-expressed as UTC.
  // A CME session labelled YYYY-MM-DD begins at 18:00 ET on the prior date.
  const day = new Date(`${sessionDate}T00:00:00Z`);
  day.setUTCDate(day.getUTCDate() - 1);
  return Math.floor(
    Date.UTC(day.getUTCFullYear(), day.getUTCMonth(), day.getUTCDate(), 18, 0, 0) / 1000,
  );
}

function priorCalendarDate(date: string): string {
  const day = new Date(`${date}T00:00:00Z`);
  day.setUTCDate(day.getUTCDate() - 1);
  return day.toISOString().slice(0, 10);
}

// ─── withAlpha helper ─────────────────────────────────────────────────────────

function withAlpha(color: string, alpha: number): string {
  if (color.startsWith("rgba(")) {
    return color.replace(/[\d.]+\)$/, `${alpha})`);
  }
  if (color.startsWith("#") && color.length >= 7) {
    const r = parseInt(color.slice(1, 3), 16);
    const g = parseInt(color.slice(3, 5), 16);
    const b = parseInt(color.slice(5, 7), 16);
    return `rgba(${r},${g},${b},${alpha})`;
  }
  return color;
}

// Text is deliberately terse — "V9" not "VWAP (full session) #9 open" — because
// with 4 levels × open+close markers, episodes cluster tightly in time whenever
// price sits near multiple levels at once (see e.g. the opening range, where
// VWAP/ORH/ORL are all close together). Full detail is one click away via
// onMarkerClick → EpisodeInspector; the marker itself only needs to be scannable.
function buildMarkers(episodes: Episode[], selectedId: string | null, showLabels: boolean): SeriesMarker<Time>[] {
  const markers: SeriesMarker<Time>[] = [];
  const hasSelection = selectedId !== null;
  for (const ep of episodes) {
    const { color, short } = styleFor(ep.level_type, ep.session_scope);
    const isSelected = ep.episode_id === selectedId;
    const markerColor = hasSelection && !isSelected ? withAlpha(color, 0.2) : color;
    const text = (isSelected || showLabels) ? `${short}${ep.interaction_index}` : "";
    markers.push({
      time: toEpoch(ep.started_at) as Time,
      position: ep.approach_side === "from_below" ? "belowBar" : "aboveBar",
      color: markerColor,
      shape: "circle",
      text,
      id: ep.episode_id,
    });
    if (ep.ended_at) {
      markers.push({
        time: toEpoch(ep.ended_at) as Time,
        position: ep.end_side === "below" ? "belowBar" : "aboveBar",
        color: ep.is_valid_for_research ? markerColor : withAlpha("#a0a0a0", 0.5),
        shape: "square",
        text: "",
        id: ep.episode_id,
      });
    }
  }
  return markers.sort((a, b) => (a.time as number) - (b.time as number));
}

// ─── Proximity band + RTH VWAP line builders ─────────────────────────────────

/**
 * Dynamic (ATR-following) band around a per-bar center price.
 *
 * Two distinct thresholds exist (see LevelInteractionEngine/episode.py):
 *   - proximity_ticks (0.20x ATR) — episode ENTRY: opens once the bar's
 *     range overlaps this (narrower) band.
 *   - separation_ticks (0.50x ATR, ~2.5x wider) — episode EXIT: closes only
 *     after the bar's full range clears *this* band for 3 consecutive bars
 *     on the same side. An episode routinely stays open well past the
 *     proximity line — that's expected, not a bug — because exit needs the
 *     wider band, not the entry one.
 */
function buildBand(
  idPrefix: string,
  bars: ResearchBar[],
  centerAt: (b: ResearchBar) => number | null,
  color: string,
  ticksAt: (b: ResearchBar) => number,
  lineStyle: number,
): [ExtraLineSeries, ExtraLineSeries] {
  const upper: { time: Time; value: number }[] = [];
  const lower: { time: Time; value: number }[] = [];
  for (const b of bars) {
    const center = centerAt(b);
    if (center == null) continue;
    const t = toEpoch(b.timestamp) as Time;
    const radius = ticksAt(b) * b.tick_size;
    upper.push({ time: t, value: center + radius });
    lower.push({ time: t, value: center - radius });
  }
  const base = { color, lineWidth: 1 as const, lineStyle };
  return [
    { id: `${idPrefix}-upper`, ...base, data: upper },
    { id: `${idPrefix}-lower`, ...base, data: lower },
  ];
}

function buildRthVwapLine(bars: ResearchBar[]): ExtraLineSeries {
  const data = bars
    .filter((b) => b.rth_vwap != null)
    .map((b) => ({ time: toEpoch(b.timestamp) as Time, value: b.rth_vwap as number }));
  return { id: "rth-vwap", color: LEVEL_STYLE["vwap:rth"].color, lineWidth: 1, data };
}

// ─── Style constants ──────────────────────────────────────────────────────────

const selectStyle: React.CSSProperties = {
  background: "#111111",
  color: "#fff",
  border: "0.5px solid rgba(255,255,255,0.15)",
  borderRadius: 4,
  padding: "4px 8px",
  fontFamily: "'IBM Plex Mono', monospace",
  fontSize: 12,
};

const checkboxLabelStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 6,
  fontSize: 11,
  color: "rgba(255,255,255,0.6)",
  fontFamily: "'IBM Plex Mono', monospace",
  cursor: "pointer",
};

const filterLabelStyle: React.CSSProperties = {
  fontSize: 10,
  color: "rgba(255,255,255,0.35)",
  fontFamily: "'IBM Plex Mono', monospace",
  letterSpacing: "0.06em",
  textTransform: "uppercase",
};

function filterBtnStyle(active: boolean): React.CSSProperties {
  return {
    background: active ? "rgba(255,255,255,0.12)" : "transparent",
    color: active ? "#fff" : "rgba(255,255,255,0.45)",
    border: `0.5px solid ${active ? "rgba(255,255,255,0.25)" : "rgba(255,255,255,0.08)"}`,
    borderRadius: 3,
    padding: "2px 8px",
    fontFamily: "'IBM Plex Mono', monospace",
    fontSize: 11,
    cursor: "pointer",
  };
}

// ─── Component ────────────────────────────────────────────────────────────────

export function ResearchChart() {
  const [symbol, setSymbol] = useState("");
  const [symbols, setSymbols] = useState<string[]>([]);
  const [dates, setDates] = useState<string[]>([]);
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [payload, setPayload] = useState<ChartPayload | null>(null);
  const [selectedEpisodeId, setSelectedEpisodeId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [showRthVwap, setShowRthVwap] = useState(true);
  const [showBands, setShowBands] = useState(false);
  const [showVP, setShowVP] = useState(true);

  // Filter state
  const [levelFilter, setLevelFilter] = useState<string>("all");   // "all"|"vwap:full_session"|"vwap:rth"|"orh:rth"|"orl:rth"
  const [sessionFilter, setSessionFilter] = useState<string>("all");  // "all"|"full_session"|"rth"
  const [minDuration, setMinDuration] = useState(0);
  const [showLabels, setShowLabels] = useState(false);
  const [showAllVP, setShowAllVP] = useState(false);

  // Discover symbols from research storage rather than duplicating a
  // hard-coded contract list in the UI. Prefer the currently-active MNQ
  // contract when present, otherwise select the first available symbol.
  useEffect(() => {
    let cancelled = false;
    fetchJson<{ symbols: string[] }>("/research/symbols")
      .then((res) => {
        if (cancelled) return;
        setSymbols(res.symbols);
        setSymbol(res.symbols.includes("MNQ-09") ? "MNQ-09" : (res.symbols[0] ?? ""));
        setError(null);
      })
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, []);

  // Load available session dates whenever the symbol changes.
  useEffect(() => {
    if (!symbol) {
      setDates([]);
      setSelectedDate(null);
      return;
    }
    let cancelled = false;
    fetchJson<{ symbol: string; dates: string[] }>(`/research/sessions?symbol=${symbol}`)
      .then((res) => {
        if (cancelled) return;
        setDates(res.dates);
        setSelectedDate(res.dates.length > 0 ? res.dates[res.dates.length - 1] : null);
        setError(null);
      })
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, [symbol]);

  // Load chart payload whenever the selected date changes
  useEffect(() => {
    if (!symbol || !selectedDate) return;
    let cancelled = false;
    setLoading(true);
    setSelectedEpisodeId(null);
    fetchJson<ChartPayload>(`/research/chart?symbol=${symbol}&date=${selectedDate}`)
      .then((res) => {
        if (cancelled) return;
        setPayload(res);
        setError(null);
      })
      .catch((e) => !cancelled && setError(String(e)))
      .finally(() => !cancelled && setLoading(false));
    return () => {
      cancelled = true;
    };
  }, [symbol, selectedDate]);

  const filteredEpisodes = useMemo(() => {
    if (!payload) return [];
    return payload.episodes.filter((ep) => {
      if (levelFilter !== "all" && levelKey(ep.level_type, ep.session_scope) !== levelFilter) return false;
      if (sessionFilter !== "all" && ep.session_scope !== sessionFilter) return false;
      if (ep.bar_count < minDuration) return false;
      return true;
    });
  }, [payload, levelFilter, sessionFilter, minDuration]);

  const overlays = useMemo(() => {
    if (!payload) return [];
    const out: { label: string; price: number; color: string; style?: number }[] = [];

    if (payload.orb_high != null) out.push({ label: "ORH", price: payload.orb_high, color: LEVEL_STYLE["orh:rth"].color });
    if (payload.orb_low != null) out.push({ label: "ORL", price: payload.orb_low, color: LEVEL_STYLE["orl:rth"].color });

    if (showVP && payload.vp_rth) {
      const vp = payload.vp_rth;
      const lastClose = payload.bars.length > 0 ? payload.bars[payload.bars.length - 1].close : null;

      // Primary VP levels
      out.push({ label: "POC", price: vp.poc, color: VP_COLORS.poc });
      out.push({ label: "VAH", price: vp.vah, color: VP_COLORS.vah });
      out.push({ label: "VAL", price: vp.val, color: VP_COLORS.val });

      // Secondary HVN/LVN: nearest 2 by default, all if showAllVP
      const hvnsToShow = showAllVP || lastClose == null
        ? vp.hvn_levels
        : [...vp.hvn_levels].sort((a, b) => Math.abs(a - lastClose) - Math.abs(b - lastClose)).slice(0, 2);
      const lvnsToShow = showAllVP || lastClose == null
        ? vp.lvn_levels
        : [...vp.lvn_levels].sort((a, b) => Math.abs(a - lastClose) - Math.abs(b - lastClose)).slice(0, 2);

      hvnsToShow.forEach((p, i) => out.push({ label: `HVN${i + 1}`, price: p, color: VP_COLORS.hvn, style: LineStyle.Dashed }));
      lvnsToShow.forEach((p, i) => out.push({ label: `LVN${i + 1}`, price: p, color: VP_COLORS.lvn, style: LineStyle.Dashed }));
    }

    if (showVP && payload.vp_globex) {
      out.push({ label: "GPOC", price: payload.vp_globex.poc, color: VP_COLORS.globex_poc, style: LineStyle.Dashed });
    }

    return out;
  }, [payload, showVP, showAllVP]);

  const markers = useMemo(
    () => buildMarkers(filteredEpisodes, selectedEpisodeId, showLabels),
    [filteredEpisodes, selectedEpisodeId, showLabels]
  );

  // RTH VWAP line + dynamic ATR-proximity bands around VWAP / RTH VWAP / ORH / ORL —
  // this is literally the same threshold LevelInteractionEngine uses to decide when an
  // episode opens (price enters the band) or separates (price clears it), so overlaying
  // it directly answers "why did this episode start/end exactly here".
  const extraLines = useMemo(() => {
    if (!payload) return [];
    const lines: ExtraLineSeries[] = [];
    if (showRthVwap) lines.push(buildRthVwapLine(payload.bars));
    if (showBands) {
      const levels: [string, (b: ResearchBar) => number | null, string][] = [
        ["vwap", (b) => b.vwap, LEVEL_STYLE["vwap:full_session"].color],
        ["rth-vwap", (b) => b.rth_vwap, LEVEL_STYLE["vwap:rth"].color],
        // orb_high/orb_low are session-wide (last-seen) values — gate each point on
        // that bar's own orb_state so the band doesn't appear before the opening
        // range actually locked in (it can't be an active interaction target yet).
        ...(payload.orb_high != null
          ? ([["orh", (b) => (b.orb_state === "not_set" ? null : payload.orb_high), LEVEL_STYLE["orh:rth"].color]] as typeof levels)
          : []),
        ...(payload.orb_low != null
          ? ([["orl", (b) => (b.orb_state === "not_set" ? null : payload.orb_low), LEVEL_STYLE["orl:rth"].color]] as typeof levels)
          : []),
      ];
      for (const [key, centerAt, color] of levels) {
        // Proximity (entry, narrow, dotted) and separation (exit, wider, dashed) —
        // both drawn so it's visually clear an episode can sit between the two
        // lines for a while before actually closing.
        lines.push(...buildBand(`band-${key}-prox`, payload.bars, centerAt, color, (b) => b.proximity_ticks, LineStyle.Dotted));
        lines.push(...buildBand(`band-${key}-sep`, payload.bars, centerAt, color, (b) => b.separation_ticks, LineStyle.Dashed));
      }
    }
    return lines;
  }, [payload, showRthVwap, showBands]);

  const selectedEpisode = useMemo(
    () => payload?.episodes.find((e) => e.episode_id === selectedEpisodeId) ?? null,
    [payload, selectedEpisodeId]
  );

  return (
    <div style={{ padding: 20, background: "#0a0a0a", minHeight: "100vh", color: "#fff" }}>
      {/* Header bar */}
      <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 8, flexWrap: "wrap" }}>
        <h1 style={{ fontFamily: "'IBM Plex Mono', monospace", fontSize: 14, letterSpacing: "0.08em", margin: 0 }}>
          RESEARCH — LEVEL INTERACTION VIEWER
        </h1>
        <select
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
          style={selectStyle}
        >
          {symbols.length === 0 && <option value="">no research symbols</option>}
          {symbols.map((availableSymbol) => (
            <option key={availableSymbol} value={availableSymbol}>{availableSymbol}</option>
          ))}
        </select>
        <select
          value={selectedDate ?? ""}
          onChange={(e) => setSelectedDate(e.target.value)}
          style={selectStyle}
          disabled={dates.length === 0}
        >
          {dates.length === 0 && <option value="">no sessions</option>}
          {dates.map((d) => (
            <option key={d} value={d}>{d}</option>
          ))}
        </select>
        <label style={checkboxLabelStyle}>
          <input type="checkbox" checked={showRthVwap} onChange={(e) => setShowRthVwap(e.target.checked)} />
          RTH VWAP
        </label>
        <label style={checkboxLabelStyle}>
          <input type="checkbox" checked={showBands} onChange={(e) => setShowBands(e.target.checked)} />
          Proximity bands
        </label>
        <label style={checkboxLabelStyle}>
          <input type="checkbox" checked={showVP} onChange={(e) => setShowVP(e.target.checked)} />
          VP levels
        </label>
        {loading && <span style={{ fontSize: 11, color: "rgba(255,255,255,0.4)" }}>loading…</span>}
        {payload && (
          <span style={{ fontSize: 11, color: "rgba(255,255,255,0.4)" }}>
            {payload.bars.length} bars · {payload.episodes.length} episodes
          </span>
        )}
      </div>

      {/* Filter bar — only show when payload exists */}
      {payload && (
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10, flexWrap: "wrap" }}>
          {/* Level type filter buttons */}
          <span style={filterLabelStyle}>Level:</span>
          {[
            { key: "all", label: "All" },
            { key: "vwap:full_session", label: "V" },
            { key: "vwap:rth", label: "R" },
            { key: "orh:rth", label: "H" },
            { key: "orl:rth", label: "L" },
          ].map(({ key, label }) => (
            <button key={key} onClick={() => setLevelFilter(key)} style={filterBtnStyle(levelFilter === key)}>{label}</button>
          ))}
          <span style={{ ...filterLabelStyle, marginLeft: 8 }}>Session:</span>
          {[
            { key: "all", label: "All" },
            { key: "full_session", label: "Full" },
            { key: "rth", label: "RTH" },
          ].map(({ key, label }) => (
            <button key={key} onClick={() => setSessionFilter(key)} style={filterBtnStyle(sessionFilter === key)}>{label}</button>
          ))}
          <span style={{ ...filterLabelStyle, marginLeft: 8 }}>Min bars:</span>
          <input
            type="number"
            min={0}
            max={100}
            value={minDuration}
            onChange={(e) => setMinDuration(Math.max(0, parseInt(e.target.value) || 0))}
            style={{ ...selectStyle, width: 48, padding: "3px 6px" }}
          />
          <label style={{ ...checkboxLabelStyle, marginLeft: 8 }}>
            <input type="checkbox" checked={showLabels} onChange={(e) => setShowLabels(e.target.checked)} />
            Labels
          </label>
          {showVP && (
            <label style={{ ...checkboxLabelStyle }}>
              <input type="checkbox" checked={showAllVP} onChange={(e) => setShowAllVP(e.target.checked)} />
              All VP
            </label>
          )}
          <span style={{ fontSize: 11, color: "rgba(255,255,255,0.3)", marginLeft: 4 }}>
            {filteredEpisodes.length} / {payload.episodes.length} episodes
          </span>
        </div>
      )}

      {error && (
        <div style={{ padding: 10, marginBottom: 12, background: "rgba(239,68,68,0.1)", border: "0.5px solid rgba(239,68,68,0.3)", borderRadius: 6, fontSize: 12, fontFamily: "monospace" }}>
          {error}
        </div>
      )}

      {payload && payload.bars.length > 0 && (
        <>
          {/* Two-column: chart + right inspector */}
          <div style={{ display: "flex", gap: 12, alignItems: "flex-start" }}>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ border: "0.5px solid rgba(255,255,255,0.08)", borderRadius: 6, overflow: "hidden" }}>
                <CandlesChart
                  bars={payload.bars}
                  overlays={overlays}
                  emas={RESEARCH_EMAS}
                  extraLines={extraLines}
                  markers={markers}
                  viewportKey={`${payload.symbol}:${payload.session_date}`}
                  onMarkerClick={(id) => setSelectedEpisodeId(prev => prev === id ? null : id)}
                  is24h
                  prevDayDate={priorCalendarDate(payload.session_date)}
                  sessionStartEpoch={cmeSessionStartEpoch(payload.session_date)}
                />
              </div>
              {/* Legend below chart */}
              <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginTop: 10, fontSize: 11, color: "rgba(255,255,255,0.45)" }}>
                <LegendDot color={LEVEL_STYLE["vwap:full_session"].color} label="VWAP" />
                <LegendDot color={LEVEL_STYLE["vwap:rth"].color} label="VWAP RTH" />
                <LegendDot color={LEVEL_STYLE["orh:rth"].color} label="ORH" />
                <LegendDot color={LEVEL_STYLE["orl:rth"].color} label="ORL" />
                <LegendDot color="#60a5fa" label="EMA 9" />
                <LegendDot color="#fbbf24" label="EMA 21" />
                {showVP && payload.vp_rth && (
                  <>
                    <LegendDot color={VP_COLORS.poc} label="POC" />
                    <LegendDot color={VP_COLORS.vah} label="VAH" />
                    <LegendDot color={VP_COLORS.val} label="VAL" />
                    <LegendDot color={VP_COLORS.hvn} label="HVN" />
                    <LegendDot color={VP_COLORS.lvn} label="LVN" />
                  </>
                )}
                {showVP && payload.vp_globex && <LegendDot color={VP_COLORS.globex_poc} label="Globex POC" />}
                <span style={{ color: "rgba(255,255,255,0.3)" }}>
                  ● open · ■ close · click to inspect
                </span>
              </div>
            </div>

            {/* Right inspector panel — always rendered to keep chart width stable */}
            <EpisodeInspector
              episode={selectedEpisode}
              tickSize={payload.bars[0]?.tick_size ?? 0.25}
              onClose={() => setSelectedEpisodeId(null)}
            />
          </div>
        </>
      )}
    </div>
  );
}

function LegendDot({ color, label }: { color: string; label: string }) {
  return (
    <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <span style={{ width: 8, height: 8, borderRadius: 999, background: color, display: "inline-block" }} />
      {label}
    </span>
  );
}

function EpisodeInspector({
  episode,
  tickSize,
  onClose,
}: {
  episode: Episode | null;
  tickSize: number;
  onClose: () => void;
}) {
  const panelStyle: React.CSSProperties = {
    width: 272,
    flexShrink: 0,
    background: "#0f0f0f",
    border: "0.5px solid rgba(255,255,255,0.08)",
    borderRadius: 6,
    fontFamily: "'IBM Plex Mono', monospace",
    fontSize: 11,
    minHeight: 200,
    maxHeight: "calc(100vh - 180px)",
    overflowY: "auto",
  };

  if (!episode) {
    return (
      <div style={{ ...panelStyle, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <span style={{ color: "rgba(255,255,255,0.2)", textAlign: "center", padding: 20 }}>
          Select an episode marker to inspect
        </span>
      </div>
    );
  }

  const style = styleFor(episode.level_type, episode.session_scope);
  const anchorPrice = episode.bars.length > 0 ? episode.bars[0].level_value_at_timestamp : null;
  const durationMins = episode.bar_count; // 1 bar = 1 min
  const maxAbovePts = (episode.max_above_ticks * tickSize).toFixed(2);
  const maxBelowPts = (episode.max_below_ticks * tickSize).toFixed(2);

  const startTime = episode.started_at.slice(11, 16);
  const endTime = episode.ended_at?.slice(11, 16) ?? "—";

  return (
    <div style={panelStyle}>
      <div style={{
        padding: "10px 12px 8px",
        borderBottom: "0.5px solid rgba(255,255,255,0.06)",
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        position: "sticky",
        top: 0,
        background: "#0f0f0f",
      }}>
        <span style={{ color: style.color, fontWeight: "bold", fontSize: 12 }}>
          {style.label} #{episode.interaction_index}
        </span>
        <button
          onClick={onClose}
          style={{ background: "none", border: "none", color: "rgba(255,255,255,0.4)", cursor: "pointer", fontSize: 14, padding: 0 }}
        >×</button>
      </div>

      {!episode.is_valid_for_research && (
        <div style={{ padding: "6px 12px", background: "rgba(245,158,11,0.1)", fontSize: 10, color: "#f59e0b" }}>
          excluded from research — gap detected
        </div>
      )}

      <div style={{ padding: "10px 12px", display: "flex", flexDirection: "column", gap: 10 }}>

        {/* Anchor */}
        <InspectorSection label="Anchor">
          <Field label="Level" value={style.label} />
          <Field label="Price at open" value={anchorPrice != null ? anchorPrice.toFixed(2) : "—"} />
        </InspectorSection>

        {/* Timing */}
        <InspectorSection label="Timing">
          <Field label="Start" value={startTime} />
          <Field label="End" value={endTime} />
          <Field label="Duration" value={`${durationMins} bars · ${durationMins} min`} />
          <Field label="Phase" value={`${episode.start_session_phase}${episode.end_session_phase && episode.end_session_phase !== episode.start_session_phase ? ` → ${episode.end_session_phase}` : ""}`} />
        </InspectorSection>

        {/* Geometry */}
        <InspectorSection label="Geometry">
          <Field label="Approach" value={episode.approach_side} />
          <Field label="End side" value={episode.end_side ?? "—"} />
          <Field label="End reason" value={episode.end_reason} />
          <Field label="Max above" value={`+${maxAbovePts} pts`} />
          <Field label="Max below" value={`−${maxBelowPts} pts`} />
          <Field label="Range-spans" value={`${episode.range_span_count} bars`} />
        </InspectorSection>

        {/* Behaviour */}
        <InspectorSection label="Behaviour">
          <Field label="Flip count" value={String(episode.close_side_flip_count)} />
          <Field label="Consec. above" value={String(episode.max_consecutive_closes_above)} />
          <Field label="Consec. below" value={String(episode.max_consecutive_closes_below)} />
          <Field label="Bar records" value={String(episode.bars.length)} />
        </InspectorSection>

      </div>
    </div>
  );
}

function InspectorSection({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div style={{ fontSize: 9, textTransform: "uppercase", letterSpacing: "0.1em", color: "rgba(255,255,255,0.25)", marginBottom: 6 }}>
        {label}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px 8px" }}>
        {children}
      </div>
    </div>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div style={{ fontSize: 9, textTransform: "uppercase", letterSpacing: "0.06em", color: "rgba(255,255,255,0.3)" }}>{label}</div>
      <div style={{ color: "rgba(255,255,255,0.8)", fontSize: 11, wordBreak: "break-word" }}>{value}</div>
    </div>
  );
}
