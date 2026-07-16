"use client";

import React, { useEffect, useMemo, useState } from "react";
import { LineStyle, SeriesMarker, Time } from "lightweight-charts";
import { CandlesChart, ExtraLineSeries, toETEpoch } from "@/components/candles-chart";

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

type ChartPayload = {
  symbol: string;
  session_date: string;
  bars: ResearchBar[];
  orb_high: number | null;
  orb_low: number | null;
  episodes: Episode[];
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

// Text is deliberately terse — "V9" not "VWAP (full session) #9 open" — because
// with 4 levels × open+close markers, episodes cluster tightly in time whenever
// price sits near multiple levels at once (see e.g. the opening range, where
// VWAP/ORH/ORL are all close together). Full detail is one click away via
// onMarkerClick → EpisodeDetail; the marker itself only needs to be scannable.
function buildMarkers(episodes: Episode[]): SeriesMarker<Time>[] {
  const markers: SeriesMarker<Time>[] = [];
  for (const ep of episodes) {
    const { color, short } = styleFor(ep.level_type, ep.session_scope);
    markers.push({
      time: toEpoch(ep.started_at) as Time,
      position: ep.approach_side === "from_below" ? "belowBar" : "aboveBar",
      color,
      shape: "circle",
      text: `${short}${ep.interaction_index}`,
      id: ep.episode_id,
    });
    if (ep.ended_at) {
      markers.push({
        time: toEpoch(ep.ended_at) as Time,
        position: ep.end_side === "below" ? "belowBar" : "aboveBar",
        color: ep.is_valid_for_research ? color : "rgba(160,160,160,0.5)",
        shape: "square",
        // No text on close markers — pairing with the same-colour open marker's
        // number plus the square shape is enough to scan; a text label here
        // was the single biggest contributor to overlapping-label clutter.
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

// ─── Component ────────────────────────────────────────────────────────────────

export function ResearchChart() {
  const [symbol, setSymbol] = useState("MNQ");
  const [dates, setDates] = useState<string[]>([]);
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [payload, setPayload] = useState<ChartPayload | null>(null);
  const [selectedEpisodeId, setSelectedEpisodeId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [showRthVwap, setShowRthVwap] = useState(true);
  const [showBands, setShowBands] = useState(false);

  // Load available session dates whenever the symbol changes
  useEffect(() => {
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
    if (!selectedDate) return;
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

  const overlays = useMemo(() => {
    if (!payload) return [];
    const out: { label: string; price: number; color: string }[] = [];
    if (payload.orb_high != null) out.push({ label: "ORH", price: payload.orb_high, color: LEVEL_STYLE["orh:rth"].color });
    if (payload.orb_low != null) out.push({ label: "ORL", price: payload.orb_low, color: LEVEL_STYLE["orl:rth"].color });
    return out;
  }, [payload]);

  const markers = useMemo(() => (payload ? buildMarkers(payload.episodes) : []), [payload]);

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
      <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 12 }}>
        <h1 style={{ fontFamily: "'IBM Plex Mono', monospace", fontSize: 14, letterSpacing: "0.08em", margin: 0 }}>
          RESEARCH — LEVEL INTERACTION VIEWER
        </h1>
        <select
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
          style={selectStyle}
        >
          <option value="MNQ">MNQ</option>
          <option value="MNQ-09">MNQ-09</option>
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
        {loading && <span style={{ fontSize: 11, color: "rgba(255,255,255,0.4)" }}>loading…</span>}
        {payload && (
          <span style={{ fontSize: 11, color: "rgba(255,255,255,0.4)" }}>
            {payload.bars.length} bars · {payload.episodes.length} episodes
          </span>
        )}
      </div>

      {error && (
        <div style={{ padding: 10, marginBottom: 12, background: "rgba(239,68,68,0.1)", border: "0.5px solid rgba(239,68,68,0.3)", borderRadius: 6, fontSize: 12, fontFamily: "monospace" }}>
          {error}
        </div>
      )}

      {payload && payload.bars.length > 0 && (
        <>
          <div style={{ border: "0.5px solid rgba(255,255,255,0.08)", borderRadius: 6, overflow: "hidden" }}>
            <CandlesChart
              bars={payload.bars}
              overlays={overlays}
              extraLines={extraLines}
              markers={markers}
              viewportKey={`${payload.symbol}:${payload.session_date}`}
              onMarkerClick={(id) => setSelectedEpisodeId(id)}
              is24h
            />
          </div>

          <div style={{ display: "flex", flexWrap: "wrap", gap: 16, marginTop: 12, fontSize: 11, color: "rgba(255,255,255,0.5)" }}>
            <LegendDot color={LEVEL_STYLE["vwap:full_session"].color} label="VWAP (full session)" />
            <LegendDot color={LEVEL_STYLE["vwap:rth"].color} label="VWAP (RTH)" />
            <LegendDot color={LEVEL_STYLE["orh:rth"].color} label="ORH" />
            <LegendDot color={LEVEL_STYLE["orl:rth"].color} label="ORL" />
            <span>circle = episode open · square = episode close (click a marker for detail) · dotted = ±proximity band (entry threshold, narrower) · dashed = ±separation band (exit threshold, wider — must clear for 3 consecutive bars)</span>
          </div>

          {selectedEpisode && <EpisodeDetail episode={selectedEpisode} onClose={() => setSelectedEpisodeId(null)} />}
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

function EpisodeDetail({ episode, onClose }: { episode: Episode; onClose: () => void }) {
  return (
    <div style={{
      marginTop: 16, padding: 14, background: "#111111",
      border: "0.5px solid rgba(255,255,255,0.08)", borderRadius: 6,
      fontFamily: "'IBM Plex Mono', monospace", fontSize: 12,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
        <strong>
          {styleFor(episode.level_type, episode.session_scope).label} interaction #{episode.interaction_index}
          {!episode.is_valid_for_research && (
            <span style={{ color: "#f59e0b", marginLeft: 8 }}>(excluded from research — gap detected)</span>
          )}
        </strong>
        <button onClick={onClose} style={{ background: "none", border: "none", color: "rgba(255,255,255,0.5)", cursor: "pointer", fontSize: 14 }}>×</button>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8, color: "rgba(255,255,255,0.75)" }}>
        <Field label="Started" value={episode.started_at.slice(11, 16)} />
        <Field label="Ended" value={episode.ended_at?.slice(11, 16) ?? "—"} />
        <Field label="Approach" value={episode.approach_side} />
        <Field label="End reason" value={episode.end_reason} />
        <Field label="Bars" value={String(episode.bar_count)} />
        <Field label="Range-spans-level bars" value={String(episode.range_span_count)} />
        <Field label="Max above (ticks)" value={String(episode.max_above_ticks)} />
        <Field label="Max below (ticks)" value={String(episode.max_below_ticks)} />
        <Field label="Close-side flips" value={String(episode.close_side_flip_count)} />
        <Field label="Max consec. closes above" value={String(episode.max_consecutive_closes_above)} />
        <Field label="Max consec. closes below" value={String(episode.max_consecutive_closes_below)} />
        <Field label="End side" value={episode.end_side ?? "—"} />
      </div>
      <div style={{ marginTop: 10, color: "rgba(255,255,255,0.4)", fontSize: 10 }}>
        {episode.bars.length} bar records · session_scope={episode.session_scope} ·
        {" "}{episode.start_session_phase} → {episode.end_session_phase ?? "—"}
      </div>
    </div>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div style={{ fontSize: 9, textTransform: "uppercase", letterSpacing: "0.08em", color: "rgba(255,255,255,0.35)" }}>{label}</div>
      <div>{value}</div>
    </div>
  );
}

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
