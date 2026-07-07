"use client";

import { useEffect, useRef } from "react";
import {
  CandlestickData,
  CandlestickSeries,
  ColorType,
  HistogramSeries,
  IChartApi,
  ISeriesApi,
  LineData,
  LineSeries,
  LineStyle,
  SeriesMarker,
  TickMarkType,
  Time,
  createChart,
  createSeriesMarkers,
} from "lightweight-charts";

type BarRow = {
  timestamp: string;
  open: string | number;
  high: string | number;
  low: string | number;
  close: string | number;
  volume?: number;
  vwap?: string | number | null;
};

type OverlayLine = {
  label: string;
  price: number;
  color: string;
  style?: number;
};

export type EmaConfig = {
  period: number;
  color: string;
};

type CandlesChartProps = {
  bars: BarRow[];
  overlays: OverlayLine[];
  emas?: EmaConfig[];
  markers?: SeriesMarker<Time>[];
  viewportKey?: string;
  onMarkerClick?: (setupId: string) => void;
  focusTime?: Time;
  /** True for instruments that trade ~24h (futures: MNQ, NQ, ES, etc.) */
  is24h?: boolean;
  /**
   * ISO date string ("YYYY-MM-DD") of the prior calendar day whose bars are
   * shown as overnight context.  Bars with timestamps on this date receive a
   * distinct slate background instead of the normal session colour, making it
   * visually clear they belong to the previous session.
   */
  prevDayDate?: string;
};

// ─── ET timezone helpers ──────────────────────────────────────────────────────
//
// lightweight-charts has no native timezone support. The reliable workaround:
// convert each Unix timestamp to "ET wall-clock time re-expressed as UTC",
// so the chart positions and labels bars at ET time regardless of the browser's
// local timezone. All formatters then read UTC fields on the shifted Date.

const _etFmt = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

function toETEpoch(timestamp: string): number {
  const raw = new Date(timestamp).getTime();
  const parts = _etFmt.formatToParts(new Date(raw));
  const g = (t: string) => parts.find((p) => p.type === t)?.value ?? "00";
  // Build a UTC ISO string that has the ET wall-clock values
  const iso = `${g("year")}-${g("month")}-${g("day")}T${g("hour")}:${g("minute")}:${g("second")}Z`;
  return Math.floor(new Date(iso).getTime() / 1000);
}

// Formatters — time is already "ET as UTC", so just read UTC fields
const tickMarkFormatter = (time: Time, type: TickMarkType): string => {
  const d = new Date((time as number) * 1000);
  const mo = d.toLocaleString("en-US", { month: "short", timeZone: "UTC" });
  if (type === TickMarkType.Year)       return String(d.getUTCFullYear());
  if (type === TickMarkType.Month)      return `${mo} ${d.getUTCFullYear()}`;
  if (type === TickMarkType.DayOfMonth) return `${mo} ${d.getUTCDate()}`;
  const h = String(d.getUTCHours()).padStart(2, "0");
  const m = String(d.getUTCMinutes()).padStart(2, "0");
  return `${h}:${m}`;
};

const timeFormatter = (time: Time): string => {
  const d = new Date((time as number) * 1000);
  const mo = d.toLocaleString("en-US", { month: "short", timeZone: "UTC" });
  const h = String(d.getUTCHours()).padStart(2, "0");
  const m = String(d.getUTCMinutes()).padStart(2, "0");
  return `${mo} ${d.getUTCDate()} ${h}:${m} ET`;
};

// ─── Incremental state types ──────────────────────────────────────────────────

type VwapState = { cumTPV: number; cumVol: number; prevTime: number | null; session: string | null };
type EmaState = { value: number; index: number };

// ─── CME session helpers ──────────────────────────────────────────────────────
//
// CME Globex equity-index futures roll their session at 18:00 ET (23:00 UTC).
// Bars timestamped at 18:00+ ET belong to the *next* calendar day's session,
// matching the FeatureEngine's session_key logic.
//
// `t` here is the ET wall-clock epoch (already shifted by toETEpoch), so
// reading UTC date/hour fields gives us ET date/hour directly.

function cmeSessionId(etEpoch: number): string {
  const d = new Date(etEpoch * 1000);
  const h = d.getUTCHours();
  if (h >= 18) {
    const next = new Date(d);
    next.setUTCDate(next.getUTCDate() + 1);
    return next.toISOString().slice(0, 10);
  }
  return d.toISOString().slice(0, 10);
}

function shouldResetVwap(prevTime: number | null, t: number, is24h: boolean): boolean {
  if (prevTime === null) return false;
  if (is24h) return cmeSessionId(t) !== cmeSessionId(prevTime);
  // Equities: reset on any gap > 1 hour (overnight, weekend, holiday)
  return t - prevTime > 3600;
}

// ─── Data helpers ─────────────────────────────────────────────────────────────

function getCachedEpoch(cache: Map<string, number>, timestamp: string): number {
  let v = cache.get(timestamp);
  if (v === undefined) {
    v = toETEpoch(timestamp);
    cache.set(timestamp, v);
  }
  return v;
}

function buildSortedDeduped(
  bars: BarRow[],
  cache: Map<string, number>
): Array<{ t: number; b: BarRow }> {
  const map = new Map<number, { t: number; b: BarRow }>();
  for (const b of bars) {
    const t = getCachedEpoch(cache, b.timestamp);
    map.set(t, { t, b });
  }
  return [...map.values()].sort((a, b) => a.t - b.t);
}

function buildCandleData(
  bars: BarRow[],
  cache: Map<string, number>
): CandlestickData<Time>[] {
  return buildSortedDeduped(bars, cache).map(({ t, b }) => ({
    time: t as Time,
    open: Number(b.open),
    high: Number(b.high),
    low: Number(b.low),
    close: Number(b.close),
  }));
}

type VwapResult = {
  data: LineData<Time>[];
  statePreLast: VwapState;
  stateLast: VwapState;
};

const VWAP_STATE_INIT: VwapState = { cumTPV: 0, cumVol: 0, prevTime: null, session: null };

function buildVwapData(bars: BarRow[], cache: Map<string, number>, is24h: boolean): VwapResult {
  const items = buildSortedDeduped(bars, cache).filter((i) =>
    isFinite(Number(i.b.close))
  );

  let cumTPV = 0,
    cumVol = 0,
    prevTime: number | null = null;
  let statePreLast: VwapState = { ...VWAP_STATE_INIT };
  const data: LineData<Time>[] = [];

  for (let i = 0; i < items.length; i++) {
    const { t, b } = items[i];

    // Capture state before last bar (before any session reset at this bar)
    if (i === items.length - 1) {
      statePreLast = { cumTPV, cumVol, prevTime, session: prevTime !== null ? cmeSessionId(prevTime) : null };
    }

    // Session reset: CME session boundary for 24h futures, gap > 1h for equities
    if (shouldResetVwap(prevTime, t, is24h)) {
      cumTPV = 0;
      cumVol = 0;
      if (i === items.length - 1) {
        statePreLast = { cumTPV: 0, cumVol: 0, prevTime, session: prevTime !== null ? cmeSessionId(prevTime) : null };
      }
    }

    const typical = (Number(b.high) + Number(b.low) + Number(b.close)) / 3;
    const vol = Number(b.volume ?? 0);
    cumTPV += typical * vol;
    cumVol += vol;
    prevTime = t;
    data.push({ time: t as Time, value: cumVol > 0 ? cumTPV / cumVol : Number(b.close) });
  }

  return {
    data,
    statePreLast,
    stateLast: { cumTPV, cumVol, prevTime, session: prevTime !== null ? cmeSessionId(prevTime) : null },
  };
}

type EmaResult = {
  data: LineData<Time>[];
  statePreLast: EmaState | null;
  stateLast: EmaState | null;
};

function buildEmaData(
  bars: BarRow[],
  period: number,
  cache: Map<string, number>
): EmaResult {
  const items = buildSortedDeduped(bars, cache).filter((i) =>
    isFinite(Number(i.b.close))
  );

  if (items.length === 0) return { data: [], statePreLast: null, stateLast: null };

  const alpha = 2 / (period + 1);
  let ema = Number(items[0].b.close);
  let statePreLast: EmaState | null = null;
  const data: LineData<Time>[] = [];

  for (let i = 0; i < items.length; i++) {
    if (i === items.length - 1 && i > 0) {
      statePreLast = { value: ema, index: i - 1 };
    }
    ema = i === 0 ? Number(items[i].b.close) : Number(items[i].b.close) * alpha + ema * (1 - alpha);
    if (i >= period - 1) {
      data.push({ time: items[i].t as Time, value: ema });
    }
  }

  return {
    data,
    statePreLast,
    stateLast: items.length > 0 ? { value: ema, index: items.length - 1 } : null,
  };
}

function vwapPointFromState(
  state: VwapState,
  bar: BarRow,
  t: number,
  is24h: boolean,
): { value: number; nextState: VwapState } {
  let { cumTPV, cumVol } = state;
  if (shouldResetVwap(state.prevTime, t, is24h)) {
    cumTPV = 0;
    cumVol = 0;
  }
  const typical = (Number(bar.high) + Number(bar.low) + Number(bar.close)) / 3;
  const vol = Number(bar.volume ?? 0);
  cumTPV += typical * vol;
  cumVol += vol;
  return {
    value: cumVol > 0 ? cumTPV / cumVol : Number(bar.close),
    nextState: { cumTPV, cumVol, prevTime: t, session: cmeSessionId(t) },
  };
}

function emaPointFromState(
  state: EmaState,
  close: number,
  period: number
): { value: number; nextState: EmaState } {
  const alpha = 2 / (period + 1);
  const value = close * alpha + state.value * (1 - alpha);
  return { value, nextState: { value, index: state.index + 1 } };
}

const VOL_UP   = "rgba(34,  197, 94,  0.45)";
const VOL_DOWN = "rgba(239, 68,  68,  0.45)";

// ─── Session band helpers ─────────────────────────────────────────────────────

type SessionBand = "premarket" | "regular" | "aftermarket";

const SESSION_BAND_COLORS: Record<SessionBand, string> = {
  premarket:   "rgba(250, 204, 21, 0.07)",  // amber tint
  regular:     "rgba(34, 197, 94, 0.04)",   // faint green
  aftermarket: "rgba(139, 92, 246, 0.08)",  // violet tint
};

// Bars from the prior calendar day (loaded as overnight context for 24h futures)
// get a distinct slate tint so they are immediately distinguishable from the
// target session.
const PREV_DAY_COLOR = "rgba(100, 116, 139, 0.14)"; // slate-500

// etEpoch is already "ET wall-clock as UTC" seconds, so UTC fields equal ET time.
function classifySession(etEpoch: number, is24h: boolean): SessionBand | null {
  const d = new Date(etEpoch * 1000);
  const minutes = d.getUTCHours() * 60 + d.getUTCMinutes();
  if (minutes >= 570 && minutes < 960) return "regular";   // 09:30–16:00 ET
  if (minutes >= 960) {
    // After 16:00 ET
    if (is24h) return minutes < 1020 ? "aftermarket" : "premarket"; // 16–17 | 17+ (overnight)
    return minutes < 1200 ? "aftermarket" : null;                    // 16–20 for equities
  }
  // Before 09:30 ET
  if (is24h) return "premarket";
  return minutes >= 240 ? "premarket" : null; // 04:00–09:30 for equities
}

// ─── Component ────────────────────────────────────────────────────────────────

export function CandlesChart({
  bars,
  overlays,
  emas = [],
  markers = [],
  viewportKey,
  onMarkerClick,
  focusTime,
  is24h = false,
  prevDayDate,
}: CandlesChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  // Stable refs so subscribeClick closure doesn't go stale
  const markersRef = useRef<SeriesMarker<Time>[]>(markers);
  const onMarkerClickRef = useRef<((setupId: string) => void) | undefined>(onMarkerClick);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const vwapRef = useRef<ISeriesApi<"Line"> | null>(null);
  const emaSeriesRef = useRef<Map<number, ISeriesApi<"Line">>>(new Map());
  const markerApiRef = useRef<ReturnType<typeof createSeriesMarkers<Time>> | null>(null);
  const lastViewportKeyRef = useRef<string | undefined>(undefined);

  // Epoch cache: avoids repeated Intl.DateTimeFormat calls for already-seen timestamps
  const epochCacheRef = useRef<Map<string, number>>(new Map());

  // Incremental VWAP state: state before and at the last bar
  const vwapPreLastRef = useRef<VwapState>({ ...VWAP_STATE_INIT });
  const vwapLastRef = useRef<VwapState>({ ...VWAP_STATE_INIT });

  // Incremental EMA state per period
  const emaPreLastRef = useRef<Map<number, EmaState>>(new Map());
  const emaLastRef = useRef<Map<number, EmaState>>(new Map());

  // Previous bars reference for detecting live updates
  const prevBarsRef = useRef<BarRow[]>([]);

  // Volume histogram
  const volRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  // Session background band series
  const bandPreRef    = useRef<ISeriesApi<"Histogram"> | null>(null);
  const bandRegRef    = useRef<ISeriesApi<"Histogram"> | null>(null);
  const bandAftRef    = useRef<ISeriesApi<"Histogram"> | null>(null);
  const bandPrevDayRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  // Chart init effect — runs once
  useEffect(() => {
    if (!containerRef.current || chartRef.current) return;

    const chart = createChart(containerRef.current, {
      autoSize: true,
      localization: { timeFormatter },
      layout: {
        background: { type: ColorType.Solid, color: "#111111" },
        textColor: "rgba(255,255,255,0.35)",
        fontFamily: "'IBM Plex Mono', monospace",
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.04)" },
        horzLines: { color: "rgba(255,255,255,0.04)" },
      },
      rightPriceScale: { borderColor: "rgba(255,255,255,0.08)" },
      timeScale: {
        borderColor: "rgba(255,255,255,0.08)",
        timeVisible: true,
        secondsVisible: false,
        tickMarkFormatter,
      },
      crosshair: {
        vertLine: { color: "rgba(255,255,255,0.15)" },
        horzLine: { color: "rgba(255,255,255,0.15)" },
      },
    });

    // Session background bands — added first so they render behind all other series
    const SESSION_BAND_SCALE = "session-bands";
    const bandPrevDay = chart.addSeries(HistogramSeries, {
      color: PREV_DAY_COLOR,
      priceScaleId: SESSION_BAND_SCALE,
      priceLineVisible: false,
      lastValueVisible: false,
      base: 0,
    });
    bandPrevDay.priceScale().applyOptions({ visible: false, scaleMargins: { top: 0, bottom: 0 } });
    const bandPre = chart.addSeries(HistogramSeries, {
      color: SESSION_BAND_COLORS.premarket,
      priceScaleId: SESSION_BAND_SCALE,
      priceLineVisible: false,
      lastValueVisible: false,
      base: 0,
    });
    bandPre.priceScale().applyOptions({ visible: false, scaleMargins: { top: 0, bottom: 0 } });
    const bandReg = chart.addSeries(HistogramSeries, {
      color: SESSION_BAND_COLORS.regular,
      priceScaleId: SESSION_BAND_SCALE,
      priceLineVisible: false,
      lastValueVisible: false,
      base: 0,
    });
    const bandAft = chart.addSeries(HistogramSeries, {
      color: SESSION_BAND_COLORS.aftermarket,
      priceScaleId: SESSION_BAND_SCALE,
      priceLineVisible: false,
      lastValueVisible: false,
      base: 0,
    });
    bandPrevDayRef.current = bandPrevDay;
    bandPreRef.current = bandPre;
    bandRegRef.current = bandReg;
    bandAftRef.current = bandAft;

    // VWAP rendered before candles so it appears behind them
    const vwap = chart.addSeries(LineSeries, {
      color: "rgba(255,255,255,0.85)",
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: true,
      crosshairMarkerVisible: false,
      title: "VWAP",
    });

    const candle = chart.addSeries(CandlestickSeries, {
      upColor: "#22c55e",
      downColor: "#ef4444",
      borderVisible: false,
      wickUpColor: "#22c55e",
      wickDownColor: "#ef4444",
    });
    const markerApi = createSeriesMarkers(candle, [], { zOrder: "aboveSeries" });

    const vol = chart.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      priceLineVisible: false,
      lastValueVisible: false,
      base: 0,
    });
    vol.priceScale().applyOptions({ visible: false, scaleMargins: { top: 0.8, bottom: 0 } });

    chartRef.current = chart;
    candleRef.current = candle;
    vwapRef.current = vwap;
    volRef.current = vol;
    markerApiRef.current = markerApi;

    chart.subscribeClick((param) => {
      if (!param.time || !onMarkerClickRef.current) return;
      const matched = markersRef.current.find((m) => m.time === param.time && m.id);
      if (matched?.id) onMarkerClickRef.current(matched.id as string);
    });

    return () => {
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      vwapRef.current = null;
      volRef.current = null;
      markerApiRef.current = null;
      emaSeriesRef.current.clear();
      bandPrevDayRef.current = null;
      bandPreRef.current = null;
      bandRegRef.current = null;
      bandAftRef.current = null;
    };
  }, []);

  // Bar data effect — full setData on symbol/timeframe change, incremental update otherwise
  useEffect(() => {
    const chart = chartRef.current;
    const candle = candleRef.current;
    const vwap = vwapRef.current;
    if (!chart || !candle || !vwap) return;

    const cache = epochCacheRef.current;
    const prevBars = prevBarsRef.current;
    const isViewportChange = viewportKey !== lastViewportKeyRef.current;

    // Detect whether this is a live update (only the last bar changed or a new bar appended).
    // Conditions: same viewportKey, non-empty arrays, length diff ≤ 1, and second-to-last
    // timestamp in new bars matches what we had before (confirming only the tail changed).
    const isLiveUpdate =
      !isViewportChange &&
      bars.length > 0 &&
      prevBars.length > 0 &&
      (bars.length === prevBars.length || bars.length === prevBars.length + 1) &&
      (bars.length < 2 ||
        bars[bars.length - 2].timestamp ===
          prevBars[prevBars.length - (bars.length === prevBars.length ? 2 : 1)]?.timestamp);

    if (isLiveUpdate && bars.length > 0) {
      const lastBar = bars[bars.length - 1];
      const t = getCachedEpoch(cache, lastBar.timestamp);
      const isNewBar = bars.length === prevBars.length + 1;

      // Candle
      candle.update({
        time: t as Time,
        open: Number(lastBar.open),
        high: Number(lastBar.high),
        low: Number(lastBar.low),
        close: Number(lastBar.close),
      });

      // Volume
      const vol = Number(lastBar.volume ?? 0);
      if (vol > 0) {
        const volColor = Number(lastBar.close) >= Number(lastBar.open) ? VOL_UP : VOL_DOWN;
        volRef.current?.update({ time: t as Time, value: vol, color: volColor });
      }

      // VWAP
      const preLastVwap = isNewBar ? vwapLastRef.current : vwapPreLastRef.current;
      const { value: vwapValue, nextState: newVwapState } = vwapPointFromState(preLastVwap, lastBar, t, is24h ?? false);
      vwap.update({ time: t as Time, value: vwapValue });
      if (isNewBar) vwapPreLastRef.current = vwapLastRef.current;
      vwapLastRef.current = newVwapState;

      // EMA
      for (const { period } of emas) {
        const preLastEma = isNewBar
          ? emaLastRef.current.get(period)
          : emaPreLastRef.current.get(period);
        if (!preLastEma) continue;
        if (preLastEma.index < period - 2) continue; // not yet warmed up
        const { value: emaValue, nextState: newEmaState } = emaPointFromState(
          preLastEma,
          Number(lastBar.close),
          period
        );
        if (newEmaState.index >= period - 1) {
          emaSeriesRef.current.get(period)?.update({ time: t as Time, value: emaValue });
        }
        if (isNewBar) emaPreLastRef.current.set(period, emaLastRef.current.get(period)!);
        emaLastRef.current.set(period, newEmaState);
      }

      // Session band — only append on new bar
      if (isNewBar) {
        if (prevDayDate && lastBar.timestamp.startsWith(prevDayDate)) {
          bandPrevDayRef.current?.update({ time: t as Time, value: 1 });
        } else {
          const session = classifySession(t, is24h);
          if (session === "premarket")        bandPreRef.current?.update({ time: t as Time, value: 1 });
          else if (session === "regular")     bandRegRef.current?.update({ time: t as Time, value: 1 });
          else if (session === "aftermarket") bandAftRef.current?.update({ time: t as Time, value: 1 });
        }
      }
    } else {
      // Full reset: rebuild all series data

      // Session background bands.
      // Bars whose ISO timestamp belongs to prevDayDate get the slate "prev-day"
      // tint instead of their normal session colour — makes prior-day context bars
      // immediately distinguishable from the target session.
      const bandData: Record<SessionBand, { time: Time; value: number }[]> = {
        premarket: [], regular: [], aftermarket: [],
      };
      const bandPrevDayData: { time: Time; value: number }[] = [];
      for (const { t, b } of buildSortedDeduped(bars, cache)) {
        if (prevDayDate && b.timestamp.startsWith(prevDayDate)) {
          bandPrevDayData.push({ time: t as Time, value: 1 });
        } else {
          const session = classifySession(t, is24h);
          if (session) bandData[session].push({ time: t as Time, value: 1 });
        }
      }
      bandPrevDayRef.current?.setData(bandPrevDayData);
      bandPreRef.current?.setData(bandData.premarket);
      bandRegRef.current?.setData(bandData.regular);
      bandAftRef.current?.setData(bandData.aftermarket);

      candle.setData(buildCandleData(bars, cache));

      // Volume
      const volData = buildSortedDeduped(bars, cache)
        .filter(({ b }) => Number(b.volume ?? 0) > 0)
        .map(({ t, b }) => ({
          time: t as Time,
          value: Number(b.volume),
          color: Number(b.close) >= Number(b.open) ? VOL_UP : VOL_DOWN,
        }));
      volRef.current?.setData(volData);

      const { data: vwapData, statePreLast, stateLast } = buildVwapData(bars, cache, is24h ?? false);
      vwap.setData(vwapData);
      vwapPreLastRef.current = statePreLast;
      vwapLastRef.current = stateLast;

      // Reconcile EMA series
      const wanted = new Set(emas.map((e) => e.period));
      for (const [period, series] of emaSeriesRef.current) {
        if (!wanted.has(period)) {
          chart.removeSeries(series);
          emaSeriesRef.current.delete(period);
        }
      }
      for (const { period, color } of emas) {
        if (!emaSeriesRef.current.has(period)) {
          const series = chart.addSeries(LineSeries, {
            color,
            lineWidth: 1,
            priceLineVisible: false,
            lastValueVisible: true,
            crosshairMarkerVisible: false,
            title: `EMA${period}`,
          });
          emaSeriesRef.current.set(period, series);
        }
        const { data: emaData, statePreLast: ePre, stateLast: eLast } = buildEmaData(
          bars,
          period,
          cache
        );
        emaSeriesRef.current.get(period)!.setData(emaData);
        if (ePre) emaPreLastRef.current.set(period, ePre);
        if (eLast) emaLastRef.current.set(period, eLast);
      }

      if (isViewportChange) {
        chart.timeScale().fitContent();
        candle.priceScale().applyOptions({ autoScale: true });
        lastViewportKeyRef.current = viewportKey;
      }
    }

    prevBarsRef.current = bars;
  }, [bars, emas, viewportKey, is24h, prevDayDate]);

  // Overlay / marker effect — price lines and markers update independently of bar data
  useEffect(() => {
    const candle = candleRef.current;
    if (!candle) return;

    candle.priceLines().forEach((line) => candle.removePriceLine(line));
    for (const overlay of overlays) {
      candle.createPriceLine({
        price: overlay.price,
        color: overlay.color,
        lineWidth: 1,
        lineStyle: overlay.style ?? LineStyle.Solid,
        axisLabelVisible: true,
        title: overlay.label,
      });
    }
    markerApiRef.current?.setMarkers(markers);
    markersRef.current = markers;
  }, [overlays, markers]);

  // Keep callback ref in sync
  useEffect(() => {
    onMarkerClickRef.current = onMarkerClick;
  }, [onMarkerClick]);

  // Scroll chart to focusTime, preserving current zoom level
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || focusTime === undefined) return;
    const range = chart.timeScale().getVisibleRange();
    if (!range) return;
    const half = ((range.to as number) - (range.from as number)) / 2;
    chart.timeScale().setVisibleRange({
      from: ((focusTime as number) - half) as Time,
      to: ((focusTime as number) + half) as Time,
    });
  }, [focusTime]);

  // 'R' key — reset view (fit content + re-enable price auto-scale), like TradingView
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.code !== "KeyR" || !e.altKey) return;
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      const chart = chartRef.current;
      const candle = candleRef.current;
      if (!chart || !candle) return;
      chart.timeScale().fitContent();
      candle.priceScale().applyOptions({ autoScale: true });
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, []);

  return <div ref={containerRef} style={{ height: 420, width: "100%" }} />;
}
