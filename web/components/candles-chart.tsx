"use client";

import { useEffect, useRef } from "react";
import {
  CandlestickData,
  CandlestickSeries,
  ColorType,
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

type VwapState = { cumTPV: number; cumVol: number; prevTime: number | null };
type EmaState = { value: number; index: number };

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

function buildVwapData(bars: BarRow[], cache: Map<string, number>): VwapResult {
  const items = buildSortedDeduped(bars, cache).filter((i) =>
    isFinite(Number(i.b.close))
  );

  let cumTPV = 0,
    cumVol = 0,
    prevTime: number | null = null;
  let statePreLast: VwapState = { cumTPV: 0, cumVol: 0, prevTime: null };
  const data: LineData<Time>[] = [];

  for (let i = 0; i < items.length; i++) {
    const { t, b } = items[i];

    // Capture state before last bar
    if (i === items.length - 1) {
      statePreLast = { cumTPV, cumVol, prevTime };
    }

    // Session reset on gap > 1 hour
    if (prevTime !== null && t - prevTime > 3600) {
      cumTPV = 0;
      cumVol = 0;
      if (i === items.length - 1) {
        statePreLast = { cumTPV: 0, cumVol: 0, prevTime };
      }
    }

    const typical = (Number(b.high) + Number(b.low) + Number(b.close)) / 3;
    const vol = Number(b.volume ?? 0);
    cumTPV += typical * vol;
    cumVol += vol;
    prevTime = t;
    data.push({ time: t as Time, value: cumVol > 0 ? cumTPV / cumVol : Number(b.close) });
  }

  return { data, statePreLast, stateLast: { cumTPV, cumVol, prevTime } };
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
  t: number
): { value: number; nextState: VwapState } {
  let { cumTPV, cumVol } = state;
  if (state.prevTime !== null && t - state.prevTime > 3600) {
    cumTPV = 0;
    cumVol = 0;
  }
  const typical = (Number(bar.high) + Number(bar.low) + Number(bar.close)) / 3;
  const vol = Number(bar.volume ?? 0);
  cumTPV += typical * vol;
  cumVol += vol;
  return {
    value: cumVol > 0 ? cumTPV / cumVol : Number(bar.close),
    nextState: { cumTPV, cumVol, prevTime: t },
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

// ─── Component ────────────────────────────────────────────────────────────────

export function CandlesChart({
  bars,
  overlays,
  emas = [],
  markers = [],
  viewportKey,
}: CandlesChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const vwapRef = useRef<ISeriesApi<"Line"> | null>(null);
  const emaSeriesRef = useRef<Map<number, ISeriesApi<"Line">>>(new Map());
  const markerApiRef = useRef<ReturnType<typeof createSeriesMarkers<Time>> | null>(null);
  const lastViewportKeyRef = useRef<string | undefined>(undefined);

  // Epoch cache: avoids repeated Intl.DateTimeFormat calls for already-seen timestamps
  const epochCacheRef = useRef<Map<string, number>>(new Map());

  // Incremental VWAP state: state before and at the last bar
  const vwapPreLastRef = useRef<VwapState>({ cumTPV: 0, cumVol: 0, prevTime: null });
  const vwapLastRef = useRef<VwapState>({ cumTPV: 0, cumVol: 0, prevTime: null });

  // Incremental EMA state per period
  const emaPreLastRef = useRef<Map<number, EmaState>>(new Map());
  const emaLastRef = useRef<Map<number, EmaState>>(new Map());

  // Previous bars reference for detecting live updates
  const prevBarsRef = useRef<BarRow[]>([]);

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

    chartRef.current = chart;
    candleRef.current = candle;
    vwapRef.current = vwap;
    markerApiRef.current = markerApi;

    return () => {
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      vwapRef.current = null;
      markerApiRef.current = null;
      emaSeriesRef.current.clear();
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

      // VWAP
      const preLastVwap = isNewBar ? vwapLastRef.current : vwapPreLastRef.current;
      const { value: vwapValue, nextState: newVwapState } = vwapPointFromState(preLastVwap, lastBar, t);
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
    } else {
      // Full reset: rebuild all series data
      candle.setData(buildCandleData(bars, cache));

      const { data: vwapData, statePreLast, stateLast } = buildVwapData(bars, cache);
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
        lastViewportKeyRef.current = viewportKey;
      }
    }

    prevBarsRef.current = bars;
  }, [bars, emas, viewportKey]);

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
  }, [overlays, markers]);

  return <div ref={containerRef} style={{ height: 420, width: "100%" }} />;
}
