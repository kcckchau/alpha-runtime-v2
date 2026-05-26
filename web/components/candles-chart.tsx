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
  TickMarkType,
  Time,
  createChart,
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

// ─── Data helpers ─────────────────────────────────────────────────────────────

function sortedDeduped<T extends { time: number }>(items: T[]): T[] {
  const map = new Map<number, T>();
  for (const item of items) map.set(item.time, item);
  return [...map.values()].sort((a, b) => a.time - b.time);
}

function toCandleData(bars: BarRow[]): CandlestickData<Time>[] {
  return sortedDeduped(
    bars.map((b) => ({
      time: toETEpoch(b.timestamp),
      open: Number(b.open),
      high: Number(b.high),
      low: Number(b.low),
      close: Number(b.close),
    }))
  ).map((d) => ({ ...d, time: d.time as Time }));
}

// VWAP computed from OHLV (IBKR bars don't carry per-bar VWAP).
// Resets on gaps > 1 hour so each trading session gets its own VWAP.
function computeVwap(bars: BarRow[]): LineData<Time>[] {
  const sorted = sortedDeduped(
    bars
      .map((b) => ({
        time: toETEpoch(b.timestamp),
        high: Number(b.high),
        low: Number(b.low),
        close: Number(b.close),
        volume: Number(b.volume ?? 0),
      }))
      .filter((b) => isFinite(b.close))
  );

  let cumTPV = 0;
  let cumVol = 0;
  let prevTime: number | null = null;

  return sorted.map((bar) => {
    if (prevTime !== null && bar.time - prevTime > 3600) {
      cumTPV = 0;
      cumVol = 0;
    }
    prevTime = bar.time;
    const typical = (bar.high + bar.low + bar.close) / 3;
    cumTPV += typical * bar.volume;
    cumVol += bar.volume;
    return { time: bar.time as Time, value: cumVol > 0 ? cumTPV / cumVol : bar.close };
  });
}

// EMA from bar closes; only emitted after `period` warmup bars
function computeEma(bars: BarRow[], period: number): LineData<Time>[] {
  const sorted = sortedDeduped(
    bars
      .map((b) => ({ time: toETEpoch(b.timestamp), value: Number(b.close) }))
      .filter((d) => isFinite(d.value))
  );
  if (sorted.length < period) return [];
  const alpha = 2 / (period + 1);
  let ema = sorted[0].value;
  const result: LineData<Time>[] = [];
  for (let i = 0; i < sorted.length; i++) {
    ema = i === 0 ? sorted[i].value : sorted[i].value * alpha + ema * (1 - alpha);
    if (i >= period - 1) result.push({ time: sorted[i].time as Time, value: ema });
  }
  return result;
}

// ─── Component ────────────────────────────────────────────────────────────────

export function CandlesChart({ bars, overlays, emas = [] }: CandlesChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const vwapRef = useRef<ISeriesApi<"Line"> | null>(null);
  const emaSeriesRef = useRef<Map<number, ISeriesApi<"Line">>>(new Map());

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

    chartRef.current = chart;
    candleRef.current = candle;
    vwapRef.current = vwap;

    return () => {
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      vwapRef.current = null;
      emaSeriesRef.current.clear();
    };
  }, []);

  useEffect(() => {
    const chart = chartRef.current;
    const candle = candleRef.current;
    const vwap = vwapRef.current;
    if (!chart || !candle || !vwap) return;

    candle.setData(toCandleData(bars));
    vwap.setData(computeVwap(bars));

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
      emaSeriesRef.current.get(period)!.setData(computeEma(bars, period));
    }

    // Setup level price lines (entry / SL / TP)
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

    chart.timeScale().fitContent();
  }, [bars, overlays, emas]);

  return <div ref={containerRef} style={{ height: 420, width: "100%" }} />;
}
