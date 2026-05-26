"use client";

import { useEffect, useRef } from "react";
import {
  CandlestickData,
  CandlestickSeries,
  ColorType,
  IChartApi,
  ISeriesApi,
  LineStyle,
  Time,
  createChart,
} from "lightweight-charts";

type BarRow = {
  timestamp: string;
  open: string | number;
  high: string | number;
  low: string | number;
  close: string | number;
};

type OverlayLine = {
  label: string;
  price: number;
  color: string;
  style?: number;
};

type CandlesChartProps = {
  bars: BarRow[];
  overlays: OverlayLine[];
};

function toChartData(bars: BarRow[]): CandlestickData<Time>[] {
  const deduped = new Map<number, CandlestickData<Time>>();

  for (const bar of bars) {
    const epochSeconds = Math.floor(new Date(bar.timestamp).getTime() / 1000);
    deduped.set(epochSeconds, {
      time: epochSeconds as Time,
      open: Number(bar.open),
      high: Number(bar.high),
      low: Number(bar.low),
      close: Number(bar.close),
    });
  }

  return [...deduped.entries()]
    .sort(([a], [b]) => a - b)
    .map(([, bar]) => bar);
}

export function CandlesChart({ bars, overlays }: CandlesChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);

  useEffect(() => {
    if (!containerRef.current || chartRef.current) return;

    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "#111111" },
        textColor: "rgba(255,255,255,0.35)",
        fontFamily: "'IBM Plex Mono', monospace",
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.04)" },
        horzLines: { color: "rgba(255,255,255,0.04)" },
      },
      rightPriceScale: {
        borderColor: "rgba(255,255,255,0.08)",
      },
      timeScale: {
        borderColor: "rgba(255,255,255,0.08)",
        timeVisible: true,
        secondsVisible: false,
      },
      crosshair: {
        vertLine: { color: "rgba(255,255,255,0.15)" },
        horzLine: { color: "rgba(255,255,255,0.15)" },
      },
    });

    const series = chart.addSeries(CandlestickSeries, {
      upColor: "#22c55e",
      downColor: "#ef4444",
      borderVisible: false,
      wickUpColor: "#22c55e",
      wickDownColor: "#ef4444",
    });

    chartRef.current = chart;
    seriesRef.current = series;

    return () => {
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) return;

    series.setData(toChartData(bars));
    series.priceLines().forEach((line) => series.removePriceLine(line));

    overlays.forEach((overlay) => {
      series.createPriceLine({
        price: overlay.price,
        color: overlay.color,
        lineWidth: 1,
        lineStyle: overlay.style ?? LineStyle.Solid,
        axisLabelVisible: true,
        title: overlay.label,
      });
    });

    chart.timeScale().fitContent();
  }, [bars, overlays]);

  return <div ref={containerRef} style={{ height: 420, width: "100%" }} />;
}
