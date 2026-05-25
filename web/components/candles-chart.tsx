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
    .sort(([left], [right]) => left - right)
    .map(([, bar]) => bar);
}

export function CandlesChart({ bars, overlays }: CandlesChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);

  useEffect(() => {
    if (!containerRef.current || chartRef.current) {
      return;
    }

    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "rgba(255, 252, 246, 0.9)" },
        textColor: "#30261d",
        fontFamily: 'Georgia, "Times New Roman", serif',
      },
      grid: {
        vertLines: { color: "rgba(63, 49, 36, 0.08)" },
        horzLines: { color: "rgba(63, 49, 36, 0.08)" },
      },
      rightPriceScale: {
        borderColor: "rgba(63, 49, 36, 0.18)",
      },
      timeScale: {
        borderColor: "rgba(63, 49, 36, 0.18)",
        timeVisible: true,
        secondsVisible: false,
      },
      crosshair: {
        vertLine: { color: "rgba(15, 108, 92, 0.25)" },
        horzLine: { color: "rgba(15, 108, 92, 0.25)" },
      },
    });

    const series = chart.addSeries(CandlestickSeries, {
      upColor: "#157f6b",
      downColor: "#bd4f36",
      borderVisible: false,
      wickUpColor: "#157f6b",
      wickDownColor: "#bd4f36",
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
    if (!chart || !series) {
      return;
    }

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

  return <div ref={containerRef} style={{ height: 460, width: "100%" }} />;
}
