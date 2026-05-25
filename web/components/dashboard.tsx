"use client";

import { useEffect, useMemo, useState } from "react";
import { LineStyle } from "lightweight-charts";
import { CandlesChart } from "@/components/candles-chart";

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
};

type SymbolContext = {
  symbol: string;
  bar_counts: Record<string, number>;
  ema_levels: Record<string, Record<string, string | null>>;
  levels: Record<string, string | null>;
};

const API_BASE =
  process.env.NEXT_PUBLIC_ALPHA_API_BASE_URL?.replace(/\/$/, "") ?? "http://127.0.0.1:8000";

const TIMEFRAME_COLORS: Record<string, string> = {
  "1h": "#0f6c5c",
  "1d": "#b8892d",
  "1mo": "#6b4da7",
};

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Request failed for ${path}: ${response.status}`);
  }
  return (await response.json()) as T;
}

function formatPrice(value: string | number | null | undefined): string {
  if (value === null || value === undefined) {
    return "—";
  }
  const price = Number(value);
  if (!Number.isFinite(price)) {
    return "—";
  }
  return price.toLocaleString(undefined, {
    minimumFractionDigits: price >= 1000 ? 1 : 2,
    maximumFractionDigits: 2,
  });
}

function levelLabel(timeframe: string, period: string): string {
  return `${timeframe.toUpperCase()} EMA ${period}`;
}

function historyStartDate(symbol: string): string {
  const now = new Date();
  const lookbackDays = symbol === "MNQ" ? 7 : 5;
  const start = new Date(now.getTime() - lookbackDays * 24 * 60 * 60 * 1000);
  return start.toISOString().slice(0, 10);
}

function todayDate(): string {
  return new Date().toISOString().slice(0, 10);
}

export function Dashboard() {
  const [status, setStatus] = useState<RuntimeStatus | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState("MNQ");
  const [contexts, setContexts] = useState<Record<string, SymbolContext | null>>({});
  const [quotes, setQuotes] = useState<Record<string, QuoteRow | null>>({});
  const [bars, setBars] = useState<BarHistoryRow[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function loadAll() {
      try {
        const statusData = await fetchJson<RuntimeStatus>("/runtime/status");
        const symbol = statusData.symbols.includes(selectedSymbol)
          ? selectedSymbol
          : (statusData.symbols[0] ?? "MNQ");

        const [contextData, quoteData, barData] = await Promise.all([
          fetchJson<Record<string, SymbolContext | null>>(`/runtime/contexts?symbol=${symbol}`),
          fetchJson<Record<string, QuoteRow | null>>(`/runtime/quotes?symbol=${symbol}`),
          fetchJson<BarHistoryRow[]>(
            `/runtime/bars/history?symbol=${symbol}&timeframe=1m&start=${historyStartDate(symbol)}&end=${todayDate()}`,
          ),
        ]);

        if (cancelled) {
          return;
        }

        setStatus(statusData);
        setSelectedSymbol(symbol);
        setContexts((prev) => ({ ...prev, ...contextData }));
        setQuotes((prev) => ({ ...prev, ...quoteData }));
        setBars(barData);
        setError(null);
      } catch (err) {
        if (cancelled) {
          return;
        }
        setError(err instanceof Error ? err.message : "Unknown dashboard error");
      }
    }

    void loadAll();
    const interval = window.setInterval(() => void loadAll(), 15000);

    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [selectedSymbol]);

  const currentContext = contexts[selectedSymbol] ?? null;
  const currentQuote = quotes[selectedSymbol] ?? null;

  const overlayLines = useMemo(() => {
    if (!currentContext) {
      return [];
    }

    const overlays: Array<{ label: string; price: number; color: string; style?: number }> = [];
    for (const [timeframe, levels] of Object.entries(currentContext.ema_levels)) {
      for (const [period, value] of Object.entries(levels)) {
        if (!value) {
          continue;
        }
        overlays.push({
          label: levelLabel(timeframe, period),
          price: Number(value),
          color: TIMEFRAME_COLORS[timeframe] ?? "#444",
          style: timeframe === "1mo" ? LineStyle.Dashed : LineStyle.Solid,
        });
      }
    }

    for (const [label, value] of Object.entries(currentContext.levels)) {
      if (!value) {
        continue;
      }
      overlays.push({
        label: label.replaceAll("_", " "),
        price: Number(value),
        color: label.includes("high") ? "#b64b33" : "#3756a6",
        style: LineStyle.LargeDashed,
      });
    }

    return overlays;
  }, [currentContext]);

  return (
    <main style={{ padding: "24px" }}>
      <section
        style={{
          maxWidth: 1440,
          margin: "0 auto",
          display: "grid",
          gap: 20,
        }}
      >
        <header
          style={{
            background: "var(--panel)",
            border: "1px solid var(--line)",
            borderRadius: 24,
            boxShadow: "var(--shadow)",
            padding: 24,
          }}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "end",
              gap: 16,
              flexWrap: "wrap",
            }}
          >
            <div>
              <div style={{ color: "var(--muted)", fontSize: 13, letterSpacing: "0.12em" }}>
                ALPHA RUNTIME V2
              </div>
              <h1 style={{ margin: "8px 0 4px", fontSize: "clamp(2rem, 4vw, 3.4rem)" }}>
                Multi-timeframe operator board
              </h1>
              <p style={{ margin: 0, color: "var(--muted)", maxWidth: 760 }}>
                Startup context, intraday history, and live quote snapshots in one place so we can
                verify catch-up behavior before layering in more strategy logic.
              </p>
            </div>

            <label
              style={{
                display: "grid",
                gap: 8,
                color: "var(--muted)",
                fontSize: 13,
                minWidth: 180,
              }}
            >
              Active symbol
              <select
                value={selectedSymbol}
                onChange={(event) => setSelectedSymbol(event.target.value)}
                style={{
                  borderRadius: 14,
                  border: "1px solid var(--line)",
                  padding: "10px 14px",
                  background: "var(--panel-strong)",
                  color: "var(--ink)",
                  fontSize: 16,
                }}
              >
                {(status?.symbols ?? ["MNQ"]).map((symbol) => (
                  <option key={symbol} value={symbol}>
                    {symbol}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </header>

        {error ? (
          <div
            style={{
              background: "rgba(182, 75, 51, 0.1)",
              border: "1px solid rgba(182, 75, 51, 0.18)",
              color: "var(--danger)",
              borderRadius: 20,
              padding: 18,
            }}
          >
            {error}
          </div>
        ) : null}

        <section
          style={{
            display: "grid",
            gridTemplateColumns: "minmax(0, 2.1fr) minmax(320px, 0.9fr)",
            gap: 20,
          }}
        >
          <article
            style={{
              background: "var(--panel)",
              border: "1px solid var(--line)",
              borderRadius: 24,
              boxShadow: "var(--shadow)",
              padding: 20,
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div>
                <h2 style={{ margin: 0, fontSize: 24 }}>{selectedSymbol} intraday chart</h2>
                <p style={{ margin: "6px 0 0", color: "var(--muted)" }}>
                  1-minute history with current daily, hourly, and monthly levels overlaid.
                </p>
              </div>
              <div
                style={{
                  background: "var(--accent-soft)",
                  color: "var(--accent)",
                  borderRadius: 999,
                  padding: "8px 12px",
                  fontSize: 13,
                }}
              >
                Polling every 15s
              </div>
            </div>
            <div style={{ marginTop: 18 }}>
              <CandlesChart bars={bars} overlays={overlayLines} />
            </div>
          </article>

          <aside style={{ display: "grid", gap: 20 }}>
            <Panel title="Runtime">
              <KeyValue label="Mode" value={status?.mode ?? "—"} />
              <KeyValue label="State" value={status?.runtime_state ?? "—"} />
              <KeyValue
                label="Updated"
                value={status?.updated_at ? new Date(status.updated_at).toLocaleString() : "—"}
              />
              <KeyValue
                label="API snapshot"
                value={status?.runtime_available ? "available" : "not ready"}
              />
            </Panel>

            <Panel title="Quote">
              <KeyValue label="Bid" value={formatPrice(currentQuote?.bid_price)} />
              <KeyValue label="Ask" value={formatPrice(currentQuote?.ask_price)} />
              <KeyValue label="Bid size" value={currentQuote?.bid_size?.toString() ?? "—"} />
              <KeyValue label="Ask size" value={currentQuote?.ask_size?.toString() ?? "—"} />
              <KeyValue
                label="Quote time"
                value={currentQuote?.timestamp ? new Date(currentQuote.timestamp).toLocaleString() : "—"}
              />
            </Panel>

            <Panel title="Session levels">
              {Object.entries(currentContext?.levels ?? {}).map(([label, value]) => (
                <KeyValue key={label} label={label.replaceAll("_", " ")} value={formatPrice(value)} />
              ))}
            </Panel>
          </aside>
        </section>

        <section
          style={{
            display: "grid",
            gridTemplateColumns: "1.1fr 0.9fr",
            gap: 20,
          }}
        >
          <Panel title="EMA context">
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
                gap: 16,
              }}
            >
              {Object.entries(currentContext?.ema_levels ?? {}).map(([timeframe, levels]) => (
                <div
                  key={timeframe}
                  style={{
                    background: "var(--panel-strong)",
                    border: "1px solid var(--line)",
                    borderRadius: 18,
                    padding: 16,
                  }}
                >
                  <div
                    style={{
                      fontSize: 13,
                      letterSpacing: "0.1em",
                      color: TIMEFRAME_COLORS[timeframe] ?? "var(--muted)",
                      marginBottom: 10,
                    }}
                  >
                    {timeframe.toUpperCase()}
                  </div>
                  {Object.entries(levels).map(([period, value]) => (
                    <KeyValue key={period} label={`EMA ${period}`} value={formatPrice(value)} compact />
                  ))}
                </div>
              ))}
            </div>
          </Panel>

          <Panel title="Engine health">
            <div style={{ display: "grid", gap: 10 }}>
              {(status?.engines ?? []).map((engine) => (
                <div
                  key={engine.name}
                  style={{
                    display: "grid",
                    gridTemplateColumns: "1fr auto auto",
                    gap: 12,
                    alignItems: "center",
                    paddingBottom: 10,
                    borderBottom: "1px solid var(--line)",
                  }}
                >
                  <div>{engine.name}</div>
                  <Pill>{engine.state}</Pill>
                  <Pill tone={engine.health === "healthy" ? "good" : "warn"}>{engine.health}</Pill>
                </div>
              ))}
            </div>
          </Panel>
        </section>
      </section>
    </main>
  );
}

function Panel({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <article
      style={{
        background: "var(--panel)",
        border: "1px solid var(--line)",
        borderRadius: 24,
        boxShadow: "var(--shadow)",
        padding: 20,
      }}
    >
      <h2 style={{ marginTop: 0, marginBottom: 16, fontSize: 22 }}>{title}</h2>
      {children}
    </article>
  );
}

function KeyValue({
  label,
  value,
  compact = false,
}: {
  label: string;
  value: string;
  compact?: boolean;
}) {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        gap: 12,
        padding: compact ? "6px 0" : "10px 0",
        borderBottom: compact ? "none" : "1px solid var(--line)",
        textTransform: "capitalize",
      }}
    >
      <span style={{ color: "var(--muted)" }}>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function Pill({
  children,
  tone = "neutral",
}: {
  children: React.ReactNode;
  tone?: "neutral" | "good" | "warn";
}) {
  const colors = {
    neutral: {
      background: "rgba(63, 49, 36, 0.08)",
      color: "#4a3c2f",
    },
    good: {
      background: "rgba(15, 108, 92, 0.12)",
      color: "#0f6c5c",
    },
    warn: {
      background: "rgba(184, 137, 45, 0.16)",
      color: "#8a631a",
    },
  }[tone];

  return (
    <span
      style={{
        ...colors,
        borderRadius: 999,
        fontSize: 12,
        padding: "6px 10px",
        textTransform: "uppercase",
        letterSpacing: "0.06em",
      }}
    >
      {children}
    </span>
  );
}
