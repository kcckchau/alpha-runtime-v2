"use client";

/**
 * TradingHotkeys — keyboard-driven trade intent submission.
 *
 * Renders:
 *   - An ARM button (shown in topbar area — caller positions it)
 *   - A trade preview overlay on the chart when an intent is pending
 *
 * Hotkeys (only active when armed and chart is focused):
 *   Ctrl+Q  → open long
 *   Ctrl+A  → open short
 *   Escape  → cancel pending confirmation / close overlay / disarm
 *   Enter   → confirm an amber (awaiting_confirmation) intent
 *
 * Safety guards:
 *   - event.repeat → ignored (no accidental key-hold fire)
 *   - input/textarea focused → hotkeys disabled
 *   - isLive=false (historical mode) → arm button hidden, hotkeys disabled
 *   - submitting in flight → second hotkey ignored
 *   - overlay auto-dismisses after 4s for terminal states (rejected, submitted)
 */

import { useCallback, useEffect, useId, useRef, useState } from "react";

// ── API base (same resolution as dashboard.tsx) ─────────────────────────────
function resolveApiBase(): string {
  const raw = (process.env.NEXT_PUBLIC_ALPHA_API_BASE_URL ?? "http://127.0.0.1:8000").replace(/\/$/, "");
  try {
    const parsed = new URL(raw);
    if (parsed.hostname === "0.0.0.0") parsed.hostname = "localhost";
    return parsed.toString().replace(/\/$/, "");
  } catch {
    return raw;
  }
}
const API_BASE = resolveApiBase();

// ── Types ────────────────────────────────────────────────────────────────────

type Action = "open_long" | "open_short" | "flatten";

type IntentResponse = {
  intent_id: string;
  status: string;
  decision?: string;
  entry_price?: number;
  stop_price?: number;
  target_price?: number;
  risk_usd?: number;
  reward_risk?: number;
  explanation?: string[];
  risk_flags?: string[];
  requires_confirmation?: boolean;
};

type OverlayState =
  | {
      type: "preview";
      action: Action;
      entry: number;
      stop: number;
      target: number | null;
      risk: number;
      rr: number;
      requires_confirmation: boolean;
      intent_id: string;
    }
  | { type: "active"; action: Action; entry: number; intent_id: string }
  | { type: "rejected"; reasons: string[]; flags: string[] }
  | { type: "error"; message: string };

type QuoteData = {
  bid_price: string | null;
  ask_price: string | null;
  last_price: string | null;
  timestamp: string;
};

type Props = {
  symbol: string;
  quote: QuoteData | null;
  isLive: boolean;
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function isInputFocused(): boolean {
  const el = document.activeElement;
  return (
    el instanceof HTMLInputElement ||
    el instanceof HTMLTextAreaElement ||
    el instanceof HTMLSelectElement ||
    (el instanceof HTMLElement && el.isContentEditable)
  );
}

function actionLabel(action: Action): string {
  if (action === "open_long") return "LONG";
  if (action === "open_short") return "SHORT";
  return "FLATTEN";
}

function actionColor(action: Action): string {
  if (action === "open_long") return "#22c55e";
  if (action === "open_short") return "#ef4444";
  return "#fbbf24";
}

// ── Component ─────────────────────────────────────────────────────────────────

export function TradingHotkeys({ symbol, quote, isLive }: Props) {
  const [armed, setArmed] = useState(false);
  const [overlay, setOverlay] = useState<OverlayState | null>(null);
  const submittingRef = useRef(false);
  const sessionIdRef = useRef(`sess-${Math.random().toString(36).slice(2, 9)}`);
  const hotkeyCountRef = useRef(0);
  const dismissTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Auto-dismiss terminal overlay after 4s
  const autoDismiss = useCallback((ms = 4000) => {
    if (dismissTimerRef.current) clearTimeout(dismissTimerRef.current);
    dismissTimerRef.current = setTimeout(() => setOverlay(null), ms);
  }, []);

  const clearDismiss = useCallback(() => {
    if (dismissTimerRef.current) {
      clearTimeout(dismissTimerRef.current);
      dismissTimerRef.current = null;
    }
  }, []);

  // Disarm + clear when switching to historical or symbol changes
  useEffect(() => {
    if (!isLive) {
      setArmed(false);
      setOverlay(null);
    }
  }, [isLive, symbol]);

  // ── API calls ──────────────────────────────────────────────────────────────

  const submitIntent = useCallback(async (action: Action): Promise<IntentResponse | null> => {
    if (submittingRef.current) return null;
    submittingRef.current = true;

    const key = `${sessionIdRef.current}:hk-${++hotkeyCountRef.current}`;
    const clientQuote = quote
      ? {
          bid: parseFloat(quote.bid_price ?? "0"),
          ask: parseFloat(quote.ask_price ?? "0"),
          last: parseFloat(quote.last_price ?? "0"),
          timestamp: quote.timestamp,
        }
      : null;

    try {
      const res = await fetch(`${API_BASE}/trade-intents`, {
        method: "POST",
        cache: "no-store",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": key,
        },
        body: JSON.stringify({
          instrument_id: symbol,
          action,
          quantity: 1,
          price_mode: "marketable_limit",
          stop_distance_ticks: 8,
          target_distance_ticks: 16,
          client_quote: clientQuote,
        }),
      });
      return (await res.json()) as IntentResponse;
    } catch (e) {
      return null;
    } finally {
      submittingRef.current = false;
    }
  }, [symbol, quote]);

  const confirmIntent = useCallback(async (intentId: string): Promise<IntentResponse | null> => {
    if (submittingRef.current) return null;
    submittingRef.current = true;
    try {
      const res = await fetch(`${API_BASE}/trade-intents/${intentId}/confirm`, {
        method: "POST",
        cache: "no-store",
      });
      return (await res.json()) as IntentResponse;
    } catch {
      return null;
    } finally {
      submittingRef.current = false;
    }
  }, []);

  const cancelIntent = useCallback(async (intentId: string) => {
    try {
      await fetch(`${API_BASE}/trade-intents/${intentId}`, {
        method: "DELETE",
        cache: "no-store",
      });
    } catch {}
  }, []);

  // ── Intent handler ─────────────────────────────────────────────────────────

  const handleAction = useCallback(async (action: Action) => {
    const res = await submitIntent(action);
    if (!res) {
      setOverlay({ type: "error", message: "Network error — could not reach runtime" });
      autoDismiss();
      return;
    }

    if (res.status === "rejected" || res.decision === "rejected") {
      setOverlay({
        type: "rejected",
        reasons: res.explanation ?? [],
        flags: res.risk_flags ?? [],
      });
      autoDismiss();
      return;
    }

    setOverlay({
      type: "preview",
      action,
      entry: res.entry_price ?? 0,
      stop: res.stop_price ?? 0,
      target: res.target_price ?? null,
      risk: res.risk_usd ?? 0,
      rr: res.reward_risk ?? 0,
      requires_confirmation: res.requires_confirmation ?? false,
      intent_id: res.intent_id,
    });

    // Auto-confirm green intents (no second press needed)
    if (!res.requires_confirmation) {
      autoDismiss(3000);
    }
    // Amber intents: wait for Enter / second Ctrl+Q or 10s timeout
    if (res.requires_confirmation) {
      autoDismiss(10000);
    }
  }, [submitIntent, autoDismiss]);

  const handleConfirm = useCallback(async () => {
    if (!overlay || overlay.type !== "preview" || !overlay.requires_confirmation) return;
    clearDismiss();
    const intentId = overlay.intent_id;
    const res = await confirmIntent(intentId);
    if (!res) {
      setOverlay({ type: "error", message: "Confirm failed — network error" });
      autoDismiss();
      return;
    }
    if (res.status === "rejected") {
      setOverlay({
        type: "rejected",
        reasons: ["Revalidation failed: " + (res.explanation?.[0] ?? "unknown")],
        flags: res.risk_flags ?? [],
      });
      autoDismiss();
      return;
    }
    setOverlay({
      type: "active",
      action: overlay.action,
      entry: overlay.entry,
      intent_id: res.intent_id,
    });
    autoDismiss(3000);
  }, [overlay, confirmIntent, clearDismiss, autoDismiss]);

  const handleCancel = useCallback(async () => {
    if (overlay?.type === "preview" && overlay.requires_confirmation) {
      await cancelIntent(overlay.intent_id);
    }
    clearDismiss();
    setOverlay(null);
  }, [overlay, cancelIntent, clearDismiss]);

  // ── Keyboard listener ──────────────────────────────────────────────────────

  useEffect(() => {
    if (!isLive) return;

    const onKeyDown = async (e: KeyboardEvent) => {
      // Never fire on key-hold repeat
      if (e.repeat) return;
      // Never intercept while typing
      if (isInputFocused()) return;

      // Escape: cancel / close / disarm
      if (e.key === "Escape") {
        await handleCancel();
        setArmed(false);
        return;
      }

      // Enter: confirm amber intent
      if (e.key === "Enter" && overlay?.type === "preview" && overlay.requires_confirmation) {
        e.preventDefault();
        await handleConfirm();
        return;
      }

      if (!armed) return;

      // Ctrl+Q → long
      if (e.ctrlKey && e.key === "q") {
        e.preventDefault();
        // If amber overlay already up, second Ctrl+Q = confirm
        if (overlay?.type === "preview" && overlay.requires_confirmation) {
          await handleConfirm();
          return;
        }
        await handleAction("open_long");
        return;
      }

      // Ctrl+A → short
      if (e.ctrlKey && e.key === "a") {
        e.preventDefault();
        await handleAction("open_short");
        return;
      }
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [isLive, armed, overlay, handleAction, handleConfirm, handleCancel]);

  // ── Render ─────────────────────────────────────────────────────────────────

  if (!isLive) return null;

  return (
    <>
      {/* ── Arm button (inline in topbar) ─────────────────────────────────── */}
      <button
        onClick={() => {
          setArmed((a) => !a);
          if (armed) { clearDismiss(); setOverlay(null); }
        }}
        title={armed ? "Trading armed — click to disarm" : "Click to arm trading hotkeys"}
        style={{
          fontFamily: "'IBM Plex Mono', monospace",
          fontSize: 10,
          fontWeight: 600,
          letterSpacing: "0.08em",
          padding: "3px 10px",
          borderRadius: 4,
          cursor: "pointer",
          border: armed
            ? "0.5px solid rgba(34,197,94,0.6)"
            : "0.5px solid rgba(255,255,255,0.12)",
          background: armed ? "rgba(34,197,94,0.12)" : "rgba(255,255,255,0.04)",
          color: armed ? "#22c55e" : "rgba(255,255,255,0.35)",
          transition: "all 0.15s",
          userSelect: "none",
        }}
      >
        {armed ? "⚡ ARMED" : "TRADE"}
      </button>

      {/* ── Trade preview overlay ──────────────────────────────────────────── */}
      {overlay && (
        <div
          style={{
            position: "fixed",
            bottom: 80,
            right: 24,
            zIndex: 999,
            background: "#111111",
            border: "0.5px solid rgba(255,255,255,0.14)",
            borderRadius: 8,
            padding: "14px 18px",
            minWidth: 260,
            boxShadow: "0 8px 32px rgba(0,0,0,0.6)",
            fontFamily: "'IBM Plex Mono', monospace",
          }}
        >
          {/* Close */}
          <button
            onClick={handleCancel}
            style={{
              position: "absolute", top: 8, right: 10,
              background: "none", border: "none", cursor: "pointer",
              color: "rgba(255,255,255,0.3)", fontSize: 14, lineHeight: 1,
            }}
          >✕</button>

          {overlay.type === "preview" && (
            <>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
                <span style={{ color: actionColor(overlay.action), fontWeight: 700, fontSize: 12 }}>
                  {actionLabel(overlay.action)}
                </span>
                <span style={{ color: "rgba(255,255,255,0.4)", fontSize: 10 }}>
                  {symbol} · 1 contract
                </span>
                {overlay.requires_confirmation && (
                  <span style={{
                    marginLeft: "auto", fontSize: 9, fontWeight: 700,
                    color: "#fbbf24", border: "0.5px solid rgba(251,191,36,0.4)",
                    padding: "1px 6px", borderRadius: 3,
                  }}>AMBER</span>
                )}
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "4px 12px", fontSize: 11 }}>
                <span style={{ color: "rgba(255,255,255,0.35)" }}>ENTRY</span>
                <span style={{ color: "rgba(255,255,255,0.88)" }}>{overlay.entry.toFixed(2)}</span>
                <span style={{ color: "rgba(255,255,255,0.35)" }}>STOP</span>
                <span style={{ color: "#ef4444" }}>{overlay.stop.toFixed(2)}</span>
                {overlay.target !== null && (
                  <>
                    <span style={{ color: "rgba(255,255,255,0.35)" }}>TARGET</span>
                    <span style={{ color: "#22c55e" }}>{overlay.target.toFixed(2)}</span>
                  </>
                )}
                <span style={{ color: "rgba(255,255,255,0.35)" }}>RISK</span>
                <span style={{ color: "#fbbf24" }}>${overlay.risk.toFixed(0)}</span>
                {overlay.rr > 0 && (
                  <>
                    <span style={{ color: "rgba(255,255,255,0.35)" }}>R:R</span>
                    <span style={{ color: "rgba(255,255,255,0.6)" }}>{overlay.rr.toFixed(1)}</span>
                  </>
                )}
              </div>

              {overlay.requires_confirmation ? (
                <div style={{ marginTop: 12, fontSize: 9, color: "#fbbf24", textAlign: "center" }}>
                  CONFIRM: Enter or Ctrl+Q &nbsp;·&nbsp; Esc to cancel
                </div>
              ) : (
                <div style={{ marginTop: 12, fontSize: 9, color: "#22c55e", textAlign: "center" }}>
                  ORDER SUBMITTED
                </div>
              )}
            </>
          )}

          {overlay.type === "active" && (
            <>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                <span style={{ color: actionColor(overlay.action), fontWeight: 700, fontSize: 12 }}>
                  {actionLabel(overlay.action)} FILLED
                </span>
              </div>
              <div style={{ fontSize: 11, color: "rgba(255,255,255,0.5)" }}>
                Entry @ {overlay.entry.toFixed(2)} · stop + target active
              </div>
            </>
          )}

          {overlay.type === "rejected" && (
            <>
              <div style={{ color: "#ef4444", fontWeight: 700, fontSize: 11, marginBottom: 8 }}>
                REJECTED
              </div>
              {overlay.flags.length > 0 && (
                <div style={{ marginBottom: 6 }}>
                  {overlay.flags.map((f) => (
                    <div key={f} style={{
                      fontSize: 9, color: "#ef4444",
                      background: "rgba(239,68,68,0.08)",
                      padding: "2px 6px", borderRadius: 3, marginBottom: 2, display: "inline-block", marginRight: 4,
                    }}>{f.replace(/_/g, " ")}</div>
                  ))}
                </div>
              )}
              {overlay.reasons.slice(0, 2).map((r, i) => (
                <div key={i} style={{ fontSize: 10, color: "rgba(255,255,255,0.4)", marginBottom: 2 }}>
                  {r}
                </div>
              ))}
            </>
          )}

          {overlay.type === "error" && (
            <div style={{ color: "#ef4444", fontSize: 11 }}>{overlay.message}</div>
          )}
        </div>
      )}

      {/* ── Hotkey hint (shown when armed, no overlay) ─────────────────────── */}
      {armed && !overlay && (
        <div style={{
          position: "fixed",
          bottom: 24,
          right: 24,
          zIndex: 998,
          fontFamily: "'IBM Plex Mono', monospace",
          fontSize: 9,
          color: "rgba(255,255,255,0.2)",
          pointerEvents: "none",
          userSelect: "none",
        }}>
          Ctrl+Q long · Ctrl+A short · Esc disarm
        </div>
      )}
    </>
  );
}
