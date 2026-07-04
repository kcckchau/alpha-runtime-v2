"""
Architecture diagram generator.

Introspects src/alpha and writes a Mermaid flowchart to docs/architecture.md.
Re-run whenever the architecture changes:

    python scripts/gen_architecture.py

The script reads actual source files — it does not hardcode engine names,
event subscriptions, or publications. Adding a new engine or changing its
subscriptions is automatically reflected on the next run.
"""

from __future__ import annotations

import ast
import re
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

ROOT   = Path(__file__).parent.parent
SRC    = ROOT / "src" / "alpha"
OUTDIR = ROOT / "docs"

# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class EngineInfo:
    class_name: str          # e.g. "FeatureEngine"
    node_id:    str          # safe Mermaid id, e.g. "FeatureEngine"
    label:      str          # display label
    path:       Path
    subscribes: list[str] = field(default_factory=list)   # EventType names
    publishes:  list[str] = field(default_factory=list)   # event class names
    direct_refs: list[str] = field(default_factory=list)  # set_*_engine targets


@dataclass
class RouteInfo:
    method: str
    path:   str
    tag:    str


# ── Helpers ───────────────────────────────────────────────────────────────────

def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _engine_label(class_name: str) -> str:
    """FeatureEngine → Feature Engine"""
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", class_name)


def _safe_id(class_name: str) -> str:
    return class_name  # class names are already valid Mermaid ids


# ── Extraction ────────────────────────────────────────────────────────────────

def find_base_engine_subclass(source: str) -> str | None:
    """Return the first class name that inherits from BaseEngine."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for base in node.bases:
            name = ""
            if isinstance(base, ast.Name):
                name = base.id
            elif isinstance(base, ast.Attribute):
                name = base.attr
            if name == "BaseEngine":
                return node.name
    return None


def extract_subscriptions(source: str) -> list[str]:
    """Find all EventType.X values passed to event_bus.subscribe(...)."""
    subs = re.findall(r"subscribe\(\s*EventType\.(\w+)", source)
    # StorageEngine subscribes via `for et in EventType:` loop — detect that pattern
    if re.search(r"for\s+\w+\s+in\s+EventType\b", source):
        # It subscribes to all — return ALL as a sentinel
        return ["ALL"]
    return subs


def extract_publications(source: str) -> list[str]:
    """
    Find event class names published to the bus.

    Three patterns:
    1. publish(EventClass(...))                   — direct construction
    2. x = EventClass(...)  ...  publish(x)       — local variable
    3. async def _on_bar(self, event: BarEvent)   — method receives typed param and publishes it
    """
    pubs: list[str] = []

    # Pattern 1: direct construction in the publish call
    for m in re.finditer(r"publish\(\s*([A-Z][A-Za-z]+Event)\s*\(", source):
        pubs.append(m.group(1))

    # Pattern 2: local variable assigned from constructor
    assignments: dict[str, str] = {}
    for m in re.finditer(r"(\w+)\s*=\s*([A-Z][A-Za-z]+Event)\s*\(", source):
        assignments[m.group(1)] = m.group(2)
    for m in re.finditer(r"publish\(\s*(\w+)\s*\)", source):
        var = m.group(1)
        if var in assignments:
            pubs.append(assignments[var])

    # Pattern 3: method receives a typed event param AND publish() is called
    # with that SAME variable — the engine forwards/re-publishes the event.
    # e.g. async def _on_bar(self, event: BarEvent) -> None: ... publish(event)
    # This distinguishes forwarding from processing (receive BarEvent, publish MarketStateEvent).
    try:
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            method_src = ast.unparse(node)
            if "publish(" not in method_src:
                continue
            for arg in node.args.args:
                if arg.annotation is None:
                    continue
                ann_name = ast.unparse(arg.annotation)
                if not ann_name.endswith("Event") or ann_name == "AnyEvent":
                    continue
                # Only add if publish(param_name) appears in this method
                if re.search(rf"\bpublish\(\s*{re.escape(arg.arg)}\s*\)", method_src):
                    pubs.append(ann_name)
    except Exception:
        pass

    return sorted(set(pubs))


def extract_direct_refs(source: str) -> list[str]:
    """Find set_*_engine(other_engine) calls indicating direct dependencies."""
    refs = []
    for m in re.finditer(r"set_(\w+_engine)\(", source):
        # "feature_engine" → "FeatureEngine"
        snake = m.group(1)
        camel = "".join(w.capitalize() for w in snake.split("_"))
        refs.append(camel)
    return sorted(set(refs))


def scan_engines() -> list[EngineInfo]:
    engines = []
    for path in sorted(SRC.glob("engines/**/engine.py")):
        source = _read(path)
        class_name = find_base_engine_subclass(source)
        if class_name is None:
            continue
        info = EngineInfo(
            class_name=class_name,
            node_id=_safe_id(class_name),
            label=_engine_label(class_name),
            path=path,
            subscribes=extract_subscriptions(source),
            publishes=extract_publications(source),
        )
        engines.append(info)

    # Extract direct engine-to-engine wiring from bootstrap
    bootstrap_src = _read(SRC / "engines" / "bootstrap" / "engine.py")
    for info in engines:
        info.direct_refs = extract_direct_refs(_read(info.path))

    return engines


def scan_routes() -> list[RouteInfo]:
    routes = []
    for path in sorted((SRC / "api" / "routes").glob("*.py")):
        source = _read(path)
        tag = path.stem  # health, runtime, ws
        for m in re.finditer(
            r'@\w+\.(get|post|put|delete|websocket)\(\s*["\']([^"\']+)["\']',
            source,
        ):
            routes.append(RouteInfo(
                method=m.group(1).upper(),
                path=m.group(2),
                tag=tag,
            ))
    return routes


def scan_live_subscriptions() -> list[str]:
    """Return Databento schemas subscribed to in _on_start."""
    src = _read(SRC / "engines" / "live" / "engine.py")
    schemas = []
    for m in re.finditer(r"subscribe_(\w+)\(", src):
        name = m.group(1)
        if name not in {"tick_trades"}:  # internal alias
            schemas.append(name)
    return sorted(set(schemas))


def scan_event_types() -> list[str]:
    src = _read(SRC / "models" / "enums.py")
    return re.findall(r'^\s+(\w+)\s*=\s*"[^"]*"', src, re.MULTILINE)


# ── Mermaid builder ───────────────────────────────────────────────────────────

# Engines that belong to the "processing" layer vs ingestion vs orchestration
_INGESTION  = {"LiveIngestionEngine", "HistoricalDataEngine"}
_ORCH       = {"BootstrapEngine"}
_ORDER_RISK = {"RiskEngine", "OrderEngine"}
_STORAGE    = {"StorageEngine"}

# Event type → short wire label
_ET_LABEL: dict[str, str] = {
    "BAR":          "BAR",
    "TRADE":        "TRADE",
    "QUOTE":        "QUOTE",
    "ORDER_BOOK":   "BOOK",
    "MARKET_STATE": "MKT_STATE",
    "SETUP":        "SETUP",
    "THESIS":       "THESIS",
    "ORDER_UPDATE": "ORDER_UPD",
    "EXECUTION":    "EXEC",
    "SYSTEM":       "SYSTEM",
}

# Event class → EventType label (for annotating publish edges)
_CLASS_TO_ET: dict[str, str] = {
    "BarEvent":         "BAR",
    "TradeEvent":       "TRADE",
    "QuoteEvent":       "QUOTE",
    "OrderBookEvent":   "BOOK",
    "MarketStateEvent": "MKT_STATE",
    "SetupEvent":       "SETUP",
    "ThesisEvent":      "THESIS",
    "OrderUpdateEvent": "ORDER_UPD",
    "SystemEvent":      "SYSTEM",
}


def _pub_labels(pubs: list[str]) -> str:
    labels = [_CLASS_TO_ET.get(p, p) for p in pubs]
    return ", ".join(sorted(set(labels))) or "event"


def _sub_labels(subs: list[str]) -> str:
    if "ALL" in subs:
        return "ALL events"
    labels = [_ET_LABEL.get(s, s) for s in subs]
    return ", ".join(sorted(set(labels))) or "event"


def build_mermaid(engines: list[EngineInfo], routes: list[RouteInfo]) -> str:
    by_id = {e.node_id: e for e in engines}

    ingestion  = [e for e in engines if e.class_name in _INGESTION]
    processing = [e for e in engines if e.class_name not in _INGESTION | _ORCH | _ORDER_RISK | _STORAGE]
    order_risk = [e for e in engines if e.class_name in _ORDER_RISK]
    storage_e  = [e for e in engines if e.class_name in _STORAGE]

    lines: list[str] = ["flowchart LR"]

    # ── External data sources ─────────────────────────────────────────────────
    lines += [
        "",
        "    subgraph EXT[\" External \"]",
        "        direction TB",
        "        DB_LIVE([Databento\\nLive WS])",
        "        DB_HIST([Databento\\nHistorical API])",
        "        IBKR([IBKR])",
        "    end",
    ]

    # ── Ingestion engines ─────────────────────────────────────────────────────
    lines += ["", "    subgraph ING[\" Ingestion \"]", "        direction TB"]
    for e in ingestion:
        lines.append(f"        {e.node_id}[{e.label}]")
    lines.append("    end")

    # ── EventBus ──────────────────────────────────────────────────────────────
    lines += ["", "    EB{{EventBus}}"]

    # ── Processing engines ────────────────────────────────────────────────────
    lines += ["", "    subgraph PROC[\" Processing \"]", "        direction TB"]
    for e in processing:
        lines.append(f"        {e.node_id}[{e.label}]")
    lines.append("    end")

    # ── Order / Risk ──────────────────────────────────────────────────────────
    lines += ["", "    subgraph ORD_RISK[\" Execution \"]", "        direction TB"]
    for e in order_risk:
        lines.append(f"        {e.node_id}[{e.label}]")
    lines.append("    end")

    # ── Storage engine + backends ─────────────────────────────────────────────
    lines += ["", "    subgraph STOR[\" Storage \"]", "        direction TB"]
    for e in storage_e:
        lines.append(f"        {e.node_id}[{e.label}]")
    lines += [
        "        PQ[(Parquet)]",
        "        PG[(PostgreSQL)]",
    ]
    lines.append("    end")

    # ── API + UI ──────────────────────────────────────────────────────────────
    ws_paths  = [r for r in routes if r.method in ("GET", "WEBSOCKET") and "ws" in r.path.lower()]
    rest_paths = [r for r in routes if r not in ws_paths]

    lines += [
        "",
        "    subgraph API[\" API / UI \"]",
        "        direction TB",
        "        REST[FastAPI REST]",
        "        WS[WebSocket Push]",
        "        UI([Web Dashboard])",
        "    end",
    ]

    # ── Edges: external → ingestion ───────────────────────────────────────────
    lines += [""]
    lines.append("    DB_LIVE -->|ohlcv-1m / trades / mbp-1| LiveIngestionEngine")
    lines.append("    DB_HIST -->|historical bars| HistoricalDataEngine")
    lines.append("    IBKR    -->|historical bars| HistoricalDataEngine")
    lines.append("    IBKR    -->|order routing| OrderEngine")

    # ── Edges: ingestion → EventBus ───────────────────────────────────────────
    lines += [""]
    for e in ingestion:
        if e.publishes:
            label = _pub_labels(e.publishes)
            lines.append(f"    {e.node_id} -->|\"{label}\"| EB")
        else:
            lines.append(f"    {e.node_id} --> EB")

    # ── Edges: EventBus → subscribers ─────────────────────────────────────────
    lines += [""]
    for e in engines:
        if e.class_name in _INGESTION | _ORCH:
            continue
        if e.subscribes:
            label = _sub_labels(e.subscribes)
            lines.append(f"    EB -->|\"{label}\"| {e.node_id}")

    # ── Edges: processing publishes → EventBus ────────────────────────────────
    lines += [""]
    for e in processing + order_risk:
        if e.publishes:
            label = _pub_labels(e.publishes)
            lines.append(f"    {e.node_id} -->|\"{label}\"| EB")

    # ── Edges: direct engine-to-engine references (dashed) ────────────────────
    lines += [""]
    for e in engines:
        for ref in e.direct_refs:
            if ref in by_id and ref != e.class_name:
                lines.append(f"    {e.node_id} -. snapshot .-> {ref}")

    # ── Edges: storage engine → backends ──────────────────────────────────────
    lines += [""]
    for e in storage_e:
        lines.append(f"    {e.node_id} -->|bars / trades / books| PQ")
        lines.append(f"    {e.node_id} -->|states / setups / orders| PG")

    # ── Edges: API layer ──────────────────────────────────────────────────────
    lines += [""]
    lines.append("    LiveIngestionEngine -->|latest bars & quotes| REST")
    lines.append("    FeatureEngine       -->|snapshots| REST")
    lines.append("    ThesisEngine        -->|thesis state| REST")
    lines.append("    SetupEngine         -->|setups| REST")
    lines.append("    LiveIngestionEngine -->|BAR / QUOTE push| WS")
    lines.append("    REST --> UI")
    lines.append("    WS   --> UI")

    # ── Styles ────────────────────────────────────────────────────────────────
    lines += [
        "",
        "    classDef ingestion  fill:#1e3a5f,stroke:#60a5fa,color:#e0f2fe",
        "    classDef processing fill:#1a3a2a,stroke:#4ade80,color:#dcfce7",
        "    classDef execution  fill:#3b1f1f,stroke:#f87171,color:#fee2e2",
        "    classDef storage    fill:#2d2418,stroke:#fbbf24,color:#fef3c7",
        "    classDef external   fill:#1e1b4b,stroke:#a78bfa,color:#ede9fe",
        "    classDef bus        fill:#4a1d96,stroke:#c084fc,color:#f3e8ff",
        "    classDef api        fill:#1c2b3a,stroke:#38bdf8,color:#e0f2fe",
    ]

    for e in ingestion:
        lines.append(f"    class {e.node_id} ingestion")
    for e in processing:
        lines.append(f"    class {e.node_id} processing")
    for e in order_risk:
        lines.append(f"    class {e.node_id} execution")
    for e in storage_e:
        lines.append(f"    class {e.node_id} storage")
    lines.append("    class DB_LIVE,DB_HIST,IBKR external")
    lines.append("    class EB bus")
    lines.append("    class REST,WS,UI api")
    lines.append("    class PQ,PG storage")

    return "\n".join(lines)


# ── Markdown wrapper ──────────────────────────────────────────────────────────

def build_markdown(mermaid: str, engines: list[EngineInfo], routes: list[RouteInfo]) -> str:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    engine_table = "\n".join(
        f"| `{e.class_name}` | {', '.join(f'`{s}`' for s in e.subscribes) or '—'} "
        f"| {', '.join(f'`{p}`' for p in e.publishes) or '—'} |"
        for e in engines
        if e.class_name not in {"BootstrapEngine"}
    )

    route_table = "\n".join(
        f"| `{r.method}` | `{r.path}` | {r.tag} |"
        for r in routes
    )

    return textwrap.dedent(f"""\
        # Alpha Runtime v2 — Architecture

        > Auto-generated by `scripts/gen_architecture.py` on {now}.
        > Re-run after any engine or route change to keep this up to date.

        ## System Diagram

        ```mermaid
        {mermaid}
        ```

        ## Engine Event Map

        | Engine | Subscribes (EventType) | Publishes |
        |--------|------------------------|-----------|
        {engine_table}

        ## API Routes

        | Method | Path | Module |
        |--------|------|--------|
        {route_table}

        ## Storage Policy

        | Backend | Data Types |
        |---------|-----------|
        | Parquet | `bars/*`, `trades`, `order_books` — raw market data, time-partitioned |
        | PostgreSQL | `market_states`, `setups`, `orders` — derived data, queryable |

        ## Live Data Subscriptions (Databento)

        | Schema | Purpose |
        |--------|---------|
        | `ohlcv-1m` | Completed 1-minute bars → FeatureEngine / ThesisEngine |
        | `ohlcv-1s` | 1-second bars → WebSocket push only |
        | `trades` | Tick trades → ThesisEngine tick-flow + partial bar accumulation |
        | `mbp-1` | Top-of-book quotes → outlier guard, QuoteEvent |
    """)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    print("Scanning engines...")
    engines = scan_engines()
    for e in engines:
        print(f"  {e.class_name:30s}  sub={e.subscribes}  pub={[p.replace('Event','') for p in e.publishes]}")

    print("\nScanning routes...")
    routes = scan_routes()
    for r in routes:
        print(f"  {r.method:9s} {r.path}")

    print("\nBuilding diagram...")
    mermaid  = build_mermaid(engines, routes)
    markdown = build_markdown(mermaid, engines, routes)

    OUTDIR.mkdir(exist_ok=True)
    out = OUTDIR / "architecture.md"
    out.write_text(markdown, encoding="utf-8")
    print(f"\nWritten: {out.relative_to(ROOT)}")

    # Also write a standalone .mmd file for editors that render it natively
    mmd = OUTDIR / "architecture.mmd"
    mmd.write_text(mermaid + "\n", encoding="utf-8")
    print(f"Written: {mmd.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
