"""
Pydantic-settings based configuration.

Environment variables use double-underscore nesting:
  DATABASE__HOST=localhost
  RUNTIME__MODE=PAPER

An .env file is loaded automatically when present.
"""

from __future__ import annotations

import json
import tomllib
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from alpha.models.enums import DataSourceId, RuntimeMode
from alpha.models.risk import AccountConfig

_REPO_ROOT = Path(__file__).resolve().parents[3]


class DatabaseSettings(BaseSettings):
    host: str = "localhost"
    port: int = 5432
    name: str = "alpha_runtime"
    user: str = "alpha"
    password: SecretStr = SecretStr("alpha_dev")
    pool_size: int = 10
    max_overflow: int = 20
    echo_sql: bool = False

    @property
    def url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.user}:"
            f"{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.name}"
        )

    @property
    def sync_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:"
            f"{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.name}"
        )


class StorageSettings(BaseSettings):
    parquet_root: Path = Path("data/parquet")
    volume_profiles_root: Path = Path("data/volume_profiles")
    compress: str = "snappy"            # snappy | zstd | gzip | none
    row_group_size: int = 50_000


class RuntimeSettings(BaseSettings):
    mode: RuntimeMode = RuntimeMode.PAPER
    symbols: list[str] = Field(default_factory=lambda: ["SPY"])
    log_level: str = "INFO"
    setup_debug: bool = False
    timezone: str = "America/New_York"
    orb_minutes: int = 5               # default opening-range window
    catchup_lookback_days: int = 5     # bars to load before going live

    @field_validator("mode", mode="before")
    @classmethod
    def coerce_mode(cls, v: object) -> object:
        if isinstance(v, str):
            return v.lower()
        return v

    @field_validator("symbols", mode="before")
    @classmethod
    def coerce_symbols(cls, v: object) -> object:
        if isinstance(v, list):
            return [s.upper() for s in v]
        return v


class HistoricalSettings(BaseSettings):
    primary_source: DataSourceId = DataSourceId.INTERACTIVE_BROKERS
    lookback_days: int = 30
    max_gap_seconds: int = 300         # gaps larger than this are flagged
    # Warmup bar counts by timeframe — sized to support the deepest indicator on each TF
    minute1_warmup_bars: int = 300     # 1m: EMA9/21 + full session VWAP (~1 RTH session)
    minute5_warmup_bars: int = 300     # 5m: EMA9/21 + full session VWAP (~4 RTH sessions)
    hourly_warmup_bars: int = 1000     # 1h: EMA9/21/50 + SMA100/200 (~167 trading days)
    daily_warmup_bars: int = 1000      # 1d: EMA9/21 + SMA50/100/200 (~4 trading years)
    monthly_warmup_months: int = 60    # kept for backwards compatibility
    vwap_session: str = "rth"          # "rth" (09:30 ET open) | "extended" (04:00 ET open)


class LiveSettings(BaseSettings):
    primary_source: DataSourceId = DataSourceId.INTERACTIVE_BROKERS
    reconnect_attempts: int = 5
    reconnect_delay_seconds: float = 2.0


class IBKRSettings(BaseSettings):
    host: str = "127.0.0.1"
    # 7497 = TWS paper  |  7496 = TWS live
    # 4001 = Gateway paper  |  4002 = Gateway live
    port: int = 7497
    client_id: int = 1
    # Separate clientId for on-demand backfill connections (alpha api process).
    # Must differ from client_id so the two processes can coexist in TWS.
    # Set IBKR__BACKFILL_CLIENT_ID in .env to override.
    backfill_client_id: int = 2
    timeout: float = 20.0
    use_rth: bool = False       # False = include pre/after-hours bars
    what_to_show: str = "TRADES"
    pacing_delay: float = 10.0  # seconds between historical requests (avoid pacing violations)
    is_paper: bool = True       # flip to False when routing real orders
    # Maps logical account_id → IBKR account string (e.g. {"day_trade": "U1234567"}).
    # If empty, orders are routed to the default IBKR account.
    account_map: dict[str, str] = Field(default_factory=dict)


class AlpacaSettings(BaseSettings):
    api_key: SecretStr = SecretStr("")
    secret_key: SecretStr = SecretStr("")
    base_url: str = "https://paper-api.alpaca.markets"
    data_url: str = "https://data.alpaca.markets"


class PolygonSettings(BaseSettings):
    api_key: SecretStr = SecretStr("")
    base_url: str = "https://api.polygon.io"


class DatabentoSettings(BaseSettings):
    api_key: SecretStr = SecretStr("")
    # Default dataset for CME Globex US futures (ES, NQ, CL, GC, etc.)
    dataset: str = "GLBX.MDP3"
    # Symbol type for live and historical requests — "continuous" uses roll-adjusted
    # front-month contracts (e.g. ES.c.0) without manual contract-month tracking.
    stype_in: str = "continuous"
    # Suffix appended to root_symbol to build the Databento continuous symbol.
    # ".c.0" = front-month, ".c.1" = second-month, etc.
    continuous_suffix: str = ".c.0"
    # Every historical fetch is archived here as a raw DBN file before being
    # decoded into our own event models — lets a parsing bug (e.g. a
    # taker_side mismapping) be re-checked or reprocessed offline without
    # paying for another Databento API call. None disables archiving.
    raw_archive_root: Path | None = Path("data/dbn_raw")


class RiskSettings(BaseSettings):
    # Legacy scalar fields — used when no accounts file / RISK__ACCOUNTS is set
    account_size: Decimal = Decimal("25000.00")
    max_daily_loss_pct: float = 0.02
    max_position_risk_pct: float = 0.01
    max_open_positions: int = 5
    default_stop_atr_multiple: float = 1.5

    # Logical account that receives new trade plans (must match an account_id in accounts)
    default_account_id: str = "day_trade"

    # Readable per-account config (preferred over RISK__ACCOUNTS JSON in .env)
    accounts_file: Path = Path("config/accounts.toml")

    # Optional JSON override — only used when accounts file is missing / empty
    accounts: list[AccountConfig] = Field(default_factory=list)

    @field_validator("accounts", mode="before")
    @classmethod
    def coerce_accounts(cls, v: Any) -> Any:
        if v is None or v == "":
            return []
        if isinstance(v, str):
            v = json.loads(v)
        if not isinstance(v, list):
            return v
        return [
            AccountConfig.model_validate(item) if isinstance(item, dict) else item
            for item in v
        ]

    @model_validator(mode="after")
    def load_accounts_from_file(self) -> Self:
        if self.accounts:
            return self
        path = self.accounts_file
        if not path.is_absolute():
            path = _REPO_ROOT / path
        if not path.is_file():
            return self
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        raw = data.get("accounts", [])
        if not raw:
            return self
        self.accounts = [AccountConfig.model_validate(item) for item in raw]
        return self


class ReplaySettings(BaseSettings):
    speed: float = 1.0
    start_date: date | None = None
    end_date: date | None = None


class APISettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = False


class TelegramSettings(BaseSettings):
    bot_token: str = ""
    chat_id: str = ""

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)


class AlphaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_REPO_ROOT / ".env",
        env_nested_delimiter="__",
        extra="ignore",
    )

    runtime: RuntimeSettings = Field(default_factory=RuntimeSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    historical: HistoricalSettings = Field(default_factory=HistoricalSettings)
    live: LiveSettings = Field(default_factory=LiveSettings)
    alpaca: AlpacaSettings = Field(default_factory=AlpacaSettings)
    databento: DatabentoSettings = Field(default_factory=DatabentoSettings)
    polygon: PolygonSettings = Field(default_factory=PolygonSettings)
    ibkr: IBKRSettings = Field(default_factory=IBKRSettings)
    risk: RiskSettings = Field(default_factory=RiskSettings)
    replay: ReplaySettings = Field(default_factory=ReplaySettings)
    api: APISettings = Field(default_factory=APISettings)
    telegram: TelegramSettings = Field(default_factory=TelegramSettings)
