"""
Pydantic-settings based configuration.

Environment variables use double-underscore nesting:
  DATABASE__HOST=localhost
  RUNTIME__MODE=PAPER

An .env file is loaded automatically when present.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from alpha.models.enums import DataSourceId, RuntimeMode

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
    compress: str = "snappy"            # snappy | zstd | gzip | none
    row_group_size: int = 50_000


class RuntimeSettings(BaseSettings):
    mode: RuntimeMode = RuntimeMode.PAPER
    symbols: list[str] = Field(default_factory=lambda: ["SPY"])
    log_level: str = "INFO"
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
    hourly_warmup_bars: int = 300
    daily_warmup_bars: int = 300
    monthly_warmup_months: int = 100


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
    timeout: float = 20.0
    use_rth: bool = False       # False = include pre/after-hours bars
    what_to_show: str = "TRADES"
    pacing_delay: float = 10.0  # seconds between historical requests (avoid pacing violations)
    is_paper: bool = True       # flip to False when routing real orders


class AlpacaSettings(BaseSettings):
    api_key: SecretStr = SecretStr("")
    secret_key: SecretStr = SecretStr("")
    base_url: str = "https://paper-api.alpaca.markets"
    data_url: str = "https://data.alpaca.markets"


class PolygonSettings(BaseSettings):
    api_key: SecretStr = SecretStr("")
    base_url: str = "https://api.polygon.io"


class RiskSettings(BaseSettings):
    account_size: Decimal = Decimal("25000.00")
    max_daily_loss_pct: float = 0.02     # 2%
    max_position_risk_pct: float = 0.01  # 1% per trade
    max_open_positions: int = 5
    default_stop_atr_multiple: float = 1.5


class ReplaySettings(BaseSettings):
    speed: float = 1.0
    start_date: date | None = None
    end_date: date | None = None


class APISettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = False


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
    polygon: PolygonSettings = Field(default_factory=PolygonSettings)
    ibkr: IBKRSettings = Field(default_factory=IBKRSettings)
    risk: RiskSettings = Field(default_factory=RiskSettings)
    replay: ReplaySettings = Field(default_factory=ReplaySettings)
    api: APISettings = Field(default_factory=APISettings)
