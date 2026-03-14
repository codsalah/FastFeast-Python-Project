
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import List

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Sub-configs (nested models)
class DatabaseConfig(BaseSettings):
    """PostgreSQL connection settings."""

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", extra="ignore")

    host: str = Field(default="localhost", alias="POSTGRES_HOST")
    port: int = Field(default=5432,        alias="POSTGRES_PORT")
    name: str = Field(default="fastfeast_db", alias="POSTGRES_DB")
    user: str = Field(default="fastfeast", alias="POSTGRES_USER")
    password: str = Field(default="fastfeast_pass", alias="POSTGRES_PASSWORD")
    pool_min: int = Field(default=2,  alias="DB_POOL_MIN")
    pool_max: int = Field(default=10, alias="DB_POOL_MAX")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    @property
    def dsn(self) -> str:
        """PostgreSQL DSN string for psycopg2."""
        return (
            f"host={self.host} port={self.port} dbname={self.name} "
            f"user={self.user} password={self.password}"
        )

    @field_validator("port")
    @classmethod
    def port_in_range(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError(f"port must be 1–65535, got {v}")
        return v

    @field_validator("pool_max")
    @classmethod
    def pool_max_gt_min(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"pool_max must be >= 1, got {v}")
        return v


class SLAConfig(BaseSettings):
    """SLA breach thresholds."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    response_threshold_seconds: int   = Field(default=60,   alias="SLA_RESPONSE_THRESHOLD_SECONDS")
    resolution_threshold_seconds: int = Field(default=900,  alias="SLA_RESOLUTION_THRESHOLD_SECONDS")
    breach_alert_threshold_pct: float = Field(default=0.10, alias="SLA_BREACH_ALERT_THRESHOLD_PCT")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    @field_validator("breach_alert_threshold_pct")
    @classmethod
    def pct_in_range(cls, v: float) -> float:
        if not (0.0 < v <= 1.0):
            raise ValueError(f"breach_alert_threshold_pct must be in (0, 1], got {v}")
        return v


class AlertConfig(BaseSettings):
    """SMTP alerting configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    smtp_host: str     = Field(default="smtp.gmail.com",  alias="SMTP_HOST")
    smtp_port: int     = Field(default=587,               alias="SMTP_PORT")
    smtp_user: str     = Field(default="",                alias="SMTP_USER")
    smtp_password: str = Field(default="",                alias="SMTP_PASSWORD")

    # parsing csv list of configs as list of strings 
    alert_recipients: List[str]  = Field(default_factory=list, alias="ALERT_RECIPIENTS")
    report_recipients: List[str] = Field(default_factory=list, alias="REPORT_RECIPIENTS")

    orphan_rate_threshold: float = Field(default=0.05, alias="MAX_ORPHAN_RATE")
    error_rate_threshold: float  = Field(default=0.10, alias="MAX_DUPLICATE_RATE")

    @field_validator("alert_recipients", "report_recipients", mode="before")
    @classmethod
    def parse_csv_list(cls, v):
        if isinstance(v, str):
            return [item.strip() for item in v.split(",") if item.strip()]
        return v


class QualityThresholdConfig(BaseSettings):
    """Data quality gate thresholds"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    max_null_rate: float       = Field(default=0.05, alias="MAX_NULL_RATE")
    max_duplicate_rate: float  = Field(default=0.01, alias="MAX_DUPLICATE_RATE")
    max_orphan_rate: float     = Field(default=0.05, alias="MAX_ORPHAN_RATE")
    min_integrity_rate: float  = Field(default=0.95, alias="MIN_INTEGRITY_RATE")
    min_file_success_rate: float = Field(default=0.90, alias="MIN_FILE_SUCCESS_RATE")

    @model_validator(mode="after")
    def rates_in_range(self) -> "QualityThresholdConfig":
        fields = {
            "max_null_rate": self.max_null_rate,
            "max_duplicate_rate": self.max_duplicate_rate,
            "max_orphan_rate": self.max_orphan_rate,
            "min_integrity_rate": self.min_integrity_rate,
            "min_file_success_rate": self.min_file_success_rate,
        }
        for name, val in fields.items():
            if not (0.0 <= val <= 1.0):
                raise ValueError(f"{name} must be in [0, 1], got {val}")
        return self


class Settings(BaseSettings):
    """top level pipeline configuration """
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    # Directories
    batch_input_dir: str   = Field(default="data/input/batch",  alias="BATCH_INPUT_DIR")
    stream_input_dir: str  = Field(default="data/input/stream", alias="STREAM_INPUT_DIR")
    quarantine_dir: str    = Field(default="data/quarantine",   alias="QUARANTINE_DIR")
    processed_dir: str     = Field(default="data/processed",    alias="PROCESSED_DIR")
    log_dir: str           = Field(default="logs",              alias="LOG_DIR")

    # Pipeline behaviour 
    poll_interval_seconds: int = Field(default=30,    alias="POLL_INTERVAL_SECONDS")
    batch_chunk_size: int      = Field(default=10000, alias="BATCH_CHUNK_SIZE")
    max_threads: int           = Field(default=4,     alias="MAX_THREADS")

    #  PII 
    pii_hash_pepper: str = Field(alias="PII_HASH_PEPPER")

    #  Sub-configs (composed inline) 
    # These are NOT read from .env directly; they have their own BaseSettings
    # classes that read from .env themselves.
    db:      DatabaseConfig          = Field(default_factory=DatabaseConfig)
    sla:     SLAConfig               = Field(default_factory=SLAConfig)
    alert:   AlertConfig             = Field(default_factory=AlertConfig)
    quality: QualityThresholdConfig  = Field(default_factory=QualityThresholdConfig)

    @field_validator("pii_hash_pepper")
    @classmethod
    def pepper_not_default(cls, v: str) -> str:
        """Block the default placeholder value — forces teams to set a real secret."""
        if "CHANGE_ME" in v or len(v) < 16:
            raise ValueError(
                "PII_HASH_PEPPER must be set to a real secret (min 16 chars). "
                "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
            )
        return v

    def ensure_directories(self) -> None:
        """Create all data directories if they do not exist."""
        dirs = [
            self.batch_input_dir,
            self.stream_input_dir,
            self.quarantine_dir,
            self.processed_dir,
            self.log_dir,
        ]
        for d in dirs:
            Path(d).mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
 