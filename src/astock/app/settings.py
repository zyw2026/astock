from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ASTOCK_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    aks_mcp_base_url: str = Field(default="http://localhost:8888")
    data_source_mode: str = Field(default="rest")
    local_db_path: Path = Field(default=Path("data/astock.duckdb"))
    http_timeout_sec: float = Field(default=10.0)
    http_max_retries: int = Field(default=4)
    http_retry_backoff_sec: float = Field(default=2.0)
    http_rate_limit_wait_sec: float = Field(default=20.0)
    reliability_threshold: float = Field(default=60.0)
    validation_min_sample_count: int = Field(default=5)
    default_validation_window_days: int = Field(default=250)
    default_selection_limit: int = Field(default=20)
    default_symbol_limit: int = Field(default=300)
    default_chunk_size: int = Field(default=50)
    default_max_candidates_per_logic_day: int = Field(default=10)
    api_max_rows_per_request: int = Field(default=200)


settings = Settings()
