"""Configuration models using Pydantic for validation and type safety."""

from enum import Enum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class Environment(str, Enum):
    """Application environment types."""

    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class CacheBackend(str, Enum):
    """Available caching backends."""

    REDIS = "redis"
    FILE = "file"
    HYBRID = "hybrid"


class EthereumConfig(BaseModel):
    """Ethereum API configuration."""

    alchemy_api_key: str = Field(..., description="Alchemy API key")
    infura_project_id: str | None = Field(None, description="Infura project ID")
    rpc_url: str = Field(..., description="Ethereum RPC URL")
    rate_limit: int = Field(100, description="Requests per minute", ge=1, le=1000)

    @field_validator("rpc_url")
    @classmethod
    def validate_rpc_url(cls, v: str) -> str:
        """Validate RPC URL format."""
        if not v.startswith(("http://", "https://", "wss://", "ws://")):
            raise ValueError("RPC URL must start with http://, https://, ws://, or wss://")
        return v


class CoinGeckoConfig(BaseModel):
    """CoinGecko API configuration."""

    api_key: str | None = Field(None, description="CoinGecko API key")
    base_url: HttpUrl = Field(HttpUrl("https://api.coingecko.com/api/v3"), description="CoinGecko API base URL")
    rate_limit: int = Field(30, description="Requests per minute", ge=1, le=500)
    is_pro: bool = Field(False, description="Use Pro API features")


class GoogleSheetsConfig(BaseModel):
    """Google Sheets API configuration."""

    credentials_file: Path = Field(..., description="Path to service account credentials")
    scope: str = Field("https://www.googleapis.com/auth/spreadsheets", description="Google API scope")

    @field_validator("credentials_file")
    @classmethod
    def validate_credentials_file(cls, v: Path) -> Path:
        """Validate credentials file exists."""
        if not v.exists():
            raise ValueError(f"Credentials file not found: {v}")
        return v


class CacheConfig(BaseModel):
    """Caching configuration."""

    backend: CacheBackend = Field(default=CacheBackend.HYBRID, description="Cache backend type")
    redis_url: str = Field(default="redis://localhost:6379/0", description="Redis connection URL")
    redis_password: str | None = Field(default=None, description="Redis password")
    file_cache_dir: Path = Field(default=Path("cache"), description="File cache directory")
    ttl_prices: int = Field(default=3600, description="Price cache TTL in seconds", ge=60)
    ttl_balances: int = Field(default=1800, description="Balance cache TTL in seconds", ge=60)
    max_size_mb: int = Field(default=500, description="Max file cache size in MB", ge=10)


class ProcessingConfig(BaseModel):
    """Processing configuration."""

    batch_size: int = Field(default=50, description="Wallets per batch", ge=1, le=1000)
    max_concurrent_requests: int = Field(default=10, description="Max concurrent API requests", ge=1, le=100)
    request_delay: float = Field(default=0.1, description="Delay between requests in seconds", ge=0.0)
    inactive_wallet_threshold_days: int = Field(default=365, description="Skip wallets inactive for X days", ge=1)
    retry_attempts: int = Field(default=3, description="API retry attempts", ge=1, le=10)
    retry_delay: float = Field(default=1.0, description="Retry delay in seconds", ge=0.1)


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = Field(default="INFO", description="Log level")
    file: Path | None = Field(default=Path("logs/wallet_tracker.log"), description="Log file path")
    max_size_mb: int = Field(default=100, description="Max log file size in MB", ge=1)
    backup_count: int = Field(default=5, description="Number of backup log files", ge=0)

    @classmethod
    @field_validator("level")
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()


class AppConfig(BaseModel):
    """Main application configuration."""

    # Environment
    environment: Environment = Field(Environment.DEVELOPMENT, description="Application environment")
    debug: bool = Field(False, description="Debug mode")
    dry_run: bool = Field(False, description="Dry run mode (no actual processing)")

    # API configurations
    ethereum: EthereumConfig
    coingecko: CoinGeckoConfig
    google_sheets: GoogleSheetsConfig

    # System configurations
    cache: CacheConfig = Field(default=CacheConfig())
    processing: ProcessingConfig = Field(default=ProcessingConfig())
    logging: LoggingConfig = Field(default=LoggingConfig())

    model_config = ConfigDict(
        validate_assignment=True,
    )

    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == Environment.PRODUCTION

    def is_development(self) -> bool:
        """Check if running in development."""
        return self.environment == Environment.DEVELOPMENT

    def get_cache_backend(self) -> str:
        """Get the cache backend as string."""
        return self.cache.backend.value
