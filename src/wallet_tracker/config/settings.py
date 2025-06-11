"""Settings management for the Ethereum Wallet Tracker application."""

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import ValidationError

from .models import (
    AppConfig,
    CacheBackend,
    CacheConfig,
    CoinGeckoConfig,
    Environment,
    EthereumConfig,
    GoogleSheetsConfig,
    LoggingConfig,
    ProcessingConfig,
)


class SettingsError(Exception):
    """Configuration settings error."""

    pass


class Settings:
    """Application settings manager."""

    def __init__(self, env_file: Path | None = None) -> None:
        """Initialize settings.

        Args:
            env_file: Path to .env file. If None, looks for .env in current directory.
        """
        self.env_file = env_file or Path(".env")
        self._config: AppConfig | None = None
        self._load_environment()

    def _load_environment(self) -> None:
        """Load environment variables from .env file."""
        if self.env_file.exists():
            load_dotenv(self.env_file)
        else:
            # Try common locations
            for env_path in [Path(".env"), Path("config/.env"), Path("../.env")]:
                if env_path.exists():
                    load_dotenv(env_path)
                    break

    def _get_env(self, key: str, default: Any = None, required: bool = False) -> Any:
        """Get environment variable with validation.

        Args:
            key: Environment variable name
            default: Default value if not found
            required: Whether the variable is required

        Returns:
            Environment variable value

        Raises:
            SettingsError: If required variable is missing
        """
        value = os.getenv(key, default)
        if required and value is None:
            raise SettingsError(f"Required environment variable '{key}' not found")
        return value

    def _build_ethereum_config(self) -> EthereumConfig:
        """Build Ethereum configuration from environment variables."""
        alchemy_key = self._get_env("ALCHEMY_API_KEY", required=True)

        # Build RPC URL with API key if not provided
        rpc_url = self._get_env("ETHEREUM_RPC_URL", f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}")

        return EthereumConfig(
            alchemy_api_key=alchemy_key,
            infura_project_id=self._get_env("INFURA_PROJECT_ID"),
            rpc_url=rpc_url,
            rate_limit=int(self._get_env("ALCHEMY_RATE_LIMIT", 100)),
        )

    def _build_coingecko_config(self) -> CoinGeckoConfig:
        """Build CoinGecko configuration from environment variables."""
        return CoinGeckoConfig(
            api_key=self._get_env("COINGECKO_PRO_API_KEY") if self._get_env("COINGECKO_PRO_API_KEY") else self._get_env("COINGECKO_DEMO_API_KEY"),
            base_url=self._get_env("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"),
            rate_limit=int(self._get_env("COINGECKO_RATE_LIMIT", 30)),
            is_pro=True if self._get_env("COINGECKO_PRO_API_KEY") else False,
        )

    def _build_google_sheets_config(self) -> GoogleSheetsConfig:
        """Build Google Sheets configuration from environment variables."""
        credentials_file = Path(Path(__file__).parent.parent.resolve() /
            self._get_env(
                "GOOGLE_SHEETS_CREDENTIALS_FILE",
                "config/google_sheets_credentials.json",
            )
        )

        return GoogleSheetsConfig(
            credentials_file=credentials_file,
            scope=self._get_env("GOOGLE_SHEETS_SCOPE", "https://www.googleapis.com/auth/spreadsheets"),
        )

    def _build_cache_config(self) -> CacheConfig:
        """Build cache configuration from environment variables."""
        backend_str = self._get_env("CACHE_BACKEND", "hybrid").lower()
        try:
            backend = CacheBackend(backend_str)
        except ValueError:
            backend = CacheBackend.HYBRID

        return CacheConfig(
            backend=backend,
            redis_url=self._get_env("REDIS_URL", "redis://localhost:6379/0"),
            redis_password=self._get_env("REDIS_PASSWORD"),
            file_cache_dir=Path(self._get_env("FILE_CACHE_DIR", "cache")),
            ttl_prices=int(self._get_env("CACHE_TTL_PRICES", 3600)),
            ttl_balances=int(self._get_env("CACHE_TTL_BALANCES", 1800)),
            max_size_mb=int(self._get_env("CACHE_MAX_SIZE_MB", 500)),
        )

    def _build_processing_config(self) -> ProcessingConfig:
        """Build processing configuration from environment variables."""
        return ProcessingConfig(
            batch_size=int(self._get_env("BATCH_SIZE", 50)),
            max_concurrent_requests=int(self._get_env("MAX_CONCURRENT_REQUESTS", 10)),
            request_delay=float(self._get_env("REQUEST_DELAY", 0.1)),
            inactive_wallet_threshold_days=int(self._get_env("INACTIVE_WALLET_THRESHOLD_DAYS", 365)),
            retry_attempts=int(self._get_env("RETRY_ATTEMPTS", 3)),
            retry_delay=float(self._get_env("RETRY_DELAY", 1.0)),
        )

    def _build_logging_config(self) -> LoggingConfig:
        """Build logging configuration from environment variables."""
        log_file = self._get_env("LOG_FILE")
        return LoggingConfig(
            level=self._get_env("LOG_LEVEL", "INFO"),
            file=Path(log_file) if log_file else Path("logs/wallet_tracker.log"),
            max_size_mb=int(self._get_env("LOG_MAX_SIZE_MB", 100)),
            backup_count=int(self._get_env("LOG_BACKUP_COUNT", 5)),
        )

    def load_config(self) -> AppConfig:
        """Load and validate application configuration.

        Returns:
            Validated application configuration

        Raises:
            SettingsError: If configuration is invalid
        """
        if self._config is not None:
            return self._config

        try:
            # Build configuration from environment
            environment_str = self._get_env("ENVIRONMENT", "development").lower()
            try:
                environment = Environment(environment_str)
            except ValueError:
                environment = Environment.DEVELOPMENT

            config_data = {
                "environment": environment,
                "debug": self._get_env("DEBUG", "false").lower() == "true",
                "dry_run": self._get_env("DRY_RUN", "false").lower() == "true",
                "ethereum": self._build_ethereum_config(),
                "coingecko": self._build_coingecko_config(),
                "google_sheets": self._build_google_sheets_config(),
                "cache": self._build_cache_config(),
                "processing": self._build_processing_config(),
                "logging": self._build_logging_config(),
            }

            # Validate configuration
            self._config = AppConfig(**config_data)
            return self._config

        except ValidationError as e:
            raise SettingsError(f"Configuration validation failed: {e}") from e
        except Exception as e:
            raise SettingsError(f"Failed to load configuration: {e}") from e

    def get_config(self) -> AppConfig:
        """Get the current configuration, loading if necessary."""
        if self._config is None:
            return self.load_config()
        return self._config

    def reload_config(self) -> AppConfig:
        """Reload configuration from environment."""
        self._config = None
        self._load_environment()
        return self.load_config()

    def validate_config(self) -> dict[str, Any]:
        """Validate configuration and return validation results.

        Returns:
            Dictionary with validation results and any issues found
        """
        issues = []
        warnings = []

        try:
            config = self.load_config()

            # Check for potential issues
            if config.environment == Environment.PRODUCTION and config.debug:
                warnings.append("Debug mode is enabled in production")

            if config.cache.backend == CacheBackend.REDIS:
                # Could add Redis connectivity check here
                pass

            if not config.google_sheets.credentials_file.exists():
                issues.append(f"Google Sheets credentials file not found: {config.google_sheets.credentials_file}")

            # Check if log directory exists
            if config.logging.file:
                log_dir = config.logging.file.parent
                if not log_dir.exists():
                    try:
                        log_dir.mkdir(parents=True, exist_ok=True)
                    except Exception as e:
                        issues.append(f"Cannot create log directory: {e}")

            return {
                "valid": len(issues) == 0,
                "config": config.model_dump(),
                "issues": issues,
                "warnings": warnings,
            }

        except Exception as e:
            return {
                "valid": False,
                "config": None,
                "issues": [str(e)],
                "warnings": [],
            }


# Global settings instance
@lru_cache
def get_settings() -> Settings:
    """Get global settings instance (cached)."""
    return Settings()


def get_config() -> AppConfig:
    """Get application configuration (convenience function)."""
    return get_settings().get_config()
