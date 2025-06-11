"""Configuration package for the Ethereum Wallet Tracker.

This package provides configuration management using Pydantic models
and environment variables for type safety and validation.

Example:
    from wallet_tracker.config import get_config

    config = get_config()
    print(f"Environment: {config.environment}")
    print(f"Batch size: {config.processing.batch_size}")
"""

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
from .settings import (
    Settings,
    SettingsError,
    get_config,
    get_settings,
)

__all__ = [
    # Main configuration classes
    "AppConfig",
    "EthereumConfig",
    "CoinGeckoConfig",
    "GoogleSheetsConfig",
    "CacheConfig",
    "ProcessingConfig",
    "LoggingConfig",
    # Enums
    "Environment",
    "CacheBackend",
    # Settings management
    "Settings",
    "SettingsError",
    "get_settings",
    "get_config",
]
