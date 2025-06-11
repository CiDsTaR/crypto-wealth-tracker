"""Enhanced tests for configuration management with comprehensive coverage."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from wallet_tracker.config import (
    AppConfig,
    CacheBackend,
    CacheConfig,
    CoinGeckoConfig,
    Environment,
    EthereumConfig,
    GoogleSheetsConfig,
    LoggingConfig,
    ProcessingConfig,
    Settings,
    SettingsError,
)


class TestEthereumConfig:
    """Test Ethereum configuration."""

    def test_valid_config(self) -> None:
        """Test valid Ethereum configuration."""
        config = EthereumConfig(
            alchemy_api_key="test_key",
            rpc_url="https://eth-mainnet.g.alchemy.com/v2/test_key",
        )
        assert config.alchemy_api_key == "test_key"
        assert config.rate_limit == 100  # default

    def test_invalid_rpc_url(self) -> None:
        """Test invalid RPC URL validation."""
        with pytest.raises(ValidationError):
            EthereumConfig(alchemy_api_key="test_key", rpc_url="invalid_url")

    def test_rate_limit_validation(self) -> None:
        """Test rate limit validation."""
        with pytest.raises(ValidationError):
            EthereumConfig(
                alchemy_api_key="test_key",
                rpc_url="https://test.com",
                rate_limit=0,  # Invalid: too low
            )

    def test_rpc_url_with_api_key_injection(self) -> None:
        """Test RPC URL building with API key injection."""
        config = EthereumConfig(alchemy_api_key="my_api_key", rpc_url="https://eth-mainnet.g.alchemy.com/v2/my_api_key")
        assert "my_api_key" in config.rpc_url

    def test_websocket_rpc_url_validation(self) -> None:
        """Test WebSocket RPC URL validation (ws://, wss://)."""
        # Valid WebSocket URLs
        config_ws = EthereumConfig(alchemy_api_key="test_key", rpc_url="ws://localhost:8546")
        assert config_ws.rpc_url == "ws://localhost:8546"

        config_wss = EthereumConfig(alchemy_api_key="test_key", rpc_url="wss://eth-mainnet.g.alchemy.com/v2/test_key")
        assert config_wss.rpc_url.startswith("wss://")

    def test_infura_project_id_optional(self) -> None:
        """Test that Infura project ID is optional."""
        config = EthereumConfig(alchemy_api_key="test_key", rpc_url="https://test.com")
        assert config.infura_project_id is None

        config_with_infura = EthereumConfig(
            alchemy_api_key="test_key", rpc_url="https://test.com", infura_project_id="test_infura_id"
        )
        assert config_with_infura.infura_project_id == "test_infura_id"

    def test_rate_limit_edge_cases(self) -> None:
        """Test rate limit edge cases."""
        # Test minimum valid value
        config_min = EthereumConfig(alchemy_api_key="test_key", rpc_url="https://test.com", rate_limit=1)
        assert config_min.rate_limit == 1

        # Test maximum valid value
        config_max = EthereumConfig(alchemy_api_key="test_key", rpc_url="https://test.com", rate_limit=1000)
        assert config_max.rate_limit == 1000

        # Test invalid values
        with pytest.raises(ValidationError):
            EthereumConfig(
                alchemy_api_key="test_key",
                rpc_url="https://test.com",
                rate_limit=1001,  # Too high
            )


class TestCacheConfig:
    """Test cache configuration validation and defaults."""

    def test_valid_cache_config(self) -> None:
        """Test valid cache configuration with all backends."""
        # Test with Redis backend
        config_redis = CacheConfig(
            backend=CacheBackend.REDIS, redis_url="redis://localhost:6379/0", redis_password="secret"
        )
        assert config_redis.backend == CacheBackend.REDIS
        assert config_redis.redis_password == "secret"

        # Test with File backend
        config_file = CacheConfig(backend=CacheBackend.FILE, file_cache_dir=Path("test_cache"))
        assert config_file.backend == CacheBackend.FILE
        assert config_file.file_cache_dir == Path("test_cache")

        # Test with Hybrid backend
        config_hybrid = CacheConfig(backend=CacheBackend.HYBRID)
        assert config_hybrid.backend == CacheBackend.HYBRID

    def test_cache_ttl_validation(self) -> None:
        """Test TTL validation (minimum values)."""
        # Valid TTL values
        config = CacheConfig(
            ttl_prices=60,  # Minimum valid
            ttl_balances=120,
        )
        assert config.ttl_prices == 60
        assert config.ttl_balances == 120

        # Invalid TTL values
        with pytest.raises(ValidationError):
            CacheConfig(ttl_prices=59)  # Below minimum

        with pytest.raises(ValidationError):
            CacheConfig(ttl_balances=30)  # Below minimum

    def test_cache_size_validation(self) -> None:
        """Test max_size_mb validation."""
        # Valid size
        config = CacheConfig(max_size_mb=100)
        assert config.max_size_mb == 100

        # Minimum valid size
        config_min = CacheConfig(max_size_mb=10)
        assert config_min.max_size_mb == 10

        # Invalid size
        with pytest.raises(ValidationError):
            CacheConfig(max_size_mb=5)  # Below minimum

    def test_redis_url_validation(self) -> None:
        """Test Redis URL format validation."""
        # Valid Redis URLs
        config1 = CacheConfig(redis_url="redis://localhost:6379/0")
        assert config1.redis_url == "redis://localhost:6379/0"

        config2 = CacheConfig(redis_url="redis://user:pass@localhost:6379/1")
        assert "user:pass" in config2.redis_url

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = CacheConfig()
        assert config.backend == CacheBackend.HYBRID
        assert config.redis_url == "redis://localhost:6379/0"
        assert config.redis_password is None
        assert config.file_cache_dir == Path("cache")
        assert config.ttl_prices == 3600
        assert config.ttl_balances == 1800
        assert config.max_size_mb == 500


class TestCoinGeckoConfig:
    """Test CoinGecko configuration validation."""

    def test_valid_coingecko_config(self) -> None:
        """Test valid CoinGecko configuration."""
        config = CoinGeckoConfig(api_key="test_api_key", base_url="https://api.coingecko.com/api/v3", rate_limit=50)
        assert config.api_key == "test_api_key"
        assert str(config.base_url) == "https://api.coingecko.com/api/v3"
        assert config.rate_limit == 50

    def test_base_url_validation(self) -> None:
        """Test base URL validation (must be valid HTTP URL)."""
        # Valid URLs
        config1 = CoinGeckoConfig(base_url="https://api.coingecko.com/api/v3")
        assert str(config1.base_url).startswith("https://")

        config2 = CoinGeckoConfig(base_url="http://localhost:3000/api")
        assert str(config2.base_url).startswith("http://")

        # Invalid URLs should raise validation error
        with pytest.raises(ValidationError):
            CoinGeckoConfig(base_url="not-a-url")

        with pytest.raises(ValidationError):
            CoinGeckoConfig(base_url="ftp://invalid.com")

    def test_rate_limit_bounds(self) -> None:
        """Test rate limit boundary validation."""
        # Valid values
        config_min = CoinGeckoConfig(rate_limit=1)
        assert config_min.rate_limit == 1

        config_max = CoinGeckoConfig(rate_limit=500)
        assert config_max.rate_limit == 500

        # Invalid values
        with pytest.raises(ValidationError):
            CoinGeckoConfig(rate_limit=0)  # Too low

        with pytest.raises(ValidationError):
            CoinGeckoConfig(rate_limit=501)  # Too high

    def test_optional_api_key(self) -> None:
        """Test that API key is optional."""
        config = CoinGeckoConfig()
        assert config.api_key is None

        config_with_key = CoinGeckoConfig(api_key="test_key")
        assert config_with_key.api_key == "test_key"

    def test_default_values(self) -> None:
        """Test default CoinGecko configuration values."""
        config = CoinGeckoConfig()
        assert config.api_key is None
        assert str(config.base_url) == "https://api.coingecko.com/api/v3"
        assert config.rate_limit == 30


class TestProcessingConfig:
    """Test processing configuration validation."""

    def test_batch_size_validation(self) -> None:
        """Test batch size bounds (1-1000)."""
        # Valid values
        config_min = ProcessingConfig(batch_size=1)
        assert config_min.batch_size == 1

        config_max = ProcessingConfig(batch_size=1000)
        assert config_max.batch_size == 1000

        # Invalid values
        with pytest.raises(ValidationError):
            ProcessingConfig(batch_size=0)  # Too low

        with pytest.raises(ValidationError):
            ProcessingConfig(batch_size=1001)  # Too high

    def test_concurrent_requests_validation(self) -> None:
        """Test max concurrent requests bounds."""
        # Valid values
        config_min = ProcessingConfig(max_concurrent_requests=1)
        assert config_min.max_concurrent_requests == 1

        config_max = ProcessingConfig(max_concurrent_requests=100)
        assert config_max.max_concurrent_requests == 100

        # Invalid values
        with pytest.raises(ValidationError):
            ProcessingConfig(max_concurrent_requests=0)  # Too low

        with pytest.raises(ValidationError):
            ProcessingConfig(max_concurrent_requests=101)  # Too high

    def test_delay_validation(self) -> None:
        """Test request delay validation (>= 0)."""
        # Valid values
        config_zero = ProcessingConfig(request_delay=0.0)
        assert config_zero.request_delay == 0.0

        config_positive = ProcessingConfig(request_delay=1.5)
        assert config_positive.request_delay == 1.5

        # Invalid values
        with pytest.raises(ValidationError):
            ProcessingConfig(request_delay=-0.1)  # Negative

    def test_retry_validation(self) -> None:
        """Test retry attempts validation."""
        # Valid values
        config_min = ProcessingConfig(retry_attempts=1)
        assert config_min.retry_attempts == 1

        config_max = ProcessingConfig(retry_attempts=10)
        assert config_max.retry_attempts == 10

        # Invalid values
        with pytest.raises(ValidationError):
            ProcessingConfig(retry_attempts=0)  # Too low

        with pytest.raises(ValidationError):
            ProcessingConfig(retry_attempts=11)  # Too high

    def test_threshold_days_validation(self) -> None:
        """Test inactive wallet threshold validation."""
        # Valid values
        config = ProcessingConfig(inactive_wallet_threshold_days=30)
        assert config.inactive_wallet_threshold_days == 30

        # Invalid values
        with pytest.raises(ValidationError):
            ProcessingConfig(inactive_wallet_threshold_days=0)  # Too low

    def test_retry_delay_validation(self) -> None:
        """Test retry delay validation."""
        # Valid values
        config = ProcessingConfig(retry_delay=0.1)
        assert config.retry_delay == 0.1

        # Invalid values
        with pytest.raises(ValidationError):
            ProcessingConfig(retry_delay=0.05)  # Too low

    def test_default_values(self) -> None:
        """Test default processing configuration values."""
        config = ProcessingConfig()
        assert config.batch_size == 50
        assert config.max_concurrent_requests == 10
        assert config.request_delay == 0.1
        assert config.inactive_wallet_threshold_days == 365
        assert config.retry_attempts == 3
        assert config.retry_delay == 1.0


class TestLoggingConfig:
    """Test logging configuration validation."""

    def test_valid_logging_config(self) -> None:
        """Test valid logging configuration."""
        config = LoggingConfig(level="DEBUG", file=Path("test.log"), max_size_mb=50, backup_count=3)
        assert config.level == "DEBUG"
        assert config.file == Path("test.log")
        assert config.max_size_mb == 50
        assert config.backup_count == 3

    def test_log_level_case_insensitive(self) -> None:
        """Test that log level validation is case insensitive."""
        config_lower = LoggingConfig(level="debug")
        assert config_lower.level == "DEBUG"

        config_mixed = LoggingConfig(level="iNfO")
        assert config_mixed.level == "INFO"

        config_upper = LoggingConfig(level="WARNING")
        assert config_upper.level == "WARNING"

    def test_invalid_log_level(self) -> None:
        """Test validation error for invalid log level."""
        with pytest.raises(ValidationError, match="Log level must be one of"):
            LoggingConfig(level="INVALID")

        with pytest.raises(ValidationError):
            LoggingConfig(level="TRACE")  # Not in valid levels

    def test_file_size_validation(self) -> None:
        """Test max file size validation."""
        # Valid values
        config = LoggingConfig(max_size_mb=1)
        assert config.max_size_mb == 1

        # Invalid values
        with pytest.raises(ValidationError):
            LoggingConfig(max_size_mb=0)  # Too low

    def test_backup_count_validation(self) -> None:
        """Test backup count validation."""
        # Valid values
        config_zero = LoggingConfig(backup_count=0)
        assert config_zero.backup_count == 0

        config_positive = LoggingConfig(backup_count=10)
        assert config_positive.backup_count == 10

        # Invalid values
        with pytest.raises(ValidationError):
            LoggingConfig(backup_count=-1)  # Negative

    def test_none_log_file(self) -> None:
        """Test handling of None log file path."""
        config = LoggingConfig(file=None)
        assert config.file is None

    def test_default_values(self) -> None:
        """Test default logging configuration values."""
        config = LoggingConfig()
        assert config.level == "INFO"
        assert config.file == Path("logs/wallet_tracker.log")
        assert config.max_size_mb == 100
        assert config.backup_count == 5


class TestGoogleSheetsConfig:
    """Test Google Sheets configuration."""

    def test_valid_config_with_existing_file(self) -> None:
        """Test valid configuration with existing credentials file."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_path = Path(f.name)

        try:
            config = GoogleSheetsConfig(credentials_file=temp_path)
            assert config.credentials_file == temp_path
            assert config.scope == "https://www.googleapis.com/auth/spreadsheets"
        finally:
            temp_path.unlink()

    def test_invalid_config_missing_file(self) -> None:
        """Test invalid configuration with missing credentials file."""
        with pytest.raises(ValidationError, match="Credentials file not found"):
            GoogleSheetsConfig(credentials_file=Path("/nonexistent/file.json"))

    def test_custom_scope(self) -> None:
        """Test configuration with custom scope."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_path = Path(f.name)

        try:
            config = GoogleSheetsConfig(
                credentials_file=temp_path, scope="https://www.googleapis.com/auth/spreadsheets.readonly"
            )
            assert config.scope == "https://www.googleapis.com/auth/spreadsheets.readonly"
        finally:
            temp_path.unlink()


class TestSettings:
    """Enhanced settings management tests."""

    def test_env_loading_with_custom_file(self) -> None:
        """Test loading environment from custom file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write("TEST_VAR=test_value\n")
            env_file = Path(f.name)

        try:
            Settings(env_file=env_file)
            # Should load the env file
            assert os.getenv("TEST_VAR") == "test_value"
        finally:
            env_file.unlink()
            # Clean up env var
            if "TEST_VAR" in os.environ:
                del os.environ["TEST_VAR"]

    def test_config_loading_from_env(self) -> None:
        """Test configuration loading from environment variables."""
        # Create a temporary credentials file
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "test_alchemy_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "ENVIRONMENT": "development",
                    "BATCH_SIZE": "25",
                },
            ):
                settings = Settings()
                config = settings.load_config()

                assert config.ethereum.alchemy_api_key == "test_alchemy_key"
                assert config.environment == Environment.DEVELOPMENT
                assert config.processing.batch_size == 25
                assert config.google_sheets.credentials_file == temp_creds

        finally:
            temp_creds.unlink()

    def test_missing_required_env_var(self) -> None:
        """Test error when required environment variable is missing."""
        # Create settings without loading .env file
        settings = Settings(env_file=Path("nonexistent.env"))

        with (
            patch.dict(os.environ, {}, clear=True),  # Clear all env vars
            pytest.raises(SettingsError, match="ALCHEMY_API_KEY"),
        ):
            settings.load_config()

    def test_config_validation_results(self) -> None:
        """Test configuration validation results."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "test_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "ENVIRONMENT": "production",
                    "DEBUG": "true",  # This should generate a warning
                },
            ):
                settings = Settings()
                results = settings.validate_config()

                assert results["valid"] is True
                assert len(results["warnings"]) > 0  # Should warn about debug in production
                assert "Debug mode is enabled in production" in results["warnings"]

        finally:
            temp_creds.unlink()

    def test_config_reload(self) -> None:
        """Test configuration reloading."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "test_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "BATCH_SIZE": "50",
                },
            ):
                settings = Settings()
                config1 = settings.get_config()
                assert config1.processing.batch_size == 50

                # Change environment and reload
                os.environ["BATCH_SIZE"] = "100"
                config2 = settings.reload_config()

                assert config2.processing.batch_size == 100
                assert config1 is not config2  # Should be different objects

        finally:
            temp_creds.unlink()


class TestAppConfig:
    """Test main application configuration."""

    def test_environment_helpers(self) -> None:
        """Test environment helper methods."""
        config = AppConfig(
            environment=Environment.PRODUCTION,
            ethereum=EthereumConfig(alchemy_api_key="test", rpc_url="https://test.com"),
            coingecko=CoinGeckoConfig(),
            google_sheets=GoogleSheetsConfig(
                credentials_file=Path(__file__)  # Use this test file as dummy
            ),
        )

        assert config.is_production() is True
        assert config.is_development() is False

        config.environment = Environment.DEVELOPMENT
        assert config.is_production() is False
        assert config.is_development() is True

    def test_cache_backend_helper(self) -> None:
        """Test cache backend helper method."""
        config = AppConfig(
            environment=Environment.DEVELOPMENT,
            ethereum=EthereumConfig(alchemy_api_key="test", rpc_url="https://test.com"),
            coingecko=CoinGeckoConfig(),
            google_sheets=GoogleSheetsConfig(credentials_file=Path(__file__)),
        )

        config.cache.backend = CacheBackend.REDIS
        assert config.get_cache_backend() == "redis"

        config.cache.backend = CacheBackend.FILE
        assert config.get_cache_backend() == "file"


if __name__ == "__main__":
    pytest.main([__file__])
