"""Tests for comprehensive configuration validation."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

from wallet_tracker.config import (
    AppConfig,
    CacheBackend,
    Environment,
    EthereumConfig,
    GoogleSheetsConfig,
    Settings,
    SettingsError,
)


class TestConfigurationValidation:
    """Test comprehensive configuration validation scenarios."""

    def test_production_config_strictness(self) -> None:
        """Test that production config has stricter validation."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Production with debug enabled should generate warning
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "prod_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "ENVIRONMENT": "production",
                "DEBUG": "true",  # This should generate a warning
                "CACHE_BACKEND": "redis",  # Production should use proper cache
            }, clear=True):
                settings = Settings()
                validation_result = settings.validate_config()

                assert validation_result["valid"] is True
                assert len(validation_result["warnings"]) > 0
                assert any("Debug mode is enabled in production" in warning
                          for warning in validation_result["warnings"])

        finally:
            temp_creds.unlink()

    def test_development_config_flexibility(self) -> None:
        """Test that development config is more flexible."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Development with debug should not generate warnings
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "dev_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "ENVIRONMENT": "development",
                "DEBUG": "true",
                "CACHE_BACKEND": "file",  # File cache OK in development
            }, clear=True):
                settings = Settings()
                validation_result = settings.validate_config()

                assert validation_result["valid"] is True
                # Should have no warnings about debug in development
                debug_warnings = [w for w in validation_result["warnings"]
                                if "Debug mode" in w]
                assert len(debug_warnings) == 0

        finally:
            temp_creds.unlink()

    def test_cross_field_validation(self) -> None:
        """Test validation that spans multiple configuration fields."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Test RPC URL and API key consistency
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key_123",
                "ETHEREUM_RPC_URL": "https://eth-mainnet.g.alchemy.com/v2/different_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
            }, clear=True):
                settings = Settings()
                config = settings.load_config()

                # The URL should contain a different key than the API key
                # This demonstrates potential configuration inconsistency
                assert config.ethereum.alchemy_api_key == "test_key_123"
                assert "different_key" in config.ethereum.rpc_url

        finally:
            temp_creds.unlink()

    def test_resource_availability_validation(self) -> None:
        """Test validation of file/directory availability."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Test with existing credentials file
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
            }, clear=True):
                settings = Settings()
                validation_result = settings.validate_config()

                assert validation_result["valid"] is True
                assert len([issue for issue in validation_result["issues"]
                           if "not found" in issue]) == 0

            # Test with non-existent credentials file
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": "/absolutely/nonexistent/file.json",
            }, clear=True):
                settings = Settings()
                validation_result = settings.validate_config()

                assert validation_result["valid"] is False
                assert any("not found" in issue for issue in validation_result["issues"])

        finally:
            temp_creds.unlink()

    def test_network_resource_validation(self) -> None:
        """Test validation of network resources (URLs, etc.)."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Test with valid URLs
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "ETHEREUM_RPC_URL": "https://eth-mainnet.g.alchemy.com/v2/test_key",
                "COINGECKO_BASE_URL": "https://api.coingecko.com/api/v3",
                "REDIS_URL": "redis://localhost:6379/0",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
            }, clear=True):
                settings = Settings()
                config = settings.load_config()

                # All URLs should be valid
                assert config.ethereum.rpc_url.startswith("https://")
                assert "coingecko.com" in str(config.coingecko.base_url)
                assert config.cache.redis_url.startswith("redis://")

        finally:
            temp_creds.unlink()

    def test_cache_backend_consistency(self) -> None:
        """Test cache backend configuration consistency."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Redis backend should have Redis URL
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "CACHE_BACKEND": "redis",
                "REDIS_URL": "redis://localhost:6379/0",
            }, clear=True):
                settings = Settings()
                config = settings.load_config()

                assert config.cache.backend == CacheBackend.REDIS
                assert config.cache.redis_url == "redis://localhost:6379/0"

            # File backend should have file cache directory
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "CACHE_BACKEND": "file",
                "FILE_CACHE_DIR": "/tmp/cache",
            }, clear=True):
                settings = Settings()
                config = settings.load_config()

                assert config.cache.backend == CacheBackend.FILE
                assert config.cache.file_cache_dir == Path("/tmp/cache")

        finally:
            temp_creds.unlink()

    def test_rate_limit_consistency(self) -> None:
        """Test rate limit configuration consistency across services."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "ALCHEMY_RATE_LIMIT": "100",
                "COINGECKO_RATE_LIMIT": "30",
                "MAX_CONCURRENT_REQUESTS": "5",  # Should be <= sum of API limits
            }, clear=True):
                settings = Settings()
                config = settings.load_config()

                # Concurrent requests should be reasonable relative to API limits
                total_api_capacity = config.ethereum.rate_limit + config.coingecko.rate_limit
                assert config.processing.max_concurrent_requests <= total_api_capacity

        finally:
            temp_creds.unlink()

    def test_environment_specific_defaults(self) -> None:
        """Test that different environments have appropriate defaults."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Development environment
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "ENVIRONMENT": "development",
            }, clear=True):
                settings = Settings()
                config = settings.load_config()

                assert config.environment == Environment.DEVELOPMENT
                # Development might use more lenient settings
                assert config.processing.batch_size >= 1

            # Production environment
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "ENVIRONMENT": "production",
            }, clear=True):
                settings = Settings()
                config = settings.load_config()

                assert config.environment == Environment.PRODUCTION
                assert config.debug is False  # Should default to False in production

        finally:
            temp_creds.unlink()

    def test_security_validation(self) -> None:
        """Test security-related configuration validation."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Production should use HTTPS
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "ENVIRONMENT": "production",
                "ETHEREUM_RPC_URL": "http://insecure-endpoint.com",  # HTTP in prod
            }, clear=True):
                settings = Settings()
                config = settings.load_config()

                # This demonstrates a potential security issue
                # In a real validator, you might warn about HTTP in production
                assert config.environment == Environment.PRODUCTION
                assert config.ethereum.rpc_url.startswith("http://")  # Insecure

        finally:
            temp_creds.unlink()

    def test_performance_configuration_validation(self) -> None:
        """Test performance-related configuration validation."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # High throughput configuration
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "BATCH_SIZE": "100",
                "MAX_CONCURRENT_REQUESTS": "20",
                "REQUEST_DELAY": "0.05",
                "CACHE_TTL_PRICES": "1800",  # 30 minutes
            }, clear=True):
                settings = Settings()
                config = settings.load_config()

                # Validate performance settings are consistent
                assert config.processing.batch_size == 100
                assert config.processing.max_concurrent_requests == 20
                assert config.processing.request_delay == 0.05
                assert config.cache.ttl_prices == 1800

                # High concurrent requests with low delay might overwhelm APIs
                requests_per_second = config.processing.max_concurrent_requests / config.processing.request_delay
                # This could be used to validate reasonable request rates

        finally:
            temp_creds.unlink()

    def test_logging_configuration_validation(self) -> None:
        """Test logging configuration validation."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = Path(temp_dir) / "app.log"

            try:
                with patch.dict(os.environ, {
                    "ALCHEMY_API_KEY": "test_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "LOG_FILE": str(log_file),
                    "LOG_LEVEL": "DEBUG",
                    "LOG_MAX_SIZE_MB": "50",
                    "LOG_BACKUP_COUNT": "3",
                }, clear=True):
                    settings = Settings()
                    validation_result = settings.validate_config()

                    assert validation_result["valid"] is True

                    # Log directory should be created during validation
                    assert log_file.parent.exists()

            finally:
                temp_creds.unlink()

    def test_invalid_enum_values(self) -> None:
        """Test handling of invalid enum values in environment variables."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Invalid environment value
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "ENVIRONMENT": "invalid_env",
            }, clear=True):
                settings = Settings()
                config = settings.load_config()

                # Should fall back to development
                assert config.environment == Environment.DEVELOPMENT

            # Invalid cache backend
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "CACHE_BACKEND": "invalid_backend",
            }, clear=True):
                settings = Settings()
                config = settings.load_config()

                # Should fall back to hybrid
                assert config.cache.backend == CacheBackend.HYBRID

        finally:
            temp_creds.unlink()

    def test_comprehensive_validation_flow(self) -> None:
        """Test comprehensive validation of entire configuration."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Complete valid configuration
            with patch.dict(os.environ, {
                "ALCHEMY_API_KEY": "test_key_123456",
                "INFURA_PROJECT_ID": "test_infura_id",
                "ETHEREUM_RPC_URL": "https://eth-mainnet.g.alchemy.com/v2/test_key_123456",
                "ALCHEMY_RATE_LIMIT": "100",
                "COINGECKO_API_KEY": "cg_test_key",
                "COINGECKO_BASE_URL": "https://api.coingecko.com/api/v3",
                "COINGECKO_RATE_LIMIT": "50",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "GOOGLE_SHEETS_SCOPE": "https://www.googleapis.com/auth/spreadsheets",
                "CACHE_BACKEND": "hybrid",
                "REDIS_URL": "redis://localhost:6379/0",
                "REDIS_PASSWORD": "secret",
                "FILE_CACHE_DIR": "cache",
                "CACHE_TTL_PRICES": "3600",
                "CACHE_TTL_BALANCES": "1800",
                "CACHE_MAX_SIZE_MB": "500",
                "BATCH_SIZE": "50",
                "MAX_CONCURRENT_REQUESTS": "10",
                "REQUEST_DELAY": "0.1",
                "INACTIVE_WALLET_THRESHOLD_DAYS": "365",
                "RETRY_ATTEMPTS": "3",
                "RETRY_DELAY": "1.0",
                "LOG_LEVEL": "INFO",
                "LOG_MAX_SIZE_MB": "100",
                "LOG_BACKUP_COUNT": "5",
                "ENVIRONMENT": "staging",
                "DEBUG": "false",
                "DRY_RUN": "false",
            }, clear=True):
                settings = Settings()
                validation_result = settings.validate_config()

                assert validation_result["valid"] is True
                assert len(validation_result["issues"]) == 0

                config_data = validation_result["config"]
                assert config_data is not None

                # Verify all major sections are present and valid
                assert config_data["environment"] == "staging"
                assert config_data["ethereum"]["alchemy_api_key"] == "test_key_123456"
                assert config_data["coingecko"]["api_key"] == "cg_test_key"
                assert config_data["cache"]["backend"] == "hybrid"
                assert config_data["processing"]["batch_size"] == 50
                assert config_data["logging"]["level"] == "INFO"

        finally:
            temp_creds.unlink()


if __name__ == "__main__":
    pytest.main([__file__])