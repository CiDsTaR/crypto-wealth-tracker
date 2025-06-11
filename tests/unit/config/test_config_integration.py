"""Integration tests for configuration with external systems."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from wallet_tracker.config import (
    AppConfig,
    CacheBackend,
    Environment,
    Settings,
)


class TestConfigurationIntegration:
    """Test configuration integration scenarios."""

    def test_google_sheets_credentials_integration(self) -> None:
        """Test Google Sheets credentials validation."""
        # Test with various credential file formats

        # Valid service account credentials
        valid_service_account = {
            "type": "service_account",
            "project_id": "test-project-123",
            "private_key_id": "key-id-123",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7VJT...\n-----END PRIVATE KEY-----\n",
            "client_email": "test-service@test-project-123.iam.gserviceaccount.com",
            "client_id": "123456789012345678901",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test-service%40test-project-123.iam.gserviceaccount.com",
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(valid_service_account, f)
            creds_file = Path(f.name)

        try:
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "test_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(creds_file),
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()

                assert config.google_sheets.credentials_file == creds_file

                # Verify credentials file content
                with open(creds_file) as f:
                    creds_data = json.load(f)
                    assert creds_data["type"] == "service_account"
                    assert creds_data["project_id"] == "test-project-123"
                    assert "client_email" in creds_data
                    assert creds_data["client_email"].endswith(".iam.gserviceaccount.com")

        finally:
            creds_file.unlink()

    def test_google_sheets_invalid_credentials(self) -> None:
        """Test handling of invalid Google Sheets credentials."""
        # Test with invalid JSON
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{ invalid json")
            invalid_json_file = Path(f.name)

        try:
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "test_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(invalid_json_file),
                },
                clear=True,
            ):
                settings = Settings()
                # Should still load config (validation happens at Google Sheets client level)
                config = settings.load_config()
                assert config.google_sheets.credentials_file == invalid_json_file

        finally:
            invalid_json_file.unlink()

        # Test with missing required fields
        incomplete_creds = {
            "type": "service_account",
            # Missing required fields like project_id, private_key, etc.
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(incomplete_creds, f)
            incomplete_file = Path(f.name)

        try:
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "test_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(incomplete_file),
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()
                # Config loads successfully, but Google Sheets client would fail
                assert config.google_sheets.credentials_file == incomplete_file

        finally:
            incomplete_file.unlink()

    def test_redis_connection_validation(self) -> None:
        """Test Redis connection string validation."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Test various Redis connection formats
            redis_configs = [
                {
                    "url": "redis://localhost:6379/0",
                    "expected": {"scheme": "redis", "host": "localhost", "port": "6379", "db": "0"},
                },
                {
                    "url": "redis://user:password@localhost:6379/1",
                    "expected": {
                        "scheme": "redis",
                        "auth": "user:password",
                        "host": "localhost",
                        "port": "6379",
                        "db": "1",
                    },
                },
                {
                    "url": "rediss://secure.redis.com:6380/0",
                    "expected": {
                        "scheme": "rediss",  # SSL Redis
                        "host": "secure.redis.com",
                        "port": "6380",
                    },
                },
                {
                    "url": "redis://localhost",
                    "expected": {
                        "scheme": "redis",
                        "host": "localhost",
                        # Default port and db
                    },
                },
            ]

            for redis_config in redis_configs:
                with patch.dict(
                    os.environ,
                    {
                        "ALCHEMY_API_KEY": "test_key",
                        "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                        "CACHE_BACKEND": "redis",
                        "REDIS_URL": redis_config["url"],
                    },
                    clear=True,
                ):
                    settings = Settings()
                    config = settings.load_config()

                    assert config.cache.backend == CacheBackend.REDIS
                    assert config.cache.redis_url == redis_config["url"]

                    # Verify URL components
                    url = config.cache.redis_url
                    expected = redis_config["expected"]

                    assert url.startswith(expected["scheme"] + "://")
                    assert expected["host"] in url

                    if "port" in expected:
                        assert expected["port"] in url
                    if "db" in expected:
                        assert url.endswith("/" + expected["db"])

        finally:
            temp_creds.unlink()

    def test_ethereum_rpc_validation(self) -> None:
        """Test Ethereum RPC URL validation."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Test Alchemy URLs
            alchemy_configs = [
                {
                    "api_key": "alch_test_key_123",
                    "rpc_url": "https://eth-mainnet.g.alchemy.com/v2/alch_test_key_123",
                    "network": "mainnet",
                },
                {
                    "api_key": "alch_test_key_456",
                    "rpc_url": "https://eth-goerli.g.alchemy.com/v2/alch_test_key_456",
                    "network": "goerli",
                },
                {
                    "api_key": "alch_test_key_789",
                    "rpc_url": "wss://eth-mainnet.g.alchemy.com/v2/alch_test_key_789",
                    "network": "mainnet-websocket",
                },
            ]

            for alchemy_config in alchemy_configs:
                with patch.dict(
                    os.environ,
                    {
                        "ALCHEMY_API_KEY": alchemy_config["api_key"],
                        "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                        "ETHEREUM_RPC_URL": alchemy_config["rpc_url"],
                    },
                    clear=True,
                ):
                    settings = Settings()
                    config = settings.load_config()

                    assert config.ethereum.alchemy_api_key == alchemy_config["api_key"]
                    assert config.ethereum.rpc_url == alchemy_config["rpc_url"]

                    # Verify URL structure
                    if "mainnet" in alchemy_config["network"]:
                        assert "eth-mainnet" in config.ethereum.rpc_url
                    elif "goerli" in alchemy_config["network"]:
                        assert "eth-goerli" in config.ethereum.rpc_url

                    if "websocket" in alchemy_config["network"]:
                        assert config.ethereum.rpc_url.startswith("wss://")
                    else:
                        assert config.ethereum.rpc_url.startswith("https://")

            # Test Infura URLs
            infura_configs = [
                {
                    "api_key": "alchemy_key",
                    "infura_id": "infura_project_123",
                    "rpc_url": "https://mainnet.infura.io/v3/infura_project_123",
                },
                {
                    "api_key": "alchemy_key",
                    "infura_id": "infura_project_456",
                    "rpc_url": "wss://mainnet.infura.io/ws/v3/infura_project_456",
                },
            ]

            for infura_config in infura_configs:
                with patch.dict(
                    os.environ,
                    {
                        "ALCHEMY_API_KEY": infura_config["api_key"],
                        "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                        "INFURA_PROJECT_ID": infura_config["infura_id"],
                        "ETHEREUM_RPC_URL": infura_config["rpc_url"],
                    },
                    clear=True,
                ):
                    settings = Settings()
                    config = settings.load_config()

                    assert config.ethereum.infura_project_id == infura_config["infura_id"]
                    assert config.ethereum.rpc_url == infura_config["rpc_url"]
                    assert "infura.io" in config.ethereum.rpc_url

        finally:
            temp_creds.unlink()

    def test_coingecko_api_validation(self) -> None:
        """Test CoinGecko API configuration validation."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Test public API (no key)
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "test_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "COINGECKO_BASE_URL": "https://api.coingecko.com/api/v3",
                    "COINGECKO_RATE_LIMIT": "10",  # Lower limit for public API
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()

                assert config.coingecko.api_key is None
                assert str(config.coingecko.base_url) == "https://api.coingecko.com/api/v3"
                assert config.coingecko.rate_limit == 10

            # Test Pro API (with key)
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "test_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "COINGECKO_API_KEY": "CG-pro-api-key-123",
                    "COINGECKO_BASE_URL": "https://pro-api.coingecko.com/api/v3",
                    "COINGECKO_RATE_LIMIT": "500",  # Higher limit for Pro API
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()

                assert config.coingecko.api_key == "CG-pro-api-key-123"
                assert "pro-api.coingecko.com" in str(config.coingecko.base_url)
                assert config.coingecko.rate_limit == 500

            # Test Enterprise API
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "test_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "COINGECKO_API_KEY": "CG-enterprise-key-456",
                    "COINGECKO_BASE_URL": "https://enterprise-api.coingecko.com/api/v3",
                    "COINGECKO_RATE_LIMIT": "1000",  # Even higher limit for Enterprise
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()

                assert config.coingecko.api_key == "CG-enterprise-key-456"
                assert "enterprise-api.coingecko.com" in str(config.coingecko.base_url)
                assert config.coingecko.rate_limit == 1000

        finally:
            temp_creds.unlink()

    def test_cross_service_integration(self) -> None:
        """Test configuration consistency across multiple services."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Test production-like configuration
            with patch.dict(
                os.environ,
                {
                    "ENVIRONMENT": "production",
                    "DEBUG": "false",
                    "DRY_RUN": "false",
                    # Ethereum configuration
                    "ALCHEMY_API_KEY": "prod_alchemy_key_123",
                    "ETHEREUM_RPC_URL": "https://eth-mainnet.g.alchemy.com/v2/prod_alchemy_key_123",
                    "ALCHEMY_RATE_LIMIT": "300",
                    # CoinGecko configuration
                    "COINGECKO_API_KEY": "CG-prod-key-456",
                    "COINGECKO_BASE_URL": "https://pro-api.coingecko.com/api/v3",
                    "COINGECKO_RATE_LIMIT": "1000",
                    # Google Sheets
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    # Cache configuration (Redis for production)
                    "CACHE_BACKEND": "redis",
                    "REDIS_URL": "redis://prod-redis.internal:6379/0",
                    "REDIS_PASSWORD": "prod_redis_secret",
                    "CACHE_TTL_PRICES": "1800",  # 30 minutes
                    "CACHE_TTL_BALANCES": "900",  # 15 minutes
                    # Processing configuration
                    "BATCH_SIZE": "100",
                    "MAX_CONCURRENT_REQUESTS": "25",
                    "REQUEST_DELAY": "0.05",
                    "RETRY_ATTEMPTS": "5",
                    # Logging configuration
                    "LOG_LEVEL": "WARNING",
                    "LOG_FILE": "/var/log/wallet_tracker/app.log",
                    "LOG_MAX_SIZE_MB": "200",
                    "LOG_BACKUP_COUNT": "10",
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()

                # Verify production environment
                assert config.environment == Environment.PRODUCTION
                assert config.debug is False
                assert config.dry_run is False

                # Verify service configurations are production-ready
                assert config.ethereum.rate_limit == 300
                assert config.coingecko.rate_limit == 1000
                assert config.cache.backend == CacheBackend.REDIS
                assert config.processing.max_concurrent_requests == 25
                assert config.logging.level == "WARNING"

                # Verify rate limiting is consistent
                total_api_capacity = config.ethereum.rate_limit + config.coingecko.rate_limit
                assert config.processing.max_concurrent_requests < total_api_capacity

                # Verify cache TTL is reasonable for production
                assert config.cache.ttl_prices == 1800  # 30 minutes
                assert config.cache.ttl_balances == 900  # 15 minutes
                assert config.cache.ttl_prices > config.cache.ttl_balances  # Prices cached longer

        finally:
            temp_creds.unlink()

    def test_development_integration(self) -> None:
        """Test development environment integration."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Test development-friendly configuration
            with patch.dict(
                os.environ,
                {
                    "ENVIRONMENT": "development",
                    "DEBUG": "true",
                    "DRY_RUN": "true",
                    # Local Ethereum node or test network
                    "ALCHEMY_API_KEY": "dev_alchemy_key",
                    "ETHEREUM_RPC_URL": "http://localhost:8545",  # Local node
                    "ALCHEMY_RATE_LIMIT": "50",  # Lower rate limit for dev
                    # Public CoinGecko API
                    "COINGECKO_RATE_LIMIT": "10",  # No API key, lower limit
                    # Google Sheets
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    # File cache for development
                    "CACHE_BACKEND": "file",
                    "FILE_CACHE_DIR": "./dev_cache",
                    "CACHE_TTL_PRICES": "300",  # 5 minutes (shorter for dev)
                    # Conservative processing for dev
                    "BATCH_SIZE": "10",
                    "MAX_CONCURRENT_REQUESTS": "3",
                    "REQUEST_DELAY": "0.5",  # Slower for debugging
                    # Verbose logging for development
                    "LOG_LEVEL": "DEBUG",
                    "LOG_FILE": "./dev.log",
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()

                # Verify development environment
                assert config.environment == Environment.DEVELOPMENT
                assert config.debug is True
                assert config.dry_run is True

                # Verify development-friendly settings
                assert config.ethereum.rpc_url == "http://localhost:8545"
                assert config.cache.backend == CacheBackend.FILE
                assert config.cache.ttl_prices == 300  # Short cache for dev
                assert config.processing.batch_size == 10  # Small batches
                assert config.processing.request_delay == 0.5  # Slower processing
                assert config.logging.level == "DEBUG"

        finally:
            temp_creds.unlink()

    def test_staging_integration(self) -> None:
        """Test staging environment integration."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Test staging configuration (between dev and prod)
            with patch.dict(
                os.environ,
                {
                    "ENVIRONMENT": "staging",
                    "DEBUG": "false",
                    "DRY_RUN": "false",
                    # Staging Ethereum configuration
                    "ALCHEMY_API_KEY": "staging_alchemy_key",
                    "ETHEREUM_RPC_URL": "https://eth-goerli.g.alchemy.com/v2/staging_alchemy_key",
                    "ALCHEMY_RATE_LIMIT": "200",
                    # Staging CoinGecko
                    "COINGECKO_API_KEY": "CG-staging-key",
                    "COINGECKO_RATE_LIMIT": "500",
                    # Google Sheets
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    # Hybrid cache for staging
                    "CACHE_BACKEND": "hybrid",
                    "REDIS_URL": "redis://staging-redis:6379/0",
                    "CACHE_TTL_PRICES": "1200",  # 20 minutes
                    # Moderate processing for staging
                    "BATCH_SIZE": "25",
                    "MAX_CONCURRENT_REQUESTS": "10",
                    "REQUEST_DELAY": "0.1",
                    # Info level logging for staging
                    "LOG_LEVEL": "INFO",
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()

                # Verify staging environment
                assert config.environment == Environment.STAGING
                assert config.debug is False

                # Verify staging is between dev and prod
                assert "goerli" in config.ethereum.rpc_url  # Test network
                assert config.cache.backend == CacheBackend.HYBRID
                assert config.processing.batch_size == 25  # Medium batch size
                assert config.logging.level == "INFO"

        finally:
            temp_creds.unlink()

    def test_environment_variable_override_precedence(self) -> None:
        """Test that environment variables override defaults correctly."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Test with minimal env vars (should use defaults)
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "minimal_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()

                # Should use defaults
                assert config.environment == Environment.DEVELOPMENT  # Default
                assert config.processing.batch_size == 50  # Default
                assert config.cache.backend == CacheBackend.HYBRID  # Default
                assert config.logging.level == "INFO"  # Default

            # Test with explicit overrides
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "override_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "ENVIRONMENT": "production",
                    "BATCH_SIZE": "200",
                    "CACHE_BACKEND": "redis",
                    "LOG_LEVEL": "ERROR",
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()

                # Should use overridden values
                assert config.environment == Environment.PRODUCTION
                assert config.processing.batch_size == 200
                assert config.cache.backend == CacheBackend.REDIS
                assert config.logging.level == "ERROR"

        finally:
            temp_creds.unlink()

    def test_configuration_validation_integration(self) -> None:
        """Test comprehensive configuration validation."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Test configuration that should pass validation
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "valid_key_123",
                    "ETHEREUM_RPC_URL": "https://eth-mainnet.g.alchemy.com/v2/valid_key_123",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "ENVIRONMENT": "production",
                    "DEBUG": "false",
                    "CACHE_BACKEND": "redis",
                    "REDIS_URL": "redis://prod:6379/0",
                },
                clear=True,
            ):
                settings = Settings()
                validation_result = settings.validate_config()

                assert validation_result["valid"] is True
                assert len(validation_result["issues"]) == 0

                config_data = validation_result["config"]
                assert config_data["environment"] == "production"
                assert config_data["debug"] is False

            # Test configuration that should generate warnings
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "warning_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "ENVIRONMENT": "production",
                    "DEBUG": "true",  # Warning: debug in production
                    "CACHE_BACKEND": "file",  # Warning: file cache in production
                },
                clear=True,
            ):
                settings = Settings()
                validation_result = settings.validate_config()

                assert validation_result["valid"] is True
                assert len(validation_result["warnings"]) > 0

                warnings = validation_result["warnings"]
                assert any("Debug mode is enabled in production" in w for w in warnings)

        finally:
            temp_creds.unlink()

    def test_real_world_configuration_scenarios(self) -> None:
        """Test real-world configuration scenarios."""
        # Test scenario: Small startup configuration
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Startup: Limited resources, file cache, conservative settings
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "startup_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "ENVIRONMENT": "production",
                    "CACHE_BACKEND": "file",
                    "BATCH_SIZE": "20",
                    "MAX_CONCURRENT_REQUESTS": "5",
                    "REQUEST_DELAY": "0.2",
                    "COINGECKO_RATE_LIMIT": "10",  # Free tier
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()

                assert config.environment == Environment.PRODUCTION
                assert config.cache.backend == CacheBackend.FILE
                assert config.processing.batch_size == 20
                assert config.processing.max_concurrent_requests == 5
                assert config.coingecko.rate_limit == 10

            # Enterprise: High throughput, Redis cluster, aggressive caching
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "enterprise_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "ENVIRONMENT": "production",
                    "CACHE_BACKEND": "redis",
                    "REDIS_URL": "redis://enterprise-cluster.internal:6379/0",
                    "BATCH_SIZE": "500",
                    "MAX_CONCURRENT_REQUESTS": "50",
                    "REQUEST_DELAY": "0.01",
                    "ALCHEMY_RATE_LIMIT": "1000",
                    "COINGECKO_RATE_LIMIT": "10000",  # Enterprise tier
                    "CACHE_TTL_PRICES": "3600",  # 1 hour
                    "CACHE_MAX_SIZE_MB": "2000",  # 2GB cache
                },
                clear=True,
            ):
                settings = Settings()
                config = settings.load_config()

                assert config.environment == Environment.PRODUCTION
                assert config.cache.backend == CacheBackend.REDIS
                assert config.processing.batch_size == 500
                assert config.processing.max_concurrent_requests == 50
                assert config.ethereum.rate_limit == 1000
                assert config.coingecko.rate_limit == 10000
                assert config.cache.ttl_prices == 3600
                assert config.cache.max_size_mb == 2000

        finally:
            temp_creds.unlink()

    def test_configuration_migration_scenarios(self) -> None:
        """Test configuration migration between environments."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Simulate migration from development to staging
            dev_env_vars = {
                "ALCHEMY_API_KEY": "dev_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "ENVIRONMENT": "development",
                "DEBUG": "true",
                "CACHE_BACKEND": "file",
                "BATCH_SIZE": "10",
                "LOG_LEVEL": "DEBUG",
            }

            staging_env_vars = {
                "ALCHEMY_API_KEY": "staging_key",
                "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                "ENVIRONMENT": "staging",
                "DEBUG": "false",  # Disabled in staging
                "CACHE_BACKEND": "hybrid",  # Upgraded cache
                "BATCH_SIZE": "25",  # Increased batch size
                "LOG_LEVEL": "INFO",  # Less verbose logging
                "REDIS_URL": "redis://staging:6379/0",  # Added Redis
            }

            # Test development configuration
            with patch.dict(os.environ, dev_env_vars, clear=True):
                settings = Settings()
                dev_config = settings.load_config()

                assert dev_config.environment == Environment.DEVELOPMENT
                assert dev_config.debug is True
                assert dev_config.cache.backend == CacheBackend.FILE

            # Test staging configuration (migration)
            with patch.dict(os.environ, staging_env_vars, clear=True):
                settings = Settings()
                staging_config = settings.load_config()

                assert staging_config.environment == Environment.STAGING
                assert staging_config.debug is False
                assert staging_config.cache.backend == CacheBackend.HYBRID
                assert staging_config.processing.batch_size == 25

                # Verify migration maintained compatibility
                assert staging_config.ethereum.alchemy_api_key == "staging_key"
                assert staging_config.google_sheets.credentials_file == temp_creds

        finally:
            temp_creds.unlink()

    def test_configuration_backup_and_restore(self) -> None:
        """Test configuration export and import for backup/restore."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b'{"test": "credentials"}')
            temp_creds = Path(f.name)

        try:
            # Create a production configuration
            with patch.dict(
                os.environ,
                {
                    "ALCHEMY_API_KEY": "backup_test_key",
                    "GOOGLE_SHEETS_CREDENTIALS_FILE": str(temp_creds),
                    "ENVIRONMENT": "production",
                    "CACHE_BACKEND": "redis",
                    "REDIS_URL": "redis://backup-test:6379/0",
                    "REDIS_PASSWORD": "backup_secret",
                    "BATCH_SIZE": "100",
                    "COINGECKO_API_KEY": "CG-backup-key",
                },
                clear=True,
            ):
                settings = Settings()
                original_config = settings.load_config()

                # Export configuration
                config_backup = original_config.model_dump()

                # Save to file (simulating backup)
                with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as backup_file:

                    def json_encoder(obj):
                        if isinstance(obj, Path):
                            return str(obj)
                        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

                    json.dump(config_backup, backup_file, default=json_encoder, indent=2)
                    backup_path = Path(backup_file.name)

            # Simulate configuration restoration
            try:
                with open(backup_path) as f:
                    restored_data = json.load(f)

                # Create new config from backup
                restored_config = AppConfig(**restored_data)

                # Verify restoration
                assert restored_config.environment == original_config.environment
                assert restored_config.ethereum.alchemy_api_key == original_config.ethereum.alchemy_api_key
                assert restored_config.cache.redis_password == original_config.cache.redis_password
                assert restored_config.processing.batch_size == original_config.processing.batch_size
                assert restored_config.coingecko.api_key == original_config.coingecko.api_key

            finally:
                backup_path.unlink()

        finally:
            temp_creds.unlink()


if __name__ == "__main__":
    pytest.main([__file__])
