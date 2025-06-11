"""Tests for configuration serialization and deserialization."""

import json
import tempfile
from pathlib import Path
from typing import Any

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
)


class TestConfigSerialization:
    """Test configuration serialization scenarios."""

    def test_config_to_dict(self) -> None:
        """Test configuration serialization to dictionary."""
        config = AppConfig(
            environment=Environment.DEVELOPMENT,
            debug=True,
            dry_run=False,
            ethereum=EthereumConfig(alchemy_api_key="test_key", rpc_url="https://test.com", rate_limit=100),
            coingecko=CoinGeckoConfig(api_key="cg_key", rate_limit=50),
            google_sheets=GoogleSheetsConfig(credentials_file=Path(__file__)),
        )

        config_dict = config.model_dump()

        assert isinstance(config_dict, dict)
        assert config_dict["environment"] == "development"
        assert config_dict["debug"] is True
        assert config_dict["dry_run"] is False

        # Test nested configurations
        assert config_dict["ethereum"]["alchemy_api_key"] == "test_key"
        assert config_dict["ethereum"]["rate_limit"] == 100
        assert config_dict["coingecko"]["api_key"] == "cg_key"
        assert config_dict["coingecko"]["rate_limit"] == 50

        # Path should be serialized as string
        assert isinstance(config_dict["google_sheets"]["credentials_file"], str)

    def test_config_from_dict(self) -> None:
        """Test configuration deserialization from dictionary."""
        config_data = {
            "environment": "production",
            "debug": False,
            "dry_run": True,
            "ethereum": {
                "alchemy_api_key": "prod_key",
                "infura_project_id": "infura_id",
                "rpc_url": "https://prod.com",
                "rate_limit": 200,
            },
            "coingecko": {"api_key": "cg_prod_key", "base_url": "https://api.coingecko.com/api/v3", "rate_limit": 100},
            "google_sheets": {
                "credentials_file": str(Path(__file__)),
                "scope": "https://www.googleapis.com/auth/spreadsheets",
            },
            "cache": {
                "backend": "redis",
                "redis_url": "redis://prod:6379/0",
                "redis_password": "secret",
                "file_cache_dir": "cache",
                "ttl_prices": 7200,
                "ttl_balances": 3600,
                "max_size_mb": 1000,
            },
            "processing": {
                "batch_size": 100,
                "max_concurrent_requests": 20,
                "request_delay": 0.05,
                "inactive_wallet_threshold_days": 180,
                "retry_attempts": 5,
                "retry_delay": 2.0,
            },
            "logging": {"level": "WARNING", "file": "prod.log", "max_size_mb": 200, "backup_count": 10},
        }

        config = AppConfig(**config_data)

        # Verify main config
        assert config.environment == Environment.PRODUCTION
        assert config.debug is False
        assert config.dry_run is True

        # Verify Ethereum config
        assert config.ethereum.alchemy_api_key == "prod_key"
        assert config.ethereum.infura_project_id == "infura_id"
        assert config.ethereum.rate_limit == 200

        # Verify CoinGecko config
        assert config.coingecko.api_key == "cg_prod_key"
        assert config.coingecko.rate_limit == 100

        # Verify cache config
        assert config.cache.backend == CacheBackend.REDIS
        assert config.cache.redis_password == "secret"
        assert config.cache.ttl_prices == 7200

        # Verify processing config
        assert config.processing.batch_size == 100
        assert config.processing.request_delay == 0.05

        # Verify logging config
        assert config.logging.level == "WARNING"
        assert config.logging.backup_count == 10

    def test_config_json_serialization(self) -> None:
        """Test JSON serialization compatibility."""
        config = AppConfig(
            environment=Environment.STAGING,
            ethereum=EthereumConfig(alchemy_api_key="test_key", rpc_url="https://test.com"),
            coingecko=CoinGeckoConfig(),
            google_sheets=GoogleSheetsConfig(credentials_file=Path(__file__)),
        )

        # Should be JSON serializable
        config_dict = config.model_dump()

        # Custom JSON encoder for Path objects
        def json_encoder(obj):
            if isinstance(obj, Path):
                return str(obj)
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        json_str = json.dumps(config_dict, default=json_encoder)

        assert isinstance(json_str, str)
        assert "staging" in json_str
        assert "test_key" in json_str

        # Should be able to deserialize back
        deserialized_dict = json.loads(json_str)
        restored_config = AppConfig(**deserialized_dict)

        assert restored_config.environment == Environment.STAGING
        assert restored_config.ethereum.alchemy_api_key == "test_key"

    def test_sensitive_data_masking(self) -> None:
        """Test that sensitive data can be masked in serialization."""
        config = AppConfig(
            environment=Environment.DEVELOPMENT,
            ethereum=EthereumConfig(alchemy_api_key="super_secret_key", rpc_url="https://test.com"),
            coingecko=CoinGeckoConfig(api_key="secret_coingecko_key"),
            google_sheets=GoogleSheetsConfig(credentials_file=Path(__file__)),
            cache=CacheConfig(redis_password="secret_redis_password"),
        )

        # Regular dump includes sensitive data
        config_dict = config.model_dump()
        assert config_dict["ethereum"]["alchemy_api_key"] == "super_secret_key"
        assert config_dict["coingecko"]["api_key"] == "secret_coingecko_key"
        assert config_dict["cache"]["redis_password"] == "secret_redis_password"

        # Utility function to mask sensitive fields
        def mask_sensitive_fields(data: dict[str, Any]) -> dict[str, Any]:
            """Recursively mask sensitive fields in config dict."""
            if not isinstance(data, dict):
                return data

            masked = {}
            sensitive_fields = {
                "api_key",
                "alchemy_api_key",
                "infura_project_id",
                "redis_password",
                "private_key",
                "token",
                "secret",
            }

            for key, value in data.items():
                if key.lower() in sensitive_fields or any(sens in key.lower() for sens in sensitive_fields):
                    masked[key] = "***MASKED***"
                elif isinstance(value, dict):
                    masked[key] = mask_sensitive_fields(value)
                else:
                    masked[key] = value

            return masked

        masked_dict = mask_sensitive_fields(config_dict)

        # Sensitive fields should be masked
        assert masked_dict["ethereum"]["alchemy_api_key"] == "***MASKED***"
        assert masked_dict["coingecko"]["api_key"] == "***MASKED***"
        assert masked_dict["cache"]["redis_password"] == "***MASKED***"

        # Non-sensitive fields should remain
        assert masked_dict["environment"] == "development"
        assert masked_dict["ethereum"]["rpc_url"] == "https://test.com"

    def test_round_trip_serialization(self) -> None:
        """Test that serialization is round-trip safe."""
        original_config = AppConfig(
            environment=Environment.PRODUCTION,
            debug=False,
            dry_run=True,
            ethereum=EthereumConfig(
                alchemy_api_key="original_key",
                infura_project_id="infura_123",
                rpc_url="https://original.com",
                rate_limit=150,
            ),
            coingecko=CoinGeckoConfig(api_key="cg_original", rate_limit=75),
            google_sheets=GoogleSheetsConfig(
                credentials_file=Path(__file__), scope="https://www.googleapis.com/auth/spreadsheets.readonly"
            ),
            cache=CacheConfig(
                backend=CacheBackend.HYBRID,
                redis_url="redis://original:6379/1",
                redis_password="original_pass",
                ttl_prices=5400,
                ttl_balances=2700,
            ),
            processing=ProcessingConfig(batch_size=75, max_concurrent_requests=15, request_delay=0.2, retry_attempts=4),
            logging=LoggingConfig(level="ERROR", file=Path("original.log"), max_size_mb=150, backup_count=7),
        )

        # Serialize to dict
        config_dict = original_config.model_dump()

        # Deserialize back to object
        restored_config = AppConfig(**config_dict)

        # Should be equivalent
        assert restored_config.environment == original_config.environment
        assert restored_config.debug == original_config.debug
        assert restored_config.dry_run == original_config.dry_run

        # Ethereum config
        assert restored_config.ethereum.alchemy_api_key == original_config.ethereum.alchemy_api_key
        assert restored_config.ethereum.infura_project_id == original_config.ethereum.infura_project_id
        assert restored_config.ethereum.rpc_url == original_config.ethereum.rpc_url
        assert restored_config.ethereum.rate_limit == original_config.ethereum.rate_limit

        # CoinGecko config
        assert restored_config.coingecko.api_key == original_config.coingecko.api_key
        assert restored_config.coingecko.rate_limit == original_config.coingecko.rate_limit

        # Cache config
        assert restored_config.cache.backend == original_config.cache.backend
        assert restored_config.cache.redis_url == original_config.cache.redis_url
        assert restored_config.cache.redis_password == original_config.cache.redis_password
        assert restored_config.cache.ttl_prices == original_config.cache.ttl_prices
        assert restored_config.cache.ttl_balances == original_config.cache.ttl_balances

        # Processing config
        assert restored_config.processing.batch_size == original_config.processing.batch_size
        assert restored_config.processing.max_concurrent_requests == original_config.processing.max_concurrent_requests
        assert restored_config.processing.request_delay == original_config.processing.request_delay
        assert restored_config.processing.retry_attempts == original_config.processing.retry_attempts

        # Logging config
        assert restored_config.logging.level == original_config.logging.level
        assert restored_config.logging.file == original_config.logging.file
        assert restored_config.logging.max_size_mb == original_config.logging.max_size_mb
        assert restored_config.logging.backup_count == original_config.logging.backup_count

    def test_partial_serialization(self) -> None:
        """Test serialization of individual configuration components."""
        # Test EthereumConfig serialization
        eth_config = EthereumConfig(
            alchemy_api_key="eth_key", infura_project_id="infura_123", rpc_url="https://ethereum.com", rate_limit=250
        )

        eth_dict = eth_config.model_dump()
        assert eth_dict["alchemy_api_key"] == "eth_key"
        assert eth_dict["infura_project_id"] == "infura_123"
        assert eth_dict["rate_limit"] == 250

        # Deserialize back
        restored_eth = EthereumConfig(**eth_dict)
        assert restored_eth.alchemy_api_key == eth_config.alchemy_api_key
        assert restored_eth.infura_project_id == eth_config.infura_project_id

        # Test CacheConfig serialization
        cache_config = CacheConfig(
            backend=CacheBackend.FILE, file_cache_dir=Path("/custom/cache"), ttl_prices=9000, max_size_mb=750
        )

        cache_dict = cache_config.model_dump()
        assert cache_dict["backend"] == "file"
        assert cache_dict["ttl_prices"] == 9000
        assert cache_dict["max_size_mb"] == 750

        # Deserialize back
        restored_cache = CacheConfig(**cache_dict)
        assert restored_cache.backend == cache_config.backend
        assert restored_cache.ttl_prices == cache_config.ttl_prices

    def test_serialization_with_defaults(self) -> None:
        """Test serialization includes default values."""
        # Create config with minimal required fields
        config = AppConfig(
            environment=Environment.DEVELOPMENT,
            ethereum=EthereumConfig(alchemy_api_key="min_key", rpc_url="https://min.com"),
            coingecko=CoinGeckoConfig(),
            google_sheets=GoogleSheetsConfig(credentials_file=Path(__file__)),
        )

        config_dict = config.model_dump()

        # Should include default values
        assert config_dict["debug"] is False  # Default
        assert config_dict["dry_run"] is False  # Default
        assert config_dict["ethereum"]["rate_limit"] == 100  # Default
        assert config_dict["coingecko"]["rate_limit"] == 30  # Default
        assert config_dict["cache"]["backend"] == "hybrid"  # Default
        assert config_dict["processing"]["batch_size"] == 50  # Default
        assert config_dict["logging"]["level"] == "INFO"  # Default

    def test_serialization_type_preservation(self) -> None:
        """Test that types are preserved during serialization."""
        config = AppConfig(
            environment=Environment.STAGING,
            ethereum=EthereumConfig(alchemy_api_key="type_test", rpc_url="https://types.com", rate_limit=300),
            coingecko=CoinGeckoConfig(rate_limit=60),
            google_sheets=GoogleSheetsConfig(credentials_file=Path(__file__)),
            processing=ProcessingConfig(request_delay=0.15, retry_delay=1.5),
        )

        config_dict = config.model_dump()

        # Check types in serialized dict
        assert isinstance(config_dict["environment"], str)
        assert isinstance(config_dict["debug"], bool)
        assert isinstance(config_dict["ethereum"]["rate_limit"], int)
        assert isinstance(config_dict["coingecko"]["rate_limit"], int)
        assert isinstance(config_dict["processing"]["request_delay"], float)
        assert isinstance(config_dict["processing"]["retry_delay"], float)

        # Deserialize and check types are restored
        restored_config = AppConfig(**config_dict)
        assert isinstance(restored_config.environment, Environment)
        assert isinstance(restored_config.debug, bool)
        assert isinstance(restored_config.ethereum.rate_limit, int)
        assert isinstance(restored_config.processing.request_delay, float)

    def test_serialization_edge_cases(self) -> None:
        """Test serialization of edge cases and special values."""
        # Test with None values
        config = AppConfig(
            environment=Environment.DEVELOPMENT,
            ethereum=EthereumConfig(
                alchemy_api_key="edge_test",
                rpc_url="https://edge.com",
                infura_project_id=None,  # Explicit None
            ),
            coingecko=CoinGeckoConfig(
                api_key=None  # Explicit None
            ),
            google_sheets=GoogleSheetsConfig(credentials_file=Path(__file__)),
            logging=LoggingConfig(
                file=None  # None log file
            ),
        )

        config_dict = config.model_dump()

        # None values should be preserved
        assert config_dict["ethereum"]["infura_project_id"] is None
        assert config_dict["coingecko"]["api_key"] is None
        assert config_dict["logging"]["file"] is None

        # Deserialize should handle None values
        restored_config = AppConfig(**config_dict)
        assert restored_config.ethereum.infura_project_id is None
        assert restored_config.coingecko.api_key is None
        assert restored_config.logging.file is None

    def test_serialization_validation_errors(self) -> None:
        """Test serialization behavior with validation errors."""
        # Create valid config dict
        valid_dict = {
            "environment": "development",
            "ethereum": {"alchemy_api_key": "test_key", "rpc_url": "https://test.com"},
            "coingecko": {},
            "google_sheets": {"credentials_file": str(Path(__file__))},
        }

        # This should work
        config = AppConfig(**valid_dict)
        assert config.environment == Environment.DEVELOPMENT

        # Test with invalid data
        invalid_dict = valid_dict.copy()
        invalid_dict["ethereum"]["rate_limit"] = -10  # Invalid

        with pytest.raises(ValidationError):
            AppConfig(**invalid_dict)

    def test_config_export_import(self) -> None:
        """Test configuration export/import functionality."""
        # Create a complex configuration
        config = AppConfig(
            environment=Environment.PRODUCTION,
            debug=False,
            dry_run=False,
            ethereum=EthereumConfig(
                alchemy_api_key="export_key",
                infura_project_id="export_infura",
                rpc_url="https://export.com",
                rate_limit=500,
            ),
            coingecko=CoinGeckoConfig(api_key="export_cg_key", rate_limit=150),
            google_sheets=GoogleSheetsConfig(credentials_file=Path(__file__)),
            cache=CacheConfig(
                backend=CacheBackend.REDIS, redis_url="redis://export:6379/2", redis_password="export_pass"
            ),
        )

        # Export to JSON file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            config_dict = config.model_dump()

            # Custom encoder for Path objects
            def path_encoder(obj):
                if isinstance(obj, Path):
                    return str(obj)
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

            json.dump(config_dict, f, default=path_encoder, indent=2)
            export_file = Path(f.name)

        try:
            # Import from JSON file
            with open(export_file) as f:
                imported_dict = json.load(f)

            imported_config = AppConfig(**imported_dict)

            # Verify import matches original
            assert imported_config.environment == config.environment
            assert imported_config.ethereum.alchemy_api_key == config.ethereum.alchemy_api_key
            assert imported_config.coingecko.api_key == config.coingecko.api_key
            assert imported_config.cache.redis_password == config.cache.redis_password

        finally:
            export_file.unlink()

    def test_config_comparison(self) -> None:
        """Test configuration comparison after serialization."""
        config1 = AppConfig(
            environment=Environment.DEVELOPMENT,
            ethereum=EthereumConfig(alchemy_api_key="compare_key", rpc_url="https://compare.com"),
            coingecko=CoinGeckoConfig(),
            google_sheets=GoogleSheetsConfig(credentials_file=Path(__file__)),
        )

        # Serialize and deserialize
        config_dict = config1.model_dump()
        config2 = AppConfig(**config_dict)

        # Compare individual fields (Pydantic models may not be directly comparable)
        assert config1.environment == config2.environment
        assert config1.debug == config2.debug
        assert config1.ethereum.alchemy_api_key == config2.ethereum.alchemy_api_key
        assert config1.ethereum.rpc_url == config2.ethereum.rpc_url
        assert config1.coingecko.rate_limit == config2.coingecko.rate_limit

    def test_config_schema_generation(self) -> None:
        """Test JSON schema generation for configuration."""
        # Generate JSON schema
        schema = AppConfig.model_json_schema()

        assert isinstance(schema, dict)
        assert "properties" in schema
        assert "required" in schema

        # Check that main sections are present
        properties = schema["properties"]
        assert "environment" in properties
        assert "ethereum" in properties
        assert "coingecko" in properties
        assert "google_sheets" in properties
        assert "cache" in properties
        assert "processing" in properties
        assert "logging" in properties

        # Check required fields
        required_fields = schema["required"]
        assert "ethereum" in required_fields
        assert "coingecko" in required_fields
        assert "google_sheets" in required_fields

    def test_config_model_dump_modes(self) -> None:
        """Test different model dump modes."""
        config = AppConfig(
            environment=Environment.DEVELOPMENT,
            ethereum=EthereumConfig(alchemy_api_key="mode_test", rpc_url="https://modes.com"),
            coingecko=CoinGeckoConfig(),
            google_sheets=GoogleSheetsConfig(credentials_file=Path(__file__)),
        )

        # Default mode
        default_dump = config.model_dump()
        assert isinstance(default_dump, dict)

        # JSON mode (should be JSON serializable)
        json_dump = config.model_dump(mode="json")
        assert isinstance(json_dump, dict)

        # Should be able to JSON serialize the result
        json_str = json.dumps(json_dump)
        assert isinstance(json_str, str)


if __name__ == "__main__":
    pytest.main([__file__])
