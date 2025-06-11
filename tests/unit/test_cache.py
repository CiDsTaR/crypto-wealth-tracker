"""Tests for caching system."""

import platform
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from wallet_tracker.config import CacheBackend, CacheConfig
from wallet_tracker.utils import (
    CacheFactory,
    CacheInterface,
    CacheManager,
    FileCache,
)

# Skip file cache tests on Windows
skip_on_windows = pytest.mark.skipif(
    platform.system() == "Windows", reason="File cache tests have permission issues on Windows"
)


class TestFileCache:
    """Test file cache implementation."""

    @pytest.fixture
    def temp_cache_dir(self):
        """Create temporary cache directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def file_cache(self, temp_cache_dir):
        """Create file cache instance."""
        return FileCache(
            cache_dir=temp_cache_dir,
            default_ttl=60,
            max_size_mb=10,
            key_prefix="test:",
        )

    @skip_on_windows
    @pytest.mark.asyncio
    async def test_file_cache_basic_operations(self, file_cache):
        """Test basic file cache operations."""
        # Test set and get
        key = "test_key"
        value = {"data": "test_value", "number": 42}

        result = await file_cache.set(key, value)
        assert result is True

        retrieved = await file_cache.get(key)
        assert retrieved == value

        # Test exists
        exists = await file_cache.exists(key)
        assert exists is True

        # Test delete
        deleted = await file_cache.delete(key)
        assert deleted is True

        # Test get after delete
        retrieved_after_delete = await file_cache.get(key)
        assert retrieved_after_delete is None

    @skip_on_windows
    @pytest.mark.asyncio
    async def test_file_cache_batch_operations(self, file_cache):
        """Test batch operations."""
        # Test set_many
        data = {
            "key1": {"value": 1},
            "key2": {"value": 2},
            "key3": {"value": 3},
        }

        result = await file_cache.set_many(data)
        assert result is True

        # Test get_many
        retrieved = await file_cache.get_many(["key1", "key2", "key3", "nonexistent"])
        assert len(retrieved) == 3
        assert retrieved["key1"] == {"value": 1}
        assert retrieved["key2"] == {"value": 2}
        assert retrieved["key3"] == {"value": 3}
        assert "nonexistent" not in retrieved

    @skip_on_windows
    @pytest.mark.asyncio
    async def test_file_cache_clear(self, file_cache):
        """Test cache clearing."""
        # Add some data
        await file_cache.set("key1", "value1")
        await file_cache.set("key2", "value2")

        # Clear cache
        result = await file_cache.clear()
        assert result is True

        # Check data is gone
        assert await file_cache.get("key1") is None
        assert await file_cache.get("key2") is None

    @skip_on_windows
    @pytest.mark.asyncio
    async def test_file_cache_health_check(self, file_cache):
        """Test health check."""
        health = await file_cache.health_check()
        assert health is True

    def test_file_cache_stats(self, file_cache):
        """Test cache statistics."""
        stats = file_cache.get_stats()

        assert stats["backend"] == "file"
        assert "hits" in stats
        assert "misses" in stats
        assert "sets" in stats
        assert "deletes" in stats
        assert "hit_rate_percent" in stats


class TestCacheFactory:
    """Test cache factory."""

    def test_create_file_cache(self):
        """Test creating file cache."""
        config = CacheConfig(
            backend=CacheBackend.FILE,
            file_cache_dir=Path("test_cache"),
            ttl_prices=300,
            max_size_mb=50,
        )

        cache = CacheFactory.create_cache(config)
        assert isinstance(cache, FileCache)

    @patch("wallet_tracker.utils.redis_cache.redis")
    def test_create_redis_cache(self, mock_redis):
        """Test creating Redis cache."""
        config = CacheConfig(
            backend=CacheBackend.REDIS,
            redis_url="redis://localhost:6379/0",
            ttl_prices=300,
        )

        cache = CacheFactory.create_cache(config)
        # This will be a RedisCache, but we can't easily test without Redis
        assert cache is not None

    def test_unsupported_backend(self):
        """Test error for unsupported backend."""
        config = CacheConfig()
        config.backend = "unsupported"  # Invalid backend

        with pytest.raises(ValueError, match="Unsupported cache backend"):
            CacheFactory.create_cache(config)


class TestCacheManager:
    """Test cache manager."""

    @pytest.fixture
    def cache_config(self):
        """Create cache configuration."""
        return CacheConfig(
            backend=CacheBackend.FILE,
            file_cache_dir=Path(tempfile.mkdtemp()),
            ttl_prices=300,
            ttl_balances=150,
            max_size_mb=10,
        )

    @pytest.fixture
    def cache_manager(self, cache_config):
        """Create cache manager."""
        return CacheManager(cache_config)

    @pytest.mark.asyncio
    async def test_price_cache_operations(self, cache_manager):
        """Test price cache operations."""
        token_id = "ethereum"
        price_data = {
            "usd": 2000.50,
            "timestamp": 1635724800,
        }

        # Set price
        result = await cache_manager.set_price(token_id, price_data)
        assert result is True

        # Get price
        retrieved = await cache_manager.get_price(token_id)
        assert retrieved == price_data

    @pytest.mark.asyncio
    async def test_balance_cache_operations(self, cache_manager):
        """Test balance cache operations."""
        wallet_address = "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"
        balance_data = {
            "eth": "1.5",
            "usdc": "1000.0",
            "timestamp": 1635724800,
        }

        # Set balance
        result = await cache_manager.set_balance(wallet_address, balance_data)
        assert result is True

        # Get balance (should handle case insensitive)
        retrieved = await cache_manager.get_balance(wallet_address.upper())
        assert retrieved == balance_data

    @pytest.mark.asyncio
    async def test_activity_cache_operations(self, cache_manager):
        """Test wallet activity cache operations."""
        wallet_address = "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"
        activity_data = {
            "last_transaction": 1635724800,
            "transaction_count": 150,
            "is_active": True,
        }

        # Set activity
        result = await cache_manager.set_wallet_activity(wallet_address, activity_data)
        assert result is True

        # Get activity
        retrieved = await cache_manager.get_wallet_activity(wallet_address)
        assert retrieved == activity_data

    @pytest.mark.asyncio
    async def test_token_metadata_operations(self, cache_manager):
        """Test token metadata cache operations."""
        token_address = "0xA0b86a33E6441e94bB0a8d0F7E5F8D69E2C0e5a0"
        metadata = {
            "name": "Test Token",
            "symbol": "TEST",
            "decimals": 18,
        }

        # Set metadata
        result = await cache_manager.set_token_metadata(token_address, metadata)
        assert result is True

        # Get metadata
        retrieved = await cache_manager.get_token_metadata(token_address)
        assert retrieved == metadata

    @pytest.mark.asyncio
    async def test_clear_wallet_data(self, cache_manager):
        """Test clearing wallet-specific data."""
        wallet_address = "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"

        # Set some wallet data
        await cache_manager.set_balance(wallet_address, {"eth": "1.0"})
        await cache_manager.set_wallet_activity(wallet_address, {"active": True})

        # Clear wallet data
        await cache_manager.clear_wallet_data(wallet_address)

        # Check data is cleared
        balance = await cache_manager.get_balance(wallet_address)
        activity = await cache_manager.get_wallet_activity(wallet_address)

        assert balance is None
        assert activity is None

    @pytest.mark.asyncio
    async def test_health_check(self, cache_manager):
        """Test cache health check."""
        # Initialize a cache by accessing it
        cache_manager.get_price_cache()

        health = await cache_manager.health_check()
        assert isinstance(health, dict)
        assert "price_cache" in health

    @pytest.mark.asyncio
    async def test_get_stats(self, cache_manager):
        """Test getting cache statistics."""
        # Initialize a cache by accessing it
        cache_manager.get_price_cache()

        stats = await cache_manager.get_stats()
        assert isinstance(stats, dict)
        assert "price_cache" in stats

    @pytest.mark.asyncio
    async def test_close(self, cache_manager):
        """Test closing cache manager."""
        # Initialize caches
        cache_manager.get_price_cache()
        cache_manager.get_balance_cache()

        # Should not raise any exceptions
        await cache_manager.close()


class MockCacheInterface(CacheInterface):
    """Mock cache implementation for testing."""

    def __init__(self):
        """Initialize mock cache."""
        self.data = {}
        self.closed = False

    async def get(self, key: str):
        """Mock get."""
        return self.data.get(key)

    async def set(self, key: str, value, ttl=None):
        """Mock set."""
        self.data[key] = value
        return True

    async def delete(self, key: str):
        """Mock delete."""
        return self.data.pop(key, None) is not None

    async def exists(self, key: str):
        """Mock exists."""
        return key in self.data

    async def clear(self):
        """Mock clear."""
        self.data.clear()
        return True

    async def get_many(self, keys):
        """Mock get_many."""
        return {key: self.data[key] for key in keys if key in self.data}

    async def set_many(self, mapping, ttl=None):
        """Mock set_many."""
        self.data.update(mapping)
        return True

    async def close(self):
        """Mock close."""
        self.closed = True

    async def health_check(self):
        """Mock health check."""
        return not self.closed

    def get_stats(self):
        """Mock get stats."""
        return {
            "backend": "mock",
            "keys": len(self.data),
            "closed": self.closed,
        }


class TestCacheInterface:
    """Test cache interface compliance."""

    @pytest.mark.asyncio
    async def test_interface_compliance(self):
        """Test that mock implementation follows interface."""
        cache = MockCacheInterface()

        # Test all interface methods
        await cache.set("key1", "value1")
        value = await cache.get("key1")
        assert value == "value1"

        exists = await cache.exists("key1")
        assert exists is True

        await cache.set_many({"key2": "value2", "key3": "value3"})
        many = await cache.get_many(["key1", "key2", "key3"])
        assert len(many) == 3

        deleted = await cache.delete("key1")
        assert deleted is True

        health = await cache.health_check()
        assert health is True

        stats = cache.get_stats()
        assert isinstance(stats, dict)

        await cache.clear()
        await cache.close()

        health_after_close = await cache.health_check()
        assert health_after_close is False
