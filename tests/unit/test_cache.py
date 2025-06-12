"""Tests for caching system."""

import contextlib
import gc
import platform
import shutil
import tempfile
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from wallet_tracker.config import CacheBackend, CacheConfig
from wallet_tracker.utils.cache import CacheFactory, CacheInterface, CacheManager, FileCache


class TestFileCache:
    """Test file cache implementation."""

    @pytest.fixture
    def file_cache(self) -> Generator[FileCache]:
        """Create file cache instance with unique temp dir."""
        # Create a unique temporary directory for this test
        temp_dir = Path(tempfile.mkdtemp(prefix="test_cache_"))

        cache = FileCache(
            cache_dir=temp_dir,
            default_ttl=60,
            max_size_mb=10,
            key_prefix="test:",
        )

        yield cache

        # Cleanup process for Windows
        try:
            # First, close the cache
            import asyncio

            loop = asyncio.get_event_loop()
            loop.run_until_complete(cache.close())

            # Force garbage collection
            del cache
            gc.collect()

            # Small delay to ensure file handles are released
            if platform.system() == "Windows":
                time.sleep(0.1)

            # Now try to remove the directory
            shutil.rmtree(temp_dir, ignore_errors=True)

            # If directory still exists, try again with more aggressive approach
            if temp_dir.exists():
                try:
                    # Windows-specific: try to remove read-only attributes
                    import stat

                    for root, dirs, files in temp_dir.walk():
                        for name in files + dirs:
                            path = root / name
                            with contextlib.suppress(Exception):
                                path.chmod(stat.S_IWRITE)
                        shutil.rmtree(temp_dir, ignore_errors=True)
                except Exception:
                    print("Cache file dir deletion still fails. Ignoring...")
                    pass  # If it still fails, ignore
        except Exception as e:
            # Log the error but don't fail the test
            print(f"Warning: Failed to cleanup temp dir {temp_dir}: {e}")

    @pytest.mark.asyncio
    async def test_file_cache_basic_operations(self, file_cache: FileCache) -> None:
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

    @pytest.mark.asyncio
    async def test_file_cache_batch_operations(self, file_cache: FileCache) -> None:
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

    @pytest.mark.asyncio
    async def test_file_cache_clear(self, file_cache: FileCache) -> None:
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

    @pytest.mark.asyncio
    async def test_file_cache_health_check(self, file_cache: FileCache) -> None:
        """Test health check."""
        health = await file_cache.health_check()
        assert health is True

    def test_file_cache_stats(self, file_cache: FileCache) -> None:
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

    def test_create_file_cache(self) -> None:
        """Test creating file cache."""
        config = CacheConfig(
            backend=CacheBackend.FILE,
            file_cache_dir=Path("test_cache"),
            ttl_prices=300,
            max_size_mb=50,
        )

        cache = CacheFactory.create_cache(config)
        assert isinstance(cache, FileCache)

    @patch("wallet_tracker.utils.cache.redis_cache.redis")
    def test_create_redis_cache(self, mock_redis) -> None:
        """Test creating Redis cache."""
        config = CacheConfig(
            backend=CacheBackend.REDIS,
            redis_url="redis://localhost:6379/0",
            ttl_prices=300,
        )

        cache = CacheFactory.create_cache(config)
        # This will be a RedisCache, but we can't easily test without Redis
        assert cache is not None

    def test_unsupported_backend(self) -> None:
        """Test error for unsupported backend."""
        config = CacheConfig()
        config.backend = "unsupported"  # type: ignore[assignment]

        with pytest.raises(ValueError, match="Unsupported cache backend"):
            CacheFactory.create_cache(config)


class TestCacheManager:
    """Test cache manager."""

    @pytest.fixture
    def cache_config(self) -> CacheConfig:
        """Create cache configuration."""
        return CacheConfig(
            backend=CacheBackend.FILE,
            file_cache_dir=Path(tempfile.mkdtemp()),
            ttl_prices=300,
            ttl_balances=150,
            max_size_mb=10,
        )

    @pytest.fixture
    def cache_manager(self, cache_config: CacheConfig) -> CacheManager:
        """Create cache manager."""
        return CacheManager(cache_config)

    @pytest.mark.asyncio
    async def test_price_cache_operations(self, cache_manager: CacheManager) -> None:
        """Test price cache operations."""
        token_id = "ethereum"  # noqa: S105
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
    async def test_balance_cache_operations(self, cache_manager: CacheManager) -> None:
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
    async def test_activity_cache_operations(self, cache_manager: CacheManager) -> None:
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
    async def test_token_metadata_operations(self, cache_manager: CacheManager) -> None:
        """Test token metadata cache operations."""
        token_address = "0xA0b86a33E6441e94bB0a8d0F7E5F8D69E2C0e5a0"  # noqa: S105
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
    async def test_clear_wallet_data(self, cache_manager: CacheManager) -> None:
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
    async def test_health_check(self, cache_manager: CacheManager) -> None:
        """Test cache health check."""
        # Initialize a cache by accessing it
        cache_manager.get_price_cache()

        health = await cache_manager.health_check()
        assert isinstance(health, dict)
        assert "price_cache" in health

    @pytest.mark.asyncio
    async def test_get_stats(self, cache_manager: CacheManager) -> None:
        """Test getting cache statistics."""
        # Initialize a cache by accessing it
        cache_manager.get_price_cache()

        stats = await cache_manager.get_stats()
        assert isinstance(stats, dict)
        assert "price_cache" in stats

    @pytest.mark.asyncio
    async def test_close(self, cache_manager: CacheManager) -> None:
        """Test closing cache manager."""
        # Initialize caches
        cache_manager.get_price_cache()
        cache_manager.get_balance_cache()

        # Should not raise any exceptions
        await cache_manager.close()


class MockCacheInterface(CacheInterface):
    """Mock cache implementation for testing."""

    def __init__(self) -> None:
        """Initialize mock cache."""
        self.data: dict[str, Any] = {}
        self.closed = False

    async def get(self, key: str) -> Any:
        """Mock get."""
        return self.data.get(key)

    async def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Mock set."""
        self.data[key] = value
        return True

    async def delete(self, key: str) -> bool:
        """Mock delete."""
        return self.data.pop(key, None) is not None

    async def exists(self, key: str) -> bool:
        """Mock exists."""
        return key in self.data

    async def clear(self) -> bool:
        """Mock clear."""
        self.data.clear()
        return True

    async def get_many(self, keys: list[str]) -> dict[str, str | None]:
        """Mock get_many."""
        return {key: self.data[key] for key in keys if key in self.data}

    async def set_many(self, mapping: dict[str, Any], ttl: int | None = None) -> bool:
        """Mock set_many."""
        self.data.update(mapping)
        return True

    async def close(self) -> None:
        """Mock close."""
        self.closed = True

    async def health_check(self) -> bool:
        """Mock health check."""
        return not self.closed

    def get_stats(self) -> dict[str, Any]:
        """Mock get stats."""
        return {
            "backend": "mock",
            "keys": len(self.data),
            "closed": self.closed,
        }


class TestCacheInterface:
    """Test cache interface compliance."""

    @pytest.mark.asyncio
    async def test_interface_compliance(self) -> None:
        """Test that mock implementation follows interface."""
        cache: CacheInterface = MockCacheInterface()

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
