"""Tests for hybrid cache implementation."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from wallet_tracker.utils import FileCache, HybridCache, RedisCache
from wallet_tracker.utils.cache.cache_interface import CacheConnectionError, CacheError


class TestHybridCache:
    """Test hybrid cache implementation."""

    @pytest.fixture
    def mock_redis_cache(self):
        """Create mock Redis cache."""
        mock_cache = AsyncMock(spec=RedisCache)
        mock_cache.get.return_value = None
        mock_cache.set.return_value = True
        mock_cache.delete.return_value = True
        mock_cache.exists.return_value = False
        mock_cache.clear.return_value = True
        mock_cache.get_many.return_value = {}
        mock_cache.set_many.return_value = True
        mock_cache.close = AsyncMock()
        mock_cache.health_check.return_value = True
        mock_cache.get_stats.return_value = {"backend": "redis", "hits": 0}
        return mock_cache

    @pytest.fixture
    def mock_file_cache(self):
        """Create mock file cache."""
        mock_cache = AsyncMock(spec=FileCache)
        mock_cache.get.return_value = None
        mock_cache.set.return_value = True
        mock_cache.delete.return_value = True
        mock_cache.exists.return_value = False
        mock_cache.clear.return_value = True
        mock_cache.get_many.return_value = {}
        mock_cache.set_many.return_value = True
        mock_cache.close = AsyncMock()
        mock_cache.health_check.return_value = True
        mock_cache.get_stats.return_value = {"backend": "file", "hits": 0}
        return mock_cache

    @pytest.fixture
    def hybrid_cache(self, mock_redis_cache, mock_file_cache):
        """Create hybrid cache instance."""
        return HybridCache(redis_cache=mock_redis_cache, file_cache=mock_file_cache, fallback_on_error=True)

    @pytest.mark.asyncio
    async def test_get_redis_success(self, hybrid_cache, mock_redis_cache):
        """Test successful get from Redis."""
        test_data = {"key": "value"}
        mock_redis_cache.get.return_value = test_data

        result = await hybrid_cache.get("test_key")

        assert result == test_data
        mock_redis_cache.get.assert_called_once_with("test_key")

    @pytest.mark.asyncio
    async def test_get_redis_fallback_to_file(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test fallback to file cache when Redis fails."""
        test_data = {"key": "value"}
        mock_redis_cache.get.side_effect = CacheConnectionError("Redis down")
        mock_file_cache.get.return_value = test_data

        result = await hybrid_cache.get("test_key")

        assert result == test_data
        mock_redis_cache.get.assert_called_once_with("test_key")
        mock_file_cache.get.assert_called_once_with("test_key")

    @pytest.mark.asyncio
    async def test_get_redis_miss_file_hit_promotion(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test promotion from file cache to Redis when Redis misses."""
        test_data = {"key": "value"}
        mock_redis_cache.get.return_value = None  # Redis miss
        mock_file_cache.get.return_value = test_data  # File hit

        result = await hybrid_cache.get("test_key")

        assert result == test_data
        # Should try to promote to Redis
        mock_redis_cache.set.assert_called_once_with("test_key", test_data)

    @pytest.mark.asyncio
    async def test_set_both_caches(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test setting in both caches when Redis succeeds."""
        test_data = {"key": "value"}
        mock_redis_cache.set.return_value = True

        result = await hybrid_cache.set("test_key", test_data, ttl=300)

        assert result is True
        mock_redis_cache.set.assert_called_once_with("test_key", test_data, 300)
        mock_file_cache.set.assert_called_once_with("test_key", test_data, 300)

    @pytest.mark.asyncio
    async def test_set_redis_failure_fallback(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test fallback to file cache when Redis set fails."""
        test_data = {"key": "value"}
        mock_redis_cache.set.side_effect = CacheConnectionError("Redis down")
        mock_file_cache.set.return_value = True

        result = await hybrid_cache.set("test_key", test_data)

        assert result is True
        mock_file_cache.set.assert_called_once_with("test_key", test_data, None)

    @pytest.mark.asyncio
    async def test_delete_both_caches(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test deleting from both caches."""
        mock_redis_cache.delete.return_value = True
        mock_file_cache.delete.return_value = True

        result = await hybrid_cache.delete("test_key")

        assert result is True
        mock_redis_cache.delete.assert_called_once_with("test_key")
        mock_file_cache.delete.assert_called_once_with("test_key")

    @pytest.mark.asyncio
    async def test_exists_redis_then_file(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test exists check in Redis then file cache."""
        mock_redis_cache.exists.return_value = False
        mock_file_cache.exists.return_value = True

        result = await hybrid_cache.exists("test_key")

        assert result is True
        mock_redis_cache.exists.assert_called_once_with("test_key")
        mock_file_cache.exists.assert_called_once_with("test_key")

    @pytest.mark.asyncio
    async def test_clear_both_caches(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test clearing both caches."""
        mock_redis_cache.clear.return_value = True
        mock_file_cache.clear.return_value = True

        result = await hybrid_cache.clear()

        assert result is True
        mock_redis_cache.clear.assert_called_once()
        mock_file_cache.clear.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_many_with_promotion(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test get_many with promotion from file to Redis."""
        keys = ["key1", "key2", "key3"]
        redis_data = {"key1": "value1"}
        file_data = {"key2": "value2"}

        mock_redis_cache.get_many.return_value = redis_data
        mock_file_cache.get_many.return_value = file_data

        result = await hybrid_cache.get_many(keys)

        assert result == {"key1": "value1", "key2": "value2"}
        # Should promote file data to Redis
        mock_redis_cache.set_many.assert_called_once_with(file_data)

    @pytest.mark.asyncio
    async def test_set_many_both_caches(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test set_many in both caches."""
        data = {"key1": "value1", "key2": "value2"}
        mock_redis_cache.set_many.return_value = True

        result = await hybrid_cache.set_many(data, ttl=300)

        assert result is True
        mock_redis_cache.set_many.assert_called_once_with(data, 300)
        mock_file_cache.set_many.assert_called_once_with(data, 300)

    @pytest.mark.asyncio
    async def test_close_both_caches(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test closing both caches."""
        await hybrid_cache.close()

        mock_redis_cache.close.assert_called_once()
        mock_file_cache.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_check_both_caches(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test health check of both caches."""
        mock_redis_cache.health_check.return_value = True
        mock_file_cache.health_check.return_value = True

        result = await hybrid_cache.health_check()

        assert result is True

    @pytest.mark.asyncio
    async def test_health_check_one_healthy(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test health check when only one cache is healthy."""
        mock_redis_cache.health_check.return_value = False
        mock_file_cache.health_check.return_value = True

        result = await hybrid_cache.health_check()

        assert result is True  # At least one cache is healthy

    @pytest.mark.asyncio
    async def test_health_check_both_unhealthy(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test health check when both caches are unhealthy."""
        mock_redis_cache.health_check.return_value = False
        mock_file_cache.health_check.return_value = False

        result = await hybrid_cache.health_check()

        assert result is False

    def test_get_stats(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test getting combined statistics."""
        redis_stats = {"backend": "redis", "hits": 10}
        file_stats = {"backend": "file", "hits": 5}

        mock_redis_cache.get_stats.return_value = redis_stats
        mock_file_cache.get_stats.return_value = file_stats

        stats = hybrid_cache.get_stats()

        assert stats["backend"] == "hybrid"
        assert stats["redis_stats"] == redis_stats
        assert stats["file_stats"] == file_stats
        assert "hybrid_stats" in stats
        assert "redis_operations" in stats["hybrid_stats"]
        assert "file_operations" in stats["hybrid_stats"]
        assert "fallbacks" in stats["hybrid_stats"]

    @pytest.mark.asyncio
    async def test_fallback_disabled(self, mock_redis_cache, mock_file_cache):
        """Test behavior when fallback is disabled."""
        hybrid_cache = HybridCache(redis_cache=mock_redis_cache, file_cache=mock_file_cache, fallback_on_error=False)

        mock_redis_cache.get.side_effect = CacheConnectionError("Redis down")

        with pytest.raises(CacheConnectionError):
            await hybrid_cache.get("test_key")

    @pytest.mark.asyncio
    async def test_both_caches_fail(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test behavior when both caches fail."""
        mock_redis_cache.get.side_effect = CacheConnectionError("Redis down")
        mock_file_cache.get.side_effect = CacheError("File cache error")

        with pytest.raises(CacheError, match="Both Redis and file cache failed"):
            await hybrid_cache.get("test_key")

    @pytest.mark.asyncio
    async def test_stats_tracking(self, hybrid_cache, mock_redis_cache, mock_file_cache):
        """Test that statistics are properly tracked."""
        # Successful Redis operation
        mock_redis_cache.get.return_value = "value"
        await hybrid_cache.get("key1")

        # Redis failure, file fallback
        mock_redis_cache.set.side_effect = CacheConnectionError("Redis down")
        mock_file_cache.set.return_value = True
        await hybrid_cache.set("key2", "value")

        stats = hybrid_cache.get_stats()
        hybrid_stats = stats["hybrid_stats"]

        assert hybrid_stats["redis_operations"] == 1
        assert hybrid_stats["file_operations"] == 1
        assert hybrid_stats["fallbacks"] == 1
        assert hybrid_stats["errors"] == 1


class TestHybridCacheIntegration:
    """Integration tests with real cache instances."""

    @pytest.fixture
    def temp_cache_dir(self):
        """Create temporary cache directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.mark.asyncio
    async def test_real_file_cache_integration(self, temp_cache_dir):
        """Test hybrid cache with real file cache and mock Redis."""
        # Real file cache
        file_cache = FileCache(cache_dir=temp_cache_dir, default_ttl=60, max_size_mb=1, key_prefix="test:")

        # Mock Redis cache that always fails
        mock_redis = AsyncMock(spec=RedisCache)
        mock_redis.get.side_effect = CacheConnectionError("Redis unavailable")
        mock_redis.set.side_effect = CacheConnectionError("Redis unavailable")
        mock_redis.health_check.return_value = False
        mock_redis.get_stats.return_value = {"backend": "redis", "connected": False}
        mock_redis.close = AsyncMock()

        hybrid_cache = HybridCache(redis_cache=mock_redis, file_cache=file_cache, fallback_on_error=True)

        try:
            # Test fallback to file cache
            test_data = {"test": "data", "number": 42}

            # Set should fallback to file cache
            result = await hybrid_cache.set("test_key", test_data)
            assert result is True

            # Get should fallback to file cache
            retrieved = await hybrid_cache.get("test_key")
            assert retrieved == test_data

            # Health check should return True (file cache is healthy)
            health = await hybrid_cache.health_check()
            assert health is True

            # Stats should show fallback operations
            stats = hybrid_cache.get_stats()
            assert stats["hybrid_stats"]["file_operations"] > 0
            assert stats["hybrid_stats"]["fallbacks"] > 0

        finally:
            await hybrid_cache.close()
