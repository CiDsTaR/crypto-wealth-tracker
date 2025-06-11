"""File-based cache implementation using diskcache."""

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

import diskcache

from .cache_interface import CacheInterface, CacheOperationError

logger = logging.getLogger(__name__)


class FileCache(CacheInterface):
    """File-based cache implementation using diskcache."""

    def __init__(
        self,
        cache_dir: Path,
        default_ttl: int = 3600,
        max_size_mb: int = 500,
        key_prefix: str = "wallet_tracker:",
    ):
        """Initialize file cache.

        Args:
            cache_dir: Directory for cache files
            default_ttl: Default TTL in seconds
            max_size_mb: Maximum cache size in MB
            key_prefix: Prefix for all cache keys
        """
        self.cache_dir = Path(cache_dir)
        self.default_ttl = default_ttl
        self.max_size_mb = max_size_mb
        self.key_prefix = key_prefix

        self._cache: diskcache.Cache | None = None
        self._stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0,
            "errors": 0,
        }

    def _get_cache(self) -> diskcache.Cache:
        """Get cache instance, creating if needed."""
        if self._cache is None:
            try:
                # Create cache directory if it doesn't exist
                self.cache_dir.mkdir(parents=True, exist_ok=True)

                # Create cache with size limit
                max_size_bytes = self.max_size_mb * 1024 * 1024

                self._cache = diskcache.Cache(
                    directory=str(self.cache_dir),
                    size_limit=max_size_bytes,
                    eviction_policy="least-recently-used",
                    cull_limit=10,  # Remove 10% when culling
                )

                logger.info(f"File cache initialized at {self.cache_dir}")

            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"Failed to initialize file cache: {e}")
                raise CacheOperationError(f"File cache initialization failed: {e}") from e

        return self._cache

    def _make_key(self, key: str) -> str:
        """Create prefixed cache key."""
        return f"{self.key_prefix}{key}"

    async def get(self, key: str) -> Any | None:
        """Get value from file cache."""
        try:
            cache = self._get_cache()
            cache_key = self._make_key(key)

            # Run in thread pool to avoid blocking
            def _get():
                return cache.get(cache_key)

            value = await asyncio.get_event_loop().run_in_executor(None, _get)

            if value is None:
                self._stats["misses"] += 1
                return None

            self._stats["hits"] += 1
            return value

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"File cache get error for key '{key}': {e}")
            raise CacheOperationError(f"File cache get failed: {e}") from e

    async def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Set value in file cache."""
        try:
            cache = self._get_cache()
            cache_key = self._make_key(key)
            ttl_seconds = ttl or self.default_ttl

            # Calculate expiration time
            expire_time = time.time() + ttl_seconds

            # Run in thread pool to avoid blocking
            def _set():
                return cache.set(cache_key, value, expire=expire_time)

            result = await asyncio.get_event_loop().run_in_executor(None, _set)

            if result:
                self._stats["sets"] += 1

            return bool(result)

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"File cache set error for key '{key}': {e}")
            raise CacheOperationError(f"File cache set failed: {e}") from e

    async def delete(self, key: str) -> bool:
        """Delete key from file cache."""
        try:
            cache = self._get_cache()
            cache_key = self._make_key(key)

            # Run in thread pool to avoid blocking
            def _delete():
                return cache.delete(cache_key)

            result = await asyncio.get_event_loop().run_in_executor(None, _delete)

            if result:
                self._stats["deletes"] += 1

            return bool(result)

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"File cache delete error for key '{key}': {e}")
            raise CacheOperationError(f"File cache delete failed: {e}") from e

    async def exists(self, key: str) -> bool:
        """Check if key exists in file cache."""
        try:
            cache = self._get_cache()
            cache_key = self._make_key(key)

            # Run in thread pool to avoid blocking
            def _exists():
                return cache_key in cache

            result = await asyncio.get_event_loop().run_in_executor(None, _exists)
            return bool(result)

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"File cache exists error for key '{key}': {e}")
            return False

    async def clear(self) -> bool:
        """Clear all cache entries with our prefix."""
        try:
            cache = self._get_cache()

            # Run in thread pool to avoid blocking
            def _clear():
                keys_to_delete = []
                for key in cache:
                    if isinstance(key, str) and key.startswith(self.key_prefix):
                        keys_to_delete.append(key)

                deleted_count = 0
                for key in keys_to_delete:
                    if cache.delete(key):
                        deleted_count += 1

                return deleted_count

            deleted_count = await asyncio.get_event_loop().run_in_executor(None, _clear)

            self._stats["deletes"] += deleted_count
            logger.info(f"Cleared {deleted_count} file cache entries")

            return True

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"File cache clear error: {e}")
            raise CacheOperationError(f"File cache clear failed: {e}") from e

    async def get_many(self, keys: list[str]) -> dict[str, Any]:
        """Get multiple values from file cache."""
        if not keys:
            return {}

        try:
            cache = self._get_cache()
            cache_keys = [self._make_key(key) for key in keys]

            # Run in thread pool to avoid blocking
            def _get_many():
                result = {}
                for i, cache_key in enumerate(cache_keys):
                    value = cache.get(cache_key)
                    if value is not None:
                        result[keys[i]] = value
                return result

            result = await asyncio.get_event_loop().run_in_executor(None, _get_many)

            # Update stats
            self._stats["hits"] += len(result)
            self._stats["misses"] += len(keys) - len(result)

            return result

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"File cache get_many error: {e}")
            raise CacheOperationError(f"File cache get_many failed: {e}") from e

    async def set_many(self, mapping: dict[str, Any], ttl: int | None = None) -> bool:
        """Set multiple values in file cache."""
        if not mapping:
            return True

        try:
            cache = self._get_cache()
            ttl_seconds = ttl or self.default_ttl
            expire_time = time.time() + ttl_seconds

            # Run in thread pool to avoid blocking
            def _set_many():
                successful = 0
                for key, value in mapping.items():
                    cache_key = self._make_key(key)
                    if cache.set(cache_key, value, expire=expire_time):
                        successful += 1
                return successful

            successful = await asyncio.get_event_loop().run_in_executor(None, _set_many)

            self._stats["sets"] += successful

            return successful == len(mapping)

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"File cache set_many error: {e}")
            raise CacheOperationError(f"File cache set_many failed: {e}") from e

    async def close(self) -> None:
        """Close file cache."""
        if self._cache:
            try:
                # Run in thread pool to avoid blocking
                def _close():
                    self._cache.close()

                await asyncio.get_event_loop().run_in_executor(None, _close)
                logger.info("File cache closed")
            except Exception as e:
                logger.error(f"Error closing file cache: {e}")
            finally:
                self._cache = None

    async def health_check(self) -> bool:
        """Check file cache health."""
        try:
            cache = self._get_cache()

            # Test basic operations
            def _health_check():
                test_key = f"{self.key_prefix}__health_check__"
                test_value = {"timestamp": time.time()}

                # Try set and get
                if not cache.set(test_key, test_value, expire=time.time() + 10):
                    return False

                retrieved = cache.get(test_key)
                if retrieved != test_value:
                    return False

                # Clean up
                cache.delete(test_key)
                return True

            result = await asyncio.get_event_loop().run_in_executor(None, _health_check)
            return result

        except Exception as e:
            logger.error(f"File cache health check failed: {e}")
            return False

    def get_stats(self) -> dict[str, Any]:
        """Get file cache statistics."""
        total_operations = self._stats["hits"] + self._stats["misses"]
        hit_rate = (self._stats["hits"] / total_operations * 100) if total_operations > 0 else 0

        stats = {
            "backend": "file",
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "sets": self._stats["sets"],
            "deletes": self._stats["deletes"],
            "errors": self._stats["errors"],
            "hit_rate_percent": round(hit_rate, 2),
            "total_operations": total_operations,
            "cache_dir": str(self.cache_dir),
            "max_size_mb": self.max_size_mb,
        }

        # Add cache-specific stats if available
        if self._cache:
            try:
                cache_stats = self._cache.stats()
                stats.update(
                    {
                        "current_size_bytes": cache_stats[0],
                        "current_size_mb": round(cache_stats[0] / (1024 * 1024), 2),
                        "key_count": cache_stats[1],
                    }
                )
            except Exception:
                pass  # Stats not available

        return stats
