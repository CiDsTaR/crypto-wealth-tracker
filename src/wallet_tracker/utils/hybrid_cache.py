"""Hybrid cache implementation with Redis primary and file fallback."""

import logging
from typing import Any

from .cache_interface import CacheConnectionError, CacheError, CacheInterface
from .file_cache import FileCache
from .redis_cache import RedisCache

logger = logging.getLogger(__name__)


class HybridCache(CacheInterface):
    """Hybrid cache with Redis primary and file cache fallback."""

    def __init__(
        self,
        redis_cache: RedisCache,
        file_cache: FileCache,
        fallback_on_error: bool = True,
    ):
        """Initialize hybrid cache.

        Args:
            redis_cache: Redis cache instance
            file_cache: File cache instance
            fallback_on_error: Whether to fallback to file cache on Redis errors
        """
        self.redis_cache = redis_cache
        self.file_cache = file_cache
        self.fallback_on_error = fallback_on_error

        self._redis_healthy = True
        self._stats = {
            "redis_operations": 0,
            "file_operations": 0,
            "fallbacks": 0,
            "errors": 0,
        }

    async def _try_redis_operation(self, operation_name: str, operation_func, *args, **kwargs):
        """Try Redis operation with fallback to file cache."""
        try:
            # Try Redis first
            result = await operation_func(*args, **kwargs)
            self._redis_healthy = True
            self._stats["redis_operations"] += 1
            return result, "redis"

        except (CacheConnectionError, CacheError) as e:
            self._redis_healthy = False
            self._stats["errors"] += 1

            if self.fallback_on_error:
                logger.warning(f"Redis {operation_name} failed, falling back to file cache: {e}")
                self._stats["fallbacks"] += 1

                # Try file cache
                try:
                    if operation_name == "get":
                        result = await self.file_cache.get(*args, **kwargs)
                    elif operation_name == "set":
                        result = await self.file_cache.set(*args, **kwargs)
                    elif operation_name == "delete":
                        result = await self.file_cache.delete(*args, **kwargs)
                    elif operation_name == "exists":
                        result = await self.file_cache.exists(*args, **kwargs)
                    elif operation_name == "get_many":
                        result = await self.file_cache.get_many(*args, **kwargs)
                    elif operation_name == "set_many":
                        result = await self.file_cache.set_many(*args, **kwargs)
                    else:
                        raise ValueError(f"Unknown operation: {operation_name}")

                    self._stats["file_operations"] += 1
                    return result, "file"

                except Exception as file_error:
                    logger.error(f"File cache {operation_name} also failed: {file_error}")
                    self._stats["errors"] += 1
                    raise CacheError(f"Both Redis and file cache failed for {operation_name}") from file_error
            else:
                raise

    async def get(self, key: str) -> Any | None:
        """Get value with Redis primary, file fallback."""
        result, backend = await self._try_redis_operation("get", self.redis_cache.get, key)

        # If Redis is working but key not found, check file cache too
        if result is None and backend == "redis" and self._redis_healthy:
            try:
                file_result = await self.file_cache.get(key)
                if file_result is not None:
                    # Found in file cache, promote to Redis
                    await self.redis_cache.set(key, file_result)
                    logger.debug(f"Promoted cache key '{key}' from file to Redis")
                    return file_result
            except Exception as e:
                logger.debug(f"File cache promotion check failed for key '{key}': {e}")

        return result

    async def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Set value in both caches when possible."""
        primary_result, backend = await self._try_redis_operation("set", self.redis_cache.set, key, value, ttl)

        # If Redis worked, also try to set in file cache for redundancy
        if backend == "redis" and primary_result:
            try:
                await self.file_cache.set(key, value, ttl)
                logger.debug(f"Set cache key '{key}' in both Redis and file cache")
            except Exception as e:
                logger.debug(f"Failed to set key '{key}' in file cache: {e}")

        return primary_result

    async def delete(self, key: str) -> bool:
        """Delete key from both caches."""
        primary_result, backend = await self._try_redis_operation("delete", self.redis_cache.delete, key)

        # Always try to delete from file cache too
        try:
            file_result = await self.file_cache.delete(key)
            logger.debug(f"Deleted cache key '{key}' from both caches")
        except Exception as e:
            logger.debug(f"Failed to delete key '{key}' from file cache: {e}")

        return primary_result

    async def exists(self, key: str) -> bool:
        """Check if key exists in either cache."""
        result, backend = await self._try_redis_operation("exists", self.redis_cache.exists, key)

        # If not found in Redis, check file cache
        if not result and backend == "redis":
            try:
                file_result = await self.file_cache.exists(key)
                if file_result:
                    logger.debug(f"Found key '{key}' in file cache but not Redis")
                return file_result
            except Exception as e:
                logger.debug(f"File cache exists check failed for key '{key}': {e}")

        return result

    async def clear(self) -> bool:
        """Clear both caches."""
        redis_success = False
        file_success = False

        # Try Redis first
        try:
            redis_success = await self.redis_cache.clear()
            self._stats["redis_operations"] += 1
        except Exception as e:
            logger.error(f"Failed to clear Redis cache: {e}")
            self._stats["errors"] += 1

        # Try file cache
        try:
            file_success = await self.file_cache.clear()
            self._stats["file_operations"] += 1
        except Exception as e:
            logger.error(f"Failed to clear file cache: {e}")
            self._stats["errors"] += 1

        return redis_success or file_success

    async def get_many(self, keys: list[str]) -> dict[str, Any]:
        """Get multiple values with Redis primary, file fallback."""
        result, backend = await self._try_redis_operation("get_many", self.redis_cache.get_many, keys)

        # If Redis is working but some keys missing, check file cache
        if backend == "redis" and len(result) < len(keys):
            missing_keys = [key for key in keys if key not in result]
            try:
                file_results = await self.file_cache.get_many(missing_keys)
                if file_results:
                    # Promote found keys to Redis
                    await self.redis_cache.set_many(file_results)
                    result.update(file_results)
                    logger.debug(f"Promoted {len(file_results)} cache keys from file to Redis")
            except Exception as e:
                logger.debug(f"File cache get_many failed: {e}")

        return result

    async def set_many(self, mapping: dict[str, Any], ttl: int | None = None) -> bool:
        """Set multiple values in both caches when possible."""
        primary_result, backend = await self._try_redis_operation("set_many", self.redis_cache.set_many, mapping, ttl)

        # If Redis worked, also set in file cache
        if backend == "redis" and primary_result:
            try:
                await self.file_cache.set_many(mapping, ttl)
                logger.debug(f"Set {len(mapping)} cache keys in both Redis and file cache")
            except Exception as e:
                logger.debug(f"Failed to set keys in file cache: {e}")

        return primary_result

    async def close(self) -> None:
        """Close both cache connections."""
        # Close Redis
        try:
            await self.redis_cache.close()
        except Exception as e:
            logger.error(f"Error closing Redis cache: {e}")

        # Close file cache
        try:
            await self.file_cache.close()
        except Exception as e:
            logger.error(f"Error closing file cache: {e}")

    async def health_check(self) -> bool:
        """Check health of both caches."""
        redis_healthy = await self.redis_cache.health_check()
        file_healthy = await self.file_cache.health_check()

        self._redis_healthy = redis_healthy

        # At least one cache should be healthy
        return redis_healthy or file_healthy

    def get_stats(self) -> dict[str, Any]:
        """Get combined cache statistics."""
        redis_stats = self.redis_cache.get_stats()
        file_stats = self.file_cache.get_stats()

        return {
            "backend": "hybrid",
            "redis_healthy": self._redis_healthy,
            "redis_stats": redis_stats,
            "file_stats": file_stats,
            "hybrid_stats": {
                "redis_operations": self._stats["redis_operations"],
                "file_operations": self._stats["file_operations"],
                "fallbacks": self._stats["fallbacks"],
                "errors": self._stats["errors"],
                "fallback_rate_percent": round(
                    (self._stats["fallbacks"] / max(1, self._stats["redis_operations"] + self._stats["fallbacks"]))
                    * 100,
                    2,
                ),
            },
        }
