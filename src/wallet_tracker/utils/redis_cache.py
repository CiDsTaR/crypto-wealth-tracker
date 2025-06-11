"""Redis cache implementation."""

import json
import logging
from typing import Any

import redis.asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError

from .cache_interface import CacheConnectionError, CacheError, CacheInterface, CacheOperationError

logger = logging.getLogger(__name__)


class RedisCache(CacheInterface):
    """Redis-based cache implementation."""

    def __init__(
        self,
        redis_url: str,
        password: str | None = None,
        default_ttl: int = 3600,
        key_prefix: str = "wallet_tracker:",
        max_connections: int = 20,
    ):
        """Initialize Redis cache.

        Args:
            redis_url: Redis connection URL
            password: Redis password (optional)
            default_ttl: Default TTL in seconds
            key_prefix: Prefix for all cache keys
            max_connections: Maximum connection pool size
        """
        self.redis_url = redis_url
        self.password = password
        self.default_ttl = default_ttl
        self.key_prefix = key_prefix
        self.max_connections = max_connections

        self._client: redis.Redis | None = None
        self._connection_pool: redis.ConnectionPool | None = None
        self._stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0,
            "errors": 0,
        }

    async def _get_client(self) -> redis.Redis:
        """Get Redis client, creating connection if needed."""
        if self._client is None:
            try:
                # Create connection pool
                self._connection_pool = redis.ConnectionPool.from_url(
                    self.redis_url,
                    password=self.password,
                    max_connections=self.max_connections,
                    decode_responses=True,
                    retry_on_error=[RedisConnectionError],
                    retry_on_timeout=True,
                )

                # Create client
                self._client = redis.Redis(connection_pool=self._connection_pool)

                # Test connection
                await self._client.ping()
                logger.info("Connected to Redis successfully")

            except (RedisError, Exception) as e:
                self._stats["errors"] += 1
                logger.error(f"Failed to connect to Redis: {e}")
                raise CacheConnectionError(f"Redis connection failed: {e}") from e

        return self._client

    def _make_key(self, key: str) -> str:
        """Create prefixed cache key."""
        return f"{self.key_prefix}{key}"

    def _serialize(self, value: Any) -> str:
        """Serialize value for storage."""
        try:
            return json.dumps(value, default=str)
        except (TypeError, ValueError) as e:
            raise CacheOperationError(f"Failed to serialize value: {e}") from e

    def _deserialize(self, value: str) -> Any:
        """Deserialize value from storage."""
        try:
            return json.loads(value)
        except (TypeError, ValueError) as e:
            raise CacheOperationError(f"Failed to deserialize value: {e}") from e

    async def get(self, key: str) -> Any | None:
        """Get value from Redis cache."""
        try:
            client = await self._get_client()
            cache_key = self._make_key(key)

            value = await client.get(cache_key)
            if value is None:
                self._stats["misses"] += 1
                return None

            self._stats["hits"] += 1
            return self._deserialize(value)

        except CacheError:
            raise
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Redis get error for key '{key}': {e}")
            raise CacheOperationError(f"Redis get failed: {e}") from e

    async def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Set value in Redis cache."""
        try:
            client = await self._get_client()
            cache_key = self._make_key(key)
            serialized_value = self._serialize(value)

            ttl_seconds = ttl or self.default_ttl

            result = await client.setex(cache_key, ttl_seconds, serialized_value)

            if result:
                self._stats["sets"] += 1

            return bool(result)

        except CacheError:
            raise
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Redis set error for key '{key}': {e}")
            raise CacheOperationError(f"Redis set failed: {e}") from e

    async def delete(self, key: str) -> bool:
        """Delete key from Redis cache."""
        try:
            client = await self._get_client()
            cache_key = self._make_key(key)

            result = await client.delete(cache_key)

            if result:
                self._stats["deletes"] += 1

            return bool(result)

        except CacheError:
            raise
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Redis delete error for key '{key}': {e}")
            raise CacheOperationError(f"Redis delete failed: {e}") from e

    async def exists(self, key: str) -> bool:
        """Check if key exists in Redis cache."""
        try:
            client = await self._get_client()
            cache_key = self._make_key(key)

            result = await client.exists(cache_key)
            return bool(result)

        except CacheError:
            raise
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Redis exists error for key '{key}': {e}")
            return False

    async def clear(self) -> bool:
        """Clear all cache entries with our prefix."""
        try:
            client = await self._get_client()

            # Find all keys with our prefix
            pattern = f"{self.key_prefix}*"
            keys = []

            async for key in client.scan_iter(match=pattern):
                keys.append(key)

            if keys:
                deleted = await client.delete(*keys)
                self._stats["deletes"] += deleted
                logger.info(f"Cleared {deleted} cache entries")

            return True

        except CacheError:
            raise
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Redis clear error: {e}")
            raise CacheOperationError(f"Redis clear failed: {e}") from e

    async def get_many(self, keys: list[str]) -> dict[str, Any]:
        """Get multiple values from Redis cache."""
        if not keys:
            return {}

        try:
            client = await self._get_client()
            cache_keys = [self._make_key(key) for key in keys]

            values = await client.mget(cache_keys)

            result = {}
            for i, (original_key, value) in enumerate(zip(keys, values, strict=False)):
                if value is not None:
                    try:
                        result[original_key] = self._deserialize(value)
                        self._stats["hits"] += 1
                    except CacheOperationError:
                        # Skip invalid values
                        self._stats["errors"] += 1
                        continue
                else:
                    self._stats["misses"] += 1

            return result

        except CacheError:
            raise
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Redis get_many error: {e}")
            raise CacheOperationError(f"Redis get_many failed: {e}") from e

    async def set_many(self, mapping: dict[str, Any], ttl: int | None = None) -> bool:
        """Set multiple values in Redis cache."""
        if not mapping:
            return True

        try:
            client = await self._get_client()
            ttl_seconds = ttl or self.default_ttl

            # Use pipeline for efficiency
            pipe = client.pipeline()

            for key, value in mapping.items():
                cache_key = self._make_key(key)
                serialized_value = self._serialize(value)
                pipe.setex(cache_key, ttl_seconds, serialized_value)

            results = await pipe.execute()

            # Count successful sets
            successful = sum(1 for result in results if result)
            self._stats["sets"] += successful

            return successful == len(mapping)

        except CacheError:
            raise
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Redis set_many error: {e}")
            raise CacheOperationError(f"Redis set_many failed: {e}") from e

    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            try:
                await self._client.aclose()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")
            finally:
                self._client = None
                self._connection_pool = None

    async def health_check(self) -> bool:
        """Check Redis health."""
        try:
            client = await self._get_client()
            await client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False

    def get_stats(self) -> dict[str, Any]:
        """Get Redis cache statistics."""
        total_operations = self._stats["hits"] + self._stats["misses"]
        hit_rate = (self._stats["hits"] / total_operations * 100) if total_operations > 0 else 0

        return {
            "backend": "redis",
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "sets": self._stats["sets"],
            "deletes": self._stats["deletes"],
            "errors": self._stats["errors"],
            "hit_rate_percent": round(hit_rate, 2),
            "total_operations": total_operations,
            "connected": self._client is not None,
        }
