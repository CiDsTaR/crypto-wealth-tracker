"""Cache factory for creating appropriate cache instances."""

import logging

from ..config import CacheBackend, CacheConfig
from .cache_interface import CacheInterface
from .file_cache import FileCache
from .hybrid_cache import HybridCache
from .redis_cache import RedisCache

logger = logging.getLogger(__name__)


class CacheFactory:
    """Factory for creating cache instances based on configuration."""

    @staticmethod
    def create_cache(config: CacheConfig) -> CacheInterface:
        """Create cache instance based on configuration.

        Args:
            config: Cache configuration

        Returns:
            Cache instance

        Raises:
            ValueError: If cache backend is not supported
        """
        if config.backend == CacheBackend.REDIS:
            return CacheFactory._create_redis_cache(config)

        elif config.backend == CacheBackend.FILE:
            return CacheFactory._create_file_cache(config)

        elif config.backend == CacheBackend.HYBRID:
            return CacheFactory._create_hybrid_cache(config)

        else:
            raise ValueError(f"Unsupported cache backend: {config.backend}")

    @staticmethod
    def _create_redis_cache(config: CacheConfig) -> RedisCache:
        """Create Redis cache instance."""
        logger.info("Creating Redis cache")

        return RedisCache(
            redis_url=config.redis_url,
            password=config.redis_password,
            default_ttl=config.ttl_prices,
            key_prefix="wallet_tracker:",
            max_connections=20,
        )

    @staticmethod
    def _create_file_cache(config: CacheConfig) -> FileCache:
        """Create file cache instance."""
        logger.info(f"Creating file cache at {config.file_cache_dir}")

        return FileCache(
            cache_dir=config.file_cache_dir,
            default_ttl=config.ttl_prices,
            max_size_mb=config.max_size_mb,
            key_prefix="wallet_tracker:",
        )

    @staticmethod
    def _create_hybrid_cache(config: CacheConfig) -> HybridCache:
        """Create hybrid cache instance."""
        logger.info("Creating hybrid cache (Redis + File)")

        redis_cache = CacheFactory._create_redis_cache(config)
        file_cache = CacheFactory._create_file_cache(config)

        return HybridCache(
            redis_cache=redis_cache,
            file_cache=file_cache,
            fallback_on_error=True,
        )


class CacheManager:
    """Cache manager for handling multiple cache instances with different TTLs."""

    def __init__(self, config: CacheConfig):
        """Initialize cache manager.

        Args:
            config: Cache configuration
        """
        self.config = config
        self._price_cache: CacheInterface | None = None
        self._balance_cache: CacheInterface | None = None
        self._general_cache: CacheInterface | None = None

    def get_price_cache(self) -> CacheInterface:
        """Get cache for token prices with appropriate TTL."""
        if self._price_cache is None:
            self._price_cache = CacheFactory.create_cache(self.config)
            logger.info(f"Price cache initialized with TTL: {self.config.ttl_prices}s")
        return self._price_cache

    def get_balance_cache(self) -> CacheInterface:
        """Get cache for wallet balances with appropriate TTL."""
        if self._balance_cache is None:
            self._balance_cache = CacheFactory.create_cache(self.config)
            logger.info(f"Balance cache initialized with TTL: {self.config.ttl_balances}s")
        return self._balance_cache

    def get_general_cache(self) -> CacheInterface:
        """Get cache for general data with default TTL."""
        if self._general_cache is None:
            self._general_cache = CacheFactory.create_cache(self.config)
            logger.info("General cache initialized")
        return self._general_cache

    async def set_price(self, token_id: str, price_data: dict) -> bool:
        """Set token price with appropriate TTL."""
        cache = self.get_price_cache()
        key = f"price:{token_id}"
        return await cache.set(key, price_data, ttl=self.config.ttl_prices)

    async def get_price(self, token_id: str) -> dict | None:
        """Get token price from cache."""
        cache = self.get_price_cache()
        key = f"price:{token_id}"
        return await cache.get(key)

    async def set_balance(self, wallet_address: str, balance_data: dict) -> bool:
        """Set wallet balance with appropriate TTL."""
        cache = self.get_balance_cache()
        key = f"balance:{wallet_address.lower()}"
        return await cache.set(key, balance_data, ttl=self.config.ttl_balances)

    async def get_balance(self, wallet_address: str) -> dict | None:
        """Get wallet balance from cache."""
        cache = self.get_balance_cache()
        key = f"balance:{wallet_address.lower()}"
        return await cache.get(key)

    async def set_wallet_activity(self, wallet_address: str, activity_data: dict) -> bool:
        """Set wallet activity with longer TTL."""
        cache = self.get_general_cache()
        key = f"activity:{wallet_address.lower()}"
        # Activity data can be cached longer since it changes less frequently
        ttl = self.config.ttl_prices * 24  # 24x longer than prices
        return await cache.set(key, activity_data, ttl=ttl)

    async def get_wallet_activity(self, wallet_address: str) -> dict | None:
        """Get wallet activity from cache."""
        cache = self.get_general_cache()
        key = f"activity:{wallet_address.lower()}"
        return await cache.get(key)

    async def set_token_metadata(self, token_address: str, metadata: dict) -> bool:
        """Set token metadata with very long TTL."""
        cache = self.get_general_cache()
        key = f"token_meta:{token_address.lower()}"
        # Token metadata rarely changes
        ttl = self.config.ttl_prices * 168  # 1 week
        return await cache.set(key, metadata, ttl=ttl)

    async def get_token_metadata(self, token_address: str) -> dict | None:
        """Get token metadata from cache."""
        cache = self.get_general_cache()
        key = f"token_meta:{token_address.lower()}"
        return await cache.get(key)

    async def clear_wallet_data(self, wallet_address: str) -> None:
        """Clear all cached data for a specific wallet."""
        wallet_lower = wallet_address.lower()

        # Clear from all caches
        for cache in [self.get_balance_cache(), self.get_general_cache()]:
            try:
                await cache.delete(f"balance:{wallet_lower}")
                await cache.delete(f"activity:{wallet_lower}")
            except Exception as e:
                logger.warning(f"Failed to clear wallet data for {wallet_address}: {e}")

    async def health_check(self) -> dict:
        """Check health of all cache instances."""
        results = {}

        # Check each cache if initialized
        if self._price_cache:
            results["price_cache"] = await self._price_cache.health_check()

        if self._balance_cache:
            results["balance_cache"] = await self._balance_cache.health_check()

        if self._general_cache:
            results["general_cache"] = await self._general_cache.health_check()

        return results

    async def get_stats(self) -> dict:
        """Get statistics from all cache instances."""
        stats = {}

        if self._price_cache:
            stats["price_cache"] = self._price_cache.get_stats()

        if self._balance_cache:
            stats["balance_cache"] = self._balance_cache.get_stats()

        if self._general_cache:
            stats["general_cache"] = self._general_cache.get_stats()

        return stats

    async def close(self) -> None:
        """Close all cache connections."""
        for cache in [self._price_cache, self._balance_cache, self._general_cache]:
            if cache:
                try:
                    await cache.close()
                except Exception as e:
                    logger.error(f"Error closing cache: {e}")

        logger.info("All cache connections closed")
