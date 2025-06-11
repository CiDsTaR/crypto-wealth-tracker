"""Utilities package for the Ethereum Wallet Tracker.

This package provides caching implementations, rate limiting, throttling,
and other utility functions for the application.
"""

from .cache_factory import CacheFactory, CacheManager
from .cache_interface import CacheConnectionError, CacheError, CacheInterface, CacheOperationError
from .file_cache import FileCache
from .hybrid_cache import HybridCache
from .redis_cache import RedisCache

# Rate limiting and throttling utilities
try:
    from .rate_limiter import (
        AdaptiveLimiter,
        RateLimit,
        RateLimitManager,
        RateLimitScope,
        RateLimitStatus,
        RateLimitStrategy,
        SlidingWindowLimiter,
        TokenBucketLimiter,
        create_coingecko_rate_limiter,
        create_ethereum_rate_limiter,
        create_sheets_rate_limiter,
        rate_limited,
    )
except ImportError:
    # Rate limiter components not available
    pass

try:
    from .throttle import (
        BackoffConfig,
        BackoffStrategy,
        CombinedThrottleAndRateLimit,
        Throttle,
        ThrottleConfig,
        ThrottleManager,
        ThrottleMode,
        ThrottleState,
        create_aggressive_backoff,
        create_coingecko_throttle,
        create_ethereum_throttle,
        create_gentle_backoff,
        create_sheets_throttle,
        throttled,
    )
except ImportError:
    # Throttle components not available
    pass

__all__ = [
    # Cache interfaces and errors
    "CacheInterface",
    "CacheError",
    "CacheConnectionError",
    "CacheOperationError",
    # Cache implementations
    "RedisCache",
    "FileCache",
    "HybridCache",
    # Factory and manager
    "CacheFactory",
    "CacheManager",
    # Rate limiting (if available)
    "RateLimit",
    "RateLimitManager",
    "RateLimitStatus",
    "RateLimitStrategy",
    "RateLimitScope",
    "TokenBucketLimiter",
    "SlidingWindowLimiter",
    "AdaptiveLimiter",
    "rate_limited",
    "create_ethereum_rate_limiter",
    "create_coingecko_rate_limiter",
    "create_sheets_rate_limiter",
    # Throttling (if available)
    "Throttle",
    "ThrottleConfig",
    "ThrottleManager",
    "ThrottleMode",
    "ThrottleState",
    "BackoffConfig",
    "BackoffStrategy",
    "CombinedThrottleAndRateLimit",
    "throttled",
    "create_ethereum_throttle",
    "create_coingecko_throttle",
    "create_sheets_throttle",
    "create_aggressive_backoff",
    "create_gentle_backoff",
]