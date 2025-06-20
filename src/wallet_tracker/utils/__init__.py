"""Utilities package for the Crypto Wealth Tracker.

This package provides caching implementations, rate limiting, throttling,
and other utility functions for the application.
"""

# Rate limiting and throttling utilities

from .rate_limiter import (
    AdaptiveLimiter,
    RateLimit,
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

__all__ = [
    # Rate limiting (if available)
    "RateLimit",
    # "RateLimitManager",
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
