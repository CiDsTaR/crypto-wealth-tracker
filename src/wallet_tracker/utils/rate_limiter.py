"""Advanced rate limiting system for API requests."""

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class RateLimitStrategy(str, Enum):
    """Rate limiting strategies."""

    TOKEN_BUCKET = "token_bucket"  # nosec B105
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


class RateLimitScope(str, Enum):
    """Scope of rate limiting."""

    GLOBAL = "global"
    PER_SERVICE = "per_service"
    PER_ENDPOINT = "per_endpoint"
    PER_USER = "per_user"
    PER_IP = "per_ip"


@dataclass
class RateLimit:
    """Rate limit configuration."""

    requests_per_second: float
    requests_per_minute: int | None = None
    requests_per_hour: int | None = None
    requests_per_day: int | None = None

    # Burst handling
    burst_size: int | None = None
    burst_duration_seconds: float = 1.0

    # Backoff configuration
    enable_backoff: bool = True
    backoff_factor: float = 2.0
    max_backoff_seconds: float = 60.0

    # Strategy
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET

    def __post_init__(self) -> None:
        """Validate rate limit configuration."""
        if self.requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")

        if self.burst_size is None:
            # Default burst size to 2x rate per second
            self.burst_size = max(1, int(self.requests_per_second * 2))

        if self.requests_per_minute is None:
            self.requests_per_minute = int(self.requests_per_second * 60)

        if self.requests_per_hour is None:
            self.requests_per_hour = int(self.requests_per_second * 3600)


@dataclass
class RateLimitStatus:
    """Current status of a rate limiter."""

    requests_made: int = 0
    requests_remaining: int = 0
    reset_time: datetime | None = None
    retry_after_seconds: float | None = None

    # Performance metrics
    total_requests: int = 0
    total_delays: int = 0
    total_delay_time: float = 0.0
    average_delay: float = 0.0

    # Current state
    is_rate_limited: bool = False
    current_backoff_seconds: float = 0.0
    consecutive_limits: int = 0

    def update_metrics(self, delay_time: float) -> None:
        """Update performance metrics."""
        self.total_requests += 1

        if delay_time > 0:
            self.total_delays += 1
            self.total_delay_time += delay_time
            self.average_delay = self.total_delay_time / self.total_delays


class TokenBucketLimiter:
    """Token bucket rate limiter implementation."""

    def __init__(self, rate_limit: RateLimit):
        """Initialize token bucket limiter.

        Args:
            rate_limit: Rate limit configuration
        """
        self.rate_limit = rate_limit
        self.tokens = float(rate_limit.burst_size)
        self.max_tokens = float(rate_limit.burst_size)
        self.refill_rate = rate_limit.requests_per_second
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> float:
        """Acquire tokens from bucket.

        Args:
            tokens: Number of tokens to acquire

        Returns:
            Delay time in seconds (0 if no delay needed)
        """
        async with self._lock:
            now = time.time()

            # Refill tokens based on elapsed time
            time_passed = now - self.last_refill
            tokens_to_add = time_passed * self.refill_rate
            self.tokens = min(self.max_tokens, self.tokens + tokens_to_add)
            self.last_refill = now

            # Check if we have enough tokens
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0

            # Calculate delay needed
            tokens_needed = tokens - self.tokens
            delay = tokens_needed / self.refill_rate

            # Reset tokens (we'll wait for refill)
            self.tokens = 0.0

            return delay

    def get_status(self) -> RateLimitStatus:
        """Get current limiter status."""
        return RateLimitStatus(
            requests_remaining=int(self.tokens),
            reset_time=datetime.now(UTC) + timedelta(seconds=1 / self.refill_rate),
        )


class SlidingWindowLimiter:
    """Sliding window rate limiter implementation."""

    def __init__(self, rate_limit: RateLimit):
        """Initialize sliding window limiter.

        Args:
            rate_limit: Rate limit configuration
        """
        self.rate_limit = rate_limit
        self.window_size = 60.0  # 1 minute window
        self.max_requests = rate_limit.requests_per_minute or int(rate_limit.requests_per_second * 60)
        self.requests: deque = deque()
        self._lock = asyncio.Lock()

    async def acquire(self, requests: int = 1) -> float:
        """Acquire requests from sliding window.

        Args:
            requests: Number of requests to acquire

        Returns:
            Delay time in seconds (0 if no delay needed)
        """
        async with self._lock:
            now = time.time()

            # Remove old requests outside window
            while self.requests and self.requests[0] <= now - self.window_size:
                self.requests.popleft()

            # Check if we can make the request
            if len(self.requests) + requests <= self.max_requests:
                # Add current requests
                for _ in range(requests):
                    self.requests.append(now)
                return 0.0

            # Calculate delay until oldest request expires
            if self.requests:
                oldest_request = self.requests[0]
                delay = (oldest_request + self.window_size) - now
                return max(0.0, delay)

            return 0.0

    def get_status(self) -> RateLimitStatus:
        """Get current limiter status."""
        now = time.time()

        # Clean old requests
        while self.requests and self.requests[0] <= now - self.window_size:
            self.requests.popleft()

        return RateLimitStatus(
            requests_made=len(self.requests),
            requests_remaining=max(0, self.max_requests - len(self.requests)),
            reset_time=datetime.now(UTC) + timedelta(seconds=self.window_size),
        )


class AdaptiveLimiter:
    """Adaptive rate limiter that adjusts based on response patterns."""

    def __init__(self, rate_limit: RateLimit):
        """Initialize adaptive limiter.

        Args:
            rate_limit: Base rate limit configuration
        """
        self.base_rate_limit = rate_limit
        self.current_rate = rate_limit.requests_per_second
        self.base_limiter = TokenBucketLimiter(rate_limit)

        # Adaptation parameters
        self.success_count = 0
        self.error_count = 0
        self.last_adjustment = time.time()
        self.adjustment_interval = 30.0  # Adjust every 30 seconds

        # Performance tracking
        self.response_times: deque = deque(maxlen=100)
        self.error_rates: deque = deque(maxlen=20)

        self._lock = asyncio.Lock()

    async def acquire(self, requests: int = 1) -> float:
        """Acquire requests with adaptive rate limiting.

        Args:
            requests: Number of requests to acquire

        Returns:
            Delay time in seconds
        """
        await self._maybe_adjust_rate()
        return await self.base_limiter.acquire(requests)

    async def report_response(self, success: bool, response_time: float) -> None:
        """Report API response for adaptation.

        Args:
            success: Whether the request was successful
            response_time: Response time in seconds
        """
        async with self._lock:
            if success:
                self.success_count += 1
                self.response_times.append(response_time)
            else:
                self.error_count += 1

            # Track error rate
            total_requests = self.success_count + self.error_count
            if total_requests > 0:
                error_rate = self.error_count / total_requests
                self.error_rates.append(error_rate)

    async def _maybe_adjust_rate(self) -> None:
        """Adjust rate based on recent performance."""
        now = time.time()

        if now - self.last_adjustment < self.adjustment_interval:
            return

        async with self._lock:
            # Calculate adjustment factor
            adjustment_factor = 1.0

            # Adjust based on error rate
            if self.error_rates:
                recent_error_rate = sum(list(self.error_rates)[-5:]) / min(5, len(self.error_rates))

                if recent_error_rate > 0.1:  # > 10% error rate
                    adjustment_factor *= 0.8  # Reduce rate by 20%
                elif recent_error_rate < 0.01:  # < 1% error rate
                    adjustment_factor *= 1.1  # Increase rate by 10%

            # Adjust based on response times
            if self.response_times:
                avg_response_time = sum(list(self.response_times)[-10:]) / min(10, len(self.response_times))

                if avg_response_time > 2.0:  # Slow responses
                    adjustment_factor *= 0.9  # Reduce rate
                elif avg_response_time < 0.5:  # Fast responses
                    adjustment_factor *= 1.05  # Slight increase

            # Apply adjustment
            new_rate = self.current_rate * adjustment_factor

            # Bounds checking
            min_rate = self.base_rate_limit.requests_per_second * 0.1  # Never go below 10%
            max_rate = self.base_rate_limit.requests_per_second * 2.0  # Never exceed 200%

            self.current_rate = max(min_rate, min(max_rate, new_rate))

            # Update base limiter
            self.base_limiter.refill_rate = self.current_rate

            self.last_adjustment = now

            logger.debug(f"Adjusted rate limit to {self.current_rate:.2f} req/s (factor: {adjustment_factor:.2f})")

    def get_status(self) -> RateLimitStatus:
        """Get current limiter status."""
        status = self.base_limiter.get_status()

        # Add adaptive metrics
        total_requests = self.success_count + self.error_count
        if total_requests > 0:
            error_rate = self.error_count / total_requests
            status.average_delay = error_rate  # Reuse field for error rate

        return status


class RateLimiterManager:
    """Manages multiple rate limiters for different services and endpoints."""

    def __init__(self) -> None:
        """Initialize rate limiter manager."""
        self.limiters: dict[str, TokenBucketLimiter | SlidingWindowLimiter | AdaptiveLimiter] = {}
        self.rate_limits: dict[str, RateLimit] = {}
        self.status_cache: dict[str, RateLimitStatus] = {}
        self._global_lock = asyncio.Lock()

    def register_limiter(self, name: str, rate_limit: RateLimit, scope: RateLimitScope = RateLimitScope.GLOBAL) -> None:
        """Register a new rate limiter.

        Args:
            name: Unique name for the limiter
            rate_limit: Rate limit configuration
            scope: Scope of the rate limiter
        """
        self.rate_limits[name] = rate_limit

        # Create appropriate limiter based on strategy
        if rate_limit.strategy == RateLimitStrategy.TOKEN_BUCKET:
            limiter = TokenBucketLimiter(rate_limit)
        elif rate_limit.strategy == RateLimitStrategy.SLIDING_WINDOW:
            limiter = SlidingWindowLimiter(rate_limit)
        elif rate_limit.strategy == RateLimitStrategy.ADAPTIVE:
            limiter = AdaptiveLimiter(rate_limit)
        else:
            # Default to token bucket
            limiter = TokenBucketLimiter(rate_limit)

        self.limiters[name] = limiter

        logger.info(f"Registered rate limiter '{name}': {rate_limit.requests_per_second} req/s")

    async def acquire(self, limiter_name: str, requests: int = 1) -> float:
        """Acquire requests from a specific limiter.

        Args:
            limiter_name: Name of the rate limiter
            requests: Number of requests to acquire

        Returns:
            Delay time in seconds

        Raises:
            KeyError: If limiter not found
        """
        if limiter_name not in self.limiters:
            raise KeyError(f"Rate limiter '{limiter_name}' not found")

        limiter = self.limiters[limiter_name]
        delay = await limiter.acquire(requests)

        # Update status cache
        self.status_cache[limiter_name] = limiter.get_status()

        return delay

    async def wait_for_capacity(self, limiter_name: str, requests: int = 1) -> None:
        """Wait until capacity is available in the limiter.

        Args:
            limiter_name: Name of the rate limiter
            requests: Number of requests to wait for
        """
        delay = await self.acquire(limiter_name, requests)

        if delay > 0:
            logger.debug(f"Rate limited on '{limiter_name}', waiting {delay:.2f}s")
            await asyncio.sleep(delay)

    def get_status(self, limiter_name: str) -> RateLimitStatus | None:
        """Get status of a specific limiter.

        Args:
            limiter_name: Name of the rate limiter

        Returns:
            Rate limit status or None if not found
        """
        if limiter_name not in self.limiters:
            return None

        return self.limiters[limiter_name].get_status()

    def get_all_status(self) -> dict[str, RateLimitStatus]:
        """Get status of all registered limiters.

        Returns:
            Dictionary mapping limiter names to their status
        """
        return {name: limiter.get_status() for name, limiter in self.limiters.items()}

    async def report_response(self, limiter_name: str, success: bool, response_time: float) -> None:
        """Report API response for adaptive limiters.

        Args:
            limiter_name: Name of the rate limiter
            success: Whether the request was successful
            response_time: Response time in seconds
        """
        if limiter_name not in self.limiters:
            return

        limiter = self.limiters[limiter_name]
        if isinstance(limiter, AdaptiveLimiter):
            await limiter.report_response(success, response_time)

    def update_limiter(self, limiter_name: str, rate_limit: RateLimit) -> None:
        """Update configuration of an existing limiter.

        Args:
            limiter_name: Name of the rate limiter
            rate_limit: New rate limit configuration
        """
        if limiter_name in self.limiters:
            self.register_limiter(limiter_name, rate_limit)
            logger.info(f"Updated rate limiter '{limiter_name}': {rate_limit.requests_per_second} req/s")

    def remove_limiter(self, limiter_name: str) -> None:
        """Remove a rate limiter.

        Args:
            limiter_name: Name of the rate limiter to remove
        """
        if limiter_name in self.limiters:
            del self.limiters[limiter_name]
            del self.rate_limits[limiter_name]
            if limiter_name in self.status_cache:
                del self.status_cache[limiter_name]

            logger.info(f"Removed rate limiter '{limiter_name}'")

    def get_stats(self) -> dict[str, Any]:
        """Get overall rate limiter statistics.

        Returns:
            Dictionary with statistics
        """
        stats = {
            "total_limiters": len(self.limiters),
            "limiter_types": {},
            "active_limits": 0,
        }

        # Count limiter types
        for limiter in self.limiters.values():
            limiter_type = type(limiter).__name__
            stats["limiter_types"][limiter_type] = stats["limiter_types"].get(limiter_type, 0) + 1

        # Count active limits
        for status in self.status_cache.values():
            if status.is_rate_limited:
                stats["active_limits"] += 1

        return stats


# Decorator for automatic rate limiting
def rate_limited(
    limiter_name: str, requests: int = 1, manager: RateLimiterManager | None = None
) -> Callable:
    """Decorator to automatically apply rate limiting to functions.

    Args:
        limiter_name: Name of the rate limiter to use
        requests: Number of requests this function consumes
        manager: Rate limiter manager (uses global if None)
    """

    def decorator(func: Callable) -> Callable:
        async def wrapper(*args: tuple[Any, ...], **kwargs: dict[str, Any]) -> Any:
            nonlocal manager

            if manager is None:
                # Use global manager (would need to be defined)
                raise ValueError("No rate limiter manager provided")

            # Wait for capacity
            await manager.wait_for_capacity(limiter_name, requests)

            # Execute function
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                response_time = time.time() - start_time

                # Report success
                await manager.report_response(limiter_name, True, response_time)

                return result

            except Exception:
                response_time = time.time() - start_time

                # Report failure
                await manager.report_response(limiter_name, False, response_time)

                raise

        return wrapper

    return decorator


# Utility functions
def create_ethereum_rate_limiter(requests_per_second: float = 10) -> RateLimit:
    """Create a rate limiter configuration for Ethereum RPC calls.

    Args:
        requests_per_second: Maximum requests per second

    Returns:
        Rate limit configuration
    """
    return RateLimit(
        requests_per_second=requests_per_second,
        burst_size=int(requests_per_second * 2),
        strategy=RateLimitStrategy.TOKEN_BUCKET,
        enable_backoff=True,
    )


def create_coingecko_rate_limiter(requests_per_minute: int = 30, has_pro_api: bool = False) -> RateLimit:
    """Create a rate limiter configuration for CoinGecko API.

    Args:
        requests_per_minute: Maximum requests per minute
        has_pro_api: Whether using API Pro key (higher limits)

    Returns:
        Rate limit configuration
    """
    # Pro plan limits vs Free plan limits
    requests_per_second = (
        min(50.0, requests_per_minute / 60.0) if has_pro_api else min(requests_per_minute / 60.0, 0.5)
    )  # Max 30/minute = 0.5/second

    return RateLimit(
        requests_per_second=requests_per_second,
        requests_per_minute=requests_per_minute,
        strategy=RateLimitStrategy.SLIDING_WINDOW,
        enable_backoff=True,
    )


def create_sheets_rate_limiter(requests_per_minute: int = 100) -> RateLimit:
    """Create a rate limiter configuration for Google Sheets API.

    Args:
        requests_per_minute: Maximum requests per minute

    Returns:
        Rate limit configuration
    """
    return RateLimit(
        requests_per_second=requests_per_minute / 60.0,
        requests_per_minute=requests_per_minute,
        strategy=RateLimitStrategy.TOKEN_BUCKET,
        burst_size=10,  # Allow small bursts
        enable_backoff=True,
    )
