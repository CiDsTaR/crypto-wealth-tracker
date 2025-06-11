"""Advanced throttling and backoff system for API interactions."""

import asyncio
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class BackoffStrategy(str, Enum):
    """Backoff strategies for retry logic."""

    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    POLYNOMIAL = "polynomial"
    CUSTOM = "custom"


class ThrottleMode(str, Enum):
    """Throttling modes."""

    CONSTANT = "constant"
    ADAPTIVE = "adaptive"
    BURST_THEN_THROTTLE = "burst_then_throttle"
    TIME_BASED = "time_based"


@dataclass
class BackoffConfig:
    """Configuration for backoff behavior."""

    # Basic configuration
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    initial_delay: float = 1.0
    max_delay: float = 300.0  # 5 minutes max
    max_retries: int = 5

    # Strategy-specific parameters
    multiplier: float = 2.0  # For exponential/polynomial
    additive_increase: float = 1.0  # For linear
    polynomial_degree: float = 2.0  # For polynomial

    # Jitter to prevent thundering herd
    jitter: bool = True
    jitter_range: float = 0.1  # ±10%

    # Reset behavior
    reset_on_success: bool = True
    success_threshold: int = 3  # Reset after N consecutive successes

    # Custom function for custom strategy
    custom_function: Callable[[int, float], float] | None = None

    def calculate_delay(self, attempt: int, base_delay: float | None = None) -> float:
        """Calculate delay for given attempt number.

        Args:
            attempt: Attempt number (0-based)
            base_delay: Override base delay

        Returns:
            Delay in seconds
        """
        if attempt < 0:
            return 0.0

        delay = base_delay or self.initial_delay

        if self.strategy == BackoffStrategy.FIXED:
            calculated_delay = delay

        elif self.strategy == BackoffStrategy.LINEAR:
            calculated_delay = delay + (attempt * self.additive_increase)

        elif self.strategy == BackoffStrategy.EXPONENTIAL:
            calculated_delay = delay * (self.multiplier**attempt)

        elif self.strategy == BackoffStrategy.FIBONACCI:
            calculated_delay = delay * self._fibonacci(attempt + 1)

        elif self.strategy == BackoffStrategy.POLYNOMIAL:
            calculated_delay = delay * (attempt**self.polynomial_degree)

        elif self.strategy == BackoffStrategy.CUSTOM and self.custom_function:
            calculated_delay = self.custom_function(attempt, delay)

        else:
            # Default to exponential
            calculated_delay = delay * (self.multiplier**attempt)

        # Apply jitter
        if self.jitter and calculated_delay > 0:
            jitter_amount = calculated_delay * self.jitter_range
            jitter_offset = random.uniform(-jitter_amount, jitter_amount)
            calculated_delay += jitter_offset

        # Ensure within bounds
        return max(0.0, min(self.max_delay, calculated_delay))

    @staticmethod
    def _fibonacci(n: int) -> int:
        """Calculate nth Fibonacci number."""
        if n <= 1:
            return n

        a, b = 0, 1
        for _ in range(2, n + 1):
            a, b = b, a + b

        return b


@dataclass
class ThrottleConfig:
    """Configuration for throttling behavior."""

    # Basic throttling
    mode: ThrottleMode = ThrottleMode.CONSTANT
    requests_per_second: float = 10.0
    burst_size: int = 5

    # Adaptive throttling
    min_delay: float = 0.1
    max_delay: float = 10.0
    adaptation_factor: float = 1.2
    adaptation_threshold: float = 0.9  # Success rate threshold

    # Time-based throttling
    peak_hours: list[int] = field(default_factory=lambda: [9, 10, 11, 14, 15, 16])  # Business hours
    peak_multiplier: float = 0.5  # Reduce rate during peak hours

    # Burst then throttle
    burst_window_seconds: float = 60.0
    throttle_after_burst: bool = True

    def get_delay_for_time(self, current_time: datetime | None = None) -> float:
        """Get delay based on current time and mode.

        Args:
            current_time: Current time (uses now if None)

        Returns:
            Delay in seconds
        """
        base_delay = 1.0 / self.requests_per_second

        if self.mode == ThrottleMode.CONSTANT:
            return base_delay

        elif self.mode == ThrottleMode.TIME_BASED:
            current_time = current_time or datetime.now()
            if current_time.hour in self.peak_hours:
                return base_delay * (1.0 / self.peak_multiplier)
            return base_delay

        else:
            return base_delay


@dataclass
class ThrottleState:
    """State tracking for throttling."""

    # Request tracking
    request_count: int = 0
    success_count: int = 0
    failure_count: int = 0

    # Timing
    last_request_time: float | None = None
    window_start_time: float | None = None

    # Adaptive state
    current_delay: float = 0.1
    consecutive_successes: int = 0
    consecutive_failures: int = 0

    # Burst tracking
    burst_requests: int = 0
    burst_start_time: float | None = None

    def update_success(self) -> None:
        """Update state for successful request."""
        self.request_count += 1
        self.success_count += 1
        self.consecutive_successes += 1
        self.consecutive_failures = 0
        self.last_request_time = time.time()

    def update_failure(self) -> None:
        """Update state for failed request."""
        self.request_count += 1
        self.failure_count += 1
        self.consecutive_failures += 1
        self.consecutive_successes = 0
        self.last_request_time = time.time()

    def get_success_rate(self) -> float:
        """Get current success rate."""
        if self.request_count == 0:
            return 1.0
        return self.success_count / self.request_count

    def reset_burst(self) -> None:
        """Reset burst tracking."""
        self.burst_requests = 0
        self.burst_start_time = None


class Throttle:
    """Advanced throttling system with multiple strategies."""

    def __init__(self, config: ThrottleConfig, backoff_config: BackoffConfig | None = None, name: str = "throttle"):
        """Initialize throttle.

        Args:
            config: Throttle configuration
            backoff_config: Backoff configuration (optional)
            name: Name for logging and identification
        """
        self.config = config
        self.backoff_config = backoff_config or BackoffConfig()
        self.name = name

        self.state = ThrottleState()
        self._lock = asyncio.Lock()

        # Backoff state
        self.current_attempt = 0
        self.backoff_until = None

        logger.debug(f"Initialized throttle '{name}' with {config.requests_per_second} req/s")

    async def acquire(self) -> float:
        """Acquire permission to make a request.

        Returns:
            Delay time that was applied
        """
        async with self._lock:
            now = time.time()

            # Check if we're in backoff period
            if self.backoff_until and now < self.backoff_until:
                remaining_backoff = self.backoff_until - now
                logger.debug(f"Throttle '{self.name}' in backoff, waiting {remaining_backoff:.2f}s")
                return remaining_backoff

            # Calculate delay based on mode
            delay = self._calculate_delay(now)

            # Update state
            self.state.last_request_time = now

            return delay

    async def wait(self) -> None:
        """Wait for permission to make a request."""
        delay = await self.acquire()

        if delay > 0:
            logger.debug(f"Throttle '{self.name}' waiting {delay:.2f}s")
            await asyncio.sleep(delay)

    def _calculate_delay(self, now: float) -> float:
        """Calculate delay based on current mode and state."""
        if self.config.mode == ThrottleMode.CONSTANT:
            return self._calculate_constant_delay(now)

        elif self.config.mode == ThrottleMode.ADAPTIVE:
            return self._calculate_adaptive_delay(now)

        elif self.config.mode == ThrottleMode.BURST_THEN_THROTTLE:
            return self._calculate_burst_delay(now)

        elif self.config.mode == ThrottleMode.TIME_BASED:
            return self._calculate_time_based_delay(now)

        else:
            # Default to constant
            return self._calculate_constant_delay(now)

    def _calculate_constant_delay(self, now: float) -> float:
        """Calculate delay for constant throttling."""
        base_delay = 1.0 / self.config.requests_per_second

        if self.state.last_request_time is None:
            return 0.0

        time_since_last = now - self.state.last_request_time
        required_delay = base_delay - time_since_last

        return max(0.0, required_delay)

    def _calculate_adaptive_delay(self, now: float) -> float:
        """Calculate delay for adaptive throttling."""
        success_rate = self.state.get_success_rate()

        # Adjust delay based on success rate
        if success_rate < self.config.adaptation_threshold:
            # Increase delay if success rate is low
            self.state.current_delay = min(
                self.config.max_delay, self.state.current_delay * self.config.adaptation_factor
            )
        else:
            # Decrease delay if success rate is good
            self.state.current_delay = max(
                self.config.min_delay, self.state.current_delay / self.config.adaptation_factor
            )

        if self.state.last_request_time is None:
            return 0.0

        time_since_last = now - self.state.last_request_time
        required_delay = self.state.current_delay - time_since_last

        return max(0.0, required_delay)

    def _calculate_burst_delay(self, now: float) -> float:
        """Calculate delay for burst-then-throttle mode."""
        # Initialize burst window if needed
        if self.state.burst_start_time is None:
            self.state.burst_start_time = now
            self.state.burst_requests = 0

        # Check if burst window has expired
        if now - self.state.burst_start_time > self.config.burst_window_seconds:
            self.state.reset_burst()
            self.state.burst_start_time = now

        # Allow burst requests
        if self.state.burst_requests < self.config.burst_size:
            self.state.burst_requests += 1
            return 0.0

        # Apply throttling after burst
        if self.config.throttle_after_burst:
            base_delay = 1.0 / self.config.requests_per_second

            if self.state.last_request_time is None:
                return base_delay

            time_since_last = now - self.state.last_request_time
            required_delay = base_delay - time_since_last

            return max(0.0, required_delay)

        return 0.0

    def _calculate_time_based_delay(self, now: float) -> float:
        """Calculate delay for time-based throttling."""
        current_time = datetime.fromtimestamp(now)
        delay = self.config.get_delay_for_time(current_time)

        if self.state.last_request_time is None:
            return 0.0

        time_since_last = now - self.state.last_request_time
        required_delay = delay - time_since_last

        return max(0.0, required_delay)

    async def report_success(self) -> None:
        """Report successful request."""
        async with self._lock:
            self.state.update_success()

            # Reset backoff on success
            if self.backoff_config.reset_on_success:
                if self.state.consecutive_successes >= self.backoff_config.success_threshold:
                    self.current_attempt = 0
                    self.backoff_until = None

    async def report_failure(self, trigger_backoff: bool = True) -> None:
        """Report failed request.

        Args:
            trigger_backoff: Whether to trigger backoff logic
        """
        async with self._lock:
            self.state.update_failure()

            if trigger_backoff and self.current_attempt < self.backoff_config.max_retries:
                # Calculate backoff delay
                backoff_delay = self.backoff_config.calculate_delay(self.current_attempt)
                self.backoff_until = time.time() + backoff_delay
                self.current_attempt += 1

                logger.warning(
                    f"Throttle '{self.name}' backing off for {backoff_delay:.2f}s "
                    f"(attempt {self.current_attempt}/{self.backoff_config.max_retries})"
                )

    def get_stats(self) -> dict[str, Any]:
        """Get throttle statistics."""
        return {
            "name": self.name,
            "mode": self.config.mode.value,
            "requests_per_second": self.config.requests_per_second,
            "total_requests": self.state.request_count,
            "success_count": self.state.success_count,
            "failure_count": self.state.failure_count,
            "success_rate": self.state.get_success_rate(),
            "consecutive_successes": self.state.consecutive_successes,
            "consecutive_failures": self.state.consecutive_failures,
            "current_delay": self.state.current_delay,
            "current_attempt": self.current_attempt,
            "in_backoff": self.backoff_until is not None and time.time() < self.backoff_until,
            "burst_requests": self.state.burst_requests,
        }

    def reset(self) -> None:
        """Reset throttle state."""
        self.state = ThrottleState()
        self.current_attempt = 0
        self.backoff_until = None
        logger.debug(f"Reset throttle '{self.name}'")


class ThrottleManager:
    """Manages multiple throttles for different services."""

    def __init__(self):
        """Initialize throttle manager."""
        self.throttles: dict[str, Throttle] = {}
        self._global_lock = asyncio.Lock()

    def register_throttle(self, name: str, config: ThrottleConfig, backoff_config: BackoffConfig | None = None) -> None:
        """Register a new throttle.

        Args:
            name: Unique name for the throttle
            config: Throttle configuration
            backoff_config: Backoff configuration (optional)
        """
        throttle = Throttle(config, backoff_config, name)
        self.throttles[name] = throttle

        logger.info(f"Registered throttle '{name}': {config.requests_per_second} req/s")

    async def acquire(self, throttle_name: str) -> float:
        """Acquire from a specific throttle.

        Args:
            throttle_name: Name of the throttle

        Returns:
            Delay time in seconds

        Raises:
            KeyError: If throttle not found
        """
        if throttle_name not in self.throttles:
            raise KeyError(f"Throttle '{throttle_name}' not found")

        return await self.throttles[throttle_name].acquire()

    async def wait(self, throttle_name: str) -> None:
        """Wait for a specific throttle.

        Args:
            throttle_name: Name of the throttle
        """
        if throttle_name not in self.throttles:
            raise KeyError(f"Throttle '{throttle_name}' not found")

        await self.throttles[throttle_name].wait()

    async def report_success(self, throttle_name: str) -> None:
        """Report success to a specific throttle.

        Args:
            throttle_name: Name of the throttle
        """
        if throttle_name in self.throttles:
            await self.throttles[throttle_name].report_success()

    async def report_failure(self, throttle_name: str, trigger_backoff: bool = True) -> None:
        """Report failure to a specific throttle.

        Args:
            throttle_name: Name of the throttle
            trigger_backoff: Whether to trigger backoff logic
        """
        if throttle_name in self.throttles:
            await self.throttles[throttle_name].report_failure(trigger_backoff)

    def get_throttle(self, name: str) -> Throttle | None:
        """Get a specific throttle.

        Args:
            name: Name of the throttle

        Returns:
            Throttle instance or None if not found
        """
        return self.throttles.get(name)

    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all throttles.

        Returns:
            Dictionary mapping throttle names to their stats
        """
        return {name: throttle.get_stats() for name, throttle in self.throttles.items()}

    def reset_throttle(self, throttle_name: str) -> None:
        """Reset a specific throttle.

        Args:
            throttle_name: Name of the throttle
        """
        if throttle_name in self.throttles:
            self.throttles[throttle_name].reset()

    def reset_all(self) -> None:
        """Reset all throttles."""
        for throttle in self.throttles.values():
            throttle.reset()

        logger.info("Reset all throttles")

    def remove_throttle(self, throttle_name: str) -> None:
        """Remove a throttle.

        Args:
            throttle_name: Name of the throttle to remove
        """
        if throttle_name in self.throttles:
            del self.throttles[throttle_name]
            logger.info(f"Removed throttle '{throttle_name}'")


# Decorator for automatic throttling
def throttled(throttle_name: str, manager: ThrottleManager | None = None):
    """Decorator to automatically apply throttling to functions.

    Args:
        throttle_name: Name of the throttle to use
        manager: Throttle manager (uses global if None)
    """

    def decorator(func: Callable) -> Callable:
        async def wrapper(*args, **kwargs):
            nonlocal manager

            if manager is None:
                raise ValueError("No throttle manager provided")

            # Wait for throttle
            await manager.wait(throttle_name)

            # Execute function
            try:
                result = await func(*args, **kwargs)

                # Report success
                await manager.report_success(throttle_name)

                return result

            except Exception:
                # Report failure
                await manager.report_failure(throttle_name)
                raise

        return wrapper

    return decorator


# Utility functions
def create_ethereum_throttle(requests_per_second: float = 10.0) -> ThrottleConfig:
    """Create throttle configuration for Ethereum RPC calls.

    Args:
        requests_per_second: Maximum requests per second

    Returns:
        Throttle configuration
    """
    return ThrottleConfig(
        mode=ThrottleMode.ADAPTIVE,
        requests_per_second=requests_per_second,
        burst_size=int(requests_per_second * 2),
        min_delay=0.05,
        max_delay=5.0,
        adaptation_factor=1.5,
    )


def create_coingecko_throttle(requests_per_minute: int = 30) -> ThrottleConfig:
    """Create throttle configuration for CoinGecko API.

    Args:
        requests_per_minute: Maximum requests per minute

    Returns:
        Throttle configuration
    """
    requests_per_second = requests_per_minute / 60.0

    return ThrottleConfig(
        mode=ThrottleMode.BURST_THEN_THROTTLE,
        requests_per_second=requests_per_second,
        burst_size=5,  # Allow small burst
        burst_window_seconds=60.0,
        throttle_after_burst=True,
    )


def create_sheets_throttle(requests_per_minute: int = 100) -> ThrottleConfig:
    """Create throttle configuration for Google Sheets API.

    Args:
        requests_per_minute: Maximum requests per minute

    Returns:
        Throttle configuration
    """
    return ThrottleConfig(
        mode=ThrottleMode.TIME_BASED,
        requests_per_second=requests_per_minute / 60.0,
        peak_hours=[9, 10, 11, 14, 15, 16],  # Business hours
        peak_multiplier=0.7,  # Reduce rate during peak
    )


def create_aggressive_backoff() -> BackoffConfig:
    """Create aggressive backoff configuration for unreliable services.

    Returns:
        Backoff configuration
    """
    return BackoffConfig(
        strategy=BackoffStrategy.EXPONENTIAL,
        initial_delay=2.0,
        max_delay=600.0,  # 10 minutes
        max_retries=8,
        multiplier=2.5,
        jitter=True,
        jitter_range=0.2,  # ±20%
    )


def create_gentle_backoff() -> BackoffConfig:
    """Create gentle backoff configuration for reliable services.

    Returns:
        Backoff configuration
    """
    return BackoffConfig(
        strategy=BackoffStrategy.LINEAR,
        initial_delay=0.5,
        max_delay=30.0,
        max_retries=3,
        additive_increase=1.0,
        jitter=True,
        jitter_range=0.1,  # ±10%
    )


class CombinedThrottleAndRateLimit:
    """Combines throttling and rate limiting for comprehensive control."""

    def __init__(
        self,
        throttle: Throttle,
        rate_limiter: Any,  # Would import from rate_limiter.py
        name: str = "combined",
    ):
        """Initialize combined throttle and rate limiter.

        Args:
            throttle: Throttle instance
            rate_limiter: Rate limiter instance
            name: Name for identification
        """
        self.throttle = throttle
        self.rate_limiter = rate_limiter
        self.name = name

    async def acquire(self) -> float:
        """Acquire from both throttle and rate limiter.

        Returns:
            Total delay time applied
        """
        # Get delays from both systems
        throttle_delay = await self.throttle.acquire()
        rate_limit_delay = await self.rate_limiter.acquire()

        # Use the maximum delay
        total_delay = max(throttle_delay, rate_limit_delay)

        return total_delay

    async def wait(self) -> None:
        """Wait for both throttle and rate limiter."""
        delay = await self.acquire()

        if delay > 0:
            logger.debug(f"Combined '{self.name}' waiting {delay:.2f}s")
            await asyncio.sleep(delay)

    async def report_success(self) -> None:
        """Report success to both systems."""
        await self.throttle.report_success()
        # Rate limiter typically doesn't need success reporting

    async def report_failure(self, trigger_backoff: bool = True) -> None:
        """Report failure to both systems."""
        await self.throttle.report_failure(trigger_backoff)
        # Rate limiter typically doesn't need failure reporting

    def get_stats(self) -> dict[str, Any]:
        """Get combined statistics."""
        return {
            "name": self.name,
            "throttle_stats": self.throttle.get_stats(),
            "rate_limiter_stats": getattr(self.rate_limiter, "get_status", lambda: {})(),
        }
