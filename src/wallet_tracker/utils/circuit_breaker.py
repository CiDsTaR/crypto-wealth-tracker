"""Circuit breaker pattern implementation for handling service failures."""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from enum import Enum
from typing import Any, Callable, Dict, Optional, Type, Union

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Circuit is open, failing fast
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreakerError(Exception):
    """Circuit breaker is open."""

    def __init__(self, message: str = "Circuit breaker is open"):
        super().__init__(message)
        self.message = message


class CircuitBreaker:
    """Circuit breaker for handling service failures gracefully."""

    def __init__(
            self,
            failure_threshold: int = 5,
            recovery_timeout: float = 60.0,
            expected_exception: Type[Exception] = Exception,
            success_threshold: int = 1,
            name: str = "circuit_breaker",
            fallback: Optional[Callable] = None,
            health_check: Optional[Callable] = None,
            call_timeout: Optional[float] = None,
            retry_attempts: int = 0,
            retry_delay: float = 0.1,
            on_state_change: Optional[Callable] = None,
            on_failure: Optional[Callable] = None,
            on_success: Optional[Callable] = None
    ):
        """Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time to wait before attempting recovery
            expected_exception: Exception types that trigger circuit opening
            success_threshold: Successes needed to close from half-open
            name: Name for identification and logging
            fallback: Fallback function when circuit is open
            health_check: Function to check service health
            call_timeout: Timeout for individual calls
            retry_attempts: Number of retry attempts before failing
            retry_delay: Delay between retry attempts
            on_state_change: Callback for state changes
            on_failure: Callback for failures
            on_success: Callback for successes
        """
        if failure_threshold < 0:
            raise ValueError("failure_threshold must be non-negative")
        if recovery_timeout < 0:
            raise ValueError("recovery_timeout must be non-negative")
        if success_threshold <= 0:
            raise ValueError("success_threshold must be positive")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.success_threshold = success_threshold
        self.name = name
        self.fallback = fallback
        self.health_check = health_check
        self.call_timeout = call_timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay

        # Callbacks
        self.on_state_change = on_state_change
        self.on_failure = on_failure
        self.on_success = on_success

        # State
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._lock = asyncio.Lock()

        # Statistics
        self._total_calls = 0
        self._total_failures = 0
        self._total_successes = 0

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    @property
    def failure_count(self) -> int:
        """Get current failure count."""
        return self._failure_count

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function through circuit breaker.

        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Raises:
            CircuitBreakerError: If circuit is open
            Exception: Original function exceptions
        """
        async with self._lock:
            self._total_calls += 1

            # Check if circuit should transition to half-open
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to_half_open()
                else:
                    # Circuit is open, check for fallback
                    if self.fallback:
                        try:
                            if asyncio.iscoroutinefunction(self.fallback):
                                return await self.fallback(*args, **kwargs)
                            else:
                                return self.fallback(*args, **kwargs)
                        except Exception as e:
                            # Fallback also failed
                            raise e
                    raise CircuitBreakerError(f"Circuit breaker '{self.name}' is open")

        # Execute the function
        try:
            # Handle retries if configured
            last_exception = None
            for attempt in range(self.retry_attempts + 1):
                try:
                    if self.call_timeout:
                        result = await asyncio.wait_for(
                            func(*args, **kwargs) if asyncio.iscoroutinefunction(func)
                            else func(*args, **kwargs),
                            timeout=self.call_timeout
                        )
                    else:
                        if asyncio.iscoroutinefunction(func):
                            result = await func(*args, **kwargs)
                        else:
                            result = func(*args, **kwargs)

                    # Success
                    await self._record_success()
                    return result

                except Exception as e:
                    last_exception = e
                    if attempt < self.retry_attempts:
                        await asyncio.sleep(self.retry_delay)
                        continue
                    else:
                        break

            # All attempts failed
            await self._record_failure(last_exception)
            raise last_exception

        except Exception as e:
            await self._record_failure(e)
            raise

    async def _record_success(self):
        """Record a successful call."""
        async with self._lock:
            self._total_successes += 1

            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._transition_to_closed()
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0

            if self.on_success:
                try:
                    self.on_success()
                except Exception as e:
                    logger.warning(f"Error in success callback: {e}")

    async def _record_failure(self, exception: Exception):
        """Record a failed call."""
        async with self._lock:
            self._total_failures += 1

            # Only count expected exceptions
            if isinstance(exception, self.expected_exception):
                self._failure_count += 1
                self._last_failure_time = time.time()

                if self._state == CircuitState.CLOSED:
                    if self._failure_count >= self.failure_threshold:
                        self._transition_to_open()
                elif self._state == CircuitState.HALF_OPEN:
                    self._transition_to_open()

            if self.on_failure:
                try:
                    self.on_failure(exception)
                except Exception as e:
                    logger.warning(f"Error in failure callback: {e}")

    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt to reset."""
        if self._last_failure_time is None:
            return True

        # Check if enough time has passed
        time_passed = time.time() - self._last_failure_time
        if time_passed < self.recovery_timeout:
            return False

        # Check health if health check is configured
        if self.health_check:
            try:
                if asyncio.iscoroutinefunction(self.health_check):
                    # For async health checks, we can't await here
                    # This is a limitation - health checks should be sync
                    return True
                else:
                    return self.health_check()
            except Exception:
                return False

        return True

    def _transition_to_closed(self):
        """Transition circuit to closed state."""
        old_state = self._state
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0

        logger.info(f"Circuit breaker '{self.name}' transitioned to CLOSED")

        if self.on_state_change:
            try:
                self.on_state_change(old_state, self._state)
            except Exception as e:
                logger.warning(f"Error in state change callback: {e}")

    def _transition_to_open(self):
        """Transition circuit to open state."""
        old_state = self._state
        self._state = CircuitState.OPEN

        logger.warning(f"Circuit breaker '{self.name}' transitioned to OPEN")

        if self.on_state_change:
            try:
                self.on_state_change(old_state, self._state)
            except Exception as e:
                logger.warning(f"Error in state change callback: {e}")

    def _transition_to_half_open(self):
        """Transition circuit to half-open state."""
        old_state = self._state
        self._state = CircuitState.HALF_OPEN
        self._success_count = 0

        logger.info(f"Circuit breaker '{self.name}' transitioned to HALF_OPEN")

        if self.on_state_change:
            try:
                self.on_state_change(old_state, self._state)
            except Exception as e:
                logger.warning(f"Error in state change callback: {e}")

    def reset(self):
        """Manually reset circuit breaker to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        logger.info(f"Circuit breaker '{self.name}' manually reset")

    def open(self):
        """Manually open circuit breaker."""
        self._state = CircuitState.OPEN
        self._last_failure_time = time.time()
        logger.info(f"Circuit breaker '{self.name}' manually opened")

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        failure_rate = 0.0
        if self._total_calls > 0:
            failure_rate = (self._total_failures / self._total_calls) * 100

        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "failure_threshold": self.failure_threshold,
            "total_calls": self._total_calls,
            "total_failures": self._total_failures,
            "total_successes": self._total_successes,
            "failure_rate": round(failure_rate, 2),
            "last_failure_time": self._last_failure_time,
            "recovery_timeout": self.recovery_timeout
        }

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass


class CircuitBreakerManager:
    """Manager for multiple circuit breakers."""

    def __init__(self):
        """Initialize circuit breaker manager."""
        self._breakers: Dict[str, CircuitBreaker] = {}

    def register(
            self,
            name: str,
            failure_threshold: int = 5,
            recovery_timeout: float = 60.0,
            **kwargs
    ) -> CircuitBreaker:
        """Register a new circuit breaker.

        Args:
            name: Circuit breaker name
            failure_threshold: Failure threshold
            recovery_timeout: Recovery timeout
            **kwargs: Additional CircuitBreaker arguments

        Returns:
            Created circuit breaker
        """
        breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            name=name,
            **kwargs
        )
        self._breakers[name] = breaker
        return breaker

    async def call(self, name: str, func: Callable, *args, **kwargs) -> Any:
        """Call function through named circuit breaker.

        Args:
            name: Circuit breaker name
            func: Function to call
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Raises:
            KeyError: If circuit breaker not found
        """
        if name not in self._breakers:
            raise KeyError(f"Circuit breaker '{name}' not found")

        return await self._breakers[name].call(func, *args, **kwargs)

    def get_breaker(self, name: str) -> Optional[CircuitBreaker]:
        """Get circuit breaker by name."""
        return self._breakers.get(name)

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all circuit breakers."""
        return {
            name: breaker.get_stats()
            for name, breaker in self._breakers.items()
        }

    def reset_all(self):
        """Reset all circuit breakers."""
        for breaker in self._breakers.values():
            breaker.reset()

    def list_breakers(self) -> list[str]:
        """List all registered circuit breaker names."""
        return list(self._breakers.keys())


def circuit_breaker(
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: Type[Exception] = Exception,
        **kwargs
):
    """Decorator for circuit breaker functionality.

    Args:
        failure_threshold: Number of failures before opening
        recovery_timeout: Time to wait before recovery attempt
        expected_exception: Exception types that count as failures
        **kwargs: Additional CircuitBreaker arguments

    Returns:
        Decorated function
    """

    def decorator(func: Callable) -> Callable:
        breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=expected_exception,
            name=f"{func.__module__}.{func.__name__}",
            **kwargs
        )

        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                return await breaker.call(func, *args, **kwargs)

            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                return asyncio.run(breaker.call(func, *args, **kwargs))

            return sync_wrapper

    return decorator


# Global circuit breaker manager instance
_global_manager = CircuitBreakerManager()


def get_circuit_breaker_manager() -> CircuitBreakerManager:
    """Get global circuit breaker manager."""
    return _global_manager


def register_circuit_breaker(name: str, **kwargs) -> CircuitBreaker:
    """Register circuit breaker with global manager."""
    return _global_manager.register(name, **kwargs)