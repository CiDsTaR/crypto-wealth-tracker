"""Retry mechanisms and utilities for handling transient failures."""

import asyncio
import functools
import logging
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class BackoffStrategy(Enum):
    """Backoff strategies for retry delays."""

    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    POLYNOMIAL = "polynomial"
    CUSTOM = "custom"


class RetryExhaustedError(Exception):
    """Raised when retry attempts are exhausted."""

    def __init__(self, message: str, last_exception: Exception | None = None):
        super().__init__(message)
        self.last_exception = last_exception


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    delay: float = 1.0
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    multiplier: float = 2.0
    max_delay: float = 60.0
    jitter: bool = True
    jitter_range: float = 0.1
    exceptions: list[type[Exception]] | None = None
    retry_condition: Callable | None = None
    before_retry: Callable | None = None
    after_retry: Callable | None = None
    on_final_failure: Callable | None = None


class DelayStrategy(ABC):
    """Abstract base class for delay strategies."""

    @abstractmethod
    def get_delay(self, attempt: int) -> float:
        """Get delay for given attempt number.

        Args:
            attempt: Attempt number (1-based)

        Returns:
            Delay in seconds
        """
        pass


class FixedDelayStrategy(DelayStrategy):
    """Fixed delay strategy."""

    def __init__(self, delay: float):
        """Initialize fixed delay strategy.

        Args:
            delay: Fixed delay in seconds
        """
        self.delay = delay

    def get_delay(self, attempt: int) -> float:
        """Get fixed delay."""
        return self.delay


class ExponentialBackoffStrategy(DelayStrategy):
    """Exponential backoff delay strategy."""

    def __init__(self, initial_delay: float = 1.0, multiplier: float = 2.0, max_delay: float = 60.0):
        """Initialize exponential backoff strategy.

        Args:
            initial_delay: Initial delay in seconds
            multiplier: Exponential multiplier
            max_delay: Maximum delay in seconds
        """
        self.initial_delay = initial_delay
        self.multiplier = multiplier
        self.max_delay = max_delay

    def get_delay(self, attempt: int) -> float:
        """Get exponential backoff delay."""
        delay = self.initial_delay * (self.multiplier ** (attempt - 1))
        return min(delay, self.max_delay)


class LinearBackoffStrategy(DelayStrategy):
    """Linear backoff delay strategy."""

    def __init__(self, initial_delay: float = 1.0, increment: float = 1.0, max_delay: float = 60.0):
        """Initialize linear backoff strategy.

        Args:
            initial_delay: Initial delay in seconds
            increment: Linear increment
            max_delay: Maximum delay in seconds
        """
        self.initial_delay = initial_delay
        self.increment = increment
        self.max_delay = max_delay

    def get_delay(self, attempt: int) -> float:
        """Get linear backoff delay."""
        delay = self.initial_delay + ((attempt - 1) * self.increment)
        return min(delay, self.max_delay)


class FibonacciBackoffStrategy(DelayStrategy):
    """Fibonacci backoff delay strategy."""

    def __init__(self, base_delay: float = 1.0, max_delay: float = 60.0):
        """Initialize Fibonacci backoff strategy.

        Args:
            base_delay: Base delay multiplier
            max_delay: Maximum delay in seconds
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._fib_cache = {0: 0, 1: 1}

    def _fibonacci(self, n: int) -> int:
        """Calculate nth Fibonacci number with caching."""
        if n in self._fib_cache:
            return self._fib_cache[n]

        self._fib_cache[n] = self._fibonacci(n - 1) + self._fibonacci(n - 2)
        return self._fib_cache[n]

    def get_delay(self, attempt: int) -> float:
        """Get Fibonacci backoff delay."""
        fib_number = self._fibonacci(attempt)
        delay = self.base_delay * fib_number
        return min(delay, self.max_delay)


class CustomBackoffStrategy(DelayStrategy):
    """Custom backoff delay strategy."""

    def __init__(self, delay_function: Callable[[int], float], max_delay: float = 60.0):
        """Initialize custom backoff strategy.

        Args:
            delay_function: Function that takes attempt number and returns delay
            max_delay: Maximum delay in seconds
        """
        self.delay_function = delay_function
        self.max_delay = max_delay

    def get_delay(self, attempt: int) -> float:
        """Get custom delay."""
        delay = self.delay_function(attempt)
        return min(delay, self.max_delay)


class RetryCondition(ABC):
    """Abstract base class for retry conditions."""

    @abstractmethod
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if retry should be attempted.

        Args:
            exception: Exception that occurred
            attempt: Current attempt number

        Returns:
            True if retry should be attempted
        """
        pass


class ExceptionBasedCondition(RetryCondition):
    """Retry condition based on exception types."""

    def __init__(self, exceptions: list[type[Exception]]):
        """Initialize exception-based condition.

        Args:
            exceptions: List of exception types to retry on
        """
        self.exceptions = tuple(exceptions)

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Check if exception type should trigger retry."""
        return isinstance(exception, self.exceptions)


class AttemptBasedCondition(RetryCondition):
    """Retry condition based on attempt count."""

    def __init__(self, max_attempts: int):
        """Initialize attempt-based condition.

        Args:
            max_attempts: Maximum number of attempts
        """
        self.max_attempts = max_attempts

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Check if attempt count allows retry."""
        return attempt < self.max_attempts


class CombinedCondition(RetryCondition):
    """Combined retry condition using multiple conditions."""

    def __init__(self, conditions: list[RetryCondition]):
        """Initialize combined condition.

        Args:
            conditions: List of conditions (all must be true)
        """
        self.conditions = conditions

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Check if all conditions allow retry."""
        return all(condition.should_retry(exception, attempt) for condition in self.conditions)


class CustomCondition(RetryCondition):
    """Custom retry condition."""

    def __init__(self, condition_func: Callable[[Exception, int], bool]):
        """Initialize custom condition.

        Args:
            condition_func: Function to determine retry eligibility
        """
        self.condition_func = condition_func

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Use custom function to determine retry."""
        return self.condition_func(exception, attempt)


class Retry:
    """Retry context manager and utility."""

    def __init__(
        self,
        max_attempts: int = 3,
        delay: float = 1.0,
        backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
        multiplier: float = 2.0,
        max_delay: float = 60.0,
        jitter: bool = True,
        jitter_range: float = 0.1,
        exceptions: list[type[Exception]] | None = None,
        retry_condition: Callable | None = None,
        before_retry: Callable | None = None,
        after_retry: Callable | None = None,
        on_final_failure: Callable | None = None,
    ):
        """Initialize retry manager.

        Args:
            max_attempts: Maximum retry attempts
            delay: Base delay between retries
            backoff: Backoff strategy
            multiplier: Backoff multiplier
            max_delay: Maximum delay
            jitter: Whether to add jitter
            jitter_range: Jitter range (percentage)
            exceptions: Exception types to retry on
            retry_condition: Custom retry condition function
            before_retry: Callback before retry
            after_retry: Callback after retry
            on_final_failure: Callback on final failure
        """
        if max_attempts < 0:
            raise ValueError("max_attempts must be non-negative")

        self.max_attempts = max_attempts
        self.delay = delay
        self.backoff = backoff
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.jitter = jitter
        self.jitter_range = jitter_range
        self.exceptions = exceptions or [Exception]
        self.retry_condition = retry_condition
        self.before_retry = before_retry
        self.after_retry = after_retry
        self.on_final_failure = on_final_failure

        # State
        self.current_attempt = 0
        self._stats = {"total_attempts": 0, "success": False, "last_exception": None}

        # Initialize delay strategy
        self._delay_strategy = self._create_delay_strategy()

        # Initialize retry condition
        self._retry_condition = self._create_retry_condition()

    def _create_delay_strategy(self) -> DelayStrategy:
        """Create delay strategy based on configuration."""
        if self.backoff == BackoffStrategy.FIXED:
            return FixedDelayStrategy(self.delay)
        elif self.backoff == BackoffStrategy.LINEAR:
            return LinearBackoffStrategy(self.delay, self.multiplier, self.max_delay)
        elif self.backoff == BackoffStrategy.EXPONENTIAL:
            return ExponentialBackoffStrategy(self.delay, self.multiplier, self.max_delay)
        elif self.backoff == BackoffStrategy.FIBONACCI:
            return FibonacciBackoffStrategy(self.delay, self.max_delay)
        else:
            # Default to exponential
            return ExponentialBackoffStrategy(self.delay, self.multiplier, self.max_delay)

    def _create_retry_condition(self) -> RetryCondition:
        """Create retry condition based on configuration."""
        conditions = []

        # Add attempt-based condition
        conditions.append(AttemptBasedCondition(self.max_attempts))

        # Add exception-based condition
        if self.exceptions:
            conditions.append(ExceptionBasedCondition(self.exceptions))

        # Add custom condition if provided
        if self.retry_condition:
            conditions.append(CustomCondition(self.retry_condition))

        return CombinedCondition(conditions)

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt."""
        base_delay = self._delay_strategy.get_delay(attempt)

        if base_delay <= 0:
            return 0

        # Add jitter if enabled
        if self.jitter:
            jitter_amount = base_delay * self.jitter_range
            jitter_offset = random.uniform(-jitter_amount, jitter_amount)
            base_delay += jitter_offset

        return max(0, base_delay)

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with retry logic."""
        if exc_type is None:
            # Success
            self._stats["success"] = True
            if self.after_retry:
                try:
                    self.after_retry(self.current_attempt, None, None)
                except Exception as e:
                    logger.warning(f"Error in after_retry callback: {e}")
            return False

        # Exception occurred
        self.current_attempt += 1
        self._stats["total_attempts"] = self.current_attempt
        self._stats["last_exception"] = exc_val

        # Check if we should retry
        if self._retry_condition.should_retry(exc_val, self.current_attempt):
            # Calculate delay
            delay = self._calculate_delay(self.current_attempt)

            # Call before retry callback
            if self.before_retry:
                try:
                    self.before_retry(self.current_attempt, exc_val, delay)
                except Exception as e:
                    logger.warning(f"Error in before_retry callback: {e}")

            # Wait before retry
            if delay > 0:
                await asyncio.sleep(delay)

            # Call after retry callback
            if self.after_retry:
                try:
                    self.after_retry(self.current_attempt, None, exc_val)
                except Exception as e:
                    logger.warning(f"Error in after_retry callback: {e}")

            # Suppress exception to retry
            return True
        else:
            # No more retries, call final failure callback
            if self.on_final_failure:
                try:
                    self.on_final_failure(self.current_attempt, exc_val)
                except Exception as e:
                    logger.warning(f"Error in on_final_failure callback: {e}")

            # Raise RetryExhaustedError
            raise RetryExhaustedError(
                f"Retry exhausted after {self.current_attempt} attempts. Last exception: {exc_val}", exc_val
            ) from exc_val

    def get_stats(self) -> dict[str, Any]:
        """Get retry statistics."""
        return self._stats.copy()


class RetryableOperation:
    """Wrapper for retryable operations."""

    def __init__(self, function: Callable, max_attempts: int = 3, delay: float = 1.0, **retry_kwargs):
        """Initialize retryable operation.

        Args:
            function: Function to make retryable
            max_attempts: Maximum retry attempts
            delay: Base delay between retries
            **retry_kwargs: Additional retry configuration
        """
        self.function = function
        self.max_attempts = max_attempts
        self.delay = delay
        self.retry_kwargs = retry_kwargs

        # State
        self.attempt_count = 0
        self.last_exception = None
        self.last_result = None

    async def execute(self, *args, **kwargs) -> Any:
        """Execute the operation with retry logic.

        Args:
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Raises:
            RetryExhaustedError: If all retry attempts fail
        """
        retry_manager = Retry(max_attempts=self.max_attempts, delay=self.delay, **self.retry_kwargs)

        while True:
            try:
                async with retry_manager:
                    self.attempt_count = retry_manager.current_attempt + 1

                    if asyncio.iscoroutinefunction(self.function):
                        result = await self.function(*args, **kwargs)
                    else:
                        result = self.function(*args, **kwargs)

                    self.last_result = result
                    return result

            except RetryExhaustedError:
                self.last_exception = retry_manager._stats["last_exception"]
                raise

    def get_stats(self) -> dict[str, Any]:
        """Get operation statistics."""
        return {
            "max_attempts": self.max_attempts,
            "attempt_count": self.attempt_count,
            "success": self.last_exception is None,
            "last_result": self.last_result,
            "last_exception": self.last_exception,
        }


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
    multiplier: float = 2.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    jitter_range: float = 0.1,
    exceptions: list[type[Exception]] | None = None,
    retry_condition: Callable | None = None,
    before_retry: Callable | None = None,
    after_retry: Callable | None = None,
    on_final_failure: Callable | None = None,
    metrics: Optional["RetryMetrics"] = None,
    alerting: Optional["RetryAlerting"] = None,
):
    """Decorator for adding retry functionality to functions.

    Args:
        max_attempts: Maximum retry attempts
        delay: Base delay between retries
        backoff: Backoff strategy
        multiplier: Backoff multiplier
        max_delay: Maximum delay
        jitter: Whether to add jitter
        jitter_range: Jitter range (percentage)
        exceptions: Exception types to retry on
        retry_condition: Custom retry condition function
        before_retry: Callback before retry
        after_retry: Callback after retry
        on_final_failure: Callback on final failure
        metrics: Metrics collector
        alerting: Alerting system

    Returns:
        Decorated function
    """

    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                retry_manager = Retry(
                    max_attempts=max_attempts,
                    delay=delay,
                    backoff=backoff,
                    multiplier=multiplier,
                    max_delay=max_delay,
                    jitter=jitter,
                    jitter_range=jitter_range,
                    exceptions=exceptions,
                    retry_condition=retry_condition,
                    before_retry=before_retry,
                    after_retry=after_retry,
                    on_final_failure=on_final_failure,
                )

                start_time = time.time()

                try:
                    while True:
                        try:
                            async with retry_manager:
                                result = await func(*args, **kwargs)

                                # Record success metrics
                                if metrics:
                                    duration = time.time() - start_time
                                    metrics.record_operation(
                                        func.__name__,
                                        attempts=retry_manager.current_attempt + 1,
                                        duration=duration,
                                        success=True,
                                    )

                                return result

                        except RetryExhaustedError as e:
                            # Record failure metrics
                            if metrics:
                                duration = time.time() - start_time
                                metrics.record_operation(
                                    func.__name__,
                                    attempts=retry_manager.current_attempt,
                                    duration=duration,
                                    success=False,
                                )

                            # Check alerting
                            if alerting:
                                alerting.check_failures(func.__name__)

                            raise e.last_exception or e

                except Exception:
                    # Unexpected error
                    if metrics:
                        duration = time.time() - start_time
                        metrics.record_operation(func.__name__, attempts=1, duration=duration, success=False)
                    raise

            return async_wrapper

        else:

            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                # Convert sync function to async and run
                async def async_func():
                    return func(*args, **kwargs)

                return asyncio.run(async_wrapper(*args, **kwargs))

            return sync_wrapper

    return decorator


class RetryMetrics:
    """Metrics collection for retry operations."""

    def __init__(self):
        """Initialize retry metrics."""
        self._operations = {}
        self._total_operations = 0
        self._successful_operations = 0
        self._failed_operations = 0
        self._total_attempts = 0

    def record_operation(self, operation_name: str, attempts: int, duration: float, success: bool):
        """Record retry operation metrics.

        Args:
            operation_name: Name of the operation
            attempts: Number of attempts made
            duration: Total duration
            success: Whether operation succeeded
        """
        if operation_name not in self._operations:
            self._operations[operation_name] = {
                "total_operations": 0,
                "successful_operations": 0,
                "failed_operations": 0,
                "total_attempts": 0,
                "total_duration": 0.0,
                "attempts_list": [],
                "durations_list": [],
            }

        op_stats = self._operations[operation_name]
        op_stats["total_operations"] += 1
        op_stats["total_attempts"] += attempts
        op_stats["total_duration"] += duration
        op_stats["attempts_list"].append(attempts)
        op_stats["durations_list"].append(duration)

        if success:
            op_stats["successful_operations"] += 1
            self._successful_operations += 1
        else:
            op_stats["failed_operations"] += 1
            self._failed_operations += 1

        self._total_operations += 1
        self._total_attempts += attempts

    def get_stats(self) -> dict[str, Any]:
        """Get retry metrics statistics."""
        return {
            "total_operations": self._total_operations,
            "successful_operations": self._successful_operations,
            "failed_operations": self._failed_operations,
            "total_attempts": self._total_attempts,
            "average_attempts": self._total_attempts / self._total_operations if self._total_operations > 0 else 0,
            "success_rate": self._successful_operations / self._total_operations if self._total_operations > 0 else 0,
            "operations": self._operations,
        }


class RetryPerformanceTracker:
    """Performance tracking for retry operations."""

    def __init__(self):
        """Initialize performance tracker."""
        self._performance_data = {}

    def record_operation(self, operation_name: str, attempts: int, duration: float, success: bool):
        """Record operation performance.

        Args:
            operation_name: Name of the operation
            attempts: Number of attempts
            duration: Total duration
            success: Whether operation succeeded
        """
        if operation_name not in self._performance_data:
            self._performance_data[operation_name] = {
                "operations": [],
                "total_operations": 0,
                "successful_operations": 0,
                "failed_operations": 0,
            }

        data = self._performance_data[operation_name]
        data["operations"].append(
            {"attempts": attempts, "duration": duration, "success": success, "timestamp": time.time()}
        )

        data["total_operations"] += 1
        if success:
            data["successful_operations"] += 1
        else:
            data["failed_operations"] += 1

    def get_performance_report(self) -> dict[str, Any]:
        """Get performance report."""
        report = {}

        for operation_name, data in self._performance_data.items():
            operations = data["operations"]
            if not operations:
                continue

            attempts = [op["attempts"] for op in operations]
            durations = [op["duration"] for op in operations]

            report[operation_name] = {
                "total_operations": data["total_operations"],
                "successful_operations": data["successful_operations"],
                "failed_operations": data["failed_operations"],
                "success_rate": data["successful_operations"] / data["total_operations"],
                "average_attempts": sum(attempts) / len(attempts),
                "average_duration": sum(durations) / len(durations),
                "min_attempts": min(attempts),
                "max_attempts": max(attempts),
                "min_duration": min(durations),
                "max_duration": max(durations),
            }

        return report


class RetryAlerting:
    """Alerting system for retry failures."""

    def __init__(self, failure_threshold: int = 5, alert_handler: Callable | None = None):
        """Initialize retry alerting.

        Args:
            failure_threshold: Number of failures before alerting
            alert_handler: Function to handle alerts
        """
        self.failure_threshold = failure_threshold
        self.alert_handler = alert_handler
        self._failure_counts = {}

    def check_failures(self, operation_name: str):
        """Check for excessive failures and trigger alerts.

        Args:
            operation_name: Name of the operation
        """
        if operation_name not in self._failure_counts:
            self._failure_counts[operation_name] = 0

        self._failure_counts[operation_name] += 1

        if self._failure_counts[operation_name] >= self.failure_threshold:
            # Trigger alert
            if self.alert_handler:
                alert_data = {
                    "type": "excessive_retries",
                    "operation": operation_name,
                    "failure_count": self._failure_counts[operation_name],
                    "threshold": self.failure_threshold,
                    "timestamp": time.time(),
                }

                try:
                    self.alert_handler(alert_data)
                except Exception as e:
                    logger.error(f"Error in alert handler: {e}")

            # Reset counter
            self._failure_counts[operation_name] = 0


# Global retry metrics instance
_global_metrics = RetryMetrics()


def get_retry_metrics() -> RetryMetrics:
    """Get global retry metrics instance."""
    return _global_metrics
