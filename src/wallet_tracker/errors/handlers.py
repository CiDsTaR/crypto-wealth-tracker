"""Error handlers and recovery mechanisms."""

import asyncio
import logging
import random
import traceback
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from .exceptions import (
    ErrorCategory,
    ErrorSeverity,
    RecoveryStrategy,
    WalletTrackerError,
    create_error_from_exception,
)

logger = logging.getLogger(__name__)


class ErrorHandler:
    """
    Global error handler with recovery strategies and logging.

    Provides:
    - Centralized error handling
    - Automatic recovery strategies
    - Error logging and reporting
    - Circuit breaker patterns
    - Retry mechanisms with backoff
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        jitter: bool = True,
        enable_circuit_breaker: bool = True,
    ):
        """Initialize error handler.

        Args:
            max_retries: Maximum retry attempts
            base_delay: Base delay for exponential backoff
            max_delay: Maximum delay between retries
            jitter: Whether to add random jitter to delays
            enable_circuit_breaker: Whether to enable circuit breaker
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self.enable_circuit_breaker = enable_circuit_breaker

        # Circuit breaker state
        self._circuit_breakers: dict[str, CircuitBreaker] = {}

        # Error statistics
        self._error_stats: dict[str, ErrorStats] = {}

        # Registered error callbacks
        self._error_callbacks: list[Callable[[WalletTrackerError], None]] = []

        # Recovery callbacks
        self._recovery_callbacks: dict[RecoveryStrategy, list[Callable]] = {}

    def register_error_callback(self, callback: Callable[[WalletTrackerError], None]) -> None:
        """Register callback for error notifications.

        Args:
            callback: Function to call when errors occur
        """
        self._error_callbacks.append(callback)

    def register_recovery_callback(self, strategy: RecoveryStrategy, callback: Callable) -> None:
        """Register callback for specific recovery strategy.

        Args:
            strategy: Recovery strategy
            callback: Function to call for recovery
        """
        if strategy not in self._recovery_callbacks:
            self._recovery_callbacks[strategy] = []
        self._recovery_callbacks[strategy].append(callback)

    async def handle_error(
        self, error: Exception, context: dict[str, Any] | None = None, operation_name: str | None = None
    ) -> WalletTrackerError:
        """Handle an error with appropriate recovery strategy.

        Args:
            error: Exception to handle
            context: Additional context information
            operation_name: Name of operation that failed

        Returns:
            Processed WalletTrackerError
        """
        # Convert to our error type if needed
        if isinstance(error, WalletTrackerError):
            wallet_error = error
        else:
            wallet_error = create_error_from_exception(error, context)

        # Add operation context
        if operation_name:
            wallet_error.context["operation"] = operation_name

        # Log the error
        await self._log_error(wallet_error)

        # Update statistics
        self._update_error_stats(wallet_error)

        # Check circuit breaker
        if self.enable_circuit_breaker and operation_name:
            circuit_breaker = self._get_circuit_breaker(operation_name)
            circuit_breaker.record_failure()

        # Notify error callbacks
        for callback in self._error_callbacks:
            try:
                callback(wallet_error)
            except Exception as e:
                logger.warning(f"Error callback failed: {e}")

        # Execute recovery strategy
        await self._execute_recovery_strategy(wallet_error)

        return wallet_error

    async def _log_error(self, error: WalletTrackerError) -> None:
        """Log error with appropriate level based on severity."""
        log_data = {
            "error_code": error.error_code,
            "category": error.category.value,
            "severity": error.severity.value,
            "recovery_strategy": error.recovery_strategy.value,
            "context": error.context,
        }

        if error.severity == ErrorSeverity.CRITICAL:
            logger.critical(f"CRITICAL ERROR [{error.error_code}]: {error.message}", extra=log_data)
        elif error.severity == ErrorSeverity.HIGH:
            logger.error(f"HIGH SEVERITY [{error.error_code}]: {error.message}", extra=log_data)
        elif error.severity == ErrorSeverity.MEDIUM:
            logger.warning(f"MEDIUM SEVERITY [{error.error_code}]: {error.message}", extra=log_data)
        else:
            logger.info(f"LOW SEVERITY [{error.error_code}]: {error.message}", extra=log_data)

        # Log stack trace for critical errors
        if error.severity == ErrorSeverity.CRITICAL and error.original_error:
            logger.critical(
                f"Stack trace: {traceback.format_exception(type(error.original_error), error.original_error, error.original_error.__traceback__)}"
            )

    def _update_error_stats(self, error: WalletTrackerError) -> None:
        """Update error statistics."""
        key = f"{error.category.value}:{error.error_code}"

        if key not in self._error_stats:
            self._error_stats[key] = ErrorStats()

        self._error_stats[key].record_error(error)

    def _get_circuit_breaker(self, operation_name: str) -> "CircuitBreaker":
        """Get or create circuit breaker for operation."""
        if operation_name not in self._circuit_breakers:
            self._circuit_breakers[operation_name] = CircuitBreaker(
                failure_threshold=5, recovery_timeout=60.0, expected_exception=Exception
            )
        return self._circuit_breakers[operation_name]

    async def _execute_recovery_strategy(self, error: WalletTrackerError) -> None:
        """Execute recovery strategy for error."""
        strategy = error.recovery_strategy

        if strategy in self._recovery_callbacks:
            for callback in self._recovery_callbacks[strategy]:
                try:
                    await callback(error)
                except Exception as e:
                    logger.warning(f"Recovery callback failed: {e}")

    @asynccontextmanager
    async def handle_operation(
        self, operation_name: str, max_retries: int | None = None, context: dict[str, Any] | None = None
    ):
        """Context manager for handling operations with automatic retry.

        Args:
            operation_name: Name of the operation
            max_retries: Override default max retries
            context: Additional context
        """
        retries = max_retries or self.max_retries
        last_error = None

        for attempt in range(retries + 1):
            try:
                # Check circuit breaker
                if self.enable_circuit_breaker:
                    circuit_breaker = self._get_circuit_breaker(operation_name)
                    if circuit_breaker.state == CircuitState.OPEN:
                        raise CircuitBreakerOpenError(f"Circuit breaker open for {operation_name}")

                yield attempt

                # Success - record it
                if self.enable_circuit_breaker:
                    circuit_breaker = self._get_circuit_breaker(operation_name)
                    circuit_breaker.record_success()

                return

            except Exception as e:
                last_error = e

                # Handle the error
                wallet_error = await self.handle_error(e, context, operation_name)

                # Check if we should retry
                if attempt < retries and wallet_error.is_retryable():
                    delay = self._calculate_delay(attempt, wallet_error.recovery_strategy)
                    logger.info(f"Retrying {operation_name} in {delay:.2f}s (attempt {attempt + 1}/{retries})")
                    await asyncio.sleep(delay)
                    continue
                else:
                    # No more retries or not retryable
                    raise wallet_error

        # Should not reach here, but just in case
        if last_error:
            raise last_error

    def _calculate_delay(self, attempt: int, strategy: RecoveryStrategy) -> float:
        """Calculate delay for retry based on strategy."""
        if strategy == RecoveryStrategy.EXPONENTIAL_BACKOFF:
            delay = min(self.base_delay * (2**attempt), self.max_delay)
        else:
            delay = self.base_delay

        # Add jitter if enabled
        if self.jitter:
            jitter_range = delay * 0.1  # 10% jitter
            delay += random.uniform(-jitter_range, jitter_range)

        return max(0.1, delay)  # Minimum 0.1 second delay

    def get_error_stats(self) -> dict[str, dict[str, Any]]:
        """Get error statistics."""
        stats = {}

        for key, error_stats in self._error_stats.items():
            stats[key] = {
                "total_count": error_stats.total_count,
                "last_occurrence": error_stats.last_occurrence.isoformat() if error_stats.last_occurrence else None,
                "severity_distribution": error_stats.severity_distribution,
                "hourly_rate": error_stats.get_hourly_rate(),
                "most_common_context": error_stats.get_most_common_context(),
            }

        return stats

    def get_circuit_breaker_stats(self) -> dict[str, dict[str, Any]]:
        """Get circuit breaker statistics."""
        stats = {}

        for operation, breaker in self._circuit_breakers.items():
            stats[operation] = {
                "state": breaker.state.value,
                "failure_count": breaker.failure_count,
                "success_count": breaker.success_count,
                "last_failure_time": breaker.last_failure_time.isoformat() if breaker.last_failure_time else None,
                "next_attempt_time": breaker.next_attempt_time.isoformat() if breaker.next_attempt_time else None,
            }

        return stats

    def reset_circuit_breaker(self, operation_name: str) -> bool:
        """Reset circuit breaker for operation.

        Args:
            operation_name: Name of operation

        Returns:
            True if reset, False if not found
        """
        if operation_name in self._circuit_breakers:
            self._circuit_breakers[operation_name].reset()
            return True
        return False

    def clear_error_stats(self) -> None:
        """Clear all error statistics."""
        self._error_stats.clear()


class ErrorStats:
    """Statistics tracking for specific error types."""

    def __init__(self):
        self.total_count = 0
        self.first_occurrence: datetime | None = None
        self.last_occurrence: datetime | None = None
        self.severity_distribution: dict[str, int] = {}
        self.context_frequency: dict[str, int] = {}
        self.hourly_occurrences: list[datetime] = []

    def record_error(self, error: WalletTrackerError) -> None:
        """Record an error occurrence."""
        now = datetime.now(UTC)

        self.total_count += 1

        if self.first_occurrence is None:
            self.first_occurrence = now
        self.last_occurrence = now

        # Track severity distribution
        severity = error.severity.value
        self.severity_distribution[severity] = self.severity_distribution.get(severity, 0) + 1

        # Track context patterns
        for key, value in error.context.items():
            context_key = f"{key}:{value}"
            self.context_frequency[context_key] = self.context_frequency.get(context_key, 0) + 1

        # Track hourly occurrences (keep last 24 hours)
        self.hourly_occurrences.append(now)
        cutoff = now - timedelta(hours=24)
        self.hourly_occurrences = [t for t in self.hourly_occurrences if t > cutoff]

    def get_hourly_rate(self) -> float:
        """Get error rate per hour over last 24 hours."""
        if not self.hourly_occurrences:
            return 0.0

        hours_elapsed = min(24, (datetime.now(UTC) - self.hourly_occurrences[0]).total_seconds() / 3600)
        if hours_elapsed == 0:
            return 0.0

        return len(self.hourly_occurrences) / hours_elapsed

    def get_most_common_context(self) -> str | None:
        """Get most common context pattern."""
        if not self.context_frequency:
            return None

        return max(self.context_frequency.items(), key=lambda x: x[1])[0]


from enum import Enum


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, rejecting requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """
    Circuit breaker implementation for preventing cascading failures.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Service is failing, requests are rejected immediately
    - HALF_OPEN: Testing if service has recovered
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type[Exception] = Exception,
    ):
        """Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening
            recovery_timeout: Time to wait before testing recovery
            expected_exception: Exception type to monitor
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.success_count = 0
        self.state = CircuitState.CLOSED
        self.last_failure_time: datetime | None = None
        self.next_attempt_time: datetime | None = None

    def record_success(self) -> None:
        """Record a successful operation."""
        self.success_count += 1
        self.failure_count = 0

        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            self.next_attempt_time = None
            logger.info("Circuit breaker closed - service recovered")

    def record_failure(self) -> None:
        """Record a failed operation."""
        self.failure_count += 1
        self.last_failure_time = datetime.now(UTC)

        if (
            self.state == CircuitState.CLOSED
            and self.failure_count >= self.failure_threshold
            or self.state == CircuitState.HALF_OPEN
        ):
            self._open_circuit()

    def _open_circuit(self) -> None:
        """Open the circuit breaker."""
        self.state = CircuitState.OPEN
        self.next_attempt_time = datetime.now(UTC) + timedelta(seconds=self.recovery_timeout)
        logger.warning(f"Circuit breaker opened - {self.failure_count} failures exceeded threshold")

    def can_attempt(self) -> bool:
        """Check if operation can be attempted."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            if self.next_attempt_time and datetime.now(UTC) >= self.next_attempt_time:
                self.state = CircuitState.HALF_OPEN
                logger.info("Circuit breaker half-open - testing service recovery")
                return True
            return False

        # HALF_OPEN state
        return True

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.next_attempt_time = None
        logger.info("Circuit breaker manually reset")


class CircuitBreakerOpenError(WalletTrackerError):
    """Exception raised when circuit breaker is open."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message=message,
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.SYSTEM_RESOURCE,
            recovery_strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF,
            **kwargs,
        )


# Specialized error handlers for different components


class APIErrorHandler(ErrorHandler):
    """Specialized error handler for API operations."""

    def __init__(self, **kwargs):
        super().__init__(max_retries=5, base_delay=2.0, max_delay=120.0, **kwargs)

        # Register specific recovery strategies
        self.register_recovery_callback(RecoveryStrategy.EXPONENTIAL_BACKOFF, self._handle_rate_limit_recovery)

    async def _handle_rate_limit_recovery(self, error: WalletTrackerError) -> None:
        """Handle rate limit recovery."""
        if "retry_after" in error.context:
            retry_after = error.context["retry_after"]
            logger.info(f"Rate limit recovery: waiting {retry_after} seconds")
            await asyncio.sleep(retry_after)


class NetworkErrorHandler(ErrorHandler):
    """Specialized error handler for network operations."""

    def __init__(self, **kwargs):
        super().__init__(max_retries=3, base_delay=1.0, max_delay=30.0, jitter=True, **kwargs)


class ProcessingErrorHandler(ErrorHandler):
    """Specialized error handler for processing operations."""

    def __init__(self, **kwargs):
        super().__init__(
            max_retries=2,
            base_delay=0.5,
            max_delay=10.0,
            enable_circuit_breaker=False,  # Don't use circuit breaker for processing
            **kwargs,
        )

        # Register checkpoint recovery
        self.register_recovery_callback(RecoveryStrategy.FALLBACK, self._handle_checkpoint_recovery)

    async def _handle_checkpoint_recovery(self, error: WalletTrackerError) -> None:
        """Handle recovery using checkpoints."""
        if "checkpoint_data" in error.context:
            logger.info("Attempting recovery from checkpoint")
            # Implementation would depend on specific checkpoint format


# Global error handler instances
_global_error_handler: ErrorHandler | None = None
_api_error_handler: APIErrorHandler | None = None
_network_error_handler: NetworkErrorHandler | None = None
_processing_error_handler: ProcessingErrorHandler | None = None


def get_global_error_handler() -> ErrorHandler:
    """Get global error handler instance."""
    global _global_error_handler
    if _global_error_handler is None:
        _global_error_handler = ErrorHandler()
    return _global_error_handler


def get_api_error_handler() -> APIErrorHandler:
    """Get API error handler instance."""
    global _api_error_handler
    if _api_error_handler is None:
        _api_error_handler = APIErrorHandler()
    return _api_error_handler


def get_network_error_handler() -> NetworkErrorHandler:
    """Get network error handler instance."""
    global _network_error_handler
    if _network_error_handler is None:
        _network_error_handler = NetworkErrorHandler()
    return _network_error_handler


def get_processing_error_handler() -> ProcessingErrorHandler:
    """Get processing error handler instance."""
    global _processing_error_handler
    if _processing_error_handler is None:
        _processing_error_handler = ProcessingErrorHandler()
    return _processing_error_handler


# Decorator for automatic error handling
def handle_errors(
    operation_name: str | None = None,
    handler: ErrorHandler | None = None,
    max_retries: int | None = None,
    context: dict[str, Any] | None = None,
):
    """Decorator for automatic error handling.

    Args:
        operation_name: Name of the operation
        handler: Error handler to use (defaults to global)
        max_retries: Override max retries
        context: Additional context
    """

    def decorator(func):
        async def wrapper(*args, **kwargs):
            error_handler = handler or get_global_error_handler()
            op_name = operation_name or func.__name__

            async with error_handler.handle_operation(op_name, max_retries, context):
                return await func(*args, **kwargs)

        return wrapper

    return decorator


# Context manager for error handling
@asynccontextmanager
async def error_context(
    operation_name: str,
    handler: ErrorHandler | None = None,
    max_retries: int | None = None,
    context: dict[str, Any] | None = None,
):
    """Context manager for error handling.

    Args:
        operation_name: Name of the operation
        handler: Error handler to use
        max_retries: Override max retries
        context: Additional context
    """
    error_handler = handler or get_global_error_handler()

    async with error_handler.handle_operation(operation_name, max_retries, context) as attempt:
        yield attempt


# Utility functions


async def handle_and_log_error(
    error: Exception, operation_name: str, context: dict[str, Any] | None = None, handler: ErrorHandler | None = None
) -> WalletTrackerError:
    """Handle and log an error.

    Args:
        error: Exception to handle
        operation_name: Name of operation
        context: Additional context
        handler: Error handler to use

    Returns:
        Processed WalletTrackerError
    """
    error_handler = handler or get_global_error_handler()
    return await error_handler.handle_error(error, context, operation_name)


def setup_error_logging() -> None:
    """Setup centralized error logging."""
    # Configure logging for error handling
    error_logger = logging.getLogger("wallet_tracker.errors")
    error_logger.setLevel(logging.INFO)

    # Add structured logging handler
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    error_logger.addHandler(handler)


def setup_error_callbacks() -> None:
    """Setup default error callbacks."""
    global_handler = get_global_error_handler()

    # Add callback for critical errors
    def critical_error_callback(error: WalletTrackerError) -> None:
        if error.severity == ErrorSeverity.CRITICAL:
            logger.critical(f"CRITICAL ERROR DETECTED: {error.error_code} - {error.message}")
            # Could send alerts, notifications, etc.

    global_handler.register_error_callback(critical_error_callback)

    # Add callback for authentication errors
    def auth_error_callback(error: WalletTrackerError) -> None:
        if error.category == ErrorCategory.AUTHENTICATION:
            logger.error(f"Authentication error: {error.message}")
            # Could trigger credential refresh, user notification, etc.

    global_handler.register_error_callback(auth_error_callback)


# Exception classes for specific error conditions


class RetryExhaustedError(WalletTrackerError):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, operation_name: str, attempts: int, last_error: Exception, **kwargs):
        message = f"Operation '{operation_name}' failed after {attempts} attempts"

        super().__init__(
            message=message,
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.SYSTEM_RESOURCE,
            recovery_strategy=RecoveryStrategy.USER_INTERVENTION,
            context={"operation_name": operation_name, "attempts": attempts, "last_error": str(last_error)},
            original_error=last_error,
            **kwargs,
        )


class ErrorHandlerError(WalletTrackerError):
    """Errors in the error handling system itself."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message=message,
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.SYSTEM_RESOURCE,
            recovery_strategy=RecoveryStrategy.RESTART,
            **kwargs,
        )


# Error aggregation and reporting


class ErrorReport:
    """Generate error reports and summaries."""

    def __init__(self, error_handler: ErrorHandler):
        self.error_handler = error_handler

    def generate_summary(self, time_range_hours: int = 24) -> dict[str, Any]:
        """Generate error summary report.

        Args:
            time_range_hours: Time range for report in hours

        Returns:
            Error summary report
        """
        error_stats = self.error_handler.get_error_stats()
        circuit_stats = self.error_handler.get_circuit_breaker_stats()

        # Calculate totals
        total_errors = sum(stats["total_count"] for stats in error_stats.values())

        # Group by category
        category_counts = {}
        severity_counts = {}

        for key, stats in error_stats.items():
            category = key.split(":")[0]
            category_counts[category] = category_counts.get(category, 0) + stats["total_count"]

            for severity, count in stats["severity_distribution"].items():
                severity_counts[severity] = severity_counts.get(severity, 0) + count

        # Top errors
        top_errors = sorted(error_stats.items(), key=lambda x: x[1]["total_count"], reverse=True)[:10]

        return {
            "time_range_hours": time_range_hours,
            "total_errors": total_errors,
            "category_distribution": category_counts,
            "severity_distribution": severity_counts,
            "top_errors": [
                {"error_type": key, "count": stats["total_count"], "hourly_rate": stats["hourly_rate"]}
                for key, stats in top_errors
            ],
            "circuit_breaker_status": circuit_stats,
            "generated_at": datetime.now(UTC).isoformat(),
        }

    def get_recommendations(self) -> list[str]:
        """Get recommendations based on error patterns.

        Returns:
            List of recommendations
        """
        recommendations = []
        error_stats = self.error_handler.get_error_stats()
        circuit_stats = self.error_handler.get_circuit_breaker_stats()

        # Check for high error rates
        high_rate_errors = [
            key
            for key, stats in error_stats.items()
            if stats["hourly_rate"] > 10  # More than 10 errors per hour
        ]

        if high_rate_errors:
            recommendations.append(
                f"High error rates detected for: {', '.join(high_rate_errors)}. Consider investigating root causes."
            )

        # Check for open circuit breakers
        open_circuits = [name for name, stats in circuit_stats.items() if stats["state"] == "open"]

        if open_circuits:
            recommendations.append(
                f"Circuit breakers are open for: {', '.join(open_circuits)}. "
                "Check service health and consider manual intervention."
            )

        # Check for critical errors
        critical_errors = [key for key, stats in error_stats.items() if "critical" in stats["severity_distribution"]]

        if critical_errors:
            recommendations.append(
                f"Critical errors detected: {', '.join(critical_errors)}. Immediate attention required."
            )

        if not recommendations:
            recommendations.append("No immediate issues detected. System appears stable.")

        return recommendations
