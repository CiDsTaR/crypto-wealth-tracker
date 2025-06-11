"""Tests for error handlers module."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from wallet_tracker.errors.exceptions import (
    APIError,
    ErrorCategory,
    ErrorSeverity,
    NetworkError,
    RateLimitError,
    RecoveryStrategy,
    WalletTrackerError,
)
from wallet_tracker.errors.handlers import (
    APIErrorHandler,
    # Circuit breaker
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
    # Main classes
    ErrorHandler,
    ErrorHandlerError,
    ErrorReport,
    # Error statistics
    ErrorStats,
    NetworkErrorHandler,
    ProcessingErrorHandler,
    # Specialized errors
    RetryExhaustedError,
    error_context,
    get_api_error_handler,
    # Global handlers
    get_global_error_handler,
    get_network_error_handler,
    get_processing_error_handler,
    # Utility functions
    handle_and_log_error,
    # Decorators and context managers
    handle_errors,
    setup_error_callbacks,
    setup_error_logging,
)


class TestErrorHandler:
    """Test ErrorHandler class."""

    @pytest.fixture
    def error_handler(self):
        """Create error handler for testing."""
        return ErrorHandler(
            max_retries=3,
            base_delay=0.1,  # Short delay for testing
            max_delay=1.0,
            jitter=False,  # Disable jitter for predictable tests
            enable_circuit_breaker=True,
        )

    @pytest.mark.asyncio
    async def test_handle_error_basic(self, error_handler):
        """Test basic error handling."""
        original_error = ValueError("Test error")

        result = await error_handler.handle_error(
            error=original_error, context={"test": "context"}, operation_name="test_operation"
        )

        assert isinstance(result, WalletTrackerError)
        assert result.original_error == original_error
        assert result.context["test"] == "context"
        assert result.context["operation"] == "test_operation"

    @pytest.mark.asyncio
    async def test_handle_error_with_wallet_tracker_error(self, error_handler):
        """Test handling WalletTrackerError directly."""
        original_error = NetworkError("Network failed")

        result = await error_handler.handle_error(error=original_error, operation_name="network_test")

        assert result == original_error
        assert result.context["operation"] == "network_test"

    def test_register_error_callback(self, error_handler):
        """Test registering error callbacks."""
        callback_called = []

        def test_callback(error):
            callback_called.append(error)

        error_handler.register_error_callback(test_callback)

        # Verify callback is registered
        assert test_callback in error_handler._error_callbacks

    def test_register_recovery_callback(self, error_handler):
        """Test registering recovery callbacks."""
        callback_called = []

        async def test_recovery(error):
            callback_called.append(error)

        error_handler.register_recovery_callback(RecoveryStrategy.FALLBACK, test_recovery)

        # Verify callback is registered
        assert test_recovery in error_handler._recovery_callbacks[RecoveryStrategy.FALLBACK]

    @pytest.mark.asyncio
    async def test_handle_operation_context_manager_success(self, error_handler):
        """Test successful operation with context manager."""
        async with error_handler.handle_operation("test_op") as attempt:
            assert attempt == 0  # First attempt
            result = "success"

        # Should complete without error
        assert result == "success"

    @pytest.mark.asyncio
    async def test_handle_operation_context_manager_retry(self, error_handler):
        """Test operation retry with context manager."""
        attempts = []

        try:
            async with error_handler.handle_operation("retry_test", max_retries=2) as attempt:
                attempts.append(attempt)
                if attempt < 2:  # Fail first two attempts
                    raise NetworkError("Network failed")
                # Success on third attempt
        except WalletTrackerError:
            pass  # Expected to fail after max retries

        # Should have attempted 3 times (0, 1, 2)
        assert len(attempts) == 3
        assert attempts == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_handle_operation_non_retryable_error(self, error_handler):
        """Test operation with non-retryable error."""
        attempts = []

        with pytest.raises(WalletTrackerError):
            async with error_handler.handle_operation("non_retryable_test") as attempt:
                attempts.append(attempt)
                # Raise non-retryable error
                raise WalletTrackerError("Non-retryable error", recovery_strategy=RecoveryStrategy.NONE)

        # Should only attempt once
        assert len(attempts) == 1

    def test_calculate_delay_exponential_backoff(self, error_handler):
        """Test delay calculation for exponential backoff."""
        # Test exponential backoff strategy
        delay1 = error_handler._calculate_delay(0, RecoveryStrategy.EXPONENTIAL_BACKOFF)
        delay2 = error_handler._calculate_delay(1, RecoveryStrategy.EXPONENTIAL_BACKOFF)
        delay3 = error_handler._calculate_delay(2, RecoveryStrategy.EXPONENTIAL_BACKOFF)

        assert delay1 == 0.1  # base_delay
        assert delay2 == 0.2  # base_delay * 2
        assert delay3 == 0.4  # base_delay * 4

    def test_calculate_delay_simple_retry(self, error_handler):
        """Test delay calculation for simple retry."""
        delay = error_handler._calculate_delay(3, RecoveryStrategy.RETRY)
        assert delay == 0.1  # base_delay, no exponential growth

    def test_get_error_stats(self, error_handler):
        """Test error statistics retrieval."""
        stats = error_handler.get_error_stats()
        assert isinstance(stats, dict)
        # Initially empty
        assert len(stats) == 0

    def test_get_circuit_breaker_stats(self, error_handler):
        """Test circuit breaker statistics."""
        # Create a circuit breaker by accessing it
        cb = error_handler._get_circuit_breaker("test_operation")

        stats = error_handler.get_circuit_breaker_stats()
        assert "test_operation" in stats
        assert stats["test_operation"]["state"] == "closed"
        assert stats["test_operation"]["failure_count"] == 0

    def test_reset_circuit_breaker(self, error_handler):
        """Test circuit breaker reset."""
        # Create and fail a circuit breaker
        cb = error_handler._get_circuit_breaker("test_op")
        cb.record_failure()

        # Reset it
        result = error_handler.reset_circuit_breaker("test_op")
        assert result is True
        assert cb.failure_count == 0

        # Test resetting non-existent breaker
        result = error_handler.reset_circuit_breaker("nonexistent")
        assert result is False

    def test_clear_error_stats(self, error_handler):
        """Test clearing error statistics."""
        # Add some stats (simulate error tracking)
        error_handler._error_stats["test:error"] = ErrorStats()

        error_handler.clear_error_stats()
        assert len(error_handler._error_stats) == 0


class TestCircuitBreaker:
    """Test CircuitBreaker class."""

    @pytest.fixture
    def circuit_breaker(self):
        """Create circuit breaker for testing."""
        return CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=1.0,  # Short timeout for testing
            expected_exception=Exception,
        )

    def test_initial_state(self, circuit_breaker):
        """Test circuit breaker initial state."""
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.success_count == 0
        assert circuit_breaker.can_attempt() is True

    def test_record_success(self, circuit_breaker):
        """Test recording successful operations."""
        circuit_breaker.record_success()

        assert circuit_breaker.success_count == 1
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.state == CircuitState.CLOSED

    def test_record_failure_below_threshold(self, circuit_breaker):
        """Test recording failures below threshold."""
        circuit_breaker.record_failure()
        circuit_breaker.record_failure()

        assert circuit_breaker.failure_count == 2
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.can_attempt() is True

    def test_record_failure_exceeds_threshold(self, circuit_breaker):
        """Test recording failures that exceed threshold."""
        # Reach threshold
        for _ in range(3):
            circuit_breaker.record_failure()

        assert circuit_breaker.failure_count == 3
        assert circuit_breaker.state == CircuitState.OPEN
        assert circuit_breaker.can_attempt() is False
        assert circuit_breaker.next_attempt_time is not None

    def test_half_open_state_success(self, circuit_breaker):
        """Test half-open state with successful recovery."""
        # Open the circuit
        for _ in range(3):
            circuit_breaker.record_failure()

        # Simulate timeout passing
        circuit_breaker.next_attempt_time = datetime.now(UTC) - timedelta(seconds=1)

        # Should allow attempt (half-open)
        assert circuit_breaker.can_attempt() is True
        assert circuit_breaker.state == CircuitState.HALF_OPEN

        # Success should close the circuit
        circuit_breaker.record_success()
        assert circuit_breaker.state == CircuitState.CLOSED

    def test_half_open_state_failure(self, circuit_breaker):
        """Test half-open state with failure."""
        # Open the circuit
        for _ in range(3):
            circuit_breaker.record_failure()

        # Simulate timeout passing
        circuit_breaker.next_attempt_time = datetime.now(UTC) - timedelta(seconds=1)

        # Should allow attempt (half-open)
        assert circuit_breaker.can_attempt() is True
        assert circuit_breaker.state == CircuitState.HALF_OPEN

        # Failure should re-open the circuit
        circuit_breaker.record_failure()
        assert circuit_breaker.state == CircuitState.OPEN

    def test_manual_reset(self, circuit_breaker):
        """Test manual circuit breaker reset."""
        # Open the circuit
        for _ in range(3):
            circuit_breaker.record_failure()

        assert circuit_breaker.state == CircuitState.OPEN

        # Manual reset
        circuit_breaker.reset()
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0
        assert circuit_breaker.next_attempt_time is None


class TestErrorStats:
    """Test ErrorStats class."""

    @pytest.fixture
    def error_stats(self):
        """Create error stats for testing."""
        return ErrorStats()

    def test_initial_state(self, error_stats):
        """Test error stats initial state."""
        assert error_stats.total_count == 0
        assert error_stats.first_occurrence is None
        assert error_stats.last_occurrence is None
        assert len(error_stats.severity_distribution) == 0
        assert len(error_stats.context_frequency) == 0
        assert len(error_stats.hourly_occurrences) == 0

    def test_record_error(self, error_stats):
        """Test recording error occurrence."""
        error = WalletTrackerError("Test error", severity=ErrorSeverity.HIGH, context={"key": "value", "type": "test"})

        error_stats.record_error(error)

        assert error_stats.total_count == 1
        assert error_stats.first_occurrence is not None
        assert error_stats.last_occurrence is not None
        assert error_stats.severity_distribution["high"] == 1
        assert error_stats.context_frequency["key:value"] == 1
        assert error_stats.context_frequency["type:test"] == 1
        assert len(error_stats.hourly_occurrences) == 1

    def test_multiple_error_recording(self, error_stats):
        """Test recording multiple errors."""
        error1 = WalletTrackerError("Error 1", severity=ErrorSeverity.HIGH)
        error2 = WalletTrackerError("Error 2", severity=ErrorSeverity.MEDIUM)
        error3 = WalletTrackerError("Error 3", severity=ErrorSeverity.HIGH)

        error_stats.record_error(error1)
        error_stats.record_error(error2)
        error_stats.record_error(error3)

        assert error_stats.total_count == 3
        assert error_stats.severity_distribution["high"] == 2
        assert error_stats.severity_distribution["medium"] == 1

    def test_get_hourly_rate(self, error_stats):
        """Test hourly rate calculation."""
        # No errors yet
        assert error_stats.get_hourly_rate() == 0.0

        # Add an error
        error = WalletTrackerError("Test error")
        error_stats.record_error(error)

        # Should have a rate > 0 (exact value depends on timing)
        rate = error_stats.get_hourly_rate()
        assert rate >= 0

    def test_get_most_common_context(self, error_stats):
        """Test most common context retrieval."""
        # No errors yet
        assert error_stats.get_most_common_context() is None

        # Add errors with context
        error1 = WalletTrackerError("Error 1", context={"type": "network"})
        error2 = WalletTrackerError("Error 2", context={"type": "network"})
        error3 = WalletTrackerError("Error 3", context={"type": "validation"})

        error_stats.record_error(error1)
        error_stats.record_error(error2)
        error_stats.record_error(error3)

        most_common = error_stats.get_most_common_context()
        assert most_common == "type:network"


class TestSpecializedErrorHandlers:
    """Test specialized error handler subclasses."""

    def test_api_error_handler(self):
        """Test APIErrorHandler configuration."""
        handler = APIErrorHandler()

        assert handler.max_retries == 5
        assert handler.base_delay == 2.0
        assert handler.max_delay == 120.0

        # Check that rate limit recovery is registered
        assert RecoveryStrategy.EXPONENTIAL_BACKOFF in handler._recovery_callbacks

    def test_network_error_handler(self):
        """Test NetworkErrorHandler configuration."""
        handler = NetworkErrorHandler()

        assert handler.max_retries == 3
        assert handler.base_delay == 1.0
        assert handler.max_delay == 30.0
        assert handler.jitter is True

    def test_processing_error_handler(self):
        """Test ProcessingErrorHandler configuration."""
        handler = ProcessingErrorHandler()

        assert handler.max_retries == 2
        assert handler.base_delay == 0.5
        assert handler.max_delay == 10.0
        assert handler.enable_circuit_breaker is False

        # Check that fallback recovery is registered
        assert RecoveryStrategy.FALLBACK in handler._recovery_callbacks


class TestGlobalHandlers:
    """Test global handler functions."""

    def test_get_global_error_handler(self):
        """Test global error handler singleton."""
        handler1 = get_global_error_handler()
        handler2 = get_global_error_handler()

        assert handler1 is handler2
        assert isinstance(handler1, ErrorHandler)

    def test_get_api_error_handler(self):
        """Test API error handler singleton."""
        handler1 = get_api_error_handler()
        handler2 = get_api_error_handler()

        assert handler1 is handler2
        assert isinstance(handler1, APIErrorHandler)

    def test_get_network_error_handler(self):
        """Test network error handler singleton."""
        handler1 = get_network_error_handler()
        handler2 = get_network_error_handler()

        assert handler1 is handler2
        assert isinstance(handler1, NetworkErrorHandler)

    def test_get_processing_error_handler(self):
        """Test processing error handler singleton."""
        handler1 = get_processing_error_handler()
        handler2 = get_processing_error_handler()

        assert handler1 is handler2
        assert isinstance(handler1, ProcessingErrorHandler)


class TestErrorDecorators:
    """Test error handling decorators."""

    @pytest.mark.asyncio
    async def test_handle_errors_decorator_success(self):
        """Test handle_errors decorator with successful function."""
        call_count = []

        @handle_errors(operation_name="test_function", max_retries=2)
        async def test_function():
            call_count.append(1)
            return "success"

        result = await test_function()

        assert result == "success"
        assert len(call_count) == 1

    @pytest.mark.asyncio
    async def test_handle_errors_decorator_with_retry(self):
        """Test handle_errors decorator with retries."""
        call_count = []

        @handle_errors(operation_name="test_function", max_retries=2)
        async def test_function():
            call_count.append(1)
            if len(call_count) < 3:  # Fail first two attempts
                raise NetworkError("Network failed")
            return "success"

        result = await test_function()

        assert result == "success"
        assert len(call_count) == 3

    @pytest.mark.asyncio
    async def test_error_context_manager_success(self):
        """Test error_context context manager with success."""
        attempts = []

        async with error_context("test_operation", max_retries=2) as attempt:
            attempts.append(attempt)
            result = "success"

        assert result == "success"
        assert len(attempts) == 1
        assert attempts[0] == 0

    @pytest.mark.asyncio
    async def test_error_context_manager_with_retries(self):
        """Test error_context context manager with retries."""
        attempts = []

        try:
            async with error_context("test_operation", max_retries=2) as attempt:
                attempts.append(attempt)
                if attempt < 2:
                    raise NetworkError("Network failed")
                # Success on third attempt
        except WalletTrackerError:
            pass  # Expected if max retries exceeded

        assert len(attempts) == 3  # 0, 1, 2


class TestUtilityFunctions:
    """Test utility functions."""

    @pytest.mark.asyncio
    async def test_handle_and_log_error(self):
        """Test handle_and_log_error function."""
        original_error = ValueError("Test error")

        result = await handle_and_log_error(
            error=original_error, operation_name="test_operation", context={"test": "data"}
        )

        assert isinstance(result, WalletTrackerError)
        assert result.original_error == original_error
        assert result.context["operation"] == "test_operation"
        assert result.context["test"] == "data"

    def test_setup_error_logging(self):
        """Test error logging setup."""
        # This mainly tests that the function runs without error
        setup_error_logging()

        # Verify logger exists
        import logging

        logger = logging.getLogger("wallet_tracker.errors")
        assert logger is not None

    def test_setup_error_callbacks(self):
        """Test error callbacks setup."""
        # This mainly tests that the function runs without error
        setup_error_callbacks()

        # Verify callbacks were registered
        handler = get_global_error_handler()
        assert len(handler._error_callbacks) > 0


class TestSpecializedErrors:
    """Test specialized error classes."""

    def test_retry_exhausted_error(self):
        """Test RetryExhaustedError."""
        last_error = ValueError("Last attempt failed")

        error = RetryExhaustedError(operation_name="test_operation", attempts=3, last_error=last_error)

        assert error.severity == ErrorSeverity.HIGH
        assert error.category == ErrorCategory.SYSTEM_RESOURCE
        assert error.recovery_strategy == RecoveryStrategy.USER_INTERVENTION
        assert error.context["operation_name"] == "test_operation"
        assert error.context["attempts"] == 3
        assert error.original_error == last_error

    def test_error_handler_error(self):
        """Test ErrorHandlerError."""
        error = ErrorHandlerError("Handler malfunction")

        assert error.severity == ErrorSeverity.HIGH
        assert error.category == ErrorCategory.SYSTEM_RESOURCE
        assert error.recovery_strategy == RecoveryStrategy.RESTART

    def test_circuit_breaker_open_error(self):
        """Test CircuitBreakerOpenError."""
        error = CircuitBreakerOpenError("Circuit breaker is open")

        assert error.severity == ErrorSeverity.MEDIUM
        assert error.category == ErrorCategory.SYSTEM_RESOURCE
        assert error.recovery_strategy == RecoveryStrategy.EXPONENTIAL_BACKOFF


class TestErrorReport:
    """Test ErrorReport class."""

    @pytest.fixture
    def error_handler_with_stats(self):
        """Create error handler with some stats."""
        handler = ErrorHandler()

        # Simulate some errors
        handler._error_stats["network:timeout"] = ErrorStats()
        handler._error_stats["api:rate_limit"] = ErrorStats()

        # Add some data to stats
        network_error = WalletTrackerError(
            "Network timeout", severity=ErrorSeverity.MEDIUM, category=ErrorCategory.NETWORK
        )
        handler._error_stats["network:timeout"].record_error(network_error)

        api_error = WalletTrackerError(
            "Rate limit exceeded", severity=ErrorSeverity.HIGH, category=ErrorCategory.API_LIMIT
        )
        handler._error_stats["api:rate_limit"].record_error(api_error)
        handler._error_stats["api:rate_limit"].record_error(api_error)  # Record twice

        return handler

    def test_error_report_generation(self, error_handler_with_stats):
        """Test error report generation."""
        report = ErrorReport(error_handler_with_stats)
        summary = report.generate_summary(time_range_hours=24)

        assert "time_range_hours" in summary
        assert "total_errors" in summary
        assert "category_distribution" in summary
        assert "severity_distribution" in summary
        assert "top_errors" in summary
        assert "circuit_breaker_status" in summary
        assert "generated_at" in summary

        assert summary["total_errors"] == 3  # 1 + 2 errors
        assert summary["category_distribution"]["network"] == 1
        assert summary["category_distribution"]["api_limit"] == 2

    def test_error_report_recommendations(self, error_handler_with_stats):
        """Test error report recommendations."""
        # Mock high error rates
        handler = error_handler_with_stats
        handler._error_stats["high_rate:error"] = ErrorStats()

        # Mock the hourly rate to be high
        with patch.object(handler._error_stats["high_rate:error"], "get_hourly_rate", return_value=15):
            report = ErrorReport(handler)
            recommendations = report.get_recommendations()

            assert len(recommendations) > 0
            assert any("high error rates" in rec.lower() for rec in recommendations)

    def test_error_report_no_issues(self):
        """Test error report with no issues."""
        clean_handler = ErrorHandler()
        report = ErrorReport(clean_handler)
        recommendations = report.get_recommendations()

        assert len(recommendations) == 1
        assert "no immediate issues" in recommendations[0].lower()


class TestIntegrationScenarios:
    """Test integration scenarios combining multiple components."""

    @pytest.mark.asyncio
    async def test_full_error_handling_flow(self):
        """Test complete error handling flow."""
        handler = ErrorHandler(max_retries=2, base_delay=0.01)

        # Register callbacks
        callback_calls = []

        def error_callback(error):
            callback_calls.append(error)

        handler.register_error_callback(error_callback)

        # Test operation that fails then succeeds
        attempt_count = []

        async with handler.handle_operation("integration_test") as attempt:
            attempt_count.append(attempt)
            if attempt < 1:  # Fail first attempt
                raise NetworkError("Network failed")
            # Success on second attempt

        assert len(attempt_count) == 2
        assert len(callback_calls) == 1  # One error callback
        assert isinstance(callback_calls[0], NetworkError)

    @pytest.mark.asyncio
    async def test_circuit_breaker_integration(self):
        """Test circuit breaker integration with error handling."""
        handler = ErrorHandler(max_retries=1, base_delay=0.01, enable_circuit_breaker=True)

        # Get circuit breaker and set low threshold for testing
        cb = handler._get_circuit_breaker("cb_test")
        cb.failure_threshold = 2

        # Fail enough times to open circuit breaker
        for i in range(3):
            try:
                async with handler.handle_operation("cb_test") as attempt:
                    raise NetworkError(f"Failure {i}")
            except:
                pass

        # Circuit should be open now
        assert cb.state == CircuitState.OPEN

        # Next attempt should fail immediately
        with pytest.raises(CircuitBreakerOpenError):
            async with handler.handle_operation("cb_test") as attempt:
                pass  # Should not reach here

    @pytest.mark.asyncio
    async def test_error_stats_integration(self):
        """Test error statistics integration."""
        handler = ErrorHandler()

        # Generate some errors
        errors = [
            NetworkError("Network timeout"),
            APIError("API failed"),
            NetworkError("Network connection lost"),
            RateLimitError("Rate limited"),
        ]

        for error in errors:
            await handler.handle_error(error, operation_name="stats_test")

        # Check stats
        stats = handler.get_error_stats()

        # Should have entries for different error types
        assert len(stats) >= 3  # Network, API, Rate limit categories

        # Check that stats contain expected information
        for key, stat_data in stats.items():
            assert "total_count" in stat_data
            assert "severity_distribution" in stat_data
            assert stat_data["total_count"] > 0
