"""Tests for errors package __init__ module."""

from pathlib import Path

import pytest

from wallet_tracker.errors import (
    ERROR_CODES,
    APIError,
    APIErrorHandler,
    AuthenticationError,
    BatchProcessingError,
    CacheError,
    # Recovery classes
    CheckpointData,
    CheckpointManager,
    CircuitBreaker,
    CircuitState,
    CoinGeckoError,
    CommandLineError,
    # Specific errors
    ConfigurationError,
    DataNotFoundError,
    ErrorCategory,
    # Handler classes
    ErrorHandler,
    ErrorReport,
    ErrorSeverity,
    ErrorStats,
    EthereumClientError,
    GoogleSheetsError,
    InsufficientBalanceError,
    InvalidAddressError,
    NetworkError,
    NetworkErrorHandler,
    ProcessingError,
    ProcessingErrorHandler,
    ProgressTracker,
    RateLimitError,
    RecoveryManager,
    RecoverySession,
    RecoveryStrategy,
    SystemResourceError,
    TimeoutError,
    UserInputError,
    ValidationError,
    # Base error classes
    WalletTrackerError,
    classify_error_severity,
    create_custom_error,
    # Utility functions
    create_error_from_exception,
    create_recovery_context,
    get_api_error_handler,
    get_error_code,
    get_error_name,
    # Package-level functions
    get_global_checkpoint_manager,
    # Global handlers
    get_global_error_handler,
    get_global_recovery_manager,
    get_recovery_strategy,
    handle_and_log_error,
    handle_api_error,
    # Decorators and context managers
    handle_network_operation,
    handle_processing_operation,
    setup_error_callbacks,
    setup_error_logging,
    setup_error_system,
    with_checkpointing,
)


class TestPackageImports:
    """Test that all expected components can be imported."""

    def test_error_classes_import(self):
        """Test that all error classes are importable."""
        # Base classes
        assert WalletTrackerError is not None
        assert ErrorSeverity is not None
        assert ErrorCategory is not None
        assert RecoveryStrategy is not None

        # Specific error classes
        error_classes = [
            ConfigurationError,
            AuthenticationError,
            NetworkError,
            APIError,
            RateLimitError,
            ValidationError,
            InvalidAddressError,
            DataNotFoundError,
            InsufficientBalanceError,
            ProcessingError,
            BatchProcessingError,
            SystemResourceError,
            CacheError,
            TimeoutError,
            EthereumClientError,
            CoinGeckoError,
            GoogleSheetsError,
            UserInputError,
            CommandLineError,
        ]

        for error_class in error_classes:
            assert error_class is not None
            assert issubclass(error_class, WalletTrackerError)

    def test_handler_classes_import(self):
        """Test that all handler classes are importable."""
        handler_classes = [
            ErrorHandler,
            APIErrorHandler,
            NetworkErrorHandler,
            ProcessingErrorHandler,
            CircuitBreaker,
            ErrorStats,
            ErrorReport,
        ]

        for handler_class in handler_classes:
            assert handler_class is not None

    def test_recovery_classes_import(self):
        """Test that all recovery classes are importable."""
        recovery_classes = [CheckpointData, CheckpointManager, RecoveryManager, ProgressTracker, RecoverySession]

        for recovery_class in recovery_classes:
            assert recovery_class is not None

    def test_utility_functions_import(self):
        """Test that all utility functions are importable."""
        utility_functions = [
            create_error_from_exception,
            classify_error_severity,
            get_recovery_strategy,
            get_error_code,
            get_error_name,
            handle_and_log_error,
            setup_error_logging,
            setup_error_callbacks,
            with_checkpointing,
            create_recovery_context,
        ]

        for func in utility_functions:
            assert func is not None
            assert callable(func)


class TestGlobalManagers:
    """Test global manager functions."""

    def test_get_global_checkpoint_manager_default(self):
        """Test getting global checkpoint manager with defaults."""
        manager1 = get_global_checkpoint_manager()
        manager2 = get_global_checkpoint_manager()

        # Should be singleton
        assert manager1 is manager2
        assert isinstance(manager1, CheckpointManager)

        # Should have default storage path
        assert manager1.storage_path == Path("checkpoints")

    def test_get_global_checkpoint_manager_custom_path(self):
        """Test getting global checkpoint manager with custom path."""
        custom_path = Path("/tmp/custom_checkpoints")

        # Clear global instance first
        import wallet_tracker.errors

        wallet_tracker.errors._global_checkpoint_manager = None

        manager = get_global_checkpoint_manager(storage_path=custom_path)

        assert isinstance(manager, CheckpointManager)
        assert manager.storage_path == custom_path

    def test_get_global_recovery_manager(self):
        """Test getting global recovery manager."""
        manager1 = get_global_recovery_manager()
        manager2 = get_global_recovery_manager()

        # Should be singleton
        assert manager1 is manager2
        assert isinstance(manager1, RecoveryManager)

        # Should have checkpoint manager
        assert isinstance(manager1.checkpoint_manager, CheckpointManager)


class TestErrorSystemSetup:
    """Test error system setup function."""

    def test_setup_error_system_defaults(self):
        """Test setting up error system with defaults."""
        # Clear global instances
        import wallet_tracker.errors

        wallet_tracker.errors._global_checkpoint_manager = None
        wallet_tracker.errors._global_recovery_manager = None

        components = setup_error_system()

        assert "checkpoint_manager" in components
        assert "recovery_manager" in components
        assert "error_handler" in components
        assert "api_handler" in components
        assert "network_handler" in components
        assert "processing_handler" in components

        # Verify component types
        assert isinstance(components["checkpoint_manager"], CheckpointManager)
        assert isinstance(components["recovery_manager"], RecoveryManager)
        assert isinstance(components["error_handler"], ErrorHandler)
        assert isinstance(components["api_handler"], APIErrorHandler)
        assert isinstance(components["network_handler"], NetworkErrorHandler)
        assert isinstance(components["processing_handler"], ProcessingErrorHandler)

    def test_setup_error_system_custom_config(self):
        """Test setting up error system with custom configuration."""
        # Clear global instances
        import wallet_tracker.errors

        wallet_tracker.errors._global_checkpoint_manager = None
        wallet_tracker.errors._global_recovery_manager = None

        custom_path = Path("/tmp/test_checkpoints")

        components = setup_error_system(
            storage_path=custom_path, enable_file_checkpoints=True, max_retries=5, enable_circuit_breakers=False
        )

        # Verify custom configuration
        assert components["checkpoint_manager"].storage_path == custom_path
        assert components["error_handler"].max_retries == 5
        assert components["error_handler"].enable_circuit_breaker is False

    def test_setup_error_system_no_file_checkpoints(self):
        """Test setting up error system without file checkpoints."""
        # Clear global instances
        import wallet_tracker.errors

        wallet_tracker.errors._global_checkpoint_manager = None

        components = setup_error_system(enable_file_checkpoints=False)

        # Should have no storage path
        assert components["checkpoint_manager"].storage_path is None


class TestConvenienceFunctions:
    """Test convenience functions for common error scenarios."""

    @pytest.mark.asyncio
    async def test_handle_api_error_success(self):
        """Test handle_api_error with successful function."""
        call_count = []

        async def test_api_call():
            call_count.append(1)
            return "api_result"

        result = await handle_api_error(test_api_call, service_name="TestAPI", max_retries=3)

        assert result == "api_result"
        assert len(call_count) == 1

    @pytest.mark.asyncio
    async def test_handle_api_error_with_retry(self):
        """Test handle_api_error with retries."""
        call_count = []

        async def failing_api_call():
            call_count.append(1)
            if len(call_count) < 3:
                raise APIError("API temporarily unavailable")
            return "success_after_retry"

        result = await handle_api_error(failing_api_call, service_name="RetryAPI", max_retries=5)

        assert result == "success_after_retry"
        assert len(call_count) == 3

    @pytest.mark.asyncio
    async def test_handle_network_operation_success(self):
        """Test handle_network_operation with successful function."""

        async def test_network_call():
            return "network_result"

        result = await handle_network_operation(test_network_call, operation_name="test_network")

        assert result == "network_result"

    @pytest.mark.asyncio
    async def test_handle_network_operation_with_retry(self):
        """Test handle_network_operation with retries."""
        call_count = []

        async def failing_network_call():
            call_count.append(1)
            if len(call_count) < 2:
                raise NetworkError("Network timeout")
            return "network_success"

        result = await handle_network_operation(failing_network_call, operation_name="retry_network")

        assert result == "network_success"
        assert len(call_count) == 2

    @pytest.mark.asyncio
    async def test_handle_processing_operation_simple(self):
        """Test handle_processing_operation without checkpoints."""

        async def test_processing():
            return "processed_data"

        result = await handle_processing_operation(
            test_processing, operation_name="simple_processing", enable_checkpoints=False
        )

        assert result == "processed_data"

    @pytest.mark.asyncio
    async def test_handle_processing_operation_with_checkpoints(self):
        """Test handle_processing_operation with checkpoints."""
        recovery_session_provided = []

        async def test_processing_with_session(recovery_session=None):
            if recovery_session:
                recovery_session_provided.append(recovery_session)
                await recovery_session.update_progress(processed=10)
            return "processed_with_checkpoints"

        result = await handle_processing_operation(
            test_processing_with_session,
            operation_name="checkpoint_processing",
            enable_checkpoints=True,
            total_items=100,
        )

        assert result == "processed_with_checkpoints"
        assert len(recovery_session_provided) == 1
        assert isinstance(recovery_session_provided[0], RecoverySession)

    def test_create_custom_error(self):
        """Test creating custom errors."""
        error = create_custom_error(
            message="Custom test error",
            error_code="CUSTOM_001",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.BUSINESS_LOGIC,
            recovery_strategy=RecoveryStrategy.FALLBACK,
            custom_context="test_data",
        )

        assert isinstance(error, WalletTrackerError)
        assert error.message == "Custom test error"
        assert error.error_code == "CUSTOM_001"
        assert error.severity == ErrorSeverity.HIGH
        assert error.category == ErrorCategory.BUSINESS_LOGIC
        assert error.recovery_strategy == RecoveryStrategy.FALLBACK
        assert error.context["custom_context"] == "test_data"


class TestPackageMetadata:
    """Test package metadata and constants."""

    def test_package_version(self):
        """Test package version is defined."""
        from wallet_tracker.errors import __version__

        assert __version__ is not None
        assert isinstance(__version__, str)

    def test_package_author(self):
        """Test package author is defined."""
        from wallet_tracker.errors import __author__

        assert __author__ is not None
        assert isinstance(__author__, str)

    def test_package_description(self):
        """Test package description is defined."""
        from wallet_tracker.errors import __description__

        assert __description__ is not None
        assert isinstance(__description__, str)


class TestDefaultFallbackHandler:
    """Test default fallback handler."""

    @pytest.mark.asyncio
    async def test_default_fallback_handler_with_checkpoint(self):
        """Test default fallback handler with existing checkpoint."""
        # Create checkpoint manager and add a checkpoint
        checkpoint_manager = get_global_checkpoint_manager()
        await checkpoint_manager.create_checkpoint("fallback_test", {"fallback": "data"})

        # Import the default fallback handler
        from wallet_tracker.errors import _default_fallback_handler

        error = ProcessingError("Test error")
        result = await _default_fallback_handler(error, "fallback_test")

        assert result is not None
        assert result.state_data["fallback"] == "data"

    @pytest.mark.asyncio
    async def test_default_fallback_handler_no_checkpoint(self):
        """Test default fallback handler without checkpoint."""
        from wallet_tracker.errors import _default_fallback_handler

        error = ProcessingError("Test error")
        result = await _default_fallback_handler(error, "nonexistent_operation")

        assert result is None


class TestErrorSystemIntegration:
    """Test integration between different error system components."""

    @pytest.mark.asyncio
    async def test_full_error_system_integration(self):
        """Test complete error system integration."""
        # Setup error system
        components = setup_error_system(enable_file_checkpoints=False, max_retries=2)

        # Test error handling with recovery
        processing_steps = []

        async def complex_operation():
            processing_steps.append("step1")

            # Simulate failure
            if len(processing_steps) < 3:
                raise ProcessingError("Processing failed")

            processing_steps.append("step2")
            return "completed"

        # Use convenience function that integrates all components
        result = await handle_processing_operation(
            complex_operation, operation_name="integration_test", enable_checkpoints=True, total_items=10
        )

        assert result == "completed"
        assert len(processing_steps) >= 3  # Should have retried

    @pytest.mark.asyncio
    async def test_error_callbacks_integration(self):
        """Test integration of error callbacks with global handlers."""
        callback_errors = []

        def test_callback(error):
            callback_errors.append(error)

        # Setup error system and register callback
        setup_error_callbacks()
        handler = get_global_error_handler()
        handler.register_error_callback(test_callback)

        # Trigger an error
        test_error = NetworkError("Integration test error")
        await handler.handle_error(test_error, operation_name="callback_test")

        # Verify callback was called
        assert len(callback_errors) == 1
        assert callback_errors[0] == test_error

    def test_circuit_breaker_integration(self):
        """Test circuit breaker integration with error handlers."""
        # Get handler with circuit breaker enabled
        handler = get_api_error_handler()
        assert handler.enable_circuit_breaker is True

        # Get circuit breaker for operation
        cb = handler._get_circuit_breaker("integration_cb_test")
        assert cb.state == CircuitState.CLOSED

        # Trigger failures to open circuit breaker
        for _ in range(6):  # More than failure threshold (5)
            cb.record_failure()

        assert cb.state == CircuitState.OPEN
        assert cb.can_attempt() is False


class TestErrorLoggingSetup:
    """Test error logging setup."""

    def test_setup_error_logging(self):
        """Test that error logging setup works."""
        # This should not raise any exceptions
        setup_error_logging()

        # Verify logger exists and is configured
        import logging

        logger = logging.getLogger("wallet_tracker.errors")
        assert logger is not None
        assert logger.level <= logging.INFO

    def test_setup_error_callbacks(self):
        """Test that error callbacks setup works."""
        # Clear any existing callbacks
        handler = get_global_error_handler()
        handler._error_callbacks.clear()

        # Setup callbacks
        setup_error_callbacks()

        # Verify callbacks were registered
        assert len(handler._error_callbacks) > 0

        # Test that callbacks can handle different error types
        critical_error = WalletTrackerError("Critical test error", severity=ErrorSeverity.CRITICAL)

        auth_error = AuthenticationError("Auth test error")

        # These should not raise exceptions
        for callback in handler._error_callbacks:
            callback(critical_error)
            callback(auth_error)


class TestPackageStructure:
    """Test package structure and organization."""

    def test_all_expected_exports(self):
        """Test that all expected symbols are exported."""
        import wallet_tracker.errors as errors_module

        # Test major categories of exports
        error_classes = [
            "WalletTrackerError",
            "ConfigurationError",
            "AuthenticationError",
            "NetworkError",
            "APIError",
            "ValidationError",
            "ProcessingError",
        ]

        handler_classes = ["ErrorHandler", "APIErrorHandler", "CircuitBreaker", "ErrorStats"]

        recovery_classes = ["CheckpointManager", "RecoveryManager", "ProgressTracker", "RecoverySession"]

        utility_functions = ["create_error_from_exception", "get_error_code", "handle_api_error", "setup_error_system"]

        all_expected = error_classes + handler_classes + recovery_classes + utility_functions

        for symbol in all_expected:
            assert hasattr(errors_module, symbol), f"Missing export: {symbol}"

    def test_error_codes_availability(self):
        """Test that ERROR_CODES is properly exported."""
        assert ERROR_CODES is not None
        assert isinstance(ERROR_CODES, dict)
        assert len(ERROR_CODES) > 0

        # Test some expected error codes
        expected_codes = [
            "INVALID_CONFIG",
            "AUTH_FAILED",
            "NETWORK_TIMEOUT",
            "RATE_LIMIT_EXCEEDED",
            "PROCESSING_FAILED",
        ]

        for code in expected_codes:
            assert code in ERROR_CODES
            assert isinstance(ERROR_CODES[code], int)

    def test_enum_exports(self):
        """Test that enums are properly exported."""
        # Test ErrorSeverity enum
        assert hasattr(ErrorSeverity, "LOW")
        assert hasattr(ErrorSeverity, "MEDIUM")
        assert hasattr(ErrorSeverity, "HIGH")
        assert hasattr(ErrorSeverity, "CRITICAL")

        # Test ErrorCategory enum
        assert hasattr(ErrorCategory, "CONFIGURATION")
        assert hasattr(ErrorCategory, "NETWORK")
        assert hasattr(ErrorCategory, "API_LIMIT")

        # Test RecoveryStrategy enum
        assert hasattr(RecoveryStrategy, "RETRY")
        assert hasattr(RecoveryStrategy, "EXPONENTIAL_BACKOFF")
        assert hasattr(RecoveryStrategy, "FALLBACK")

        # Test CircuitState enum
        assert hasattr(CircuitState, "CLOSED")
        assert hasattr(CircuitState, "OPEN")
        assert hasattr(CircuitState, "HALF_OPEN")


class TestBackwardCompatibility:
    """Test backward compatibility and deprecation handling."""

    def test_function_signatures_stable(self):
        """Test that public function signatures are stable."""
        # These functions should maintain their basic signatures

        # Test create_error_from_exception signature
        error = create_error_from_exception(ValueError("test"))
        assert isinstance(error, WalletTrackerError)

        # Test get_error_code signature
        code = get_error_code("INVALID_CONFIG")
        assert isinstance(code, int)

        # Test get_error_name signature
        name = get_error_name(1001)
        assert isinstance(name, str)

    def test_class_inheritance_stable(self):
        """Test that class inheritance is stable."""
        # All specific errors should inherit from WalletTrackerError
        specific_errors = [
            ConfigurationError,
            AuthenticationError,
            NetworkError,
            APIError,
            ValidationError,
            ProcessingError,
        ]

        for error_class in specific_errors:
            error_instance = error_class("test message")
            assert isinstance(error_instance, WalletTrackerError)
            assert isinstance(error_instance, Exception)

    def test_global_singletons_stable(self):
        """Test that global singletons maintain consistency."""
        # Multiple calls should return same instances
        handler1 = get_global_error_handler()
        handler2 = get_global_error_handler()
        assert handler1 is handler2

        checkpoint_mgr1 = get_global_checkpoint_manager()
        checkpoint_mgr2 = get_global_checkpoint_manager()
        assert checkpoint_mgr1 is checkpoint_mgr2

        recovery_mgr1 = get_global_recovery_manager()
        recovery_mgr2 = get_global_recovery_manager()
        assert recovery_mgr1 is recovery_mgr2
