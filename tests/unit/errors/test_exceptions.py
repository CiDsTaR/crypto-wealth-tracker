"""Tests for error exceptions module."""

import pytest
from datetime import datetime

from wallet_tracker.errors.exceptions import (
    # Base classes
    WalletTrackerError,
    ErrorSeverity,
    ErrorCategory,
    RecoveryStrategy,

    # Specific errors
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

    # Utility functions
    create_error_from_exception,
    classify_error_severity,
    get_recovery_strategy,
    get_error_code,
    get_error_name,
    ERROR_CODES,
)


class TestWalletTrackerError:
    """Test base WalletTrackerError class."""

    def test_basic_initialization(self):
        """Test basic error initialization."""
        error = WalletTrackerError("Test error message")

        assert error.message == "Test error message"
        assert error.severity == ErrorSeverity.MEDIUM
        assert error.category == ErrorCategory.UNKNOWN
        assert error.recovery_strategy == RecoveryStrategy.NONE
        assert error.context == {}
        assert error.original_error is None
        assert "Test error message" in error.user_message

    def test_full_initialization(self):
        """Test error initialization with all parameters."""
        original_exc = ValueError("Original error")
        context = {"key": "value", "number": 42}

        error = WalletTrackerError(
            message="Detailed error message",
            error_code="TEST_ERROR_001",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.BUSINESS_LOGIC,
            recovery_strategy=RecoveryStrategy.RETRY,
            context=context,
            original_error=original_exc,
            user_message="User-friendly message"
        )

        assert error.message == "Detailed error message"
        assert error.error_code == "TEST_ERROR_001"
        assert error.severity == ErrorSeverity.CRITICAL
        assert error.category == ErrorCategory.BUSINESS_LOGIC
        assert error.recovery_strategy == RecoveryStrategy.RETRY
        assert error.context["key"] == "value"
        assert error.context["number"] == 42
        assert error.original_error == original_exc
        assert error.user_message == "User-friendly message"

        # Check that original error info is added to context
        assert error.context["original_error_type"] == "ValueError"
        assert error.context["original_error_message"] == "Original error"

    def test_error_code_generation(self):
        """Test automatic error code generation."""
        error = WalletTrackerError("Test message")
        assert error.error_code == "WALLET_TRACKER_ERROR"

    def test_to_dict(self):
        """Test error serialization to dictionary."""
        error = WalletTrackerError(
            message="Test message",
            error_code="TEST_001",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.NETWORK,
            recovery_strategy=RecoveryStrategy.RETRY,
            context={"test": "data"},
            user_message="User message"
        )

        result = error.to_dict()

        assert result["error_code"] == "TEST_001"
        assert result["message"] == "Test message"
        assert result["user_message"] == "User message"
        assert result["severity"] == "high"
        assert result["category"] == "network"
        assert result["recovery_strategy"] == "retry"
        assert result["context"] == {"test": "data"}
        assert result["exception_type"] == "WalletTrackerError"

    def test_is_retryable(self):
        """Test retryable error detection."""
        # Retryable errors
        retry_error = WalletTrackerError("Test", recovery_strategy=RecoveryStrategy.RETRY)
        backoff_error = WalletTrackerError("Test", recovery_strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF)

        assert retry_error.is_retryable() is True
        assert backoff_error.is_retryable() is True

        # Non-retryable errors
        none_error = WalletTrackerError("Test", recovery_strategy=RecoveryStrategy.NONE)
        skip_error = WalletTrackerError("Test", recovery_strategy=RecoveryStrategy.SKIP)

        assert none_error.is_retryable() is False
        assert skip_error.is_retryable() is False

    def test_is_critical(self):
        """Test critical error detection."""
        critical_error = WalletTrackerError("Test", severity=ErrorSeverity.CRITICAL)
        non_critical_error = WalletTrackerError("Test", severity=ErrorSeverity.MEDIUM)

        assert critical_error.is_critical() is True
        assert non_critical_error.is_critical() is False


class TestConfigurationError:
    """Test ConfigurationError class."""

    def test_basic_initialization(self):
        """Test basic configuration error."""
        error = ConfigurationError("Invalid config value")

        assert error.message == "Invalid config value"
        assert error.severity == ErrorSeverity.HIGH
        assert error.category == ErrorCategory.CONFIGURATION
        assert error.recovery_strategy == RecoveryStrategy.USER_INTERVENTION

    def test_with_config_key(self):
        """Test configuration error with config key."""
        error = ConfigurationError("Missing required value", config_key="api_key")

        assert error.context["config_key"] == "api_key"
        assert "api_key" in error.user_message


class TestAuthenticationError:
    """Test AuthenticationError class."""

    def test_basic_initialization(self):
        """Test basic authentication error."""
        error = AuthenticationError("Authentication failed")

        assert error.message == "Authentication failed"
        assert error.severity == ErrorSeverity.HIGH
        assert error.category == ErrorCategory.AUTHENTICATION
        assert error.recovery_strategy == RecoveryStrategy.USER_INTERVENTION

    def test_with_service(self):
        """Test authentication error with service name."""
        error = AuthenticationError("Invalid credentials", service="CoinGecko")

        assert error.context["service"] == "CoinGecko"
        assert "CoinGecko" in error.user_message


class TestNetworkError:
    """Test NetworkError class."""

    def test_basic_initialization(self):
        """Test basic network error."""
        error = NetworkError("Connection timeout")

        assert error.message == "Connection timeout"
        assert error.severity == ErrorSeverity.MEDIUM
        assert error.category == ErrorCategory.NETWORK
        assert error.recovery_strategy == RecoveryStrategy.EXPONENTIAL_BACKOFF

    def test_with_endpoint(self):
        """Test network error with endpoint."""
        error = NetworkError("Connection failed", endpoint="https://api.example.com")

        assert error.context["endpoint"] == "https://api.example.com"


class TestAPIError:
    """Test APIError class."""

    def test_basic_initialization(self):
        """Test basic API error."""
        error = APIError("API request failed")

        assert error.message == "API request failed"
        assert error.severity == ErrorSeverity.MEDIUM
        assert error.category == ErrorCategory.EXTERNAL_SERVICE
        assert error.recovery_strategy == RecoveryStrategy.RETRY

    def test_with_full_context(self):
        """Test API error with full context."""
        error = APIError(
            "Server error",
            service="TestAPI",
            status_code=500,
            response_body='{"error": "Internal server error"}'
        )

        assert error.context["service"] == "TestAPI"
        assert error.context["status_code"] == 500
        assert error.context["response_body"] == '{"error": "Internal server error"}'
        assert "TestAPI" in error.user_message


class TestRateLimitError:
    """Test RateLimitError class."""

    def test_basic_initialization(self):
        """Test basic rate limit error."""
        error = RateLimitError("Rate limit exceeded")

        assert error.message == "Rate limit exceeded"
        assert error.recovery_strategy == RecoveryStrategy.EXPONENTIAL_BACKOFF

    def test_with_retry_after(self):
        """Test rate limit error with retry after."""
        error = RateLimitError("Rate limit exceeded", service="API", retry_after=60)

        assert error.context["retry_after"] == 60
        assert "60 seconds" in error.user_message


class TestValidationError:
    """Test ValidationError class."""

    def test_basic_initialization(self):
        """Test basic validation error."""
        error = ValidationError("Invalid data format")

        assert error.message == "Invalid data format"
        assert error.severity == ErrorSeverity.MEDIUM
        assert error.category == ErrorCategory.DATA_VALIDATION
        assert error.recovery_strategy == RecoveryStrategy.USER_INTERVENTION

    def test_with_field_details(self):
        """Test validation error with field details."""
        error = ValidationError(
            "Value must be positive",
            field="amount",
            value=-100,
            expected_type="positive number"
        )

        assert error.context["field"] == "amount"
        assert error.context["value"] == "-100"
        assert error.context["expected_type"] == "positive number"
        assert "amount" in error.user_message


class TestInvalidAddressError:
    """Test InvalidAddressError class."""

    def test_initialization(self):
        """Test invalid address error."""
        address = "0xinvalid"
        error = InvalidAddressError(address)

        assert address in error.message
        assert error.context["field"] == "ethereum_address"
        assert error.context["value"] == address
        assert error.context["expected_type"] == "valid Ethereum address"
        assert address in error.user_message


class TestDataNotFoundError:
    """Test DataNotFoundError class."""

    def test_basic_initialization(self):
        """Test basic data not found error."""
        error = DataNotFoundError("Resource not found")

        assert error.message == "Resource not found"
        assert error.severity == ErrorSeverity.LOW
        assert error.category == ErrorCategory.DATA_VALIDATION
        assert error.recovery_strategy == RecoveryStrategy.SKIP

    def test_with_resource_details(self):
        """Test data not found error with resource details."""
        error = DataNotFoundError(
            "Token not found",
            resource_type="token",
            identifier="0x123abc"
        )

        assert error.context["resource_type"] == "token"
        assert error.context["identifier"] == "0x123abc"
        assert "Token" in error.user_message
        assert "0x123abc" in error.user_message


class TestProcessingError:
    """Test ProcessingError class."""

    def test_basic_initialization(self):
        """Test basic processing error."""
        error = ProcessingError("Processing failed")

        assert error.message == "Processing failed"
        assert error.severity == ErrorSeverity.MEDIUM
        assert error.category == ErrorCategory.BUSINESS_LOGIC
        assert error.recovery_strategy == RecoveryStrategy.RETRY

    def test_with_operation_details(self):
        """Test processing error with operation details."""
        error = ProcessingError(
            "Analysis failed",
            operation="wallet_analysis",
            stage="token_balance_calculation"
        )

        assert error.context["operation"] == "wallet_analysis"
        assert error.context["stage"] == "token_balance_calculation"
        assert "wallet_analysis" in error.user_message


class TestBatchProcessingError:
    """Test BatchProcessingError class."""

    def test_initialization(self):
        """Test batch processing error."""
        error = BatchProcessingError(
            "Batch failed",
            batch_id="batch_123",
            processed_count=50,
            failed_count=10
        )

        assert error.context["batch_id"] == "batch_123"
        assert error.context["processed_count"] == 50
        assert error.context["failed_count"] == 10
        assert error.context["operation"] == "batch_processing"
        assert "50 successful" in error.user_message
        assert "10 failed" in error.user_message


class TestSystemResourceError:
    """Test SystemResourceError class."""

    def test_basic_initialization(self):
        """Test basic system resource error."""
        error = SystemResourceError("Memory limit exceeded")

        assert error.message == "Memory limit exceeded"
        assert error.severity == ErrorSeverity.HIGH
        assert error.category == ErrorCategory.SYSTEM_RESOURCE
        assert error.recovery_strategy == RecoveryStrategy.RESTART

    def test_with_resource_details(self):
        """Test system resource error with details."""
        error = SystemResourceError(
            "Memory limit exceeded",
            resource_type="memory",
            current_usage="8GB",
            limit="8GB"
        )

        assert error.context["resource_type"] == "memory"
        assert error.context["current_usage"] == "8GB"
        assert error.context["limit"] == "8GB"
        assert "memory" in error.user_message


class TestCacheError:
    """Test CacheError class."""

    def test_initialization(self):
        """Test cache error."""
        error = CacheError(
            "Cache connection failed",
            cache_backend="redis",
            operation="get"
        )

        assert error.message == "Cache connection failed"
        assert error.severity == ErrorSeverity.LOW
        assert error.category == ErrorCategory.SYSTEM_RESOURCE
        assert error.recovery_strategy == RecoveryStrategy.FALLBACK
        assert error.context["cache_backend"] == "redis"
        assert error.context["operation"] == "get"


class TestTimeoutError:
    """Test TimeoutError class."""

    def test_initialization(self):
        """Test timeout error."""
        error = TimeoutError(
            "Operation timed out",
            operation="wallet_analysis",
            timeout_seconds=30.0
        )

        assert error.message == "Operation timed out"
        assert error.severity == ErrorSeverity.MEDIUM
        assert error.category == ErrorCategory.SYSTEM_RESOURCE
        assert error.recovery_strategy == RecoveryStrategy.RETRY
        assert error.context["operation"] == "wallet_analysis"
        assert error.context["timeout_seconds"] == 30.0
        assert "30" in error.user_message


class TestExternalServiceErrors:
    """Test external service specific errors."""

    def test_ethereum_client_error(self):
        """Test Ethereum client error."""
        error = EthereumClientError("RPC call failed")

        assert error.message == "RPC call failed"
        assert error.context["service"] == "Ethereum RPC"

    def test_coingecko_error(self):
        """Test CoinGecko error."""
        error = CoinGeckoError("API rate limit exceeded")

        assert error.message == "API rate limit exceeded"
        assert error.context["service"] == "CoinGecko"

    def test_google_sheets_error(self):
        """Test Google Sheets error."""
        error = GoogleSheetsError(
            "Permission denied",
            spreadsheet_id="1234567890"
        )

        assert error.message == "Permission denied"
        assert error.context["service"] == "Google Sheets"
        assert error.context["spreadsheet_id"] == "1234567890"
        assert "1234567890" in error.user_message


class TestUserInputErrors:
    """Test user input errors."""

    def test_user_input_error(self):
        """Test user input error."""
        error = UserInputError(
            "Invalid format",
            input_field="wallet_address",
            provided_value="invalid_address"
        )

        assert error.message == "Invalid format"
        assert error.severity == ErrorSeverity.LOW
        assert error.category == ErrorCategory.USER_INPUT
        assert error.recovery_strategy == RecoveryStrategy.USER_INTERVENTION
        assert error.context["input_field"] == "wallet_address"
        assert error.context["provided_value"] == "invalid_address"

    def test_command_line_error(self):
        """Test command line error."""
        error = CommandLineError("Invalid argument", argument="--batch-size")

        assert error.message == "Invalid argument"
        assert error.context["input_field"] == "--batch-size"
        assert "--batch-size" in error.user_message


class TestUtilityFunctions:
    """Test utility functions."""

    def test_create_error_from_timeout_exception(self):
        """Test creating error from timeout exception."""
        original = Exception("Connection timeout occurred")
        error = create_error_from_exception(original, {"endpoint": "api.example.com"})

        assert isinstance(error, TimeoutError)
        assert error.original_error == original
        assert error.context["endpoint"] == "api.example.com"

    def test_create_error_from_network_exception(self):
        """Test creating error from network exception."""
        original = Exception("Network connection failed")
        error = create_error_from_exception(original)

        assert isinstance(error, NetworkError)
        assert error.original_error == original

    def test_create_error_from_auth_exception(self):
        """Test creating error from auth exception."""
        original = Exception("Authentication failed")
        error = create_error_from_exception(original)

        assert isinstance(error, AuthenticationError)
        assert error.original_error == original

    def test_create_error_from_rate_limit_exception(self):
        """Test creating error from rate limit exception."""
        original = Exception("Rate limit exceeded")
        error = create_error_from_exception(original)

        assert isinstance(error, RateLimitError)
        assert error.original_error == original

    def test_create_error_from_config_exception(self):
        """Test creating error from config exception."""
        original = Exception("Configuration error")
        error = create_error_from_exception(original)

        assert isinstance(error, ConfigurationError)
        assert error.original_error == original

    def test_create_error_from_validation_exception(self):
        """Test creating error from validation exception."""
        original = Exception("Validation failed")
        error = create_error_from_exception(original)

        assert isinstance(error, ValidationError)
        assert error.original_error == original

    def test_create_error_from_generic_exception(self):
        """Test creating error from generic exception."""
        original = Exception("Something went wrong")
        error = create_error_from_exception(original)

        assert isinstance(error, WalletTrackerError)
        assert not isinstance(error, (TimeoutError, NetworkError, AuthenticationError))
        assert error.original_error == original

    def test_classify_error_severity(self):
        """Test error severity classification."""
        # Test critical severity
        memory_error = Exception("Out of memory")
        assert classify_error_severity(memory_error) == ErrorSeverity.CRITICAL

        # Test high severity
        auth_error = Exception("Unauthorized access")
        assert classify_error_severity(auth_error) == ErrorSeverity.HIGH

        # Test low severity
        validation_error = Exception("Validation failed")
        assert classify_error_severity(validation_error) == ErrorSeverity.LOW

        # Test medium severity (default)
        generic_error = Exception("Something happened")
        assert classify_error_severity(generic_error) == ErrorSeverity.MEDIUM

        # Test with WalletTrackerError (should return its own severity)
        wallet_error = WalletTrackerError("Test", severity=ErrorSeverity.HIGH)
        assert classify_error_severity(wallet_error) == ErrorSeverity.HIGH

    def test_get_recovery_strategy(self):
        """Test recovery strategy determination."""
        # Test no recovery
        critical_error = Exception("Critical system failure")
        assert get_recovery_strategy(critical_error) == RecoveryStrategy.NONE

        # Test exponential backoff
        rate_limit_error = Exception("Rate limit exceeded")
        assert get_recovery_strategy(rate_limit_error) == RecoveryStrategy.EXPONENTIAL_BACKOFF

        # Test simple retry
        timeout_error = Exception("Connection timeout")
        assert get_recovery_strategy(timeout_error) == RecoveryStrategy.RETRY

        # Test user intervention
        auth_error = Exception("Authentication failed")
        assert get_recovery_strategy(auth_error) == RecoveryStrategy.USER_INTERVENTION

        # Test skip
        not_found_error = Exception("Resource not found")
        assert get_recovery_strategy(not_found_error) == RecoveryStrategy.SKIP

        # Test default retry
        generic_error = Exception("Something happened")
        assert get_recovery_strategy(generic_error) == RecoveryStrategy.RETRY

        # Test with WalletTrackerError (should return its own strategy)
        wallet_error = WalletTrackerError("Test", recovery_strategy=RecoveryStrategy.FALLBACK)
        assert get_recovery_strategy(wallet_error) == RecoveryStrategy.FALLBACK

    def test_get_error_code(self):
        """Test getting error code by name."""
        assert get_error_code("INVALID_CONFIG") == 1001
        assert get_error_code("AUTH_FAILED") == 1101
        assert get_error_code("RATE_LIMIT_EXCEEDED") == 1302
        assert get_error_code("NONEXISTENT_ERROR") is None

        # Test case insensitive
        assert get_error_code("invalid_config") == 1001

    def test_get_error_name(self):
        """Test getting error name by code."""
        assert get_error_name(1001) == "INVALID_CONFIG"
        assert get_error_name(1101) == "AUTH_FAILED"
        assert get_error_name(1302) == "RATE_LIMIT_EXCEEDED"
        assert get_error_name(9999) is None

    def test_error_codes_registry(self):
        """Test error codes registry structure."""
        # Test that all expected error types have codes
        expected_errors = [
            'INVALID_CONFIG', 'AUTH_FAILED', 'NETWORK_TIMEOUT',
            'RATE_LIMIT_EXCEEDED', 'INVALID_ADDRESS', 'PROCESSING_FAILED',
            'OUT_OF_MEMORY', 'ETHEREUM_RPC_ERROR', 'INVALID_USER_INPUT'
        ]

        for error_name in expected_errors:
            assert error_name in ERROR_CODES
            assert isinstance(ERROR_CODES[error_name], int)

        # Test that codes are unique
        codes = list(ERROR_CODES.values())
        assert len(codes) == len(set(codes)), "Error codes must be unique"

        # Test that codes are in expected ranges
        for error_name, code in ERROR_CODES.items():
            assert 1000 <= code <= 1999, f"Error code {code} for {error_name} should be in range 1000-1999"


class TestErrorInheritance:
    """Test error class inheritance and polymorphism."""

    def test_all_errors_inherit_from_base(self):
        """Test that all error classes inherit from WalletTrackerError."""
        error_classes = [
            ConfigurationError, AuthenticationError, NetworkError, APIError,
            RateLimitError, ValidationError, InvalidAddressError, DataNotFoundError,
            InsufficientBalanceError, ProcessingError, BatchProcessingError,
            SystemResourceError, CacheError, TimeoutError, EthereumClientError,
            CoinGeckoError, GoogleSheetsError, UserInputError, CommandLineError
        ]

        for error_class in error_classes:
            error_instance = error_class("Test message")
            assert isinstance(error_instance, WalletTrackerError)
            assert isinstance(error_instance, Exception)

    def test_error_class_defaults(self):
        """Test that error classes have appropriate defaults."""
        # Test that API errors have correct service context
        eth_error = EthereumClientError("Test")
        assert eth_error.context["service"] == "Ethereum RPC"

        coingecko_error = CoinGeckoError("Test")
        assert coingecko_error.context["service"] == "CoinGecko"

        sheets_error = GoogleSheetsError("Test")
        assert sheets_error.context["service"] == "Google Sheets"

        # Test that validation errors have correct category
        validation_error = ValidationError("Test")
        assert validation_error.category == ErrorCategory.DATA_VALIDATION

        # Test that network errors have correct recovery strategy
        network_error = NetworkError("Test")
        assert network_error.recovery_strategy == RecoveryStrategy.EXPONENTIAL_BACKOFF


class TestErrorContextHandling:
    """Test error context handling and serialization."""

    def test_context_preservation(self):
        """Test that context is preserved through error operations."""
        context = {
            "wallet_address": "0x123abc",
            "timestamp": datetime.now().isoformat(),
            "batch_id": "batch_001",
            "retry_count": 3
        }

        error = WalletTrackerError("Test error", context=context)

        # Test that context is preserved
        assert error.context["wallet_address"] == "0x123abc"
        assert error.context["batch_id"] == "batch_001"
        assert error.context["retry_count"] == 3

        # Test serialization preserves context
        error_dict = error.to_dict()
        assert error_dict["context"]["wallet_address"] == "0x123abc"
        assert error_dict["context"]["batch_id"] == "batch_001"

    def test_context_merging_with_original_error(self):
        """Test that original error info is added to context."""
        original = ValueError("Original message")
        context = {"existing_key": "existing_value"}

        error = WalletTrackerError(
            "Wrapper error",
            context=context,
            original_error=original
        )

        # Test that original context is preserved
        assert error.context["existing_key"] == "existing_value"

        # Test that original error info is added
        assert error.context["original_error_type"] == "ValueError"
        assert error.context["original_error_message"] == "Original message"