"""Custom exception hierarchy for comprehensive error handling."""

from enum import Enum
from typing import Any, Dict, List, Optional, Union


class ErrorSeverity(str, Enum):
    """Error severity levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(str, Enum):
    """Error categories for classification."""

    CONFIGURATION = "configuration"
    AUTHENTICATION = "authentication"
    NETWORK = "network"
    API_LIMIT = "api_limit"
    DATA_VALIDATION = "data_validation"
    BUSINESS_LOGIC = "business_logic"
    SYSTEM_RESOURCE = "system_resource"
    EXTERNAL_SERVICE = "external_service"
    USER_INPUT = "user_input"
    UNKNOWN = "unknown"


class RecoveryStrategy(str, Enum):
    """Recovery strategies for different error types."""

    NONE = "none"  # No recovery possible
    RETRY = "retry"  # Simple retry
    EXPONENTIAL_BACKOFF = "exponential_backoff"  # Retry with backoff
    FALLBACK = "fallback"  # Use alternative approach
    SKIP = "skip"  # Skip and continue
    USER_INTERVENTION = "user_intervention"  # Requires user action
    RESTART = "restart"  # Restart component/application


class WalletTrackerError(Exception):
    """
    Base exception class for all Wallet Tracker errors.

    Provides structured error information including:
    - Error codes and messages
    - Severity and category classification
    - Recovery strategies
    - Additional context data
    """

    def __init__(
            self,
            message: str,
            error_code: Optional[str] = None,
            severity: ErrorSeverity = ErrorSeverity.MEDIUM,
            category: ErrorCategory = ErrorCategory.UNKNOWN,
            recovery_strategy: RecoveryStrategy = RecoveryStrategy.NONE,
            context: Optional[Dict[str, Any]] = None,
            original_error: Optional[Exception] = None,
            user_message: Optional[str] = None
    ):
        """Initialize error with comprehensive information.

        Args:
            message: Technical error message
            error_code: Unique error code for identification
            severity: Error severity level
            category: Error category
            recovery_strategy: Suggested recovery strategy
            context: Additional context data
            original_error: Original exception that caused this error
            user_message: User-friendly error message
        """
        super().__init__(message)

        self.message = message
        self.error_code = error_code or self._generate_error_code()
        self.severity = severity
        self.category = category
        self.recovery_strategy = recovery_strategy
        self.context = context or {}
        self.original_error = original_error
        self.user_message = user_message or self._generate_user_message()

        # Add original error to context if provided
        if original_error:
            self.context['original_error_type'] = type(original_error).__name__
            self.context['original_error_message'] = str(original_error)

    def _generate_error_code(self) -> str:
        """Generate error code based on exception class."""
        class_name = self.__class__.__name__
        # Convert CamelCase to UPPER_SNAKE_CASE
        import re
        error_code = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', class_name)
        error_code = re.sub('([a-z0-9])([A-Z])', r'\1_\2', error_code).upper()
        return error_code

    def _generate_user_message(self) -> str:
        """Generate user-friendly message."""
        # Default implementation - subclasses can override
        return f"An error occurred: {self.message}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for serialization."""
        return {
            'error_code': self.error_code,
            'message': self.message,
            'user_message': self.user_message,
            'severity': self.severity.value,
            'category': self.category.value,
            'recovery_strategy': self.recovery_strategy.value,
            'context': self.context,
            'exception_type': self.__class__.__name__
        }

    def is_retryable(self) -> bool:
        """Check if error is retryable."""
        return self.recovery_strategy in [
            RecoveryStrategy.RETRY,
            RecoveryStrategy.EXPONENTIAL_BACKOFF
        ]

    def is_critical(self) -> bool:
        """Check if error is critical."""
        return self.severity == ErrorSeverity.CRITICAL


# Configuration and Setup Errors

class ConfigurationError(WalletTrackerError):
    """Configuration-related errors."""

    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs):
        super().__init__(
            message=message,
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.CONFIGURATION,
            recovery_strategy=RecoveryStrategy.USER_INTERVENTION,
            context={'config_key': config_key} if config_key else None,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        if 'config_key' in self.context:
            return f"Configuration error with '{self.context['config_key']}': {self.message}"
        return f"Configuration error: {self.message}"


class AuthenticationError(WalletTrackerError):
    """Authentication and authorization errors."""

    def __init__(self, message: str, service: Optional[str] = None, **kwargs):
        super().__init__(
            message=message,
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.AUTHENTICATION,
            recovery_strategy=RecoveryStrategy.USER_INTERVENTION,
            context={'service': service} if service else None,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        service = self.context.get('service', 'service')
        return f"Authentication failed for {service}. Please check your credentials."


# Network and API Errors

class NetworkError(WalletTrackerError):
    """Network connectivity errors."""

    def __init__(self, message: str, endpoint: Optional[str] = None, **kwargs):
        super().__init__(
            message=message,
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.NETWORK,
            recovery_strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF,
            context={'endpoint': endpoint} if endpoint else None,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "Network connection error. Please check your internet connection."


class APIError(WalletTrackerError):
    """General API errors."""

    def __init__(
            self,
            message: str,
            service: Optional[str] = None,
            status_code: Optional[int] = None,
            response_body: Optional[str] = None,
            **kwargs
    ):
        context = {}
        if service:
            context['service'] = service
        if status_code:
            context['status_code'] = status_code
        if response_body:
            context['response_body'] = response_body

        super().__init__(
            message=message,
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.EXTERNAL_SERVICE,
            recovery_strategy=RecoveryStrategy.RETRY,
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        service = self.context.get('service', 'external service')
        return f"API error from {service}. Please try again later."


class RateLimitError(APIError):
    """API rate limit exceeded errors."""

    def __init__(
            self,
            message: str,
            service: Optional[str] = None,
            retry_after: Optional[int] = None,
            **kwargs
    ):
        context = kwargs.pop('context', {})
        if retry_after:
            context['retry_after'] = retry_after

        super().__init__(
            message=message,
            service=service,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF,
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        service = self.context.get('service', 'service')
        retry_after = self.context.get('retry_after')

        if retry_after:
            return f"Rate limit exceeded for {service}. Please wait {retry_after} seconds before retrying."
        return f"Rate limit exceeded for {service}. Please wait before retrying."


# Data and Validation Errors

class ValidationError(WalletTrackerError):
    """Data validation errors."""

    def __init__(
            self,
            message: str,
            field: Optional[str] = None,
            value: Optional[Any] = None,
            expected_type: Optional[str] = None,
            **kwargs
    ):
        context = {}
        if field:
            context['field'] = field
        if value is not None:
            context['value'] = str(value)
        if expected_type:
            context['expected_type'] = expected_type

        super().__init__(
            message=message,
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.DATA_VALIDATION,
            recovery_strategy=RecoveryStrategy.USER_INTERVENTION,
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        field = self.context.get('field')
        if field:
            return f"Invalid value for {field}: {self.message}"
        return f"Validation error: {self.message}"


class InvalidAddressError(ValidationError):
    """Invalid Ethereum address errors."""

    def __init__(self, address: str, **kwargs):
        super().__init__(
            message=f"Invalid Ethereum address format: {address}",
            field="ethereum_address",
            value=address,
            expected_type="valid Ethereum address",
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return f"The Ethereum address '{self.context['value']}' is not valid."


class DataNotFoundError(WalletTrackerError):
    """Data not found errors."""

    def __init__(
            self,
            message: str,
            resource_type: Optional[str] = None,
            identifier: Optional[str] = None,
            **kwargs
    ):
        context = {}
        if resource_type:
            context['resource_type'] = resource_type
        if identifier:
            context['identifier'] = identifier

        super().__init__(
            message=message,
            severity=ErrorSeverity.LOW,
            category=ErrorCategory.DATA_VALIDATION,
            recovery_strategy=RecoveryStrategy.SKIP,
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        resource_type = self.context.get('resource_type', 'resource')
        identifier = self.context.get('identifier')

        if identifier:
            return f"{resource_type.title()} '{identifier}' not found."
        return f"{resource_type.title()} not found."


# Business Logic Errors

class InsufficientBalanceError(WalletTrackerError):
    """Insufficient balance errors."""

    def __init__(
            self,
            message: str,
            wallet_address: Optional[str] = None,
            required_amount: Optional[str] = None,
            available_amount: Optional[str] = None,
            **kwargs
    ):
        context = {}
        if wallet_address:
            context['wallet_address'] = wallet_address
        if required_amount:
            context['required_amount'] = required_amount
        if available_amount:
            context['available_amount'] = available_amount

        super().__init__(
            message=message,
            severity=ErrorSeverity.LOW,
            category=ErrorCategory.BUSINESS_LOGIC,
            recovery_strategy=RecoveryStrategy.SKIP,
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "Wallet has insufficient balance for the requested operation."


class ProcessingError(WalletTrackerError):
    """General processing errors."""

    def __init__(
            self,
            message: str,
            operation: Optional[str] = None,
            stage: Optional[str] = None,
            **kwargs
    ):
        context = {}
        if operation:
            context['operation'] = operation
        if stage:
            context['stage'] = stage

        super().__init__(
            message=message,
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.BUSINESS_LOGIC,
            recovery_strategy=RecoveryStrategy.RETRY,
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        operation = self.context.get('operation', 'operation')
        return f"Error during {operation}. Please try again."


class BatchProcessingError(ProcessingError):
    """Batch processing specific errors."""

    def __init__(
            self,
            message: str,
            batch_id: Optional[str] = None,
            processed_count: Optional[int] = None,
            failed_count: Optional[int] = None,
            **kwargs
    ):
        context = kwargs.pop('context', {})
        if batch_id:
            context['batch_id'] = batch_id
        if processed_count is not None:
            context['processed_count'] = processed_count
        if failed_count is not None:
            context['failed_count'] = failed_count

        super().__init__(
            message=message,
            operation="batch_processing",
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        processed = self.context.get('processed_count', 0)
        failed = self.context.get('failed_count', 0)

        if processed > 0:
            return f"Batch processing partially completed: {processed} successful, {failed} failed."
        return "Batch processing failed to complete."


# System and Resource Errors

class SystemResourceError(WalletTrackerError):
    """System resource errors (memory, disk, etc.)."""

    def __init__(
            self,
            message: str,
            resource_type: Optional[str] = None,
            current_usage: Optional[str] = None,
            limit: Optional[str] = None,
            **kwargs
    ):
        context = {}
        if resource_type:
            context['resource_type'] = resource_type
        if current_usage:
            context['current_usage'] = current_usage
        if limit:
            context['limit'] = limit

        super().__init__(
            message=message,
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.SYSTEM_RESOURCE,
            recovery_strategy=RecoveryStrategy.RESTART,
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        resource_type = self.context.get('resource_type', 'system resource')
        return f"System {resource_type} limit exceeded. Please try with a smaller batch size."


class CacheError(WalletTrackerError):
    """Cache-related errors."""

    def __init__(
            self,
            message: str,
            cache_backend: Optional[str] = None,
            operation: Optional[str] = None,
            **kwargs
    ):
        context = {}
        if cache_backend:
            context['cache_backend'] = cache_backend
        if operation:
            context['operation'] = operation

        super().__init__(
            message=message,
            severity=ErrorSeverity.LOW,
            category=ErrorCategory.SYSTEM_RESOURCE,
            recovery_strategy=RecoveryStrategy.FALLBACK,
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "Cache error occurred. Processing will continue without cache."


class TimeoutError(WalletTrackerError):
    """Operation timeout errors."""

    def __init__(
            self,
            message: str,
            operation: Optional[str] = None,
            timeout_seconds: Optional[float] = None,
            **kwargs
    ):
        context = {}
        if operation:
            context['operation'] = operation
        if timeout_seconds:
            context['timeout_seconds'] = timeout_seconds

        super().__init__(
            message=message,
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.SYSTEM_RESOURCE,
            recovery_strategy=RecoveryStrategy.RETRY,
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        operation = self.context.get('operation', 'operation')
        timeout = self.context.get('timeout_seconds')

        if timeout:
            return f"Operation '{operation}' timed out after {timeout} seconds. Please try again."
        return f"Operation '{operation}' timed out. Please try again."


# External Service Errors

class EthereumClientError(APIError):
    """Ethereum client specific errors."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message=message,
            service="Ethereum RPC",
            **kwargs
        )


class CoinGeckoError(APIError):
    """CoinGecko API specific errors."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message=message,
            service="CoinGecko",
            **kwargs
        )


class GoogleSheetsError(APIError):
    """Google Sheets API specific errors."""

    def __init__(self, message: str, spreadsheet_id: Optional[str] = None, **kwargs):
        context = kwargs.pop('context', {})
        if spreadsheet_id:
            context['spreadsheet_id'] = spreadsheet_id

        super().__init__(
            message=message,
            service="Google Sheets",
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        spreadsheet_id = self.context.get('spreadsheet_id')
        if spreadsheet_id:
            return f"Error accessing Google Sheets document '{spreadsheet_id}'. Please check permissions."
        return "Error accessing Google Sheets. Please check permissions and try again."


# User Input Errors

class UserInputError(WalletTrackerError):
    """User input errors."""

    def __init__(
            self,
            message: str,
            input_field: Optional[str] = None,
            provided_value: Optional[str] = None,
            **kwargs
    ):
        context = {}
        if input_field:
            context['input_field'] = input_field
        if provided_value:
            context['provided_value'] = provided_value

        super().__init__(
            message=message,
            severity=ErrorSeverity.LOW,
            category=ErrorCategory.USER_INPUT,
            recovery_strategy=RecoveryStrategy.USER_INTERVENTION,
            context=context,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        field = self.context.get('input_field')
        if field:
            return f"Invalid input for {field}: {self.message}"
        return f"Invalid input: {self.message}"


class CommandLineError(UserInputError):
    """Command line argument errors."""

    def __init__(self, message: str, argument: Optional[str] = None, **kwargs):
        super().__init__(
            message=message,
            input_field=argument,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        arg = self.context.get('input_field')
        if arg:
            return f"Invalid command line argument '{arg}': {self.message}"
        return f"Command line error: {self.message}"


# Utility functions for error handling

def create_error_from_exception(
        exc: Exception,
        context: Optional[Dict[str, Any]] = None
) -> WalletTrackerError:
    """Create a WalletTrackerError from a generic exception.

    Args:
        exc: Original exception
        context: Additional context

    Returns:
        Appropriate WalletTrackerError subclass
    """
    exc_type = type(exc).__name__
    message = str(exc)

    # Map common exception types to our custom errors
    if 'timeout' in message.lower():
        return TimeoutError(
            message=message,
            original_error=exc,
            context=context
        )

    elif 'network' in message.lower() or 'connection' in message.lower():
        return NetworkError(
            message=message,
            original_error=exc,
            context=context
        )

    elif 'permission' in message.lower() or 'auth' in message.lower():
        return AuthenticationError(
            message=message,
            original_error=exc,
            context=context
        )

    elif 'rate limit' in message.lower():
        return RateLimitError(
            message=message,
            original_error=exc,
            context=context
        )

    elif 'config' in message.lower():
        return ConfigurationError(
            message=message,
            original_error=exc,
            context=context
        )

    elif 'validation' in message.lower() or 'invalid' in message.lower():
        return ValidationError(
            message=message,
            original_error=exc,
            context=context
        )

    else:
        # Generic error
        return WalletTrackerError(
            message=message,
            original_error=exc,
            context=context
        )


def classify_error_severity(exc: Exception) -> ErrorSeverity:
    """Classify error severity based on exception type and message.

    Args:
        exc: Exception to classify

    Returns:
        Appropriate error severity
    """
    if isinstance(exc, WalletTrackerError):
        return exc.severity

    exc_type = type(exc).__name__.lower()
    message = str(exc).lower()

    # Critical errors
    if any(keyword in exc_type for keyword in ['memory', 'system', 'critical']):
        return ErrorSeverity.CRITICAL

    if any(keyword in message for keyword in ['out of memory', 'disk full', 'critical']):
        return ErrorSeverity.CRITICAL

    # High severity errors
    if any(keyword in exc_type for keyword in ['auth', 'permission', 'config']):
        return ErrorSeverity.HIGH

    if any(keyword in message for keyword in ['unauthorized', 'forbidden', 'config']):
        return ErrorSeverity.HIGH

    # Low severity errors
    if any(keyword in exc_type for keyword in ['validation', 'not found', 'input']):
        return ErrorSeverity.LOW

    if any(keyword in message for keyword in ['not found', 'invalid input', 'validation']):
        return ErrorSeverity.LOW

    # Default to medium
    return ErrorSeverity.MEDIUM


def get_recovery_strategy(exc: Exception) -> RecoveryStrategy:
    """Determine appropriate recovery strategy for an exception.

    Args:
        exc: Exception to analyze

    Returns:
        Suggested recovery strategy
    """
    if isinstance(exc, WalletTrackerError):
        return exc.recovery_strategy

    exc_type = type(exc).__name__.lower()
    message = str(exc).lower()

    # No recovery possible
    if any(keyword in message for keyword in ['critical', 'fatal', 'out of memory']):
        return RecoveryStrategy.NONE

    # Retry with backoff
    if any(keyword in message for keyword in ['rate limit', 'too many requests', 'throttle']):
        return RecoveryStrategy.EXPONENTIAL_BACKOFF

    # Simple retry
    if any(keyword in message for keyword in ['timeout', 'network', 'connection', 'temporary']):
        return RecoveryStrategy.RETRY

    # User intervention required
    if any(keyword in message for keyword in ['auth', 'permission', 'config', 'credential']):
        return RecoveryStrategy.USER_INTERVENTION

    # Skip and continue
    if any(keyword in message for keyword in ['not found', 'invalid', 'validation']):
        return RecoveryStrategy.SKIP

    # Default to retry
    return RecoveryStrategy.RETRY


# Error code registry for consistent error identification
ERROR_CODES = {
    # Configuration errors (1000-1099)
    'INVALID_CONFIG': 1001,
    'MISSING_CONFIG': 1002,
    'CONFIG_VALIDATION_FAILED': 1003,

    # Authentication errors (1100-1199)
    'AUTH_FAILED': 1101,
    'INVALID_CREDENTIALS': 1102,
    'TOKEN_EXPIRED': 1103,
    'PERMISSION_DENIED': 1104,

    # Network errors (1200-1299)
    'NETWORK_TIMEOUT': 1201,
    'CONNECTION_FAILED': 1202,
    'DNS_RESOLUTION_FAILED': 1203,

    # API errors (1300-1399)
    'API_ERROR': 1301,
    'RATE_LIMIT_EXCEEDED': 1302,
    'API_UNAVAILABLE': 1303,
    'INVALID_API_RESPONSE': 1304,

    # Data validation errors (1400-1499)
    'INVALID_ADDRESS': 1401,
    'INVALID_INPUT_FORMAT': 1402,
    'DATA_VALIDATION_FAILED': 1403,
    'MISSING_REQUIRED_FIELD': 1404,

    # Processing errors (1500-1599)
    'PROCESSING_FAILED': 1501,
    'BATCH_PROCESSING_FAILED': 1502,
    'WALLET_ANALYSIS_FAILED': 1503,
    'INSUFFICIENT_BALANCE': 1504,

    # System resource errors (1600-1699)
    'OUT_OF_MEMORY': 1601,
    'DISK_FULL': 1602,
    'CPU_OVERLOAD': 1603,
    'CACHE_ERROR': 1604,

    # External service errors (1700-1799)
    'ETHEREUM_RPC_ERROR': 1701,
    'COINGECKO_API_ERROR': 1702,
    'GOOGLE_SHEETS_ERROR': 1703,

    # User input errors (1800-1899)
    'INVALID_USER_INPUT': 1801,
    'COMMAND_LINE_ERROR': 1802,
    'MISSING_USER_INPUT': 1803,
}


def get_error_code(error_name: str) -> Optional[int]:
    """Get numeric error code for error name.

    Args:
        error_name: Error name/identifier

    Returns:
        Numeric error code or None if not found
    """
    return ERROR_CODES.get(error_name.upper())


def get_error_name(error_code: int) -> Optional[str]:
    """Get error name for numeric error code.

    Args:
        error_code: Numeric error code

    Returns:
        Error name or None if not found
    """
    for name, code in ERROR_CODES.items():
        if code == error_code:
            return name
    return None