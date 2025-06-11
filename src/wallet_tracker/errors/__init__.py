"""Error handling package for comprehensive error management and recovery.

This package provides:
- Custom exception hierarchy with structured error information
- Error handlers with automatic retry and recovery strategies
- Circuit breaker patterns for preventing cascading failures
- Checkpoint and recovery system for long-running operations
- Progress tracking with automatic checkpointing
- Error reporting and analytics

Key Components:
- exceptions: Custom exception classes with error codes and recovery strategies
- handlers: Error handlers with retry logic and circuit breakers
- recovery: Recovery mechanisms, checkpointing, and progress tracking

Usage Examples:

Basic Error Handling:
    from wallet_tracker.errors import handle_errors, get_global_error_handler

    @handle_errors(operation_name="wallet_analysis", max_retries=3)
    async def analyze_wallet(address):
        # Your code here
        pass

Recovery Session:
    from wallet_tracker.errors import RecoverySession, CheckpointManager, RecoveryManager

    checkpoint_manager = CheckpointManager()
    recovery_manager = RecoveryManager(checkpoint_manager)

    async with RecoverySession("batch_process", checkpoint_manager, recovery_manager, total_items=1000) as session:
        for item in items:
            try:
                process_item(item)
                await session.update_progress(processed=1)
            except Exception as e:
                await session.update_progress(failed=1)

Manual Error Handling:
    from wallet_tracker.errors import error_context, InvalidAddressError

    async with error_context("validate_address", max_retries=2) as attempt:
        if not is_valid_address(address):
            raise InvalidAddressError(address)
"""

from .exceptions import (
    # Error codes
    ERROR_CODES,
    APIError,
    AuthenticationError,
    BatchProcessingError,
    CacheError,
    CoinGeckoError,
    CommandLineError,
    # Configuration errors
    ConfigurationError,
    DataNotFoundError,
    ErrorCategory,
    ErrorSeverity,
    # External service errors
    EthereumClientError,
    GoogleSheetsError,
    # Business logic errors
    InsufficientBalanceError,
    InvalidAddressError,
    # Network and API errors
    NetworkError,
    ProcessingError,
    RateLimitError,
    RecoveryStrategy,
    # System resource errors
    SystemResourceError,
    TimeoutError,
    # User input errors
    UserInputError,
    # Data validation errors
    ValidationError,
    # Base error classes
    WalletTrackerError,
    classify_error_severity,
    # Utility functions
    create_error_from_exception,
    get_error_code,
    get_error_name,
    get_recovery_strategy,
)
from .handlers import (
    APIErrorHandler,
    # Circuit breaker
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
    # Main error handler classes
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
from .recovery import (
    # Checkpoint system
    CheckpointData,
    CheckpointManager,
    # Progress tracking
    ProgressTracker,
    # Recovery system
    RecoveryManager,
    # Recovery session
    RecoverySession,
    create_recovery_context,
    # Utility functions
    with_checkpointing,
)

# Package version and metadata
__version__ = "1.0.0"
__author__ = "Wallet Tracker Team"
__description__ = "Comprehensive error handling and recovery system"

# Global instances for convenience
_global_checkpoint_manager = None
_global_recovery_manager = None


def get_global_checkpoint_manager(storage_path=None) -> CheckpointManager:
    """Get global checkpoint manager instance.

    Args:
        storage_path: Optional storage path override

    Returns:
        Global CheckpointManager instance
    """
    global _global_checkpoint_manager
    if _global_checkpoint_manager is None:
        from pathlib import Path

        default_path = Path("checkpoints") if storage_path is None else storage_path
        _global_checkpoint_manager = CheckpointManager(storage_path=default_path)
    return _global_checkpoint_manager


def get_global_recovery_manager() -> RecoveryManager:
    """Get global recovery manager instance.

    Returns:
        Global RecoveryManager instance
    """
    global _global_recovery_manager
    if _global_recovery_manager is None:
        checkpoint_manager = get_global_checkpoint_manager()
        _global_recovery_manager = RecoveryManager(checkpoint_manager)
    return _global_recovery_manager


def setup_error_system(
    storage_path=None, enable_file_checkpoints=True, max_retries=3, enable_circuit_breakers=True
) -> dict:
    """Set up the complete error handling system.

    Args:
        storage_path: Path for checkpoint storage
        enable_file_checkpoints: Whether to enable file-based checkpoints
        max_retries: Default maximum retries
        enable_circuit_breakers: Whether to enable circuit breakers

    Returns:
        Dictionary with initialized components
    """
    # Setup logging
    setup_error_logging()

    # Setup error callbacks
    setup_error_callbacks()

    # Initialize checkpoint manager
    checkpoint_path = storage_path if enable_file_checkpoints else None
    checkpoint_manager = get_global_checkpoint_manager(checkpoint_path)

    # Initialize recovery manager
    recovery_manager = get_global_recovery_manager()

    # Configure global error handler
    global_handler = get_global_error_handler()
    global_handler.max_retries = max_retries
    global_handler.enable_circuit_breaker = enable_circuit_breakers

    # Register recovery strategies
    recovery_manager.register_recovery_strategy(RecoveryStrategy.FALLBACK, _default_fallback_handler)

    return {
        "checkpoint_manager": checkpoint_manager,
        "recovery_manager": recovery_manager,
        "error_handler": global_handler,
        "api_handler": get_api_error_handler(),
        "network_handler": get_network_error_handler(),
        "processing_handler": get_processing_error_handler(),
    }


async def _default_fallback_handler(error, operation_name):
    """Default fallback handler for recovery strategies."""
    import logging

    logger = logging.getLogger(__name__)

    logger.info(f"Executing default fallback for {operation_name}: {error.message}")

    # Try to get a checkpoint to continue from
    checkpoint_manager = get_global_checkpoint_manager()
    checkpoint = await checkpoint_manager.get_latest_checkpoint(operation_name)

    if checkpoint:
        logger.info(f"Fallback using checkpoint: {checkpoint.checkpoint_id}")
        return checkpoint

    return None


# Convenience functions for common error scenarios


async def handle_api_error(func, *args, service_name=None, max_retries=5, **kwargs):
    """Handle API operations with specialized error handling.

    Args:
        func: Function to execute
        *args: Function arguments
        service_name: Name of the API service
        max_retries: Maximum retry attempts
        **kwargs: Function keyword arguments

    Returns:
        Function result
    """
    handler = get_api_error_handler()
    operation_name = service_name or func.__name__

    async with handler.handle_operation(operation_name, max_retries):
        return await func(*args, **kwargs)


async def handle_network_operation(func, *args, operation_name=None, **kwargs):
    """Handle network operations with specialized error handling.

    Args:
        func: Function to execute
        *args: Function arguments
        operation_name: Name of the operation
        **kwargs: Function keyword arguments

    Returns:
        Function result
    """
    handler = get_network_error_handler()
    op_name = operation_name or func.__name__

    async with handler.handle_operation(op_name):
        return await func(*args, **kwargs)


async def handle_processing_operation(
    func, *args, operation_name=None, enable_checkpoints=True, total_items=None, **kwargs
):
    """Handle processing operations with checkpointing and recovery.

    Args:
        func: Function to execute
        *args: Function arguments
        operation_name: Name of the operation
        enable_checkpoints: Whether to enable checkpointing
        total_items: Total items for progress tracking
        **kwargs: Function keyword arguments

    Returns:
        Function result
    """
    op_name = operation_name or func.__name__

    if enable_checkpoints and total_items:
        # Use recovery session with progress tracking
        checkpoint_manager = get_global_checkpoint_manager()
        recovery_manager = get_global_recovery_manager()

        async with RecoverySession(
            operation_name=op_name,
            checkpoint_manager=checkpoint_manager,
            recovery_manager=recovery_manager,
            total_items=total_items,
        ) as session:
            kwargs["recovery_session"] = session
            return await func(*args, **kwargs)
    else:
        # Use regular error handling
        handler = get_processing_error_handler()
        async with handler.handle_operation(op_name):
            return await func(*args, **kwargs)


def create_custom_error(
    message: str,
    error_code: str = None,
    severity: ErrorSeverity = ErrorSeverity.MEDIUM,
    category: ErrorCategory = ErrorCategory.BUSINESS_LOGIC,
    recovery_strategy: RecoveryStrategy = RecoveryStrategy.RETRY,
    **kwargs,
) -> WalletTrackerError:
    """Create a custom error with specified properties.

    Args:
        message: Error message
        error_code: Error code
        severity: Error severity
        category: Error category
        recovery_strategy: Recovery strategy
        **kwargs: Additional error properties

    Returns:
        Custom WalletTrackerError
    """
    return WalletTrackerError(
        message=message,
        error_code=error_code,
        severity=severity,
        category=category,
        recovery_strategy=recovery_strategy,
        **kwargs,
    )
