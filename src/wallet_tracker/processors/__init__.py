"""Processors package for the Crypto Wealth Tracker.

This package provides high-level processing engines for wallet analysis,
batch operations, and large-scale data processing workflows.
"""

from .batch_processor import BatchProcessor, BatchProcessorError
from .batch_types import (
    BatchConfig,
    BatchMetadata,
    BatchOperation,
    BatchProgress,
    BatchQueueItem,
    BatchQueueStats,
    BatchResourceUsage,
    BatchSchedule,
    BatchScheduleType,
    BatchState,
    BatchType,
    QueuePriority,
    ResourceLimits,
    ResourceType,
    create_price_update_batch,
    create_scheduled_batch,
    create_wallet_analysis_batch,
    estimate_batch_resources,
)
from .wallet_processor import WalletProcessor, WalletProcessorError
from .wallet_types import (
    ProcessingPriority,
    ProcessingResults,
    SkipReason,
    WalletProcessingJob,
    WalletStatus,
    WalletValidationResult,
    create_jobs_from_addresses,
    filter_jobs_by_status,
    get_retry_jobs,
    group_jobs_by_priority,
)

__all__ = [
    # Main processors
    "WalletProcessor",
    "BatchProcessor",
    # Processor errors
    "WalletProcessorError",
    "BatchProcessorError",
    # Wallet processing types
    "WalletProcessingJob",
    "WalletStatus",
    "ProcessingPriority",
    "SkipReason",
    "WalletValidationResult",
    "ProcessingResults",
    # Batch processing types
    "BatchType",
    "BatchState",
    "BatchScheduleType",
    "BatchQueueItem",
    "BatchProgress",
    "BatchConfig",
    "BatchSchedule",
    "BatchMetadata",
    "BatchResourceUsage",
    "BatchQueueStats",
    "BatchOperation",
    "QueuePriority",
    "ResourceType",
    "ResourceLimits",
    # Utility functions
    "create_jobs_from_addresses",
    "group_jobs_by_priority",
    "filter_jobs_by_status",
    "get_retry_jobs",
    "create_wallet_analysis_batch",
    "create_price_update_batch",
    "create_scheduled_batch",
    "estimate_batch_resources",
]