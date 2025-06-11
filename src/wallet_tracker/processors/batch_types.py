"""Type definitions for batch processing operations and scheduling."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Union


class BatchType(str, Enum):
    """Types of batch operations."""

    WALLET_ANALYSIS = "wallet_analysis"
    PRICE_UPDATE = "price_update"
    CACHE_REFRESH = "cache_refresh"
    DATA_EXPORT = "data_export"
    HEALTH_CHECK = "health_check"
    MAINTENANCE = "maintenance"


class BatchScheduleType(str, Enum):
    """Batch scheduling types."""

    IMMEDIATE = "immediate"
    SCHEDULED = "scheduled"
    RECURRING = "recurring"
    CONDITIONAL = "conditional"


class BatchState(str, Enum):
    """Batch execution states."""

    CREATED = "created"
    QUEUED = "queued"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class ResourceType(str, Enum):
    """Types of processing resources."""

    CPU = "cpu"
    MEMORY = "memory"
    NETWORK = "network"
    API_QUOTA = "api_quota"
    CACHE_SPACE = "cache_space"
    DISK_SPACE = "disk_space"


class QueuePriority(int, Enum):
    """Queue priority levels for batch operations."""

    LOW = 1
    NORMAL = 5
    HIGH = 10
    URGENT = 20
    CRITICAL = 50


@dataclass
class BatchConfig:
    """Configuration for batch processing operations."""

    # Basic batch settings
    batch_size: int = 50
    max_concurrent_jobs_per_batch: int = 10
    batch_delay_seconds: float = 1.0
    request_delay_seconds: float = 0.1
    timeout_seconds: int = 60

    # Processing options
    skip_inactive_wallets: bool = True
    inactive_threshold_days: int = 365
    min_value_threshold_usd: Decimal = field(default_factory=lambda: Decimal("1.0"))

    # Retry configuration
    retry_failed_jobs: bool = True
    max_retries: int = 3
    retry_delay_seconds: float = 2.0

    # Cache settings
    use_cache: bool = True
    cache_ttl_seconds: int = 3600

    # Rate limiting
    max_requests_per_minute: int = 100
    enable_adaptive_throttling: bool = True

    # Output settings
    include_inactive_in_results: bool = False
    detailed_error_reporting: bool = True

    def validate(self) -> List[str]:
        """Validate batch configuration and return any errors."""
        errors = []

        if self.batch_size < 1:
            errors.append("batch_size must be at least 1")

        if self.max_concurrent_jobs_per_batch < 1:
            errors.append("max_concurrent_jobs_per_batch must be at least 1")

        if self.timeout_seconds < 1:
            errors.append("timeout_seconds must be at least 1")

        if self.inactive_threshold_days < 1:
            errors.append("inactive_threshold_days must be at least 1")

        if self.max_retries < 0:
            errors.append("max_retries cannot be negative")

        if self.min_value_threshold_usd < 0:
            errors.append("min_value_threshold_usd cannot be negative")

        return errors


@dataclass
class BatchProgress:
    """Progress tracking for batch operations."""

    batch_id: str
    total_jobs: int
    started_at: datetime
    total_batches: int = 1
    current_batch_number: int = 1

    # Progress counters
    jobs_completed: int = 0
    jobs_failed: int = 0
    jobs_skipped: int = 0
    jobs_cached: int = 0

    # Performance metrics
    total_value_processed: Decimal = field(default_factory=lambda: Decimal("0"))
    api_calls_made: int = 0
    cache_hits: int = 0

    # Status
    current_status: str = "running"
    current_operation: str = "initializing"
    estimated_completion: Optional[datetime] = None

    def update_progress(self, job: 'WalletProcessingJob') -> None:
        """Update progress with completed job."""
        from .wallet_types import WalletStatus

        if job.status == WalletStatus.COMPLETED:
            self.jobs_completed += 1
            if job.total_value_usd:
                self.total_value_processed += job.total_value_usd
        elif job.status == WalletStatus.FAILED:
            self.jobs_failed += 1
        elif job.status == WalletStatus.SKIPPED:
            self.jobs_skipped += 1
        elif job.status == WalletStatus.CACHED:
            self.jobs_cached += 1

        self.api_calls_made += job.api_calls_made

        if job.cache_hit:
            self.cache_hits += 1

        # Update estimated completion
        self._update_estimated_completion()

    def _update_estimated_completion(self) -> None:
        """Update estimated completion time based on current progress."""
        jobs_processed = self.jobs_completed + self.jobs_failed + self.jobs_skipped + self.jobs_cached

        if jobs_processed > 0:
            elapsed = datetime.now(timezone.utc) - self.started_at
            avg_time_per_job = elapsed.total_seconds() / jobs_processed
            remaining_jobs = self.total_jobs - jobs_processed
            remaining_seconds = remaining_jobs * avg_time_per_job

            self.estimated_completion = datetime.now(timezone.utc) + timedelta(seconds=remaining_seconds)

    def get_progress_percentage(self) -> float:
        """Get completion percentage."""
        if self.total_jobs == 0:
            return 100.0

        jobs_processed = self.jobs_completed + self.jobs_failed + self.jobs_skipped + self.jobs_cached
        return (jobs_processed / self.total_jobs) * 100

    def get_success_rate(self) -> float:
        """Get success rate percentage."""
        jobs_processed = self.jobs_completed + self.jobs_failed + self.jobs_skipped + self.jobs_cached

        if jobs_processed == 0:
            return 0.0

        return (self.jobs_completed / jobs_processed) * 100

    def get_cache_hit_rate(self) -> float:
        """Get cache hit rate percentage."""
        total_requests = self.api_calls_made + self.cache_hits

        if total_requests == 0:
            return 0.0

        return (self.cache_hits / total_requests) * 100

    def get_summary(self) -> Dict[str, Any]:
        """Get progress summary for reporting."""
        jobs_processed = self.jobs_completed + self.jobs_failed + self.jobs_skipped + self.jobs_cached
        elapsed = datetime.now(timezone.utc) - self.started_at

        return {
            "batch_id": self.batch_id,
            "progress_percent": self.get_progress_percentage(),
            "jobs_processed": jobs_processed,
            "total_jobs": self.total_jobs,
            "current_batch": f"{self.current_batch_number}/{self.total_batches}",
            "status": self.current_status,
            "operation": self.current_operation,
            "elapsed_seconds": elapsed.total_seconds(),
            "estimated_completion": self.estimated_completion.isoformat() if self.estimated_completion else None,
            "success_rate": self.get_success_rate(),
            "cache_hit_rate": self.get_cache_hit_rate(),
            "total_value_usd": float(self.total_value_processed),
            "performance": {
                "completed": self.jobs_completed,
                "failed": self.jobs_failed,
                "skipped": self.jobs_skipped,
                "cached": self.jobs_cached,
                "api_calls": self.api_calls_made,
            }
        }


@dataclass
class ResourceLimits:
    """Resource limits for batch operations."""

    max_concurrent_batches: int = 3
    max_jobs_per_batch: int = 1000
    max_memory_mb: int = 2048
    max_api_calls_per_minute: int = 100
    max_cache_size_mb: int = 500
    max_processing_time_minutes: int = 120

    # Rate limiting
    ethereum_rpc_limit: int = 100
    coingecko_api_limit: int = 30
    sheets_api_limit: int = 100

    def validate(self) -> List[str]:
        """Validate resource limits and return any errors."""
        errors = []

        if self.max_concurrent_batches < 1:
            errors.append("max_concurrent_batches must be at least 1")

        if self.max_jobs_per_batch < 1:
            errors.append("max_jobs_per_batch must be at least 1")

        if self.max_memory_mb < 256:
            errors.append("max_memory_mb must be at least 256")

        if self.max_processing_time_minutes < 1:
            errors.append("max_processing_time_minutes must be at least 1")

        return errors


@dataclass
class BatchSchedule:
    """Scheduling configuration for batch operations."""

    schedule_type: BatchScheduleType

    # Immediate execution
    execute_at: Optional[datetime] = None

    # Recurring execution
    interval_minutes: Optional[int] = None
    cron_expression: Optional[str] = None

    # Conditional execution
    condition_check_interval: int = 60  # seconds
    max_wait_time_minutes: int = 60

    # Schedule constraints
    timezone: str = "UTC"
    business_hours_only: bool = False
    exclude_weekends: bool = False

    def get_next_execution_time(self, from_time: Optional[datetime] = None) -> Optional[datetime]:
        """Calculate next execution time based on schedule."""
        base_time = from_time or datetime.now(timezone.utc)

        if self.schedule_type == BatchScheduleType.IMMEDIATE:
            return base_time

        elif self.schedule_type == BatchScheduleType.SCHEDULED:
            return self.execute_at

        elif self.schedule_type == BatchScheduleType.RECURRING:
            if self.interval_minutes:
                next_time = base_time + timedelta(minutes=self.interval_minutes)
                return self._adjust_for_constraints(next_time)
            elif self.cron_expression:
                # Would need cron parser library for full implementation
                return None

        return None

    def _adjust_for_constraints(self, execution_time: datetime) -> datetime:
        """Adjust execution time for business hour and weekend constraints."""
        if not (self.business_hours_only or self.exclude_weekends):
            return execution_time

        # Simple business hours: 9 AM - 5 PM Monday-Friday
        while True:
            weekday = execution_time.weekday()
            hour = execution_time.hour

            # Skip weekends if configured
            if self.exclude_weekends and weekday >= 5:  # Saturday = 5, Sunday = 6
                # Move to next Monday 9 AM
                days_to_monday = 7 - weekday
                execution_time = execution_time.replace(hour=9, minute=0, second=0) + timedelta(days=days_to_monday)
                continue

            # Adjust for business hours if configured
            if self.business_hours_only:
                if hour < 9:
                    execution_time = execution_time.replace(hour=9, minute=0, second=0)
                elif hour >= 17:
                    # Move to next day 9 AM
                    execution_time = (execution_time + timedelta(days=1)).replace(hour=9, minute=0, second=0)
                    continue

            break

        return execution_time


@dataclass
class BatchMetadata:
    """Metadata for batch operations."""

    created_by: str = "system"
    tags: List[str] = field(default_factory=list)
    description: str = ""
    version: str = "1.0"

    # Source information
    source_spreadsheet_id: Optional[str] = None
    source_range: Optional[str] = None
    source_row_count: Optional[int] = None

    # Output information
    output_spreadsheet_id: Optional[str] = None
    output_worksheet: Optional[str] = None
    output_format: str = "google_sheets"

    # Processing preferences
    enable_cache: bool = True
    skip_validation: bool = False
    include_inactive_wallets: bool = False

    # Notification settings
    notify_on_completion: bool = False
    notification_email: Optional[str] = None
    notification_webhook: Optional[str] = None


@dataclass
class BatchResourceUsage:
    """Resource usage tracking for batch operations."""

    # Memory usage
    peak_memory_mb: float = 0.0
    average_memory_mb: float = 0.0

    # CPU usage
    cpu_time_seconds: float = 0.0
    cpu_utilization_percent: float = 0.0

    # Network usage
    bytes_downloaded: int = 0
    bytes_uploaded: int = 0
    network_requests: int = 0

    # API usage
    ethereum_rpc_calls: int = 0
    coingecko_api_calls: int = 0
    sheets_api_calls: int = 0

    # Cache usage
    cache_hits: int = 0
    cache_misses: int = 0
    cache_size_mb: float = 0.0

    # Storage usage
    temp_files_created: int = 0
    temp_storage_mb: float = 0.0

    def get_cache_hit_rate(self) -> float:
        """Calculate cache hit rate percentage."""
        total_requests = self.cache_hits + self.cache_misses
        if total_requests == 0:
            return 0.0
        return (self.cache_hits / total_requests) * 100

    def get_total_api_calls(self) -> int:
        """Get total API calls across all services."""
        return self.ethereum_rpc_calls + self.coingecko_api_calls + self.sheets_api_calls


@dataclass
class BatchQueueItem:
    """Item in the batch processing queue."""

    batch_id: str
    batch_type: BatchType
    priority: QueuePriority
    state: BatchState

    # Timing
    created_at: datetime = field(default_factory=datetime.utcnow)
    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Configuration
    schedule: Optional[BatchSchedule] = None
    metadata: BatchMetadata = field(default_factory=BatchMetadata)
    resource_limits: ResourceLimits = field(default_factory=ResourceLimits)

    # Input data
    input_data: Dict[str, Any] = field(default_factory=dict)

    # Progress tracking
    progress_percent: float = 0.0
    current_step: str = "pending"
    estimated_completion: Optional[datetime] = None

    # Results
    output_data: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    warning_messages: List[str] = field(default_factory=list)

    # Resource usage
    resource_usage: BatchResourceUsage = field(default_factory=BatchResourceUsage)

    # Dependencies
    depends_on: List[str] = field(default_factory=list)  # Other batch IDs
    blocks: List[str] = field(default_factory=list)  # Batches blocked by this one

    def can_execute(self, completed_batches: List[str]) -> bool:
        """Check if batch can execute based on dependencies."""
        return all(dep in completed_batches for dep in self.depends_on)

    def get_execution_time(self) -> Optional[timedelta]:
        """Get total execution time if completed."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None

    def get_queue_time(self) -> Optional[timedelta]:
        """Get time spent in queue."""
        if self.queued_at and self.started_at:
            return self.started_at - self.queued_at
        elif self.queued_at:
            return datetime.now(timezone.utc) - self.queued_at
        return None

    def is_expired(self, max_age_hours: int = 24) -> bool:
        """Check if batch has expired."""
        if self.state in [BatchState.COMPLETED, BatchState.FAILED, BatchState.CANCELLED]:
            return False

        age = datetime.now(timezone.utc) - self.created_at
        return age.total_seconds() > (max_age_hours * 3600)

    def get_summary(self) -> Dict[str, Any]:
        """Get batch summary for reporting."""
        execution_time = self.get_execution_time()
        queue_time = self.get_queue_time()

        return {
            "batch_id": self.batch_id,
            "type": self.batch_type.value,
            "state": self.state.value,
            "priority": self.priority.value,
            "progress_percent": self.progress_percent,
            "current_step": self.current_step,
            "created_at": self.created_at.isoformat(),
            "execution_time_seconds": execution_time.total_seconds() if execution_time else None,
            "queue_time_seconds": queue_time.total_seconds() if queue_time else None,
            "resource_usage": {
                "peak_memory_mb": self.resource_usage.peak_memory_mb,
                "total_api_calls": self.resource_usage.get_total_api_calls(),
                "cache_hit_rate": self.resource_usage.get_cache_hit_rate(),
            },
            "input_size": len(self.input_data),
            "output_size": len(self.output_data),
            "has_errors": bool(self.error_message),
            "warning_count": len(self.warning_messages),
        }


@dataclass
class BatchQueueStats:
    """Statistics for the batch processing queue."""

    # Queue counts
    total_batches: int = 0
    pending_batches: int = 0
    running_batches: int = 0
    completed_batches: int = 0
    failed_batches: int = 0

    # Performance metrics
    average_execution_time_seconds: float = 0.0
    average_queue_time_seconds: float = 0.0
    throughput_batches_per_hour: float = 0.0

    # Resource utilization
    total_memory_usage_mb: float = 0.0
    total_api_calls: int = 0
    average_cache_hit_rate: float = 0.0

    # Success rates
    success_rate_percent: float = 0.0
    retry_rate_percent: float = 0.0

    # Time tracking
    oldest_pending_batch_age_hours: float = 0.0
    last_completed_at: Optional[datetime] = None

    def update_from_queue(self, queue_items: List[BatchQueueItem]) -> None:
        """Update statistics from current queue state."""
        if not queue_items:
            return

        # Count by state
        self.total_batches = len(queue_items)
        self.pending_batches = len(
            [b for b in queue_items if b.state in [BatchState.CREATED, BatchState.QUEUED, BatchState.SCHEDULED]])
        self.running_batches = len([b for b in queue_items if b.state == BatchState.RUNNING])
        self.completed_batches = len([b for b in queue_items if b.state == BatchState.COMPLETED])
        self.failed_batches = len([b for b in queue_items if b.state == BatchState.FAILED])

        # Calculate performance metrics
        completed_items = [b for b in queue_items if b.state == BatchState.COMPLETED]

        if completed_items:
            execution_times = [b.get_execution_time().total_seconds() for b in completed_items if
                               b.get_execution_time()]
            if execution_times:
                self.average_execution_time_seconds = sum(execution_times) / len(execution_times)

            queue_times = [b.get_queue_time().total_seconds() for b in completed_items if b.get_queue_time()]
            if queue_times:
                self.average_queue_time_seconds = sum(queue_times) / len(queue_times)

            # Calculate throughput (completed batches in last hour)
            now = datetime.now(timezone.utc)
            hour_ago = now - timedelta(hours=1)
            recent_completions = [b for b in completed_items if b.completed_at and b.completed_at > hour_ago]
            self.throughput_batches_per_hour = len(recent_completions)

            # Get latest completion time
            completion_times = [b.completed_at for b in completed_items if b.completed_at]
            if completion_times:
                self.last_completed_at = max(completion_times)

        # Calculate success rate
        finished_batches = self.completed_batches + self.failed_batches
        if finished_batches > 0:
            self.success_rate_percent = (self.completed_batches / finished_batches) * 100

        # Resource utilization
        self.total_memory_usage_mb = sum(b.resource_usage.peak_memory_mb for b in queue_items)
        self.total_api_calls = sum(b.resource_usage.get_total_api_calls() for b in queue_items)

        # Average cache hit rate
        cache_rates = [b.resource_usage.get_cache_hit_rate() for b in queue_items]
        valid_rates = [r for r in cache_rates if r > 0]
        if valid_rates:
            self.average_cache_hit_rate = sum(valid_rates) / len(valid_rates)

        # Oldest pending batch
        pending_items = [b for b in queue_items if b.state in [BatchState.CREATED, BatchState.QUEUED]]
        if pending_items:
            oldest = min(pending_items, key=lambda x: x.created_at)
            age = datetime.now(timezone.utc) - oldest.created_at
            self.oldest_pending_batch_age_hours = age.total_seconds() / 3600


@dataclass
class BatchOperation:
    """Represents a single batch operation to be executed."""

    operation_id: str
    operation_type: str
    target_data: Any
    parameters: Dict[str, Any] = field(default_factory=dict)

    # Status tracking
    status: str = "pending"
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Results
    result: Optional[Any] = None
    error: Optional[str] = None
    metrics: Dict[str, float] = field(default_factory=dict)

    def mark_started(self) -> None:
        """Mark operation as started."""
        self.status = "running"
        self.started_at = datetime.now(timezone.utc)

    def mark_completed(self, result: Any = None) -> None:
        """Mark operation as completed."""
        self.status = "completed"
        self.completed_at = datetime.now(timezone.utc)
        self.result = result

    def mark_failed(self, error: str) -> None:
        """Mark operation as failed."""
        self.status = "failed"
        self.completed_at = datetime.now(timezone.utc)
        self.error = error

    def get_duration(self) -> Optional[float]:
        """Get operation duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


# Utility functions for batch operations

def create_wallet_analysis_batch(
        batch_id: str,
        addresses: List[Dict[str, str]],
        priority: QueuePriority = QueuePriority.NORMAL,
        resource_limits: Optional[ResourceLimits] = None,
        metadata: Optional[BatchMetadata] = None,
) -> BatchQueueItem:
    """Create a wallet analysis batch queue item.

    Args:
        batch_id: Unique batch identifier
        addresses: List of wallet addresses to analyze
        priority: Processing priority
        resource_limits: Resource constraints
        metadata: Additional metadata

    Returns:
        BatchQueueItem ready for queue
    """
    return BatchQueueItem(
        batch_id=batch_id,
        batch_type=BatchType.WALLET_ANALYSIS,
        priority=priority,
        state=BatchState.CREATED,
        resource_limits=resource_limits or ResourceLimits(),
        metadata=metadata or BatchMetadata(),
        input_data={
            "addresses": addresses,
            "wallet_count": len(addresses),
        },
    )


def create_price_update_batch(
        batch_id: str,
        token_addresses: List[str],
        priority: QueuePriority = QueuePriority.HIGH,
) -> BatchQueueItem:
    """Create a price update batch queue item.

    Args:
        batch_id: Unique batch identifier
        token_addresses: List of token contract addresses
        priority: Processing priority

    Returns:
        BatchQueueItem ready for queue
    """
    return BatchQueueItem(
        batch_id=batch_id,
        batch_type=BatchType.PRICE_UPDATE,
        priority=priority,
        state=BatchState.CREATED,
        input_data={
            "token_addresses": token_addresses,
            "token_count": len(token_addresses),
        },
    )


def create_scheduled_batch(
        batch_id: str,
        batch_type: BatchType,
        schedule_time: datetime,
        input_data: Dict[str, Any],
        priority: QueuePriority = QueuePriority.NORMAL,
) -> BatchQueueItem:
    """Create a scheduled batch queue item.

    Args:
        batch_id: Unique batch identifier
        batch_type: Type of batch operation
        schedule_time: When to execute the batch
        input_data: Input data for the batch
        priority: Processing priority

    Returns:
        BatchQueueItem ready for scheduling
    """
    schedule = BatchSchedule(
        schedule_type=BatchScheduleType.SCHEDULED,
        execute_at=schedule_time,
    )

    return BatchQueueItem(
        batch_id=batch_id,
        batch_type=batch_type,
        priority=priority,
        state=BatchState.SCHEDULED,
        schedule=schedule,
        input_data=input_data,
    )


def estimate_batch_resources(
        wallet_count: int,
        include_prices: bool = True,
        enable_cache: bool = True,
) -> ResourceLimits:
    """Estimate resource requirements for a wallet analysis batch.

    Args:
        wallet_count: Number of wallets to analyze
        include_prices: Whether to fetch token prices
        enable_cache: Whether caching is enabled

    Returns:
        Estimated resource limits
    """
    # Base calculation
    base_memory_mb = max(256, wallet_count * 0.5)  # ~0.5MB per wallet
    base_api_calls = wallet_count * 2  # Ethereum + metadata calls

    if include_prices:
        base_api_calls += wallet_count * 0.1  # Token price calls (batched)

    if enable_cache:
        base_api_calls *= 0.7  # 30% cache hit rate assumption

    # Calculate processing time (rough estimate)
    estimated_time_minutes = max(5, wallet_count * 0.01)  # ~0.6s per wallet

    return ResourceLimits(
        max_memory_mb=int(base_memory_mb * 1.2),  # 20% buffer
        max_api_calls_per_minute=min(100, int(base_api_calls / estimated_time_minutes)),
        max_processing_time_minutes=int(estimated_time_minutes * 1.5),  # 50% buffer
        max_jobs_per_batch=wallet_count,
    )