"""Tests for batch processing types and utilities."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from wallet_tracker.processors.batch_types import (
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


class TestBatchConfig:
    """Test BatchConfig class."""

    def test_default_config(self):
        """Test default batch configuration."""
        config = BatchConfig()

        assert config.batch_size == 50
        assert config.max_concurrent_jobs_per_batch == 10
        assert config.batch_delay_seconds == 1.0
        assert config.request_delay_seconds == 0.1
        assert config.timeout_seconds == 60
        assert config.skip_inactive_wallets is True
        assert config.inactive_threshold_days == 365
        assert config.min_value_threshold_usd == Decimal("1.0")
        assert config.retry_failed_jobs is True
        assert config.max_retries == 3
        assert config.use_cache is True

    def test_custom_config(self):
        """Test custom batch configuration."""
        config = BatchConfig(
            batch_size=100,
            max_concurrent_jobs_per_batch=20,
            timeout_seconds=120,
            min_value_threshold_usd=Decimal("10.0"),
            retry_failed_jobs=False
        )

        assert config.batch_size == 100
        assert config.max_concurrent_jobs_per_batch == 20
        assert config.timeout_seconds == 120
        assert config.min_value_threshold_usd == Decimal("10.0")
        assert config.retry_failed_jobs is False

    def test_validate_valid_config(self):
        """Test validation of valid configuration."""
        config = BatchConfig(
            batch_size=25,
            max_concurrent_jobs_per_batch=5,
            timeout_seconds=30,
            inactive_threshold_days=180,
            max_retries=2,
            min_value_threshold_usd=Decimal("5.0")
        )

        errors = config.validate()
        assert len(errors) == 0

    def test_validate_invalid_config(self):
        """Test validation of invalid configuration."""
        config = BatchConfig(
            batch_size=0,  # Invalid
            max_concurrent_jobs_per_batch=0,  # Invalid
            timeout_seconds=0,  # Invalid
            inactive_threshold_days=0,  # Invalid
            max_retries=-1,  # Invalid
            min_value_threshold_usd=Decimal("-1.0")  # Invalid
        )

        errors = config.validate()
        assert len(errors) == 6
        assert "batch_size must be at least 1" in errors
        assert "max_concurrent_jobs_per_batch must be at least 1" in errors
        assert "timeout_seconds must be at least 1" in errors
        assert "inactive_threshold_days must be at least 1" in errors
        assert "max_retries cannot be negative" in errors
        assert "min_value_threshold_usd cannot be negative" in errors


class TestBatchProgress:
    """Test BatchProgress class."""

    def test_initial_progress(self):
        """Test initial progress state."""
        started_at = datetime.now(timezone.utc)
        progress = BatchProgress(
            batch_id="test-batch",
            total_jobs=100,
            started_at=started_at,
            total_batches=10
        )

        assert progress.batch_id == "test-batch"
        assert progress.total_jobs == 100
        assert progress.started_at == started_at
        assert progress.total_batches == 10
        assert progress.current_batch_number == 1
        assert progress.jobs_completed == 0
        assert progress.jobs_failed == 0
        assert progress.jobs_skipped == 0
        assert progress.total_value_processed == Decimal("0")
        assert progress.current_status == "running"

    def test_update_progress(self):
        """Test updating progress with job results."""
        from wallet_tracker.processors.wallet_types import WalletProcessingJob, WalletStatus

        progress = BatchProgress(
            batch_id="test",
            total_jobs=10,
            started_at=datetime.now(timezone.utc)
        )

        # Create and complete a job
        job = WalletProcessingJob("0x123", "Test", 1)
        job.mark_started()
        job.total_value_usd = Decimal("1000")
        job.cache_hit = True
        job.api_calls_made = 2
        job.mark_completed(job.total_value_usd)

        progress.update_progress(job)

        assert progress.jobs_completed == 1
        assert progress.total_value_processed == Decimal("1000")
        assert progress.cache_hits == 1
        assert progress.api_calls_made == 2
        assert progress.estimated_completion is not None

    def test_progress_percentage(self):
        """Test progress percentage calculation."""
        progress = BatchProgress(
            batch_id="test",
            total_jobs=100,
            started_at=datetime.now(timezone.utc)
        )

        assert progress.get_progress_percentage() == 0.0

        progress.jobs_completed = 25
        progress.jobs_failed = 5
        progress.jobs_skipped = 10

        # Total processed = 25 + 5 + 10 = 40 out of 100 = 40%
        assert progress.get_progress_percentage() == 40.0

    def test_success_rate(self):
        """Test success rate calculation."""
        progress = BatchProgress(
            batch_id="test",
            total_jobs=100,
            started_at=datetime.now(timezone.utc)
        )

        # No jobs processed yet
        assert progress.get_success_rate() == 0.0

        progress.jobs_completed = 80
        progress.jobs_failed = 15
        progress.jobs_skipped = 5

        # Success rate = 80 / (80 + 15 + 5) = 80%
        assert progress.get_success_rate() == 80.0

    def test_cache_hit_rate(self):
        """Test cache hit rate calculation."""
        progress = BatchProgress(
            batch_id="test",
            total_jobs=100,
            started_at=datetime.now(timezone.utc)
        )

        # No requests yet
        assert progress.get_cache_hit_rate() == 0.0

        progress.cache_hits = 30
        progress.api_calls_made = 70

        # Cache hit rate = 30 / (30 + 70) = 30%
        assert progress.get_cache_hit_rate() == 30.0

    def test_get_summary(self):
        """Test getting progress summary."""
        started_at = datetime.now(timezone.utc)
        progress = BatchProgress(
            batch_id="test-batch",
            total_jobs=100,
            started_at=started_at,
            total_batches=5
        )

        progress.current_batch_number = 3
        progress.jobs_completed = 50
        progress.jobs_failed = 5
        progress.total_value_processed = Decimal("50000")

        summary = progress.get_summary()

        assert summary["batch_id"] == "test-batch"
        assert summary["progress_percent"] == 55.0  # (50 + 5) / 100
        assert summary["jobs_processed"] == 55
        assert summary["total_jobs"] == 100
        assert summary["current_batch"] == "3/5"
        assert summary["total_value_usd"] == 50000.0
        assert summary["performance"]["completed"] == 50
        assert summary["performance"]["failed"] == 5


class TestResourceLimits:
    """Test ResourceLimits class."""

    def test_default_limits(self):
        """Test default resource limits."""
        limits = ResourceLimits()

        assert limits.max_concurrent_batches == 3
        assert limits.max_jobs_per_batch == 1000
        assert limits.max_memory_mb == 2048
        assert limits.max_api_calls_per_minute == 100
        assert limits.max_cache_size_mb == 500
        assert limits.max_processing_time_minutes == 120

    def test_validate_valid_limits(self):
        """Test validation of valid limits."""
        limits = ResourceLimits(
            max_concurrent_batches=2,
            max_jobs_per_batch=500,
            max_memory_mb=1024,
            max_processing_time_minutes=60
        )

        errors = limits.validate()
        assert len(errors) == 0

    def test_validate_invalid_limits(self):
        """Test validation of invalid limits."""
        limits = ResourceLimits(
            max_concurrent_batches=0,  # Invalid
            max_jobs_per_batch=0,  # Invalid
            max_memory_mb=100,  # Invalid (too low)
            max_processing_time_minutes=0  # Invalid
        )

        errors = limits.validate()
        assert len(errors) == 4
        assert "max_concurrent_batches must be at least 1" in errors
        assert "max_jobs_per_batch must be at least 1" in errors
        assert "max_memory_mb must be at least 256" in errors
        assert "max_processing_time_minutes must be at least 1" in errors


class TestBatchSchedule:
    """Test BatchSchedule class."""

    def test_immediate_schedule(self):
        """Test immediate schedule type."""
        schedule = BatchSchedule(schedule_type=BatchScheduleType.IMMEDIATE)

        base_time = datetime.now(timezone.utc)
        next_time = schedule.get_next_execution_time(base_time)

        assert next_time == base_time

    def test_scheduled_execution(self):
        """Test scheduled execution."""
        execution_time = datetime.now(timezone.utc) + timedelta(hours=1)
        schedule = BatchSchedule(
            schedule_type=BatchScheduleType.SCHEDULED,
            execute_at=execution_time
        )

        next_time = schedule.get_next_execution_time()
        assert next_time == execution_time

    def test_recurring_schedule(self):
        """Test recurring schedule."""
        schedule = BatchSchedule(
            schedule_type=BatchScheduleType.RECURRING,
            interval_minutes=60
        )

        base_time = datetime.now(timezone.utc)
        next_time = schedule.get_next_execution_time(base_time)

        expected_time = base_time + timedelta(minutes=60)
        assert next_time is not None
        # Allow for small time differences due to processing
        assert abs((next_time - expected_time).total_seconds()) < 1

    def test_business_hours_constraint(self):
        """Test business hours constraint."""
        schedule = BatchSchedule(
            schedule_type=BatchScheduleType.RECURRING,
            interval_minutes=60,
            business_hours_only=True
        )

        # Test with time outside business hours (6 AM)
        base_time = datetime.now(timezone.utc).replace(hour=6, minute=0, second=0, microsecond=0)
        next_time = schedule.get_next_execution_time(base_time)

        # Should be adjusted to 9 AM
        assert next_time.hour == 9
        assert next_time.minute == 0

    def test_weekend_exclusion(self):
        """Test weekend exclusion."""
        schedule = BatchSchedule(
            schedule_type=BatchScheduleType.RECURRING,
            interval_minutes=60,
            exclude_weekends=True
        )

        # Find a Saturday (weekday 5)
        base_time = datetime.now(timezone.utc)
        while base_time.weekday() != 5:  # Saturday
            base_time += timedelta(days=1)

        next_time = schedule.get_next_execution_time(base_time)

        # Should be moved to Monday (weekday 0)
        assert next_time.weekday() == 0


class TestBatchQueueItem:
    """Test BatchQueueItem class."""

    def test_queue_item_creation(self):
        """Test creating a batch queue item."""
        created_at = datetime.now(timezone.utc)
        item = BatchQueueItem(
            batch_id="test-batch",
            batch_type=BatchType.WALLET_ANALYSIS,
            priority=QueuePriority.HIGH,
            state=BatchState.CREATED,
            created_at=created_at
        )

        assert item.batch_id == "test-batch"
        assert item.batch_type == BatchType.WALLET_ANALYSIS
        assert item.priority == QueuePriority.HIGH
        assert item.state == BatchState.CREATED
        assert item.created_at == created_at
        assert item.progress_percent == 0.0

    def test_can_execute_with_dependencies(self):
        """Test dependency checking for execution."""
        item = BatchQueueItem(
            batch_id="dependent-batch",
            batch_type=BatchType.WALLET_ANALYSIS,
            priority=QueuePriority.NORMAL,
            state=BatchState.QUEUED,
            depends_on=["batch-1", "batch-2"]
        )

        # Should not execute if dependencies not completed
        assert not item.can_execute([])
        assert not item.can_execute(["batch-1"])

        # Should execute if all dependencies completed
        assert item.can_execute(["batch-1", "batch-2"])
        assert item.can_execute(["batch-1", "batch-2", "batch-3"])

    def test_execution_time_calculation(self):
        """Test execution time calculation."""
        item = BatchQueueItem(
            batch_id="test",
            batch_type=BatchType.WALLET_ANALYSIS,
            priority=QueuePriority.NORMAL,
            state=BatchState.COMPLETED
        )

        # No execution time if not started
        assert item.get_execution_time() is None

        # Set start and end times
        start_time = datetime.now(timezone.utc)
        end_time = start_time + timedelta(minutes=5)
        item.started_at = start_time
        item.completed_at = end_time

        execution_time = item.get_execution_time()
        assert execution_time == timedelta(minutes=5)

    def test_queue_time_calculation(self):
        """Test queue time calculation."""
        item = BatchQueueItem(
            batch_id="test",
            batch_type=BatchType.WALLET_ANALYSIS,
            priority=QueuePriority.NORMAL,
            state=BatchState.RUNNING
        )

        # No queue time if not queued
        assert item.get_queue_time() is None

        # Set queue time
        queue_time = datetime.now(timezone.utc)
        start_time = queue_time + timedelta(minutes=2)
        item.queued_at = queue_time
        item.started_at = start_time

        queue_duration = item.get_queue_time()
        assert queue_duration == timedelta(minutes=2)

    def test_is_expired(self):
        """Test batch expiration check."""
        # Fresh batch should not be expired
        fresh_item = BatchQueueItem(
            batch_id="fresh",
            batch_type=BatchType.WALLET_ANALYSIS,
            priority=QueuePriority.NORMAL,
            state=BatchState.QUEUED,
            created_at=datetime.now(timezone.utc)
        )
        assert not fresh_item.is_expired(max_age_hours=24)

        # Old batch should be expired
        old_item = BatchQueueItem(
            batch_id="old",
            batch_type=BatchType.WALLET_ANALYSIS,
            priority=QueuePriority.NORMAL,
            state=BatchState.QUEUED,
            created_at=datetime.now(timezone.utc) - timedelta(hours=25)
        )
        assert old_item.is_expired(max_age_hours=24)

        # Completed batches should not expire
        completed_item = BatchQueueItem(
            batch_id="completed",
            batch_type=BatchType.WALLET_ANALYSIS,
            priority=QueuePriority.NORMAL,
            state=BatchState.COMPLETED,
            created_at=datetime.now(timezone.utc) - timedelta(hours=25)
        )
        assert not completed_item.is_expired(max_age_hours=24)

    def test_get_summary(self):
        """Test getting batch summary."""
        created_at = datetime.now(timezone.utc)
        item = BatchQueueItem(
            batch_id="test-batch",
            batch_type=BatchType.WALLET_ANALYSIS,
            priority=QueuePriority.HIGH,
            state=BatchState.RUNNING,
            created_at=created_at
        )

        item.progress_percent = 75.0
        item.current_step = "processing_wallets"
        item.resource_usage.peak_memory_mb = 512.0
        item.resource_usage.ethereum_rpc_calls = 100
        item.resource_usage.cache_hits = 50
        item.resource_usage.cache_misses = 50

        summary = item.get_summary()

        assert summary["batch_id"] == "test-batch"
        assert summary["type"] == "wallet_analysis"
        assert summary["state"] == "running"
        assert summary["priority"] == 10
        assert summary["progress_percent"] == 75.0
        assert summary["current_step"] == "processing_wallets"
        assert summary["created_at"] == created_at.isoformat()
        assert summary["resource_usage"]["peak_memory_mb"] == 512.0
        assert summary["resource_usage"]["total_api_calls"] == 100
        assert summary["resource_usage"]["cache_hit_rate"] == 50.0


class TestBatchResourceUsage:
    """Test BatchResourceUsage class."""

    def test_default_usage(self):
        """Test default resource usage."""
        usage = BatchResourceUsage()

        assert usage.peak_memory_mb == 0.0
        assert usage.cpu_time_seconds == 0.0
        assert usage.network_requests == 0
        assert usage.cache_hits == 0
        assert usage.cache_misses == 0

    def test_cache_hit_rate_calculation(self):
        """Test cache hit rate calculation."""
        usage = BatchResourceUsage()

        # No requests - should return 0
        assert usage.get_cache_hit_rate() == 0.0

        # With cache hits and misses
        usage.cache_hits = 75
        usage.cache_misses = 25

        assert usage.get_cache_hit_rate() == 75.0

    def test_total_api_calls(self):
        """Test total API calls calculation."""
        usage = BatchResourceUsage()
        usage.ethereum_rpc_calls = 100
        usage.coingecko_api_calls = 50
        usage.sheets_api_calls = 25

        assert usage.get_total_api_calls() == 175


class TestBatchQueueStats:
    """Test BatchQueueStats class."""

    def test_update_from_empty_queue(self):
        """Test updating stats from empty queue."""
        stats = BatchQueueStats()
        stats.update_from_queue([])

        assert stats.total_batches == 0
        assert stats.pending_batches == 0
        assert stats.running_batches == 0
        assert stats.completed_batches == 0

    def test_update_from_queue_with_items(self):
        """Test updating stats from queue with items."""
        # Create sample queue items
        items = [
            BatchQueueItem("b1", BatchType.WALLET_ANALYSIS, QueuePriority.NORMAL, BatchState.PENDING),
            BatchQueueItem("b2", BatchType.WALLET_ANALYSIS, QueuePriority.NORMAL, BatchState.RUNNING),
            BatchQueueItem("b3", BatchType.PRICE_UPDATE, QueuePriority.HIGH, BatchState.COMPLETED),
            BatchQueueItem("b4", BatchType.WALLET_ANALYSIS, QueuePriority.NORMAL, BatchState.FAILED),
        ]

        # Set completion time for completed item
        items[2].completed_at = datetime.now(timezone.utc)

        stats = BatchQueueStats()
        stats.update_from_queue(items)

        assert stats.total_batches == 4
        assert stats.pending_batches == 1
        assert stats.running_batches == 1
        assert stats.completed_batches == 1
        assert stats.failed_batches == 1
        assert stats.success_rate_percent == 50.0  # 1 completed out of 2 finished


class TestBatchOperation:
    """Test BatchOperation class."""

    def test_operation_creation(self):
        """Test creating a batch operation."""
        operation = BatchOperation(
            operation_id="op-1",
            operation_type="wallet_analysis",
            target_data={"addresses": ["0x123"]},
            parameters={"batch_size": 50}
        )

        assert operation.operation_id == "op-1"
        assert operation.operation_type == "wallet_analysis"
        assert operation.target_data == {"addresses": ["0x123"]}
        assert operation.parameters == {"batch_size": 50}
        assert operation.status == "pending"

    def test_operation_lifecycle(self):
        """Test operation lifecycle methods."""
        operation = BatchOperation("op-1", "test", {"data": "test"})

        # Mark as started
        operation.mark_started()
        assert operation.status == "running"
        assert operation.started_at is not None

        # Mark as completed
        result_data = {"processed": 10}
        operation.mark_completed(result_data)
        assert operation.status == "completed"
        assert operation.completed_at is not None
        assert operation.result == result_data

        # Check duration
        duration = operation.get_duration()
        assert duration is not None
        assert duration >= 0

    def test_operation_failure(self):
        """Test operation failure."""
        operation = BatchOperation("op-1", "test", {"data": "test"})
        operation.mark_started()

        error_msg = "Processing failed"
        operation.mark_failed(error_msg)

        assert operation.status == "failed"
        assert operation.completed_at is not None
        assert operation.error == error_msg


class TestUtilityFunctions:
    """Test utility functions for batch types."""

    def test_create_wallet_analysis_batch(self):
        """Test creating wallet analysis batch."""
        addresses = [
            {"address": "0x123", "label": "Wallet 1"},
            {"address": "0x456", "label": "Wallet 2"},
        ]

        batch = create_wallet_analysis_batch(
            batch_id="analysis-001",
            addresses=addresses,
            priority=QueuePriority.HIGH
        )

        assert batch.batch_id == "analysis-001"
        assert batch.batch_type == BatchType.WALLET_ANALYSIS
        assert batch.priority == QueuePriority.HIGH
        assert batch.state == BatchState.CREATED
        assert batch.input_data["addresses"] == addresses
        assert batch.input_data["wallet_count"] == 2

    def test_create_price_update_batch(self):
        """Test creating price update batch."""
        token_addresses = [
            "0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0",  # USDC
            "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
        ]

        batch = create_price_update_batch(
            batch_id="price-001",
            token_addresses=token_addresses,
            priority=QueuePriority.URGENT
        )

        assert batch.batch_id == "price-001"
        assert batch.batch_type == BatchType.PRICE_UPDATE
        assert batch.priority == QueuePriority.URGENT
        assert batch.input_data["token_addresses"] == token_addresses
        assert batch.input_data["token_count"] == 2

    def test_create_scheduled_batch(self):
        """Test creating scheduled batch."""
        schedule_time = datetime.now(timezone.utc) + timedelta(hours=1)
        input_data = {"test": "data"}

        batch = create_scheduled_batch(
            batch_id="scheduled-001",
            batch_type=BatchType.CACHE_REFRESH,
            schedule_time=schedule_time,
            input_data=input_data,
            priority=QueuePriority.LOW
        )

        assert batch.batch_id == "scheduled-001"
        assert batch.batch_type == BatchType.CACHE_REFRESH
        assert batch.priority == QueuePriority.LOW
        assert batch.state == BatchState.SCHEDULED
        assert batch.schedule.schedule_type == BatchScheduleType.SCHEDULED
        assert batch.schedule.execute_at == schedule_time
        assert batch.input_data == input_data

    def test_estimate_batch_resources(self):
        """Test resource estimation for batch operations."""
        # Test small batch
        small_limits = estimate_batch_resources(
            wallet_count=10,
            include_prices=False,
            enable_cache=False
        )

        assert small_limits.max_memory_mb >= 256  # Minimum
        assert small_limits.max_jobs_per_batch == 10

        # Test large batch
        large_limits = estimate_batch_resources(
            wallet_count=1000,
            include_prices=True,
            enable_cache=True
        )

        assert large_limits.max_memory_mb > small_limits.max_memory_mb
        assert large_limits.max_jobs_per_batch == 1000
        assert large_limits.max_processing_time_minutes > 5

    def test_estimate_batch_resources_with_cache(self):
        """Test resource estimation with caching enabled."""
        limits_with_cache = estimate_batch_resources(
            wallet_count=100,
            include_prices=True,
            enable_cache=True
        )

        limits_without_cache = estimate_batch_resources(
            wallet_count=100,
            include_prices=True,
            enable_cache=False
        )

        # With cache should require fewer API calls per minute
        assert limits_with_cache.max_api_calls_per_minute <= limits_without_cache.max_api_calls_per_minute


class TestEnums:
    """Test enum classes."""

    def test_batch_type_values(self):
        """Test BatchType enum values."""
        assert BatchType.WALLET_ANALYSIS == "wallet_analysis"
        assert BatchType.PRICE_UPDATE == "price_update"
        assert BatchType.CACHE_REFRESH == "cache_refresh"
        assert BatchType.DATA_EXPORT == "data_export"
        assert BatchType.HEALTH_CHECK == "health_check"
        assert BatchType.MAINTENANCE == "maintenance"

    def test_batch_state_values(self):
        """Test BatchState enum values."""
        assert BatchState.CREATED == "created"
        assert BatchState.QUEUED == "queued"
        assert BatchState.SCHEDULED == "scheduled"
        assert BatchState.RUNNING == "running"
        assert BatchState.PAUSED == "paused"
        assert BatchState.COMPLETED == "completed"
        assert BatchState.FAILED == "failed"
        assert BatchState.CANCELLED == "cancelled"
        assert BatchState.EXPIRED == "expired"

    def test_queue_priority_values(self):
        """Test QueuePriority enum values."""
        assert QueuePriority.LOW == 1
        assert QueuePriority.NORMAL == 5
        assert QueuePriority.HIGH == 10
        assert QueuePriority.URGENT == 20
        assert QueuePriority.CRITICAL == 50

        # Test ordering
        assert QueuePriority.LOW < QueuePriority.NORMAL
        assert QueuePriority.HIGH > QueuePriority.NORMAL
        assert QueuePriority.CRITICAL > QueuePriority.URGENT

    def test_resource_type_values(self):
        """Test ResourceType enum values."""
        assert ResourceType.CPU == "cpu"
        assert ResourceType.MEMORY == "memory"
        assert ResourceType.NETWORK == "network"
        assert ResourceType.API_QUOTA == "api_quota"
        assert ResourceType.CACHE_SPACE == "cache_space"
        assert ResourceType.DISK_SPACE == "disk_space"

    def test_batch_schedule_type_values(self):
        """Test BatchScheduleType enum values."""
        assert BatchScheduleType.IMMEDIATE == "immediate"
        assert BatchScheduleType.SCHEDULED == "scheduled"
        assert BatchScheduleType.RECURRING == "recurring"
        assert BatchScheduleType.CONDITIONAL == "conditional"