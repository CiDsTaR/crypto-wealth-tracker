"""Tests for wallet processing types and utilities."""

from datetime import datetime
from decimal import Decimal

from wallet_tracker.processors.wallet_types import (
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


class TestWalletProcessingJob:
    """Test WalletProcessingJob class."""

    def test_job_creation(self):
        """Test creating a wallet processing job."""
        job = WalletProcessingJob(
            address="0x123", label="Test Wallet", row_number=1, priority=ProcessingPriority.HIGH, max_retries=2
        )

        assert job.address == "0x123"
        assert job.label == "Test Wallet"
        assert job.row_number == 1
        assert job.status == WalletStatus.PENDING
        assert job.priority == ProcessingPriority.HIGH
        assert job.max_retries == 2
        assert job.retry_count == 0

    def test_mark_started(self):
        """Test marking job as started."""
        job = WalletProcessingJob(address="0x123", label="Test", row_number=1)

        job.mark_started()

        assert job.status == WalletStatus.PROCESSING
        assert job.started_at is not None
        assert isinstance(job.started_at, datetime)

    def test_mark_completed(self):
        """Test marking job as completed."""
        job = WalletProcessingJob(address="0x123", label="Test", row_number=1)
        job.mark_started()

        total_value = Decimal("1000.50")
        job.mark_completed(total_value)

        assert job.status == WalletStatus.COMPLETED
        assert job.total_value_usd == total_value
        assert job.completed_at is not None
        assert job.processing_time_seconds is not None
        assert job.processing_time_seconds >= 0

    def test_mark_failed(self):
        """Test marking job as failed."""
        job = WalletProcessingJob(address="0x123", label="Test", row_number=1)
        job.mark_started()

        error_msg = "API Error"
        skip_reason = SkipReason.API_ERROR
        job.mark_failed(error_msg, skip_reason)

        assert job.status == WalletStatus.FAILED
        assert job.error_message == error_msg
        assert job.skip_reason == skip_reason
        assert job.completed_at is not None

    def test_mark_skipped(self):
        """Test marking job as skipped."""
        job = WalletProcessingJob(address="0x123", label="Test", row_number=1)
        job.mark_started()

        reason = SkipReason.INACTIVE
        message = "Wallet inactive"
        job.mark_skipped(reason, message)

        assert job.status == WalletStatus.SKIPPED
        assert job.skip_reason == reason
        assert job.error_message == message
        assert job.completed_at is not None

    def test_should_retry_logic(self):
        """Test retry logic."""
        job = WalletProcessingJob(address="0x123", label="Test", row_number=1, max_retries=2)

        # Fresh job should not retry
        assert not job.should_retry()

        # Failed job should retry if under limit
        job.mark_failed("Temporary error")
        assert job.should_retry()

        # Failed job should not retry if over limit
        job.retry_count = 3
        assert not job.should_retry()

        # Failed job should not retry for certain skip reasons
        job.retry_count = 0
        job.mark_failed("Invalid address", SkipReason.INVALID_ADDRESS)
        assert not job.should_retry()

        job.mark_failed("Blacklisted", SkipReason.BLACKLISTED)
        assert not job.should_retry()

    def test_prepare_retry(self):
        """Test preparing job for retry."""
        job = WalletProcessingJob(address="0x123", label="Test", row_number=1, max_retries=2)
        job.mark_started()
        job.mark_failed("Temporary error")

        original_retry_count = job.retry_count
        job.prepare_retry()

        assert job.retry_count == original_retry_count + 1
        assert job.status == WalletStatus.PENDING
        assert job.started_at is None
        assert job.completed_at is None
        assert job.processing_time_seconds is None
        assert job.error_message is None

    def test_get_processing_summary(self):
        """Test getting processing summary."""
        job = WalletProcessingJob(address="0x123", label="Test Wallet", row_number=1)
        job.mark_started()
        job.total_value_usd = Decimal("1500.75")
        job.eth_balance = Decimal("0.5")
        job.token_balances = {"USDC": Decimal("1000"), "USDT": Decimal("500")}
        job.is_active = True
        job.transaction_count = 150
        job.cache_hit = True
        job.api_calls_made = 3
        job.mark_completed(job.total_value_usd)

        summary = job.get_processing_summary()

        assert summary["address"] == "0x123"
        assert summary["label"] == "Test Wallet"
        assert summary["status"] == "completed"
        assert summary["total_value_usd"] == 1500.75
        assert summary["eth_balance"] == 0.5
        assert summary["token_count"] == 2
        assert summary["is_active"] is True
        assert summary["transaction_count"] == 150
        assert summary["cache_hit"] is True
        assert summary["api_calls_made"] == 3
        assert summary["retry_count"] == 0
        assert summary["error_message"] is None
        assert summary["skip_reason"] is None


class TestProcessingResults:
    """Test ProcessingResults class."""

    def test_empty_results(self):
        """Test creating empty processing results."""
        results = ProcessingResults(total_wallets_input=10)

        assert results.total_wallets_input == 10
        assert results.wallets_processed == 0
        assert results.wallets_skipped == 0
        assert results.wallets_failed == 0
        assert results.total_portfolio_value == Decimal("0")

    def test_finalize_results_with_jobs(self):
        """Test finalizing results with completed jobs."""
        # Create sample jobs
        jobs = []

        # Completed jobs
        for i in range(3):
            job = WalletProcessingJob(address=f"0x{i}", label=f"Wallet {i}", row_number=i + 1)
            job.mark_started()
            job.total_value_usd = Decimal(str(1000 * (i + 1)))  # 1000, 2000, 3000
            job.eth_balance = Decimal("1.0")
            job.token_balances = {"USDC": Decimal("500")} if i == 0 else {}
            job.is_active = i < 2  # First two are active
            job.cache_hit = i == 0  # First one is cache hit
            job.api_calls_made = 2
            job.mark_completed(job.total_value_usd)
            jobs.append(job)

        # Failed job
        failed_job = WalletProcessingJob(address="0xfailed", label="Failed", row_number=4)
        failed_job.mark_started()
        failed_job.mark_failed("API error", SkipReason.API_ERROR)
        jobs.append(failed_job)

        # Skipped job
        skipped_job = WalletProcessingJob(address="0xskipped", label="Skipped", row_number=5)
        skipped_job.mark_started()
        skipped_job.mark_skipped(SkipReason.INACTIVE, "Inactive wallet")
        jobs.append(skipped_job)

        # Finalize results
        results = ProcessingResults(total_wallets_input=5)
        results.finalize_results(jobs)

        # Check basic counts
        assert results.wallets_processed == 3
        assert results.wallets_failed == 1
        assert results.wallets_skipped == 1

        # Check portfolio values
        assert results.total_portfolio_value == Decimal("6000")  # 1000 + 2000 + 3000
        assert results.average_portfolio_value == Decimal("2000")  # 6000 / 3
        assert results.median_portfolio_value == Decimal("2000")  # Middle value
        assert results.max_portfolio_value == Decimal("3000")
        assert results.min_portfolio_value == Decimal("1000")

        # Check activity stats
        assert results.active_wallets == 2
        assert results.inactive_wallets == 1

        # Check token holders
        assert results.eth_holders == 3
        assert results.usdc_holders == 1

        # Check performance metrics
        assert results.cache_hit_rate > 0  # Should be > 0 since one job had cache hit
        assert results.api_calls_total == 6  # 3 successful jobs * 2 calls each

        # Check error breakdown
        assert SkipReason.API_ERROR in results.skip_reasons
        assert results.skip_reasons[SkipReason.API_ERROR] == 1
        assert SkipReason.INACTIVE in results.skip_reasons
        assert results.skip_reasons[SkipReason.INACTIVE] == 1

    def test_categorize_error_types(self):
        """Test error categorization."""
        results = ProcessingResults(total_wallets_input=1)

        # Test different error types
        assert results._categorize_error("Rate limit exceeded") == "rate_limit"
        assert results._categorize_error("Too many requests") == "rate_limit"
        assert results._categorize_error("Connection timeout") == "timeout"
        assert results._categorize_error("Network error") == "network"
        assert results._categorize_error("Connection failed") == "network"
        assert results._categorize_error("Unauthorized access") == "authentication"
        assert results._categorize_error("Permission denied") == "authentication"
        assert results._categorize_error("Not found") == "not_found"
        assert results._categorize_error("Invalid data format") == "invalid_data"
        assert results._categorize_error("Unknown error occurred") == "unknown"

    def test_get_summary_dict(self):
        """Test getting summary as dictionary."""
        results = ProcessingResults(total_wallets_input=10)
        results.wallets_processed = 8
        results.wallets_skipped = 1
        results.wallets_failed = 1
        results.total_portfolio_value = Decimal("50000")
        results.average_portfolio_value = Decimal("6250")

        summary = results.get_summary_dict()

        assert summary["input"]["total_wallets"] == 10
        assert summary["results"]["processed"] == 8
        assert summary["results"]["skipped"] == 1
        assert summary["results"]["failed"] == 1
        assert summary["results"]["success_rate"] == 80.0  # 8/10 * 100
        assert summary["portfolio_values"]["total_usd"] == 50000.0
        assert summary["portfolio_values"]["average_usd"] == 6250.0


class TestWalletValidationResult:
    """Test WalletValidationResult class."""

    def test_valid_result(self):
        """Test creating valid validation result."""
        result = WalletValidationResult.valid("0xABC123", "0xabc123")

        assert result.address == "0xABC123"
        assert result.is_valid is True
        assert result.normalized_address == "0xabc123"
        assert result.error_message is None

    def test_invalid_result(self):
        """Test creating invalid validation result."""
        result = WalletValidationResult.invalid("invalid", "Not a valid address")

        assert result.address == "invalid"
        assert result.is_valid is False
        assert result.normalized_address is None
        assert result.error_message == "Not a valid address"


class TestUtilityFunctions:
    """Test utility functions for wallet types."""

    def test_create_jobs_from_addresses(self):
        """Test creating jobs from address list."""
        addresses = [
            {"address": "0x123", "label": "Wallet 1", "row_number": 1},
            {"address": "0x456", "label": "Wallet 2", "row_number": 2},
            {"address": "0x789", "label": "Wallet 3", "row_number": 3},
        ]

        jobs = create_jobs_from_addresses(addresses, priority=ProcessingPriority.HIGH, max_retries=5)

        assert len(jobs) == 3
        for i, job in enumerate(jobs):
            assert job.address == addresses[i]["address"]
            assert job.label == addresses[i]["label"]
            assert job.row_number == addresses[i]["row_number"]
            assert job.priority == ProcessingPriority.HIGH
            assert job.max_retries == 5
            assert job.status == WalletStatus.PENDING

    def test_group_jobs_by_priority(self):
        """Test grouping jobs by priority."""
        jobs = [
            WalletProcessingJob("0x1", "W1", 1, priority=ProcessingPriority.LOW),
            WalletProcessingJob("0x2", "W2", 2, priority=ProcessingPriority.HIGH),
            WalletProcessingJob("0x3", "W3", 3, priority=ProcessingPriority.LOW),
            WalletProcessingJob("0x4", "W4", 4, priority=ProcessingPriority.URGENT),
            WalletProcessingJob("0x5", "W5", 5, priority=ProcessingPriority.HIGH),
        ]

        groups = group_jobs_by_priority(jobs)

        assert len(groups[ProcessingPriority.LOW]) == 2
        assert len(groups[ProcessingPriority.NORMAL]) == 0
        assert len(groups[ProcessingPriority.HIGH]) == 2
        assert len(groups[ProcessingPriority.URGENT]) == 1

        # Check specific assignments
        assert groups[ProcessingPriority.LOW][0].address == "0x1"
        assert groups[ProcessingPriority.LOW][1].address == "0x3"
        assert groups[ProcessingPriority.URGENT][0].address == "0x4"

    def test_filter_jobs_by_status(self):
        """Test filtering jobs by status."""
        jobs = [
            WalletProcessingJob("0x1", "W1", 1),  # PENDING
            WalletProcessingJob("0x2", "W2", 2),  # Will be COMPLETED
            WalletProcessingJob("0x3", "W3", 3),  # Will be FAILED
            WalletProcessingJob("0x4", "W4", 4),  # Will be COMPLETED
        ]

        # Change some statuses
        jobs[1].mark_completed(Decimal("1000"))
        jobs[2].mark_failed("Error")
        jobs[3].mark_completed(Decimal("2000"))

        pending_jobs = filter_jobs_by_status(jobs, WalletStatus.PENDING)
        completed_jobs = filter_jobs_by_status(jobs, WalletStatus.COMPLETED)
        failed_jobs = filter_jobs_by_status(jobs, WalletStatus.FAILED)

        assert len(pending_jobs) == 1
        assert len(completed_jobs) == 2
        assert len(failed_jobs) == 1

        assert pending_jobs[0].address == "0x1"
        assert completed_jobs[0].address == "0x2"
        assert completed_jobs[1].address == "0x4"
        assert failed_jobs[0].address == "0x3"

    def test_get_retry_jobs(self):
        """Test getting jobs that should be retried."""
        jobs = [
            WalletProcessingJob("0x1", "W1", 1, max_retries=2),  # Will succeed
            WalletProcessingJob("0x2", "W2", 2, max_retries=2),  # Will fail but can retry
            WalletProcessingJob("0x3", "W3", 3, max_retries=2),  # Will fail, max retries reached
            WalletProcessingJob("0x4", "W4", 4, max_retries=2),  # Will fail with non-retryable reason
        ]

        # Set up job states
        jobs[0].mark_completed(Decimal("1000"))  # Successful - no retry

        jobs[1].mark_failed("Temporary error")  # Failed but retryable

        jobs[2].mark_failed("Another error")  # Failed but max retries
        jobs[2].retry_count = 2

        jobs[3].mark_failed("Invalid address", SkipReason.INVALID_ADDRESS)  # Non-retryable

        retry_jobs = get_retry_jobs(jobs)

        assert len(retry_jobs) == 1
        assert retry_jobs[0].address == "0x2"
        assert retry_jobs[0].status == WalletStatus.PENDING  # Should be reset
        assert retry_jobs[0].retry_count == 1  # Should be incremented


class TestEnums:
    """Test enum classes."""

    def test_wallet_status_values(self):
        """Test WalletStatus enum values."""
        assert WalletStatus.PENDING == "pending"
        assert WalletStatus.PROCESSING == "processing"
        assert WalletStatus.COMPLETED == "completed"
        assert WalletStatus.FAILED == "failed"
        assert WalletStatus.SKIPPED == "skipped"
        assert WalletStatus.CACHED == "cached"

    def test_skip_reason_values(self):
        """Test SkipReason enum values."""
        assert SkipReason.INACTIVE == "inactive"
        assert SkipReason.LOW_VALUE == "low_value"
        assert SkipReason.INVALID_ADDRESS == "invalid_address"
        assert SkipReason.API_ERROR == "api_error"
        assert SkipReason.RATE_LIMITED == "rate_limited"
        assert SkipReason.BLACKLISTED == "blacklisted"
        assert SkipReason.TIMEOUT == "timeout"

    def test_processing_priority_values(self):
        """Test ProcessingPriority enum values."""
        assert ProcessingPriority.LOW == 1
        assert ProcessingPriority.NORMAL == 2
        assert ProcessingPriority.HIGH == 3
        assert ProcessingPriority.URGENT == 4

        # Test ordering
        assert ProcessingPriority.LOW < ProcessingPriority.NORMAL
        assert ProcessingPriority.HIGH > ProcessingPriority.NORMAL
        assert ProcessingPriority.URGENT > ProcessingPriority.HIGH
