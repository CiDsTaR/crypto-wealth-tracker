"""Type definitions for individual wallet processing operations."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Union


class WalletStatus(str, Enum):
    """Wallet processing status."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CACHED = "cached"


class SkipReason(str, Enum):
    """Reasons for skipping wallet processing."""

    INACTIVE = "inactive"  # No activity in threshold period
    LOW_VALUE = "low_value"  # Below minimum value threshold
    INVALID_ADDRESS = "invalid_address"  # Invalid Ethereum address
    API_ERROR = "api_error"  # API request failed
    RATE_LIMITED = "rate_limited"  # Rate limit exceeded
    BLACKLISTED = "blacklisted"  # Wallet in blacklist
    TIMEOUT = "timeout"  # Processing timeout


class ProcessingPriority(int, Enum):
    """Processing priority levels."""

    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


@dataclass
class WalletProcessingJob:
    """Individual wallet processing job."""

    address: str
    label: str
    row_number: int
    status: WalletStatus = WalletStatus.PENDING
    priority: ProcessingPriority = ProcessingPriority.NORMAL
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Processing results
    eth_balance: Optional[Decimal] = None
    token_balances: Dict[str, Decimal] = field(default_factory=dict)
    token_prices: Dict[str, Decimal] = field(default_factory=dict)
    total_value_usd: Optional[Decimal] = None

    # Activity information
    transaction_count: int = 0
    last_transaction_timestamp: Optional[datetime] = None
    is_active: bool = True

    # Error handling
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    skip_reason: Optional[SkipReason] = None

    # Processing metadata
    processing_time_seconds: Optional[float] = None
    cache_hit: bool = False
    api_calls_made: int = 0

    def mark_started(self) -> None:
        """Mark job as started."""
        self.status = WalletStatus.PROCESSING
        self.started_at = datetime.now(timezone.utc)

    def mark_completed(self, total_value: Decimal) -> None:
        """Mark job as completed successfully."""
        self.status = WalletStatus.COMPLETED
        self.completed_at = datetime.now(timezone.utc)
        self.total_value_usd = total_value

        if self.started_at:
            duration = self.completed_at - self.started_at
            self.processing_time_seconds = duration.total_seconds()

    def mark_failed(self, error: str, skip_reason: Optional[SkipReason] = None) -> None:
        """Mark job as failed."""
        self.status = WalletStatus.FAILED
        self.completed_at = datetime.now(timezone.utc)
        self.error_message = error
        self.skip_reason = skip_reason

        if self.started_at:
            duration = self.completed_at - self.started_at
            self.processing_time_seconds = duration.total_seconds()

    def mark_skipped(self, reason: SkipReason, message: str = "") -> None:
        """Mark job as skipped."""
        self.status = WalletStatus.SKIPPED
        self.completed_at = datetime.now(timezone.utc)
        self.skip_reason = reason
        self.error_message = message or reason.value

        if self.started_at:
            duration = self.completed_at - self.started_at
            self.processing_time_seconds = duration.total_seconds()

    def should_retry(self) -> bool:
        """Check if job should be retried."""
        return (
            self.status == WalletStatus.FAILED
            and self.retry_count < self.max_retries
            and self.skip_reason not in [SkipReason.INVALID_ADDRESS, SkipReason.BLACKLISTED]
        )

    def prepare_retry(self) -> None:
        """Prepare job for retry."""
        if self.should_retry():
            self.retry_count += 1
            self.status = WalletStatus.PENDING
            self.started_at = None
            self.completed_at = None
            self.processing_time_seconds = None
            self.error_message = None

    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of processing results."""
        return {
            "address": self.address,
            "label": self.label,
            "status": self.status.value,
            "total_value_usd": float(self.total_value_usd) if self.total_value_usd else 0.0,
            "eth_balance": float(self.eth_balance) if self.eth_balance else 0.0,
            "token_count": len(self.token_balances),
            "is_active": self.is_active,
            "transaction_count": self.transaction_count,
            "processing_time_seconds": self.processing_time_seconds,
            "cache_hit": self.cache_hit,
            "api_calls_made": self.api_calls_made,
            "retry_count": self.retry_count,
            "error_message": self.error_message,
            "skip_reason": self.skip_reason.value if self.skip_reason else None,
        }


@dataclass
class ProcessingResults:
    """Final results of wallet processing operation."""

    # Input information
    total_wallets_input: int
    batch_config: Optional[Any] = None  # Will be BatchConfig but avoiding circular import

    # Processing statistics
    wallets_processed: int = 0
    wallets_skipped: int = 0
    wallets_failed: int = 0

    # Value statistics
    total_portfolio_value: Decimal = field(default_factory=lambda: Decimal("0"))
    average_portfolio_value: Decimal = field(default_factory=lambda: Decimal("0"))
    median_portfolio_value: Decimal = field(default_factory=lambda: Decimal("0"))
    max_portfolio_value: Decimal = field(default_factory=lambda: Decimal("0"))
    min_portfolio_value: Decimal = field(default_factory=lambda: Decimal("0"))

    # Activity statistics
    active_wallets: int = 0
    inactive_wallets: int = 0

    # Token distribution
    eth_holders: int = 0
    usdc_holders: int = 0
    usdt_holders: int = 0
    dai_holders: int = 0

    # Performance metrics
    total_processing_time: float = 0.0
    average_processing_time_per_wallet: float = 0.0
    cache_hit_rate: float = 0.0
    api_calls_total: int = 0

    # Error breakdown
    skip_reasons: Dict[SkipReason, int] = field(default_factory=dict)
    error_types: Dict[str, int] = field(default_factory=dict)

    # Timestamps
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def finalize_results(self, jobs: List[WalletProcessingJob]) -> None:
        """Calculate final statistics from completed jobs."""
        if not jobs:
            return

        # Basic counts
        self.wallets_processed = len([j for j in jobs if j.status == WalletStatus.COMPLETED])
        self.wallets_skipped = len([j for j in jobs if j.status == WalletStatus.SKIPPED])
        self.wallets_failed = len([j for j in jobs if j.status == WalletStatus.FAILED])

        # Value statistics
        successful_jobs = [j for j in jobs if j.status == WalletStatus.COMPLETED and j.total_value_usd]
        if successful_jobs:
            values = [j.total_value_usd for j in successful_jobs]
            self.total_portfolio_value = sum(values)
            self.average_portfolio_value = self.total_portfolio_value / len(values)
            self.max_portfolio_value = max(values)
            self.min_portfolio_value = min(values)

            # Calculate median
            sorted_values = sorted(values)
            n = len(sorted_values)
            if n % 2 == 0:
                self.median_portfolio_value = (sorted_values[n//2 - 1] + sorted_values[n//2]) / 2
            else:
                self.median_portfolio_value = sorted_values[n//2]

        # Activity statistics
        self.active_wallets = len([j for j in successful_jobs if j.is_active])
        self.inactive_wallets = len([j for j in successful_jobs if not j.is_active])

        # Token holder counts
        self.eth_holders = len([j for j in successful_jobs if j.eth_balance and j.eth_balance > 0])
        self.usdc_holders = len([j for j in successful_jobs if j.token_balances.get("USDC", 0) > 0])
        self.usdt_holders = len([j for j in successful_jobs if j.token_balances.get("USDT", 0) > 0])
        self.dai_holders = len([j for j in successful_jobs if j.token_balances.get("DAI", 0) > 0])

        # Performance metrics
        processing_times = [j.processing_time_seconds for j in jobs if j.processing_time_seconds]
        if processing_times:
            self.total_processing_time = sum(processing_times)
            self.average_processing_time_per_wallet = self.total_processing_time / len(processing_times)

        cache_hits = len([j for j in jobs if j.cache_hit])
        if jobs:
            self.cache_hit_rate = (cache_hits / len(jobs)) * 100

        self.api_calls_total = sum(j.api_calls_made for j in jobs)

        # Error breakdown
        self.skip_reasons = {}
        self.error_types = {}

        for job in jobs:
            if job.skip_reason:
                self.skip_reasons[job.skip_reason] = self.skip_reasons.get(job.skip_reason, 0) + 1

            if job.error_message and job.status == WalletStatus.FAILED:
                # Categorize error types
                error_type = self._categorize_error(job.error_message)
                self.error_types[error_type] = self.error_types.get(error_type, 0) + 1

    def _categorize_error(self, error_message: str) -> str:
        """Categorize error message into type."""
        error_lower = error_message.lower()

        if "rate limit" in error_lower or "too many requests" in error_lower:
            return "rate_limit"
        elif "timeout" in error_lower:
            return "timeout"
        elif "connection" in error_lower or "network" in error_lower:
            return "network"
        elif "unauthorized" in error_lower or "permission" in error_lower:
            return "authentication"
        elif "not found" in error_lower:
            return "not_found"
        elif "invalid" in error_lower:
            return "invalid_data"
        else:
            return "unknown"

    def get_summary_dict(self) -> Dict[str, Any]:
        """Get summary as dictionary for reporting."""
        return {
            "input": {
                "total_wallets": self.total_wallets_input,
                "batch_size": getattr(self.batch_config, 'batch_size', None) if self.batch_config else None,
            },
            "results": {
                "processed": self.wallets_processed,
                "skipped": self.wallets_skipped,
                "failed": self.wallets_failed,
                "success_rate": (self.wallets_processed / max(1, self.total_wallets_input)) * 100,
            },
            "portfolio_values": {
                "total_usd": float(self.total_portfolio_value),
                "average_usd": float(self.average_portfolio_value),
                "median_usd": float(self.median_portfolio_value),
                "max_usd": float(self.max_portfolio_value),
                "min_usd": float(self.min_portfolio_value),
            },
            "activity": {
                "active_wallets": self.active_wallets,
                "inactive_wallets": self.inactive_wallets,
                "activity_rate": (self.active_wallets / max(1, self.wallets_processed)) * 100,
            },
            "token_holders": {
                "eth": self.eth_holders,
                "usdc": self.usdc_holders,
                "usdt": self.usdt_holders,
                "dai": self.dai_holders,
            },
            "performance": {
                "total_time_seconds": self.total_processing_time,
                "average_time_per_wallet": self.average_processing_time_per_wallet,
                "cache_hit_rate": self.cache_hit_rate,
                "api_calls_total": self.api_calls_total,
            },
            "errors": {
                "skip_reasons": {reason.value: count for reason, count in self.skip_reasons.items()},
                "error_types": self.error_types,
            },
        }


@dataclass
class WalletValidationResult:
    """Result of wallet address validation."""

    address: str
    is_valid: bool
    normalized_address: Optional[str] = None
    error_message: Optional[str] = None
    checksum_valid: bool = True

    @classmethod
    def valid(cls, address: str, normalized: str) -> "WalletValidationResult":
        """Create valid result."""
        return cls(
            address=address,
            is_valid=True,
            normalized_address=normalized,
        )

    @classmethod
    def invalid(cls, address: str, reason: str) -> "WalletValidationResult":
        """Create invalid result."""
        return cls(
            address=address,
            is_valid=False,
            error_message=reason,
        )


# Utility functions for working with wallet types

def create_jobs_from_addresses(
    addresses: List[Dict[str, Union[str, int]]],
    priority: ProcessingPriority = ProcessingPriority.NORMAL,
    max_retries: int = 3
) -> List[WalletProcessingJob]:
    """Create processing jobs from address list.

    Args:
        addresses: List of address dictionaries with 'address', 'label', 'row_number'
        priority: Default priority for all jobs
        max_retries: Maximum retry attempts

    Returns:
        List of WalletProcessingJob objects
    """
    jobs = []

    for addr_data in addresses:
        job = WalletProcessingJob(
            address=addr_data["address"],
            label=addr_data["label"],
            row_number=addr_data["row_number"],
            priority=priority,
            max_retries=max_retries,
        )
        jobs.append(job)

    return jobs


def group_jobs_by_priority(jobs: List[WalletProcessingJob]) -> Dict[ProcessingPriority, List[WalletProcessingJob]]:
    """Group jobs by priority level.

    Args:
        jobs: List of processing jobs

    Returns:
        Dictionary mapping priority to job lists
    """
    groups = {priority: [] for priority in ProcessingPriority}

    for job in jobs:
        groups[job.priority].append(job)

    return groups


def filter_jobs_by_status(jobs: List[WalletProcessingJob], status: WalletStatus) -> List[WalletProcessingJob]:
    """Filter jobs by status.

    Args:
        jobs: List of processing jobs
        status: Status to filter by

    Returns:
        Filtered list of jobs
    """
    return [job for job in jobs if job.status == status]


def get_retry_jobs(jobs: List[WalletProcessingJob]) -> List[WalletProcessingJob]:
    """Get jobs that should be retried.

    Args:
        jobs: List of processing jobs

    Returns:
        List of jobs to retry
    """
    retry_jobs = []

    for job in jobs:
        if job.should_retry():
            job.prepare_retry()
            retry_jobs.append(job)

    return retry_jobs