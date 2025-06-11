"""Recovery mechanisms and checkpoint system for error handling."""

import asyncio
import json
import logging
import pickle
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .exceptions import (
    ErrorCategory,
    ErrorSeverity,
    RecoveryStrategy,
    WalletTrackerError
)

logger = logging.getLogger(__name__)


class CheckpointData:
    """Represents a checkpoint with state and metadata."""

    def __init__(
            self,
            checkpoint_id: str,
            operation_name: str,
            state_data: Dict[str, Any],
            metadata: Optional[Dict[str, Any]] = None
    ):
        self.checkpoint_id = checkpoint_id
        self.operation_name = operation_name
        self.state_data = state_data
        self.metadata = metadata or {}
        self.created_at = datetime.now(UTC)
        self.restored_count = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert checkpoint to dictionary for serialization."""
        return {
            'checkpoint_id': self.checkpoint_id,
            'operation_name': self.operation_name,
            'state_data': self.state_data,
            'metadata': self.metadata,
            'created_at': self.created_at.isoformat(),
            'restored_count': self.restored_count
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CheckpointData':
        """Create checkpoint from dictionary."""
        checkpoint = cls(
            checkpoint_id=data['checkpoint_id'],
            operation_name=data['operation_name'],
            state_data=data['state_data'],
            metadata=data.get('metadata', {})
        )
        checkpoint.created_at = datetime.fromisoformat(data['created_at'])
        checkpoint.restored_count = data.get('restored_count', 0)
        return checkpoint


class CheckpointManager:
    """
    Manages checkpoints for recovery from failures.

    Provides:
    - State checkpointing during long operations
    - Recovery from checkpoints after failures
    - Automatic cleanup of old checkpoints
    - Multiple storage backends (file, memory, database)
    """

    def __init__(
            self,
            storage_path: Optional[Path] = None,
            max_checkpoints_per_operation: int = 10,
            checkpoint_retention_hours: int = 48,
            auto_cleanup: bool = True
    ):
        """Initialize checkpoint manager.

        Args:
            storage_path: Path for checkpoint storage (None for memory only)
            max_checkpoints_per_operation: Max checkpoints per operation
            checkpoint_retention_hours: How long to keep checkpoints
            auto_cleanup: Whether to automatically cleanup old checkpoints
        """
        self.storage_path = storage_path
        self.max_checkpoints_per_operation = max_checkpoints_per_operation
        self.checkpoint_retention_hours = checkpoint_retention_hours
        self.auto_cleanup = auto_cleanup

        # In-memory checkpoint storage
        self._checkpoints: Dict[str, CheckpointData] = {}

        # Create storage directory if using file storage
        if self.storage_path:
            self.storage_path.mkdir(parents=True, exist_ok=True)

    async def create_checkpoint(
            self,
            operation_name: str,
            state_data: Dict[str, Any],
            checkpoint_id: Optional[str] = None,
            metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Create a checkpoint.

        Args:
            operation_name: Name of the operation
            state_data: State data to checkpoint
            checkpoint_id: Optional checkpoint ID (auto-generated if None)
            metadata: Additional metadata

        Returns:
            Checkpoint ID
        """
        if checkpoint_id is None:
            checkpoint_id = f"{operation_name}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S_%f')}"

        checkpoint = CheckpointData(
            checkpoint_id=checkpoint_id,
            operation_name=operation_name,
            state_data=state_data,
            metadata=metadata
        )

        # Store in memory
        self._checkpoints[checkpoint_id] = checkpoint

        # Store to file if configured
        if self.storage_path:
            await self._save_checkpoint_to_file(checkpoint)

        # Cleanup old checkpoints
        if self.auto_cleanup:
            await self._cleanup_old_checkpoints(operation_name)

        logger.debug(f"Created checkpoint: {checkpoint_id} for operation: {operation_name}")
        return checkpoint_id

    async def restore_checkpoint(self, checkpoint_id: str) -> Optional[CheckpointData]:
        """Restore a checkpoint by ID.

        Args:
            checkpoint_id: Checkpoint ID to restore

        Returns:
            Checkpoint data or None if not found
        """
        # Try memory first
        if checkpoint_id in self._checkpoints:
            checkpoint = self._checkpoints[checkpoint_id]
            checkpoint.restored_count += 1
            logger.info(f"Restored checkpoint from memory: {checkpoint_id}")
            return checkpoint

        # Try file storage
        if self.storage_path:
            checkpoint = await self._load_checkpoint_from_file(checkpoint_id)
            if checkpoint:
                checkpoint.restored_count += 1
                # Cache in memory
                self._checkpoints[checkpoint_id] = checkpoint
                logger.info(f"Restored checkpoint from file: {checkpoint_id}")
                return checkpoint

        logger.warning(f"Checkpoint not found: {checkpoint_id}")
        return None

    async def get_latest_checkpoint(self, operation_name: str) -> Optional[CheckpointData]:
        """Get the latest checkpoint for an operation.

        Args:
            operation_name: Operation name

        Returns:
            Latest checkpoint or None if not found
        """
        # Find all checkpoints for the operation
        operation_checkpoints = [
            cp for cp in self._checkpoints.values()
            if cp.operation_name == operation_name
        ]

        # Load from file if storage is configured and memory is empty
        if not operation_checkpoints and self.storage_path:
            await self._load_all_checkpoints()
            operation_checkpoints = [
                cp for cp in self._checkpoints.values()
                if cp.operation_name == operation_name
            ]

        if not operation_checkpoints:
            return None

        # Return the most recent one
        latest = max(operation_checkpoints, key=lambda x: x.created_at)
        logger.info(f"Found latest checkpoint for {operation_name}: {latest.checkpoint_id}")
        return latest

    async def list_checkpoints(
            self,
            operation_name: Optional[str] = None
    ) -> List[CheckpointData]:
        """List available checkpoints.

        Args:
            operation_name: Filter by operation name (None for all)

        Returns:
            List of checkpoints
        """
        # Load from file if storage is configured
        if self.storage_path:
            await self._load_all_checkpoints()

        checkpoints = list(self._checkpoints.values())

        if operation_name:
            checkpoints = [cp for cp in checkpoints if cp.operation_name == operation_name]

        # Sort by creation time (newest first)
        checkpoints.sort(key=lambda x: x.created_at, reverse=True)

        return checkpoints

    async def delete_checkpoint(self, checkpoint_id: str) -> bool:
        """Delete a checkpoint.

        Args:
            checkpoint_id: Checkpoint ID to delete

        Returns:
            True if deleted, False if not found
        """
        deleted = False

        # Remove from memory
        if checkpoint_id in self._checkpoints:
            del self._checkpoints[checkpoint_id]
            deleted = True

        # Remove from file storage
        if self.storage_path:
            file_deleted = await self._delete_checkpoint_file(checkpoint_id)
            deleted = deleted or file_deleted

        if deleted:
            logger.info(f"Deleted checkpoint: {checkpoint_id}")

        return deleted

    async def cleanup_old_checkpoints(
            self,
            operation_name: Optional[str] = None
    ) -> int:
        """Cleanup old checkpoints.

        Args:
            operation_name: Cleanup specific operation (None for all)

        Returns:
            Number of checkpoints deleted
        """
        cutoff_time = datetime.now(UTC) - timedelta(hours=self.checkpoint_retention_hours)
        deleted_count = 0

        # Load all checkpoints if using file storage
        if self.storage_path:
            await self._load_all_checkpoints()

        # Find checkpoints to delete
        to_delete = []

        for checkpoint_id, checkpoint in self._checkpoints.items():
            if operation_name and checkpoint.operation_name != operation_name:
                continue

            if checkpoint.created_at < cutoff_time:
                to_delete.append(checkpoint_id)

        # Delete old checkpoints
        for checkpoint_id in to_delete:
            if await self.delete_checkpoint(checkpoint_id):
                deleted_count += 1

        # Also cleanup excess checkpoints per operation
        if operation_name:
            deleted_count += await self._cleanup_excess_checkpoints(operation_name)
        else:
            # Cleanup for all operations
            operations = set(cp.operation_name for cp in self._checkpoints.values())
            for op in operations:
                deleted_count += await self._cleanup_excess_checkpoints(op)

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old checkpoints")

        return deleted_count

    async def _cleanup_old_checkpoints(self, operation_name: str) -> None:
        """Internal cleanup for specific operation."""
        await self.cleanup_old_checkpoints(operation_name)

    async def _cleanup_excess_checkpoints(self, operation_name: str) -> int:
        """Cleanup excess checkpoints for an operation."""
        operation_checkpoints = [
            (cp_id, cp) for cp_id, cp in self._checkpoints.items()
            if cp.operation_name == operation_name
        ]

        if len(operation_checkpoints) <= self.max_checkpoints_per_operation:
            return 0

        # Sort by creation time (oldest first)
        operation_checkpoints.sort(key=lambda x: x[1].created_at)

        # Delete excess checkpoints
        excess_count = len(operation_checkpoints) - self.max_checkpoints_per_operation
        deleted_count = 0

        for i in range(excess_count):
            checkpoint_id = operation_checkpoints[i][0]
            if await self.delete_checkpoint(checkpoint_id):
                deleted_count += 1

        return deleted_count

    async def _save_checkpoint_to_file(self, checkpoint: CheckpointData) -> None:
        """Save checkpoint to file."""
        if not self.storage_path:
            return

        file_path = self.storage_path / f"{checkpoint.checkpoint_id}.json"

        try:
            with open(file_path, 'w') as f:
                json.dump(checkpoint.to_dict(), f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save checkpoint to file: {e}")

    async def _load_checkpoint_from_file(self, checkpoint_id: str) -> Optional[CheckpointData]:
        """Load checkpoint from file."""
        if not self.storage_path:
            return None

        file_path = self.storage_path / f"{checkpoint_id}.json"

        if not file_path.exists():
            return None

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            return CheckpointData.from_dict(data)
        except Exception as e:
            logger.error(f"Failed to load checkpoint from file: {e}")
            return None

    async def _load_all_checkpoints(self) -> None:
        """Load all checkpoints from file storage into memory."""
        if not self.storage_path or not self.storage_path.exists():
            return

        for file_path in self.storage_path.glob("*.json"):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                checkpoint = CheckpointData.from_dict(data)
                self._checkpoints[checkpoint.checkpoint_id] = checkpoint

            except Exception as e:
                logger.warning(f"Failed to load checkpoint file {file_path}: {e}")

    async def _delete_checkpoint_file(self, checkpoint_id: str) -> bool:
        """Delete checkpoint file."""
        if not self.storage_path:
            return False

        file_path = self.storage_path / f"{checkpoint_id}.json"

        try:
            if file_path.exists():
                file_path.unlink()
                return True
        except Exception as e:
            logger.error(f"Failed to delete checkpoint file: {e}")

        return False

    def get_stats(self) -> Dict[str, Any]:
        """Get checkpoint manager statistics."""
        return {
            'total_checkpoints': len(self._checkpoints),
            'operations': list(set(cp.operation_name for cp in self._checkpoints.values())),
            'storage_path': str(self.storage_path) if self.storage_path else None,
            'retention_hours': self.checkpoint_retention_hours,
            'max_per_operation': self.max_checkpoints_per_operation
        }


class RecoveryManager:
    """
    Manages recovery strategies for different types of failures.

    Provides:
    - Automatic recovery from checkpoints
    - Fallback strategies
    - Progressive recovery attempts
    - Recovery state tracking
    """

    def __init__(self, checkpoint_manager: CheckpointManager):
        """Initialize recovery manager.

        Args:
            checkpoint_manager: Checkpoint manager instance
        """
        self.checkpoint_manager = checkpoint_manager
        self._recovery_attempts: Dict[str, int] = {}
        self._recovery_strategies: Dict[RecoveryStrategy, List[callable]] = {}

    def register_recovery_strategy(
            self,
            strategy: RecoveryStrategy,
            handler: callable
    ) -> None:
        """Register a recovery strategy handler.

        Args:
            strategy: Recovery strategy type
            handler: Handler function
        """
        if strategy not in self._recovery_strategies:
            self._recovery_strategies[strategy] = []
        self._recovery_strategies[strategy].append(handler)

    async def attempt_recovery(
            self,
            error: WalletTrackerError,
            operation_name: str,
            max_recovery_attempts: int = 3
    ) -> Optional[CheckpointData]:
        """Attempt recovery from a failure.

        Args:
            error: Error that occurred
            operation_name: Name of failed operation
            max_recovery_attempts: Maximum recovery attempts

        Returns:
            Checkpoint data if recovery possible, None otherwise
        """
        recovery_key = f"{operation_name}:{error.error_code}"

        # Check recovery attempt count
        attempt_count = self._recovery_attempts.get(recovery_key, 0)
        if attempt_count >= max_recovery_attempts:
            logger.warning(f"Max recovery attempts reached for {recovery_key}")
            return None

        self._recovery_attempts[recovery_key] = attempt_count + 1

        logger.info(f"Attempting recovery for {operation_name} (attempt {attempt_count + 1})")

        # Try recovery based on strategy
        strategy = error.recovery_strategy

        if strategy == RecoveryStrategy.RETRY:
            return await self._retry_recovery(error, operation_name)

        elif strategy == RecoveryStrategy.FALLBACK:
            return await self._fallback_recovery(error, operation_name)

        elif strategy == RecoveryStrategy.SKIP:
            return await self._skip_recovery(error, operation_name)

        elif strategy in [RecoveryStrategy.EXPONENTIAL_BACKOFF, RecoveryStrategy.RETRY]:
            return await self._checkpoint_recovery(error, operation_name)

        else:
            logger.warning(f"No recovery strategy available for {strategy}")
            return None

    async def _retry_recovery(
            self,
            error: WalletTrackerError,
            operation_name: str
    ) -> Optional[CheckpointData]:
        """Attempt recovery through retry."""
        logger.info(f"Attempting retry recovery for {operation_name}")

        # Get latest checkpoint to retry from
        checkpoint = await self.checkpoint_manager.get_latest_checkpoint(operation_name)

        if checkpoint:
            logger.info(f"Found checkpoint for retry: {checkpoint.checkpoint_id}")
        else:
            logger.info("No checkpoint found, will retry from beginning")

        return checkpoint

    async def _fallback_recovery(
            self,
            error: WalletTrackerError,
            operation_name: str
    ) -> Optional[CheckpointData]:
        """Attempt recovery through fallback strategy."""
        logger.info(f"Attempting fallback recovery for {operation_name}")

        # Execute registered fallback handlers
        if RecoveryStrategy.FALLBACK in self._recovery_strategies:
            for handler in self._recovery_strategies[RecoveryStrategy.FALLBACK]:
                try:
                    result = await handler(error, operation_name)
                    if result:
                        logger.info("Fallback recovery successful")
                        return result
                except Exception as e:
                    logger.warning(f"Fallback handler failed: {e}")

        # Try to find a checkpoint as fallback
        checkpoint = await self.checkpoint_manager.get_latest_checkpoint(operation_name)
        if checkpoint:
            logger.info(f"Using checkpoint as fallback: {checkpoint.checkpoint_id}")
            return checkpoint

        return None

    async def _skip_recovery(
            self,
            error: WalletTrackerError,
            operation_name: str
    ) -> Optional[CheckpointData]:
        """Attempt recovery by skipping the failed item."""
        logger.info(f"Attempting skip recovery for {operation_name}")

        # Create a recovery checkpoint that indicates what to skip
        skip_metadata = {
            'recovery_type': 'skip',
            'skip_reason': error.message,
            'skip_context': error.context
        }

        # Try to get the latest checkpoint and modify it
        checkpoint = await self.checkpoint_manager.get_latest_checkpoint(operation_name)

        if checkpoint:
            # Create new checkpoint with skip information
            new_checkpoint_id = await self.checkpoint_manager.create_checkpoint(
                operation_name=operation_name,
                state_data=checkpoint.state_data,
                metadata={**checkpoint.metadata, **skip_metadata}
            )

            return await self.checkpoint_manager.restore_checkpoint(new_checkpoint_id)

        return None

    async def _checkpoint_recovery(
            self,
            error: WalletTrackerError,
            operation_name: str
    ) -> Optional[CheckpointData]:
        """Attempt recovery from the latest checkpoint."""
        logger.info(f"Attempting checkpoint recovery for {operation_name}")

        checkpoint = await self.checkpoint_manager.get_latest_checkpoint(operation_name)

        if checkpoint:
            # Check if this checkpoint has been used too many times
            if checkpoint.restored_count > 5:
                logger.warning(f"Checkpoint {checkpoint.checkpoint_id} has been restored too many times")

                # Try to find an older checkpoint
                checkpoints = await self.checkpoint_manager.list_checkpoints(operation_name)
                for cp in checkpoints[1:]:  # Skip the first (latest) one
                    if cp.restored_count <= 5:
                        logger.info(f"Using older checkpoint: {cp.checkpoint_id}")
                        return cp

                return None

            logger.info(f"Recovering from checkpoint: {checkpoint.checkpoint_id}")
            return checkpoint

        logger.warning(f"No checkpoint available for recovery: {operation_name}")
        return None

    def clear_recovery_attempts(self, operation_name: Optional[str] = None) -> None:
        """Clear recovery attempt counters.

        Args:
            operation_name: Clear for specific operation (None for all)
        """
        if operation_name:
            keys_to_remove = [key for key in self._recovery_attempts.keys() if key.startswith(f"{operation_name}:")]
            for key in keys_to_remove:
                del self._recovery_attempts[key]
        else:
            self._recovery_attempts.clear()

        logger.info("Recovery attempt counters cleared")

    def get_recovery_stats(self) -> Dict[str, Any]:
        """Get recovery statistics."""
        return {
            'total_recovery_attempts': len(self._recovery_attempts),
            'recovery_attempts_by_operation': {
                key: count for key, count in self._recovery_attempts.items()
            },
            'registered_strategies': list(self._recovery_strategies.keys())
        }


class ProgressTracker:
    """
    Tracks progress of long-running operations for recovery purposes.

    Provides:
    - Progress tracking with automatic checkpointing
    - Resume capability after failures
    - Progress reporting and estimation
    """

    def __init__(
            self,
            operation_name: str,
            total_items: int,
            checkpoint_manager: CheckpointManager,
            checkpoint_interval: int = 10
    ):
        """Initialize progress tracker.

        Args:
            operation_name: Name of the operation
            total_items: Total number of items to process
            checkpoint_manager: Checkpoint manager
            checkpoint_interval: Create checkpoint every N items
        """
        self.operation_name = operation_name
        self.total_items = total_items
        self.checkpoint_manager = checkpoint_manager
        self.checkpoint_interval = checkpoint_interval

        self.processed_items = 0
        self.failed_items = 0
        self.skipped_items = 0
        self.start_time = datetime.now(UTC)
        self.last_checkpoint_time = self.start_time
        self.last_checkpoint_id: Optional[str] = None

        # Progress state
        self.current_batch: List[Any] = []
        self.processed_batches: List[Dict[str, Any]] = []
        self.failed_items_list: List[Dict[str, Any]] = []

    async def update_progress(
            self,
            processed: int = 1,
            failed: int = 0,
            skipped: int = 0,
            item_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """Update progress counters.

        Args:
            processed: Number of items processed
            failed: Number of items failed
            skipped: Number of items skipped
            item_data: Additional data about the processed item
        """
        self.processed_items += processed
        self.failed_items += failed
        self.skipped_items += skipped

        # Track failed items for recovery
        if failed > 0 and item_data:
            self.failed_items_list.append({
                'item_data': item_data,
                'timestamp': datetime.now(UTC).isoformat(),
                'error_context': item_data.get('error_context')
            })

        # Create checkpoint if interval reached
        if self.processed_items % self.checkpoint_interval == 0:
            await self._create_progress_checkpoint()

    async def _create_progress_checkpoint(self) -> None:
        """Create a progress checkpoint."""
        checkpoint_data = {
            'operation_name': self.operation_name,
            'total_items': self.total_items,
            'processed_items': self.processed_items,
            'failed_items': self.failed_items,
            'skipped_items': self.skipped_items,
            'start_time': self.start_time.isoformat(),
            'last_checkpoint_time': datetime.now(UTC).isoformat(),
            'processed_batches': self.processed_batches,
            'failed_items_list': self.failed_items_list,
            'progress_percent': self.get_progress_percent()
        }

        metadata = {
            'checkpoint_type': 'progress',
            'items_since_last': self.checkpoint_interval,
            'estimated_completion': self.get_estimated_completion().isoformat() if self.get_estimated_completion() else None
        }

        self.last_checkpoint_id = await self.checkpoint_manager.create_checkpoint(
            operation_name=self.operation_name,
            state_data=checkpoint_data,
            metadata=metadata
        )

        self.last_checkpoint_time = datetime.now(UTC)

        logger.debug(f"Progress checkpoint created: {self.last_checkpoint_id} ({self.get_progress_percent():.1f}%)")

    async def restore_from_checkpoint(self, checkpoint_id: Optional[str] = None) -> bool:
        """Restore progress from checkpoint.

        Args:
            checkpoint_id: Specific checkpoint ID (None for latest)

        Returns:
            True if restored successfully
        """
        if checkpoint_id:
            checkpoint = await self.checkpoint_manager.restore_checkpoint(checkpoint_id)
        else:
            checkpoint = await self.checkpoint_manager.get_latest_checkpoint(self.operation_name)

        if not checkpoint:
            logger.warning(f"No checkpoint found for restoration: {self.operation_name}")
            return False

        # Restore progress state
        state_data = checkpoint.state_data

        self.total_items = state_data.get('total_items', self.total_items)
        self.processed_items = state_data.get('processed_items', 0)
        self.failed_items = state_data.get('failed_items', 0)
        self.skipped_items = state_data.get('skipped_items', 0)
        self.processed_batches = state_data.get('processed_batches', [])
        self.failed_items_list = state_data.get('failed_items_list', [])

        if 'start_time' in state_data:
            self.start_time = datetime.fromisoformat(state_data['start_time'])

        self.last_checkpoint_id = checkpoint.checkpoint_id

        logger.info(
            f"Progress restored from checkpoint: {checkpoint.checkpoint_id} ({self.get_progress_percent():.1f}%)")
        return True

    def get_progress_percent(self) -> float:
        """Get progress percentage."""
        if self.total_items == 0:
            return 100.0

        total_processed = self.processed_items + self.failed_items + self.skipped_items
        return (total_processed / self.total_items) * 100

    def get_estimated_completion(self) -> Optional[datetime]:
        """Get estimated completion time."""
        if self.processed_items == 0:
            return None

        elapsed_time = datetime.now(UTC) - self.start_time
        items_per_second = self.processed_items / elapsed_time.total_seconds()

        if items_per_second <= 0:
            return None

        remaining_items = self.total_items - (self.processed_items + self.failed_items + self.skipped_items)
        remaining_seconds = remaining_items / items_per_second

        return datetime.now(UTC) + timedelta(seconds=remaining_seconds)

    def get_processing_rate(self) -> float:
        """Get items processed per second."""
        elapsed_time = datetime.now(UTC) - self.start_time
        if elapsed_time.total_seconds() <= 0:
            return 0.0

        return self.processed_items / elapsed_time.total_seconds()

    def get_failure_rate(self) -> float:
        """Get failure rate percentage."""
        total_attempted = self.processed_items + self.failed_items
        if total_attempted == 0:
            return 0.0

        return (self.failed_items / total_attempted) * 100

    def get_progress_summary(self) -> Dict[str, Any]:
        """Get progress summary."""
        return {
            'operation_name': self.operation_name,
            'total_items': self.total_items,
            'processed_items': self.processed_items,
            'failed_items': self.failed_items,
            'skipped_items': self.skipped_items,
            'progress_percent': self.get_progress_percent(),
            'processing_rate': self.get_processing_rate(),
            'failure_rate': self.get_failure_rate(),
            'estimated_completion': self.get_estimated_completion().isoformat() if self.get_estimated_completion() else None,
            'elapsed_time': (datetime.now(UTC) - self.start_time).total_seconds(),
            'last_checkpoint_id': self.last_checkpoint_id
        }


# Utility functions and decorators

def with_checkpointing(
        checkpoint_manager: CheckpointManager,
        operation_name: str,
        checkpoint_interval: Optional[int] = None
):
    """Decorator to add automatic checkpointing to functions.

    Args:
        checkpoint_manager: Checkpoint manager instance
        operation_name: Name of the operation
        checkpoint_interval: Checkpoint every N iterations
    """

    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Extract or create progress tracker
            tracker = kwargs.get('progress_tracker')
            if not tracker and checkpoint_interval:
                total_items = kwargs.get('total_items', 1)
                tracker = ProgressTracker(
                    operation_name=operation_name,
                    total_items=total_items,
                    checkpoint_manager=checkpoint_manager,
                    checkpoint_interval=checkpoint_interval
                )
                kwargs['progress_tracker'] = tracker

            try:
                result = await func(*args, **kwargs)

                # Create final checkpoint on success
                if tracker:
                    await tracker._create_progress_checkpoint()

                return result

            except Exception as e:
                # Create checkpoint on failure
                if tracker:
                    await tracker._create_progress_checkpoint()
                raise

        return wrapper

    return decorator


async def create_recovery_context(
        operation_name: str,
        checkpoint_manager: CheckpointManager,
        recovery_manager: RecoveryManager,
        auto_restore: bool = True
) -> Dict[str, Any]:
    """Create a recovery context for an operation.

    Args:
        operation_name: Name of the operation
        checkpoint_manager: Checkpoint manager
        recovery_manager: Recovery manager
        auto_restore: Whether to automatically restore from checkpoint

    Returns:
        Recovery context dictionary
    """
    context = {
        'operation_name': operation_name,
        'checkpoint_manager': checkpoint_manager,
        'recovery_manager': recovery_manager,
        'recovered_from_checkpoint': False,
        'checkpoint_data': None
    }

    if auto_restore:
        # Try to restore from latest checkpoint
        checkpoint = await checkpoint_manager.get_latest_checkpoint(operation_name)
        if checkpoint:
            context['recovered_from_checkpoint'] = True
            context['checkpoint_data'] = checkpoint
            logger.info(f"Auto-restored from checkpoint: {checkpoint.checkpoint_id}")

    return context


class RecoverySession:
    """
    Context manager for recovery-enabled operations.

    Provides automatic checkpointing, error handling, and recovery.
    """

    def __init__(
            self,
            operation_name: str,
            checkpoint_manager: CheckpointManager,
            recovery_manager: RecoveryManager,
            total_items: Optional[int] = None,
            checkpoint_interval: int = 10,
            auto_restore: bool = True
    ):
        """Initialize recovery session.

        Args:
            operation_name: Name of the operation
            checkpoint_manager: Checkpoint manager
            recovery_manager: Recovery manager
            total_items: Total items to process (for progress tracking)
            checkpoint_interval: Checkpoint interval
            auto_restore: Whether to auto-restore from checkpoint
        """
        self.operation_name = operation_name
        self.checkpoint_manager = checkpoint_manager
        self.recovery_manager = recovery_manager
        self.total_items = total_items
        self.checkpoint_interval = checkpoint_interval
        self.auto_restore = auto_restore

        self.progress_tracker: Optional[ProgressTracker] = None
        self.recovery_context: Dict[str, Any] = {}
        self.session_checkpoint_id: Optional[str] = None

    async def __aenter__(self):
        """Enter recovery session."""
        # Create recovery context
        self.recovery_context = await create_recovery_context(
            operation_name=self.operation_name,
            checkpoint_manager=self.checkpoint_manager,
            recovery_manager=self.recovery_manager,
            auto_restore=self.auto_restore
        )

        # Create progress tracker if total items specified
        if self.total_items:
            self.progress_tracker = ProgressTracker(
                operation_name=self.operation_name,
                total_items=self.total_items,
                checkpoint_manager=self.checkpoint_manager,
                checkpoint_interval=self.checkpoint_interval
            )

            # Restore progress if recovered from checkpoint
            if self.recovery_context['recovered_from_checkpoint']:
                await self.progress_tracker.restore_from_checkpoint()

        # Create session start checkpoint
        session_data = {
            'session_start': datetime.now(UTC).isoformat(),
            'operation_name': self.operation_name,
            'total_items': self.total_items,
            'auto_restore': self.auto_restore
        }

        self.session_checkpoint_id = await self.checkpoint_manager.create_checkpoint(
            operation_name=f"{self.operation_name}_session",
            state_data=session_data,
            metadata={'checkpoint_type': 'session_start'}
        )

        logger.info(f"Recovery session started: {self.operation_name}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit recovery session."""
        # Create session end checkpoint
        session_data = {
            'session_end': datetime.now(UTC).isoformat(),
            'operation_name': self.operation_name,
            'completed_successfully': exc_type is None,
            'error_type': exc_type.__name__ if exc_type else None,
            'error_message': str(exc_val) if exc_val else None
        }

        if self.progress_tracker:
            session_data['final_progress'] = self.progress_tracker.get_progress_summary()

        await self.checkpoint_manager.create_checkpoint(
            operation_name=f"{self.operation_name}_session",
            state_data=session_data,
            metadata={'checkpoint_type': 'session_end'}
        )

        # Cleanup on successful completion
        if exc_type is None:
            await self.checkpoint_manager.cleanup_old_checkpoints(self.operation_name)
            self.recovery_manager.clear_recovery_attempts(self.operation_name)

        logger.info(f"Recovery session ended: {self.operation_name} (success: {exc_type is None})")

    async def checkpoint(self, state_data: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None) -> str:
        """Create a manual checkpoint.

        Args:
            state_data: State data to checkpoint
            metadata: Additional metadata

        Returns:
            Checkpoint ID
        """
        return await self.checkpoint_manager.create_checkpoint(
            operation_name=self.operation_name,
            state_data=state_data,
            metadata=metadata
        )

    async def update_progress(self, processed: int = 1, failed: int = 0, skipped: int = 0, **kwargs) -> None:
        """Update progress if tracker is available.

        Args:
            processed: Items processed
            failed: Items failed
            skipped: Items skipped
            **kwargs: Additional progress data
        """
        if self.progress_tracker:
            await self.progress_tracker.update_progress(processed, failed, skipped, kwargs)

    def get_progress(self) -> Optional[Dict[str, Any]]:
        """Get current progress summary."""
        if self.progress_tracker:
            return self.progress_tracker.get_progress_summary()
        return None