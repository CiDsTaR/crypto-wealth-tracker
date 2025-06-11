"""Tests for recovery module."""

import asyncio
import json
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from wallet_tracker.errors.exceptions import (
    NetworkError,
    ProcessingError,
    RecoveryStrategy,
    WalletTrackerError,
)
from wallet_tracker.errors.recovery import (
    # Main classes
    CheckpointData,
    CheckpointManager,
    ProgressTracker,
    RecoveryManager,
    RecoverySession,
    create_recovery_context,
    # Utility functions
    with_checkpointing,
)


class TestCheckpointData:
    """Test CheckpointData class."""

    def test_basic_initialization(self):
        """Test basic checkpoint data initialization."""
        state_data = {"processed": 100, "current_batch": 5}
        metadata = {"batch_size": 50, "total_items": 1000}

        checkpoint = CheckpointData(
            checkpoint_id="test_checkpoint_001",
            operation_name="batch_processing",
            state_data=state_data,
            metadata=metadata,
        )

        assert checkpoint.checkpoint_id == "test_checkpoint_001"
        assert checkpoint.operation_name == "batch_processing"
        assert checkpoint.state_data == state_data
        assert checkpoint.metadata == metadata
        assert checkpoint.restored_count == 0
        assert isinstance(checkpoint.created_at, datetime)

    def test_to_dict_serialization(self):
        """Test checkpoint serialization to dictionary."""
        state_data = {"key": "value", "number": 42}
        metadata = {"version": "1.0"}

        checkpoint = CheckpointData(
            checkpoint_id="serialize_test", operation_name="test_op", state_data=state_data, metadata=metadata
        )

        result = checkpoint.to_dict()

        assert result["checkpoint_id"] == "serialize_test"
        assert result["operation_name"] == "test_op"
        assert result["state_data"] == state_data
        assert result["metadata"] == metadata
        assert result["restored_count"] == 0
        assert "created_at" in result

    def test_from_dict_deserialization(self):
        """Test checkpoint deserialization from dictionary."""
        data = {
            "checkpoint_id": "deserialize_test",
            "operation_name": "test_operation",
            "state_data": {"progress": 75},
            "metadata": {"total": 100},
            "created_at": "2024-01-15T10:30:00+00:00",
            "restored_count": 2,
        }

        checkpoint = CheckpointData.from_dict(data)

        assert checkpoint.checkpoint_id == "deserialize_test"
        assert checkpoint.operation_name == "test_operation"
        assert checkpoint.state_data == {"progress": 75}
        assert checkpoint.metadata == {"total": 100}
        assert checkpoint.restored_count == 2
        assert isinstance(checkpoint.created_at, datetime)


class TestCheckpointManager:
    """Test CheckpointManager class."""

    @pytest.fixture
    def temp_storage_path(self):
        """Create temporary storage path for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def checkpoint_manager(self, temp_storage_path):
        """Create checkpoint manager for testing."""
        return CheckpointManager(
            storage_path=temp_storage_path,
            max_checkpoints_per_operation=5,
            checkpoint_retention_hours=24,
            auto_cleanup=True,
        )

    @pytest.fixture
    def memory_checkpoint_manager(self):
        """Create memory-only checkpoint manager."""
        return CheckpointManager(
            storage_path=None, max_checkpoints_per_operation=3, checkpoint_retention_hours=1, auto_cleanup=True
        )

    @pytest.mark.asyncio
    async def test_create_checkpoint_basic(self, checkpoint_manager):
        """Test basic checkpoint creation."""
        state_data = {"processed_items": 50, "current_position": "item_50"}
        metadata = {"batch_id": "batch_001"}

        checkpoint_id = await checkpoint_manager.create_checkpoint(
            operation_name="test_operation", state_data=state_data, metadata=metadata
        )

        assert checkpoint_id is not None
        assert "test_operation" in checkpoint_id

        # Verify checkpoint exists in memory
        assert checkpoint_id in checkpoint_manager._checkpoints

        # Verify checkpoint data
        checkpoint = checkpoint_manager._checkpoints[checkpoint_id]
        assert checkpoint.operation_name == "test_operation"
        assert checkpoint.state_data == state_data
        assert checkpoint.metadata == metadata

    @pytest.mark.asyncio
    async def test_create_checkpoint_with_custom_id(self, checkpoint_manager):
        """Test checkpoint creation with custom ID."""
        custom_id = "custom_checkpoint_123"
        state_data = {"data": "test"}

        checkpoint_id = await checkpoint_manager.create_checkpoint(
            operation_name="custom_test", state_data=state_data, checkpoint_id=custom_id
        )

        assert checkpoint_id == custom_id
        assert custom_id in checkpoint_manager._checkpoints

    @pytest.mark.asyncio
    async def test_restore_checkpoint(self, checkpoint_manager):
        """Test checkpoint restoration."""
        # Create a checkpoint first
        state_data = {"progress": 75, "data": ["item1", "item2"]}
        checkpoint_id = await checkpoint_manager.create_checkpoint(operation_name="restore_test", state_data=state_data)

        # Restore the checkpoint
        restored = await checkpoint_manager.restore_checkpoint(checkpoint_id)

        assert restored is not None
        assert restored.checkpoint_id == checkpoint_id
        assert restored.state_data == state_data
        assert restored.restored_count == 1

    @pytest.mark.asyncio
    async def test_restore_nonexistent_checkpoint(self, checkpoint_manager):
        """Test restoring non-existent checkpoint."""
        restored = await checkpoint_manager.restore_checkpoint("nonexistent_id")
        assert restored is None

    @pytest.mark.asyncio
    async def test_get_latest_checkpoint(self, checkpoint_manager):
        """Test getting latest checkpoint for operation."""
        operation_name = "latest_test"

        # Create multiple checkpoints
        checkpoint_ids = []
        for i in range(3):
            state_data = {"iteration": i}
            checkpoint_id = await checkpoint_manager.create_checkpoint(
                operation_name=operation_name, state_data=state_data
            )
            checkpoint_ids.append(checkpoint_id)
            # Small delay to ensure different timestamps
            await asyncio.sleep(0.01)

        # Get latest checkpoint
        latest = await checkpoint_manager.get_latest_checkpoint(operation_name)

        assert latest is not None
        assert latest.state_data["iteration"] == 2  # Last iteration
        assert latest.checkpoint_id == checkpoint_ids[-1]

    @pytest.mark.asyncio
    async def test_get_latest_checkpoint_no_operation(self, checkpoint_manager):
        """Test getting latest checkpoint for non-existent operation."""
        latest = await checkpoint_manager.get_latest_checkpoint("nonexistent_operation")
        assert latest is None

    @pytest.mark.asyncio
    async def test_list_checkpoints(self, checkpoint_manager):
        """Test listing checkpoints."""
        # Create checkpoints for different operations
        await checkpoint_manager.create_checkpoint("op1", {"data": "1"})
        await checkpoint_manager.create_checkpoint("op2", {"data": "2"})
        await checkpoint_manager.create_checkpoint("op1", {"data": "3"})

        # List all checkpoints
        all_checkpoints = await checkpoint_manager.list_checkpoints()
        assert len(all_checkpoints) == 3

        # List checkpoints for specific operation
        op1_checkpoints = await checkpoint_manager.list_checkpoints("op1")
        assert len(op1_checkpoints) == 2
        assert all(cp.operation_name == "op1" for cp in op1_checkpoints)

        # Verify sorting (newest first)
        assert op1_checkpoints[0].created_at >= op1_checkpoints[1].created_at

    @pytest.mark.asyncio
    async def test_delete_checkpoint(self, checkpoint_manager):
        """Test checkpoint deletion."""
        # Create a checkpoint
        checkpoint_id = await checkpoint_manager.create_checkpoint("delete_test", {"data": "to_delete"})

        # Verify it exists
        assert checkpoint_id in checkpoint_manager._checkpoints

        # Delete it
        deleted = await checkpoint_manager.delete_checkpoint(checkpoint_id)
        assert deleted is True

        # Verify it's gone
        assert checkpoint_id not in checkpoint_manager._checkpoints

        # Try to delete non-existent checkpoint
        deleted = await checkpoint_manager.delete_checkpoint("nonexistent")
        assert deleted is False

    @pytest.mark.asyncio
    async def test_cleanup_old_checkpoints(self, checkpoint_manager):
        """Test cleanup of old checkpoints."""
        # Create checkpoint manager with very short retention
        short_retention_manager = CheckpointManager(
            storage_path=None,
            checkpoint_retention_hours=0.001,  # Very short retention
        )

        # Create a checkpoint
        checkpoint_id = await short_retention_manager.create_checkpoint("cleanup_test", {"data": "old"})

        # Wait for it to become "old"
        await asyncio.sleep(0.01)

        # Run cleanup
        deleted_count = await short_retention_manager.cleanup_old_checkpoints()

        assert deleted_count == 1
        assert checkpoint_id not in short_retention_manager._checkpoints

    @pytest.mark.asyncio
    async def test_max_checkpoints_per_operation(self, memory_checkpoint_manager):
        """Test maximum checkpoints per operation limit."""
        operation_name = "max_test"

        # Create more checkpoints than the limit (3)
        checkpoint_ids = []
        for i in range(5):
            checkpoint_id = await memory_checkpoint_manager.create_checkpoint(
                operation_name=operation_name, state_data={"iteration": i}
            )
            checkpoint_ids.append(checkpoint_id)

        # Should only have 3 checkpoints (max_checkpoints_per_operation)
        checkpoints = await memory_checkpoint_manager.list_checkpoints(operation_name)
        assert len(checkpoints) == 3

        # Should have the most recent ones
        iterations = [cp.state_data["iteration"] for cp in checkpoints]
        assert sorted(iterations) == [2, 3, 4]

    @pytest.mark.asyncio
    async def test_file_storage_operations(self, checkpoint_manager):
        """Test file storage operations."""
        # Create checkpoint
        state_data = {"file_test": True, "data": [1, 2, 3]}
        checkpoint_id = await checkpoint_manager.create_checkpoint("file_test", state_data)

        # Verify file was created
        storage_path = checkpoint_manager.storage_path
        checkpoint_file = storage_path / f"{checkpoint_id}.json"
        assert checkpoint_file.exists()

        # Verify file content
        with open(checkpoint_file) as f:
            file_data = json.load(f)

        assert file_data["checkpoint_id"] == checkpoint_id
        assert file_data["state_data"] == state_data

        # Clear memory and restore from file
        checkpoint_manager._checkpoints.clear()
        restored = await checkpoint_manager.restore_checkpoint(checkpoint_id)

        assert restored is not None
        assert restored.state_data == state_data

    def test_get_stats(self, checkpoint_manager):
        """Test checkpoint manager statistics."""
        stats = checkpoint_manager.get_stats()

        assert "total_checkpoints" in stats
        assert "operations" in stats
        assert "storage_path" in stats
        assert "retention_hours" in stats
        assert "max_per_operation" in stats

        assert stats["total_checkpoints"] == 0  # Initially empty
        assert isinstance(stats["operations"], list)


class TestRecoveryManager:
    """Test RecoveryManager class."""

    @pytest.fixture
    def checkpoint_manager(self):
        """Create checkpoint manager for testing."""
        return CheckpointManager(storage_path=None)

    @pytest.fixture
    def recovery_manager(self, checkpoint_manager):
        """Create recovery manager for testing."""
        return RecoveryManager(checkpoint_manager)

    def test_register_recovery_strategy(self, recovery_manager):
        """Test registering recovery strategies."""
        strategy_called = []

        async def test_handler(error, operation_name):
            strategy_called.append((error, operation_name))
            return "handled"

        recovery_manager.register_recovery_strategy(RecoveryStrategy.FALLBACK, test_handler)

        # Verify strategy is registered
        assert RecoveryStrategy.FALLBACK in recovery_manager._recovery_strategies
        assert test_handler in recovery_manager._recovery_strategies[RecoveryStrategy.FALLBACK]

    @pytest.mark.asyncio
    async def test_attempt_recovery_retry_strategy(self, recovery_manager, checkpoint_manager):
        """Test recovery with retry strategy."""
        # Create a checkpoint first
        await checkpoint_manager.create_checkpoint("retry_test", {"progress": 50})

        error = NetworkError("Network failed")
        error.recovery_strategy = RecoveryStrategy.RETRY

        result = await recovery_manager.attempt_recovery(error=error, operation_name="retry_test")

        assert result is not None
        assert result.state_data["progress"] == 50

    @pytest.mark.asyncio
    async def test_attempt_recovery_fallback_strategy(self, recovery_manager):
        """Test recovery with fallback strategy."""
        fallback_called = []

        async def test_fallback(error, operation_name):
            fallback_called.append((error, operation_name))
            return "fallback_result"

        recovery_manager.register_recovery_strategy(RecoveryStrategy.FALLBACK, test_fallback)

        error = ProcessingError("Processing failed")
        error.recovery_strategy = RecoveryStrategy.FALLBACK

        result = await recovery_manager.attempt_recovery(error=error, operation_name="fallback_test")

        assert len(fallback_called) == 1
        assert fallback_called[0][0] == error
        assert fallback_called[0][1] == "fallback_test"

    @pytest.mark.asyncio
    async def test_attempt_recovery_skip_strategy(self, recovery_manager, checkpoint_manager):
        """Test recovery with skip strategy."""
        # Create a checkpoint
        await checkpoint_manager.create_checkpoint("skip_test", {"items": ["item1", "item2"]})

        error = WalletTrackerError("Item failed")
        error.recovery_strategy = RecoveryStrategy.SKIP

        result = await recovery_manager.attempt_recovery(error=error, operation_name="skip_test")

        assert result is not None
        assert result.metadata["recovery_type"] == "skip"
        assert result.metadata["skip_reason"] == "Item failed"

    @pytest.mark.asyncio
    async def test_attempt_recovery_max_attempts(self, recovery_manager):
        """Test recovery attempt limit."""
        error = NetworkError("Persistent failure")
        operation_name = "max_attempts_test"

        # Exceed max attempts
        for _ in range(4):  # More than max_recovery_attempts (3)
            await recovery_manager.attempt_recovery(error, operation_name)

        # Next attempt should return None
        result = await recovery_manager.attempt_recovery(error, operation_name)
        assert result is None

    def test_clear_recovery_attempts(self, recovery_manager):
        """Test clearing recovery attempts."""
        # Simulate some recovery attempts
        recovery_manager._recovery_attempts["op1:ERROR_001"] = 2
        recovery_manager._recovery_attempts["op2:ERROR_002"] = 1

        # Clear specific operation
        recovery_manager.clear_recovery_attempts("op1")

        assert "op1:ERROR_001" not in recovery_manager._recovery_attempts
        assert "op2:ERROR_002" in recovery_manager._recovery_attempts

        # Clear all
        recovery_manager.clear_recovery_attempts()
        assert len(recovery_manager._recovery_attempts) == 0

    def test_get_recovery_stats(self, recovery_manager):
        """Test recovery statistics."""
        # Add some recovery attempts
        recovery_manager._recovery_attempts["test_op:ERROR_001"] = 3
        recovery_manager._recovery_attempts["test_op:ERROR_002"] = 1

        stats = recovery_manager.get_recovery_stats()

        assert "total_recovery_attempts" in stats
        assert "recovery_attempts_by_operation" in stats
        assert "registered_strategies" in stats

        assert stats["total_recovery_attempts"] == 2
        assert "test_op:ERROR_001" in stats["recovery_attempts_by_operation"]


class TestProgressTracker:
    """Test ProgressTracker class."""

    @pytest.fixture
    def checkpoint_manager(self):
        """Create checkpoint manager for testing."""
        return CheckpointManager(storage_path=None)

    @pytest.fixture
    def progress_tracker(self, checkpoint_manager):
        """Create progress tracker for testing."""
        return ProgressTracker(
            operation_name="progress_test",
            total_items=100,
            checkpoint_manager=checkpoint_manager,
            checkpoint_interval=10,
        )

    def test_initial_state(self, progress_tracker):
        """Test progress tracker initial state."""
        assert progress_tracker.operation_name == "progress_test"
        assert progress_tracker.total_items == 100
        assert progress_tracker.checkpoint_interval == 10
        assert progress_tracker.processed_items == 0
        assert progress_tracker.failed_items == 0
        assert progress_tracker.skipped_items == 0
        assert isinstance(progress_tracker.start_time, datetime)

    @pytest.mark.asyncio
    async def test_update_progress(self, progress_tracker):
        """Test progress updates."""
        # Update progress
        await progress_tracker.update_progress(processed=5, failed=1, skipped=2, item_data={"item_id": "test_001"})

        assert progress_tracker.processed_items == 5
        assert progress_tracker.failed_items == 1
        assert progress_tracker.skipped_items == 2
        assert len(progress_tracker.failed_items_list) == 1

    @pytest.mark.asyncio
    async def test_checkpoint_creation_on_interval(self, progress_tracker):
        """Test automatic checkpoint creation on interval."""
        # Update progress to trigger checkpoint
        for i in range(10):
            await progress_tracker.update_progress(processed=1)

        # Should have created a checkpoint at interval (10)
        assert progress_tracker.last_checkpoint_id is not None

        # Verify checkpoint exists
        checkpoint = await progress_tracker.checkpoint_manager.restore_checkpoint(progress_tracker.last_checkpoint_id)
        assert checkpoint is not None
        assert checkpoint.state_data["processed_items"] == 10

    @pytest.mark.asyncio
    async def test_restore_from_checkpoint(self, progress_tracker, checkpoint_manager):
        """Test restoring progress from checkpoint."""
        # Create a checkpoint manually
        checkpoint_data = {
            "operation_name": "progress_test",
            "total_items": 100,
            "processed_items": 50,
            "failed_items": 5,
            "skipped_items": 2,
            "start_time": datetime.now(UTC).isoformat(),
            "processed_batches": [{"batch": 1}, {"batch": 2}],
            "failed_items_list": [{"item": "failed_001"}],
        }

        checkpoint_id = await checkpoint_manager.create_checkpoint("progress_test", checkpoint_data)

        # Restore progress
        restored = await progress_tracker.restore_from_checkpoint(checkpoint_id)

        assert restored is True
        assert progress_tracker.processed_items == 50
        assert progress_tracker.failed_items == 5
        assert progress_tracker.skipped_items == 2
        assert len(progress_tracker.processed_batches) == 2
        assert len(progress_tracker.failed_items_list) == 1

    def test_get_progress_percent(self, progress_tracker):
        """Test progress percentage calculation."""
        # No progress initially
        assert progress_tracker.get_progress_percent() == 0.0

        # Update progress
        progress_tracker.processed_items = 25
        progress_tracker.failed_items = 10
        progress_tracker.skipped_items = 5

        # Total processed = 25 + 10 + 5 = 40 out of 100 = 40%
        assert progress_tracker.get_progress_percent() == 40.0

    def test_get_estimated_completion(self, progress_tracker):
        """Test estimated completion time calculation."""
        # No progress yet
        assert progress_tracker.get_estimated_completion() is None

        # Set some progress
        progress_tracker.processed_items = 25
        progress_tracker.start_time = datetime.now(UTC) - timedelta(seconds=10)

        estimated = progress_tracker.get_estimated_completion()
        assert estimated is not None
        assert isinstance(estimated, datetime)
        assert estimated > datetime.now(UTC)

    def test_get_processing_rate(self, progress_tracker):
        """Test processing rate calculation."""
        # Set progress and elapsed time
        progress_tracker.processed_items = 50
        progress_tracker.start_time = datetime.now(UTC) - timedelta(seconds=10)

        rate = progress_tracker.get_processing_rate()
        assert rate > 0
        assert rate == 5.0  # 50 items / 10 seconds

    def test_get_failure_rate(self, progress_tracker):
        """Test failure rate calculation."""
        # No attempts yet
        assert progress_tracker.get_failure_rate() == 0.0

        # Set some failures
        progress_tracker.processed_items = 80
        progress_tracker.failed_items = 20

        # Failure rate = 20 / (80 + 20) = 0.2 = 20%
        assert progress_tracker.get_failure_rate() == 20.0

    def test_get_progress_summary(self, progress_tracker):
        """Test progress summary generation."""
        # Update some progress
        progress_tracker.processed_items = 30
        progress_tracker.failed_items = 5
        progress_tracker.skipped_items = 3

        summary = progress_tracker.get_progress_summary()

        expected_keys = [
            "operation_name",
            "total_items",
            "processed_items",
            "failed_items",
            "skipped_items",
            "progress_percent",
            "processing_rate",
            "failure_rate",
            "estimated_completion",
            "elapsed_time",
            "last_checkpoint_id",
        ]

        for key in expected_keys:
            assert key in summary

        assert summary["operation_name"] == "progress_test"
        assert summary["total_items"] == 100
        assert summary["processed_items"] == 30


class TestRecoverySession:
    """Test RecoverySession class."""

    @pytest.fixture
    def checkpoint_manager(self):
        """Create checkpoint manager for testing."""
        return CheckpointManager(storage_path=None)

    @pytest.fixture
    def recovery_manager(self, checkpoint_manager):
        """Create recovery manager for testing."""
        return RecoveryManager(checkpoint_manager)

    @pytest.mark.asyncio
    async def test_recovery_session_basic(self, checkpoint_manager, recovery_manager):
        """Test basic recovery session usage."""
        session_result = None

        async with RecoverySession(
            operation_name="session_test",
            checkpoint_manager=checkpoint_manager,
            recovery_manager=recovery_manager,
            total_items=50,
            checkpoint_interval=5,
        ) as session:
            session_result = session

            # Test session properties
            assert session.operation_name == "session_test"
            assert session.total_items == 50
            assert session.checkpoint_interval == 5
            assert session.progress_tracker is not None
            assert session.session_checkpoint_id is not None

        assert session_result is not None

    @pytest.mark.asyncio
    async def test_recovery_session_with_progress_tracking(self, checkpoint_manager, recovery_manager):
        """Test recovery session with progress tracking."""
        async with RecoverySession(
            operation_name="progress_session",
            checkpoint_manager=checkpoint_manager,
            recovery_manager=recovery_manager,
            total_items=20,
            checkpoint_interval=5,
        ) as session:
            # Update progress
            await session.update_progress(processed=10, failed=2)

            # Check progress
            progress = session.get_progress()
            assert progress is not None
            assert progress["processed_items"] == 10
            assert progress["failed_items"] == 2

    @pytest.mark.asyncio
    async def test_recovery_session_manual_checkpoint(self, checkpoint_manager, recovery_manager):
        """Test manual checkpointing in recovery session."""
        async with RecoverySession(
            operation_name="manual_checkpoint", checkpoint_manager=checkpoint_manager, recovery_manager=recovery_manager
        ) as session:
            # Create manual checkpoint
            checkpoint_id = await session.checkpoint(state_data={"custom": "data"}, metadata={"type": "manual"})

            assert checkpoint_id is not None

            # Verify checkpoint exists
            checkpoint = await checkpoint_manager.restore_checkpoint(checkpoint_id)
            assert checkpoint is not None
            assert checkpoint.state_data["custom"] == "data"
            assert checkpoint.metadata["type"] == "manual"

    @pytest.mark.asyncio
    async def test_recovery_session_auto_restore(self, checkpoint_manager, recovery_manager):
        """Test automatic restoration in recovery session."""
        operation_name = "auto_restore_test"

        # Create an existing checkpoint
        existing_data = {"previous_progress": 75}
        await checkpoint_manager.create_checkpoint(operation_name, existing_data)

        # Start session with auto_restore
        async with RecoverySession(
            operation_name=operation_name,
            checkpoint_manager=checkpoint_manager,
            recovery_manager=recovery_manager,
            auto_restore=True,
        ) as session:
            # Check that recovery context indicates restoration
            assert session.recovery_context["recovered_from_checkpoint"] is True
            assert session.recovery_context["checkpoint_data"] is not None


class TestUtilityFunctions:
    """Test utility functions."""

    @pytest.mark.asyncio
    async def test_with_checkpointing_decorator(self):
        """Test with_checkpointing decorator."""
        checkpoint_manager = CheckpointManager(storage_path=None)
        checkpoints_created = []

        @with_checkpointing(
            checkpoint_manager=checkpoint_manager, operation_name="decorated_function", checkpoint_interval=5
        )
        async def test_function(progress_tracker=None):
            if progress_tracker:
                checkpoints_created.append("tracker_provided")
                await progress_tracker.update_progress(processed=10)
            return "success"

        result = await test_function(total_items=20)

        assert result == "success"
        assert len(checkpoints_created) == 1

    @pytest.mark.asyncio
    async def test_create_recovery_context(self):
        """Test recovery context creation."""
        checkpoint_manager = CheckpointManager(storage_path=None)
        recovery_manager = RecoveryManager(checkpoint_manager)

        # Create context without existing checkpoint
        context = await create_recovery_context(
            operation_name="context_test",
            checkpoint_manager=checkpoint_manager,
            recovery_manager=recovery_manager,
            auto_restore=True,
        )

        assert context["operation_name"] == "context_test"
        assert context["checkpoint_manager"] == checkpoint_manager
        assert context["recovery_manager"] == recovery_manager
        assert context["recovered_from_checkpoint"] is False
        assert context["checkpoint_data"] is None

    @pytest.mark.asyncio
    async def test_create_recovery_context_with_existing_checkpoint(self):
        """Test recovery context creation with existing checkpoint."""
        checkpoint_manager = CheckpointManager(storage_path=None)
        recovery_manager = RecoveryManager(checkpoint_manager)

        # Create an existing checkpoint
        await checkpoint_manager.create_checkpoint("existing_context_test", {"existing": "data"})

        # Create context with auto_restore
        context = await create_recovery_context(
            operation_name="existing_context_test",
            checkpoint_manager=checkpoint_manager,
            recovery_manager=recovery_manager,
            auto_restore=True,
        )

        assert context["recovered_from_checkpoint"] is True
        assert context["checkpoint_data"] is not None
        assert context["checkpoint_data"].state_data["existing"] == "data"


class TestIntegrationScenarios:
    """Test integration scenarios combining multiple components."""

    @pytest.mark.asyncio
    async def test_full_recovery_workflow(self):
        """Test complete recovery workflow."""
        checkpoint_manager = CheckpointManager(storage_path=None)
        recovery_manager = RecoveryManager(checkpoint_manager)

        # Simulate a processing operation that fails and recovers
        processed_items = []

        async with RecoverySession(
            operation_name="full_workflow",
            checkpoint_manager=checkpoint_manager,
            recovery_manager=recovery_manager,
            total_items=30,
            checkpoint_interval=10,
        ) as session:
            # Process items with simulated failure and recovery
            for i in range(30):
                try:
                    # Simulate failure at item 15
                    if i == 15:
                        raise ProcessingError("Simulated processing failure")

                    processed_items.append(i)
                    await session.update_progress(processed=1)

                except ProcessingError as e:
                    # Record failure and continue
                    await session.update_progress(failed=1)

                    # Simulate recovery attempt
                    recovery_result = await recovery_manager.attempt_recovery(error=e, operation_name="full_workflow")

                    # Continue processing after recovery
                    if recovery_result:
                        processed_items.append(f"recovered_{i}")
                        await session.update_progress(processed=1)

        # Verify processing completed with recovery
        assert len(processed_items) == 30  # 29 normal + 1 recovered

        # Verify progress tracking
        final_progress = session.get_progress()
        assert final_progress["processed_items"] == 30
        assert final_progress["failed_items"] == 1

    @pytest.mark.asyncio
    async def test_checkpoint_persistence_across_sessions(self):
        """Test checkpoint persistence across multiple sessions."""
        temp_dir = tempfile.mkdtemp()
        storage_path = Path(temp_dir)

        try:
            # First session - create checkpoints
            checkpoint_manager1 = CheckpointManager(storage_path=storage_path)

            checkpoint_id = await checkpoint_manager1.create_checkpoint(
                "persistent_test", {"session": 1, "data": "persistent_data"}
            )

            # Second session - should load from file
            checkpoint_manager2 = CheckpointManager(storage_path=storage_path)

            restored = await checkpoint_manager2.restore_checkpoint(checkpoint_id)

            assert restored is not None
            assert restored.state_data["session"] == 1
            assert restored.state_data["data"] == "persistent_data"

        finally:
            # Cleanup
            import shutil

            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_progress_tracking_with_different_intervals(self):
        """Test progress tracking with different checkpoint intervals."""
        checkpoint_manager = CheckpointManager(storage_path=None)

        # Test with small interval
        tracker_small = ProgressTracker(
            operation_name="small_interval",
            total_items=50,
            checkpoint_manager=checkpoint_manager,
            checkpoint_interval=5,
        )

        # Process items
        for i in range(12):
            await tracker_small.update_progress(processed=1)

        # Should have created 2 checkpoints (at 5 and 10)
        checkpoints = await checkpoint_manager.list_checkpoints("small_interval")
        progress_checkpoints = [cp for cp in checkpoints if cp.metadata.get("checkpoint_type") == "progress"]
        assert len(progress_checkpoints) >= 2

    @pytest.mark.asyncio
    async def test_recovery_strategy_integration(self):
        """Test integration of recovery strategies with checkpoint system."""
        checkpoint_manager = CheckpointManager(storage_path=None)
        recovery_manager = RecoveryManager(checkpoint_manager)

        # Register custom recovery strategy
        custom_recoveries = []

        async def custom_fallback(error, operation_name):
            custom_recoveries.append((error, operation_name))
            # Create a recovery checkpoint
            return await checkpoint_manager.create_checkpoint(
                operation_name, {"recovery_type": "custom", "error_handled": str(error)}
            )

        recovery_manager.register_recovery_strategy(RecoveryStrategy.FALLBACK, custom_fallback)

        # Simulate error and recovery
        error = ProcessingError("Custom error", recovery_strategy=RecoveryStrategy.FALLBACK)

        result = await recovery_manager.attempt_recovery(error=error, operation_name="custom_recovery_test")

        assert len(custom_recoveries) == 1
        assert isinstance(result, str)  # Should be checkpoint_id

        # Verify recovery checkpoint was created
        checkpoint = await checkpoint_manager.restore_checkpoint(result)
        assert checkpoint is not None
        assert checkpoint.state_data["recovery_type"] == "custom"
