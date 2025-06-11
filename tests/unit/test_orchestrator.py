"""Tests for orchestrator workflow system."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wallet_tracker.app import Application
from wallet_tracker.orchestrator import (
    Orchestrator,
    StepResult,
    Workflow,
    WorkflowState,
    WorkflowStep,
)


class TestWorkflowStep:
    """Test WorkflowStep class."""

    def test_workflow_step_creation(self):
        """Test WorkflowStep creation."""

        def dummy_func():
            pass

        step = WorkflowStep(
            name="test_step",
            func=dummy_func,
            description="Test step",
            required=True,
            retry_count=3,
            timeout=30.0,
            dependencies=["step1", "step2"],
        )

        assert step.name == "test_step"
        assert step.func == dummy_func
        assert step.description == "Test step"
        assert step.required is True
        assert step.retry_count == 3
        assert step.timeout == 30.0
        assert step.dependencies == ["step1", "step2"]
        assert step.executed is False
        assert step.result is None

    def test_workflow_step_defaults(self):
        """Test WorkflowStep with default values."""

        def dummy_func():
            pass

        step = WorkflowStep("simple_step", dummy_func)

        assert step.description == ""
        assert step.required is True
        assert step.retry_count == 0
        assert step.timeout is None
        assert step.dependencies == []


class TestStepResult:
    """Test StepResult class."""

    def test_step_result_creation(self):
        """Test StepResult creation."""
        result = StepResult(
            step_name="test_step",
            success=True,
            data={"result": "success"},
            error=None,
            duration=1.5,
            metadata={"attempt": 1},
        )

        assert result.step_name == "test_step"
        assert result.success is True
        assert result.data == {"result": "success"}
        assert result.error is None
        assert result.duration == 1.5
        assert result.metadata == {"attempt": 1}
        assert isinstance(result.timestamp, datetime)

    def test_step_result_failure(self):
        """Test StepResult for failed step."""
        result = StepResult(step_name="failed_step", success=False, error="Step failed", duration=0.5)

        assert result.success is False
        assert result.error == "Step failed"
        assert result.data is None
        assert result.metadata == {}


class TestWorkflow:
    """Test Workflow class."""

    def test_workflow_creation(self):
        """Test Workflow creation."""
        workflow = Workflow(name="test_workflow", description="Test workflow", timeout=3600.0, continue_on_error=True)

        assert workflow.name == "test_workflow"
        assert workflow.description == "Test workflow"
        assert workflow.timeout == 3600.0
        assert workflow.continue_on_error is True
        assert workflow.state == WorkflowState.PENDING
        assert workflow.steps == []
        assert workflow.results == []
        assert workflow.start_time is None
        assert workflow.end_time is None
        assert workflow.error is None

    def test_workflow_add_step(self):
        """Test adding steps to workflow."""
        workflow = Workflow("test")

        def step_func():
            pass

        # Add step and test chaining
        result = workflow.add_step("step1", step_func, "First step")
        assert result is workflow  # Should return self for chaining

        assert len(workflow.steps) == 1
        step = workflow.steps[0]
        assert step.name == "step1"
        assert step.func == step_func
        assert step.description == "First step"

    def test_workflow_add_multiple_steps(self):
        """Test adding multiple steps with dependencies."""
        workflow = Workflow("test")

        def step1():
            pass

        def step2():
            pass

        def step3():
            pass

        # Chain step additions
        workflow.add_step("step1", step1).add_step("step2", step2, dependencies=["step1"]).add_step(
            "step3", step3, dependencies=["step1", "step2"]
        )

        assert len(workflow.steps) == 3
        assert workflow.steps[1].dependencies == ["step1"]
        assert workflow.steps[2].dependencies == ["step1", "step2"]

    def test_workflow_get_step(self):
        """Test getting step by name."""
        workflow = Workflow("test")

        def step_func():
            pass

        workflow.add_step("target_step", step_func)

        step = workflow.get_step("target_step")
        assert step is not None
        assert step.name == "target_step"

        # Test non-existent step
        assert workflow.get_step("nonexistent") is None

    def test_workflow_get_result(self):
        """Test getting result by step name."""
        workflow = Workflow("test")

        # Add a result
        result = StepResult("step1", True, "success")
        workflow.results.append(result)

        retrieved = workflow.get_result("step1")
        assert retrieved is result

        # Test non-existent result
        assert workflow.get_result("nonexistent") is None

    def test_workflow_duration(self):
        """Test workflow duration calculation."""
        workflow = Workflow("test")

        # No times set
        assert workflow.duration is None

        # Set start time only
        workflow.start_time = datetime.now(UTC)
        assert workflow.duration is None

        # Set both times
        workflow.end_time = datetime.now(UTC)
        assert workflow.duration is not None
        assert workflow.duration >= 0

    def test_workflow_success_rate(self):
        """Test workflow success rate calculation."""
        workflow = Workflow("test")

        # No results
        assert workflow.success_rate == 0.0

        # Add results
        workflow.results.append(StepResult("step1", True))
        workflow.results.append(StepResult("step2", False))
        workflow.results.append(StepResult("step3", True))

        # 2 out of 3 successful = 66.67%
        assert abs(workflow.success_rate - 66.67) < 0.01


class TestOrchestrator:
    """Test Orchestrator class."""

    @pytest.fixture
    def mock_app(self):
        """Create mock application."""
        app = AsyncMock(spec=Application)

        # Mock processors
        app.batch_processor = AsyncMock()
        app.wallet_processor = AsyncMock()

        # Mock clients
        app.ethereum_client = AsyncMock()
        app.coingecko_client = AsyncMock()
        app.sheets_client = AsyncMock()
        app.cache_manager = AsyncMock()

        # Mock config
        app.config = MagicMock()
        app.config.is_production.return_value = False

        # Mock health check
        app.health_check = AsyncMock(
            return_value={"ethereum_client": True, "coingecko_client": True, "sheets_client": True}
        )

        return app

    @pytest.fixture
    def orchestrator(self, mock_app):
        """Create Orchestrator instance."""
        return Orchestrator(mock_app)

    def test_orchestrator_creation(self, orchestrator, mock_app):
        """Test Orchestrator creation."""
        assert orchestrator.app == mock_app
        assert orchestrator._active_workflows == {}
        assert orchestrator._global_context == {}

    def test_create_workflow(self, orchestrator):
        """Test workflow creation."""
        workflow = orchestrator.create_workflow(
            name="test_workflow", description="Test", timeout=300.0, continue_on_error=True
        )

        assert isinstance(workflow, Workflow)
        assert workflow.name == "test_workflow"
        assert workflow.description == "Test"
        assert workflow.timeout == 300.0
        assert workflow.continue_on_error is True

    @pytest.mark.asyncio
    async def test_execute_simple_workflow(self, orchestrator):
        """Test executing a simple workflow."""
        workflow = orchestrator.create_workflow("simple_test")

        # Add a simple step
        step_data = {"executed": False}

        async def simple_step(context):
            step_data["executed"] = True
            return "step_completed"

        workflow.add_step("simple_step", simple_step)

        # Execute workflow
        completed_workflow = await orchestrator.execute_workflow(workflow)

        assert completed_workflow.state == WorkflowState.COMPLETED
        assert len(completed_workflow.results) == 1
        assert completed_workflow.results[0].success is True
        assert completed_workflow.results[0].data == "step_completed"
        assert step_data["executed"] is True

    @pytest.mark.asyncio
    async def test_execute_workflow_with_dependencies(self, orchestrator):
        """Test executing workflow with step dependencies."""
        workflow = orchestrator.create_workflow("dependency_test")

        execution_order = []

        async def step1(context):
            execution_order.append("step1")
            return "step1_result"

        async def step2(context):
            execution_order.append("step2")
            # Should have access to step1 result
            assert context["step_step1_result"] == "step1_result"
            return "step2_result"

        async def step3(context):
            execution_order.append("step3")
            # Should have access to both previous results
            assert context["step_step1_result"] == "step1_result"
            assert context["step_step2_result"] == "step2_result"
            return "step3_result"

        workflow.add_step("step1", step1)
        workflow.add_step("step2", step2, dependencies=["step1"])
        workflow.add_step("step3", step3, dependencies=["step1", "step2"])

        completed_workflow = await orchestrator.execute_workflow(workflow)

        assert completed_workflow.state == WorkflowState.COMPLETED
        assert len(completed_workflow.results) == 3
        assert all(result.success for result in completed_workflow.results)

        # Check execution order
        assert execution_order == ["step1", "step2", "step3"]

    @pytest.mark.asyncio
    async def test_execute_workflow_with_failure(self, orchestrator):
        """Test executing workflow with step failure."""
        workflow = orchestrator.create_workflow("failure_test", continue_on_error=False)

        async def successful_step(context):
            return "success"

        async def failing_step(context):
            raise ValueError("Step failed")

        workflow.add_step("success_step", successful_step)
        workflow.add_step("fail_step", failing_step, dependencies=["success_step"])

        completed_workflow = await orchestrator.execute_workflow(workflow)

        assert completed_workflow.state == WorkflowState.FAILED
        assert len(completed_workflow.results) == 2
        assert completed_workflow.results[0].success is True
        assert completed_workflow.results[1].success is False
        assert "Step failed" in completed_workflow.results[1].error

    @pytest.mark.asyncio
    async def test_execute_workflow_continue_on_error(self, orchestrator):
        """Test executing workflow with continue_on_error=True."""
        workflow = orchestrator.create_workflow("continue_test", continue_on_error=True)

        async def failing_step(context):
            raise ValueError("Step failed")

        async def cleanup_step(context):
            return "cleanup_done"

        workflow.add_step("fail_step", failing_step, required=False)
        workflow.add_step("cleanup_step", cleanup_step)

        completed_workflow = await orchestrator.execute_workflow(workflow)

        assert completed_workflow.state == WorkflowState.COMPLETED
        assert len(completed_workflow.results) == 2
        assert completed_workflow.results[0].success is False
        assert completed_workflow.results[1].success is True

    @pytest.mark.asyncio
    async def test_execute_workflow_with_timeout(self, orchestrator):
        """Test executing workflow with timeout."""
        workflow = orchestrator.create_workflow("timeout_test", timeout=0.1)

        async def slow_step(context):
            await asyncio.sleep(0.2)  # Longer than workflow timeout
            return "completed"

        workflow.add_step("slow_step", slow_step)

        completed_workflow = await orchestrator.execute_workflow(workflow)

        assert completed_workflow.state == WorkflowState.FAILED
        assert "timeout" in completed_workflow.error.lower()

    @pytest.mark.asyncio
    async def test_execute_workflow_step_with_retry(self, orchestrator):
        """Test executing workflow step with retry logic."""
        workflow = orchestrator.create_workflow("retry_test")

        attempt_count = {"count": 0}

        async def flaky_step(context):
            attempt_count["count"] += 1
            if attempt_count["count"] < 3:
                raise ValueError(f"Attempt {attempt_count['count']} failed")
            return "success_on_retry"

        workflow.add_step("flaky_step", flaky_step, retry_count=3)

        completed_workflow = await orchestrator.execute_workflow(workflow)

        assert completed_workflow.state == WorkflowState.COMPLETED
        assert len(completed_workflow.results) == 1
        assert completed_workflow.results[0].success is True
        assert completed_workflow.results[0].data == "success_on_retry"
        assert completed_workflow.results[0].metadata["attempt"] == 3

    @pytest.mark.asyncio
    async def test_execute_workflow_step_timeout(self, orchestrator):
        """Test executing workflow step with step-level timeout."""
        workflow = orchestrator.create_workflow("step_timeout_test")

        async def slow_step(context):
            await asyncio.sleep(0.2)
            return "completed"

        workflow.add_step("slow_step", slow_step, timeout=0.1)

        completed_workflow = await orchestrator.execute_workflow(workflow)

        assert completed_workflow.state == WorkflowState.FAILED
        assert len(completed_workflow.results) == 1
        assert completed_workflow.results[0].success is False
        assert "timeout" in completed_workflow.results[0].error.lower()

    @pytest.mark.asyncio
    async def test_execute_workflow_with_progress_callback(self, orchestrator):
        """Test executing workflow with progress callback."""
        workflow = orchestrator.create_workflow("progress_test")

        progress_updates = []

        def progress_callback(wf):
            progress_updates.append(
                {"completed_steps": len([r for r in wf.results if r.success]), "total_steps": len(wf.steps)}
            )

        async def step1(context):
            return "step1_done"

        async def step2(context):
            return "step2_done"

        workflow.add_step("step1", step1)
        workflow.add_step("step2", step2, dependencies=["step1"])

        await orchestrator.execute_workflow(workflow, progress_callback=progress_callback)

        # Should have received progress updates
        assert len(progress_updates) >= 1

    def test_validate_workflow_dependencies_valid(self, orchestrator):
        """Test workflow dependency validation with valid dependencies."""
        workflow = orchestrator.create_workflow("valid_deps")

        def dummy_func():
            pass

        workflow.add_step("step1", dummy_func)
        workflow.add_step("step2", dummy_func, dependencies=["step1"])
        workflow.add_step("step3", dummy_func, dependencies=["step1", "step2"])

        # Should not raise exception
        orchestrator._validate_workflow_dependencies(workflow)

    def test_validate_workflow_dependencies_missing(self, orchestrator):
        """Test workflow dependency validation with missing dependency."""
        workflow = orchestrator.create_workflow("invalid_deps")

        def dummy_func():
            pass

        workflow.add_step("step1", dummy_func)
        workflow.add_step("step2", dummy_func, dependencies=["nonexistent"])

        with pytest.raises(ValueError, match="depends on unknown step"):
            orchestrator._validate_workflow_dependencies(workflow)

    def test_validate_workflow_dependencies_circular(self, orchestrator):
        """Test workflow dependency validation with circular dependency."""
        workflow = orchestrator.create_workflow("circular_deps")

        def dummy_func():
            pass

        workflow.add_step("step1", dummy_func, dependencies=["step1"])

        with pytest.raises(ValueError, match="circular dependency"):
            orchestrator._validate_workflow_dependencies(workflow)

    def test_global_context_operations(self, orchestrator):
        """Test global context get/set operations."""
        orchestrator.set_global_context("test_key", "test_value")
        assert orchestrator.get_global_context("test_key") == "test_value"

        # Test default value
        assert orchestrator.get_global_context("nonexistent", "default") == "default"

    def test_get_active_workflows(self, orchestrator):
        """Test getting active workflows."""
        # Initially empty
        active = orchestrator.get_active_workflows()
        assert active == {}

        # Add a mock workflow to active list
        mock_workflow = MagicMock()
        orchestrator._active_workflows["test_id"] = mock_workflow

        active = orchestrator.get_active_workflows()
        assert "test_id" in active
        assert active["test_id"] == mock_workflow

    def test_get_workflow_stats(self, orchestrator):
        """Test getting workflow statistics."""
        stats = orchestrator.get_workflow_stats()

        assert "active_workflows" in stats
        assert "active_workflow_names" in stats
        assert "global_context_keys" in stats
        assert stats["active_workflows"] == 0

        # Add some context and active workflow
        orchestrator.set_global_context("test", "value")
        orchestrator._active_workflows["test_workflow"] = MagicMock()

        stats = orchestrator.get_workflow_stats()
        assert stats["active_workflows"] == 1
        assert "test_workflow" in stats["active_workflow_names"]
        assert "test" in stats["global_context_keys"]


class TestPrebuiltWorkflows:
    """Test pre-built workflow factories."""

    @pytest.fixture
    def mock_app(self):
        """Create mock application with all required components."""
        app = AsyncMock(spec=Application)

        # Mock batch processor
        app.batch_processor = AsyncMock()

        # Mock clients
        app.ethereum_client = AsyncMock()
        app.coingecko_client = AsyncMock()
        app.sheets_client = AsyncMock()
        app.cache_manager = AsyncMock()

        # Mock config
        app.config = MagicMock()
        app.config.is_production.return_value = False

        return app

    @pytest.fixture
    def orchestrator(self, mock_app):
        """Create Orchestrator with mocked app."""
        return Orchestrator(mock_app)

    def test_create_wallet_analysis_workflow(self, orchestrator):
        """Test creating wallet analysis workflow."""
        workflow = orchestrator.create_wallet_analysis_workflow(
            spreadsheet_id="test_sheet_id", input_range="A:B", output_range="C1", batch_size=25, dry_run=True
        )

        assert isinstance(workflow, Workflow)
        assert workflow.name == "wallet_analysis"
        assert workflow.timeout == 3600  # 1 hour
        assert workflow.continue_on_error is False

        # Check that all expected steps are present
        step_names = [step.name for step in workflow.steps]
        expected_steps = [
            "validate_inputs",
            "health_check",
            "precache_prices",
            "read_addresses",
            "process_wallets",
            "write_results",
            "generate_summary",
        ]

        for expected_step in expected_steps:
            assert expected_step in step_names

        # Check step dependencies
        process_step = workflow.get_step("process_wallets")
        assert "read_addresses" in process_step.dependencies
        assert "precache_prices" in process_step.dependencies

        write_step = workflow.get_step("write_results")
        assert "process_wallets" in write_step.dependencies

    def test_create_price_update_workflow(self, orchestrator):
        """Test creating price update workflow."""
        token_addresses = ["0x123", "0x456"]

        workflow = orchestrator.create_price_update_workflow(
            token_addresses=token_addresses, update_popular=True, cache_duration=7200
        )

        assert isinstance(workflow, Workflow)
        assert workflow.name == "price_update"
        assert workflow.timeout == 600  # 10 minutes
        assert workflow.continue_on_error is True

        step_names = [step.name for step in workflow.steps]
        expected_steps = [
            "update_eth_price",
            "update_stablecoin_prices",
            "update_popular_tokens",
            "update_specific_tokens",
        ]

        for expected_step in expected_steps:
            assert expected_step in step_names

    def test_create_maintenance_workflow(self, orchestrator):
        """Test creating maintenance workflow."""
        workflow = orchestrator.create_maintenance_workflow()

        assert isinstance(workflow, Workflow)
        assert workflow.name == "maintenance"
        assert workflow.timeout == 1800  # 30 minutes
        assert workflow.continue_on_error is True

        step_names = [step.name for step in workflow.steps]
        expected_steps = ["cache_cleanup", "comprehensive_health_check", "refresh_configuration"]

        for expected_step in expected_steps:
            assert expected_step in step_names

    @pytest.mark.asyncio
    async def test_execute_scheduled_maintenance(self, orchestrator):
        """Test executing scheduled maintenance workflow."""
        with patch.object(orchestrator, "create_maintenance_workflow") as mock_create:
            mock_workflow = MagicMock()
            mock_create.return_value = mock_workflow

            with patch.object(orchestrator, "execute_workflow") as mock_execute:
                mock_execute.return_value = mock_workflow

                result = await orchestrator.execute_scheduled_maintenance()

                mock_create.assert_called_once()
                mock_execute.assert_called_once_with(mock_workflow)
                assert result == mock_workflow

    @pytest.mark.asyncio
    async def test_execute_price_update(self, orchestrator):
        """Test executing price update workflow."""
        token_addresses = ["0x123", "0x456"]

        with patch.object(orchestrator, "create_price_update_workflow") as mock_create:
            mock_workflow = MagicMock()
            mock_create.return_value = mock_workflow

            with patch.object(orchestrator, "execute_workflow") as mock_execute:
                mock_execute.return_value = mock_workflow

                result = await orchestrator.execute_price_update(token_addresses=token_addresses, update_popular=False)

                mock_create.assert_called_once_with(token_addresses=token_addresses, update_popular=False)
                mock_execute.assert_called_once_with(mock_workflow)
                assert result == mock_workflow


@pytest.mark.asyncio
async def test_workflow_execution_integration(mock_app):
    """Integration test for complete workflow execution."""
    # Create orchestrator
    orchestrator = Orchestrator(mock_app)

    # Create a workflow that simulates real operations
    workflow = orchestrator.create_workflow("integration_test")

    # Mock some realistic operations
    execution_log = []

    async def validate_step(context):
        execution_log.append("validate")
        return {"valid": True}

    async def process_step(context):
        execution_log.append("process")
        # Simulate accessing previous step result
        validation_result = context["step_validate_step_result"]
        assert validation_result["valid"] is True
        return {"processed": 10, "total_value": 50000}

    async def finalize_step(context):
        execution_log.append("finalize")
        process_result = context["step_process_step_result"]
        return {"summary": f"Processed {process_result['processed']} items"}

    # Add steps with dependencies
    workflow.add_step("validate_step", validate_step, "Validate inputs")
    workflow.add_step("process_step", process_step, "Process data", dependencies=["validate_step"])
    workflow.add_step("finalize_step", finalize_step, "Finalize results", dependencies=["process_step"])

    # Execute workflow
    result = await orchestrator.execute_workflow(workflow)

    # Verify execution
    assert result.state == WorkflowState.COMPLETED
    assert len(result.results) == 3
    assert all(r.success for r in result.results)
    assert execution_log == ["validate", "process", "finalize"]

    # Verify final result
    final_result = result.get_result("finalize_step")
    assert "Processed 10 items" in final_result.data["summary"]


@pytest.mark.asyncio
async def test_workflow_parallel_execution():
    """Test that independent steps can execute in parallel."""
    mock_app = AsyncMock()
    orchestrator = Orchestrator(mock_app)

    workflow = orchestrator.create_workflow("parallel_test")

    execution_times = {}

    async def step_a(context):
        start_time = asyncio.get_event_loop().time()
        await asyncio.sleep(0.1)
        execution_times["step_a"] = asyncio.get_event_loop().time() - start_time
        return "a_result"

    async def step_b(context):
        start_time = asyncio.get_event_loop().time()
        await asyncio.sleep(0.1)
        execution_times["step_b"] = asyncio.get_event_loop().time() - start_time
        return "b_result"

    async def step_c(context):
        start_time = asyncio.get_event_loop().time()
        # This step depends on both A and B
        assert context["step_step_a_result"] == "a_result"
        assert context["step_step_b_result"] == "b_result"
        await asyncio.sleep(0.05)
        execution_times["step_c"] = asyncio.get_event_loop().time() - start_time
        return "c_result"

    # Add independent steps A and B, then C which depends on both
    workflow.add_step("step_a", step_a)
    workflow.add_step("step_b", step_b)
    workflow.add_step("step_c", step_c, dependencies=["step_a", "step_b"])

    start_time = asyncio.get_event_loop().time()
    result = await orchestrator.execute_workflow(workflow)
    total_time = asyncio.get_event_loop().time() - start_time

    assert result.state == WorkflowState.COMPLETED

    # If A and B ran in parallel, total time should be less than sum of individual times
    # (allowing for some overhead)
    expected_sequential_time = execution_times["step_a"] + execution_times["step_b"] + execution_times["step_c"]
    assert total_time < expected_sequential_time * 0.8  # Should be significantly faster


class TestWorkflowContextAndData:
    """Test workflow context and data flow."""

    @pytest.fixture
    def orchestrator(self):
        mock_app = AsyncMock()
        return Orchestrator(mock_app)

    @pytest.mark.asyncio
    async def test_context_data_flow(self, orchestrator):
        """Test that data flows correctly through workflow context."""
        workflow = orchestrator.create_workflow("context_test")

        async def producer_step(context):
            return {"produced_data": "test_value", "count": 42}

        async def consumer_step(context):
            producer_result = context["step_producer_step_result"]
            assert producer_result["produced_data"] == "test_value"
            assert producer_result["count"] == 42
            return {"consumed": True, "processed_count": producer_result["count"] * 2}

        workflow.add_step("producer_step", producer_step)
        workflow.add_step("consumer_step", consumer_step, dependencies=["producer_step"])

        result = await orchestrator.execute_workflow(workflow)

        assert result.state == WorkflowState.COMPLETED
        consumer_result = result.get_result("consumer_step")
        assert consumer_result.data["processed_count"] == 84

    @pytest.mark.asyncio
    async def test_global_context_access(self, orchestrator):
        """Test that steps can access global context."""
        # Set global context
        orchestrator.set_global_context("global_setting", "important_value")
        orchestrator.set_global_context("batch_size", 100)

        workflow = orchestrator.create_workflow("global_context_test")

        async def context_aware_step(context):
            # Should have access to global context
            assert context["global_setting"] == "important_value"
            assert context["batch_size"] == 100
            # Should also have orchestrator reference
            assert context["orchestrator"] is orchestrator
            return {"used_global": True}

        workflow.add_step("context_step", context_aware_step)

        result = await orchestrator.execute_workflow(workflow)

        assert result.state == WorkflowState.COMPLETED
        assert result.get_result("context_step").data["used_global"] is True

    @pytest.mark.asyncio
    async def test_workflow_metadata_in_context(self, orchestrator):
        """Test that workflow metadata is available in context."""
        workflow = orchestrator.create_workflow("metadata_test", description="Test workflow")

        async def metadata_step(context):
            # Should have access to workflow instance
            workflow_ref = context["workflow"]
            assert workflow_ref.name == "metadata_test"
            assert workflow_ref.description == "Test workflow"
            return {"metadata_accessed": True}

        workflow.add_step("metadata_step", metadata_step)

        result = await orchestrator.execute_workflow(workflow)

        assert result.state == WorkflowState.COMPLETED
        assert result.get_result("metadata_step").data["metadata_accessed"] is True


class TestWorkflowErrorScenarios:
    """Test various error scenarios in workflow execution."""

    @pytest.fixture
    def orchestrator(self):
        mock_app = AsyncMock()
        return Orchestrator(mock_app)

    @pytest.mark.asyncio
    async def test_missing_dependency_error(self, orchestrator):
        """Test error when step has unmet dependencies."""
        workflow = orchestrator.create_workflow("missing_dep_test")

        async def dependent_step(context):
            return "result"

        # Add step with non-existent dependency
        workflow.add_step("dependent_step", dependent_step, dependencies=["nonexistent"])

        # Should fail during validation
        with pytest.raises(ValueError, match="depends on unknown step"):
            await orchestrator.execute_workflow(workflow)

    @pytest.mark.asyncio
    async def test_step_exception_handling(self, orchestrator):
        """Test handling of exceptions in individual steps."""
        workflow = orchestrator.create_workflow("exception_test", continue_on_error=True)

        async def failing_step(context):
            raise RuntimeError("Something went wrong")

        async def recovery_step(context):
            return "recovered"

        workflow.add_step("failing_step", failing_step, required=False)
        workflow.add_step("recovery_step", recovery_step)

        result = await orchestrator.execute_workflow(workflow)

        assert result.state == WorkflowState.COMPLETED
        assert len(result.results) == 2

        fail_result = result.get_result("failing_step")
        assert fail_result.success is False
        assert "Something went wrong" in fail_result.error

        recovery_result = result.get_result("recovery_step")
        assert recovery_result.success is True

    @pytest.mark.asyncio
    async def test_required_step_failure_stops_workflow(self, orchestrator):
        """Test that required step failure stops workflow execution."""
        workflow = orchestrator.create_workflow("required_fail_test")

        async def critical_step(context):
            raise ValueError("Critical failure")

        async def should_not_run(context):
            return "should_not_execute"

        workflow.add_step("critical_step", critical_step, required=True)
        workflow.add_step("should_not_run", should_not_run, dependencies=["critical_step"])

        result = await orchestrator.execute_workflow(workflow)

        assert result.state == WorkflowState.FAILED
        assert len(result.results) == 1  # Only the failed step should have executed
        assert result.get_result("critical_step").success is False
        assert result.get_result("should_not_run") is None

    @pytest.mark.asyncio
    async def test_workflow_timeout_cancellation(self, orchestrator):
        """Test that workflow timeout properly cancels running steps."""
        workflow = orchestrator.create_workflow("timeout_cancel_test", timeout=0.1)

        cancelled_steps = []

        async def long_running_step(context):
            try:
                await asyncio.sleep(1.0)  # Much longer than workflow timeout
                return "completed"
            except asyncio.CancelledError:
                cancelled_steps.append("long_running_step")
                raise

        workflow.add_step("long_running_step", long_running_step)

        result = await orchestrator.execute_workflow(workflow)

        assert result.state == WorkflowState.FAILED
        assert "timeout" in result.error.lower()
        # The step should have been cancelled
        assert "long_running_step" in cancelled_steps


class TestWorkflowStepRetry:
    """Test step retry functionality."""

    @pytest.fixture
    def orchestrator(self):
        mock_app = AsyncMock()
        return Orchestrator(mock_app)

    @pytest.mark.asyncio
    async def test_step_retry_success_on_retry(self, orchestrator):
        """Test step that succeeds after retries."""
        workflow = orchestrator.create_workflow("retry_success_test")

        attempt_counter = {"count": 0}

        async def flaky_step(context):
            attempt_counter["count"] += 1
            if attempt_counter["count"] < 3:
                raise ConnectionError(f"Attempt {attempt_counter['count']} failed")
            return f"Success on attempt {attempt_counter['count']}"

        workflow.add_step("flaky_step", flaky_step, retry_count=3)

        result = await orchestrator.execute_workflow(workflow)

        assert result.state == WorkflowState.COMPLETED
        step_result = result.get_result("flaky_step")
        assert step_result.success is True
        assert step_result.data == "Success on attempt 3"
        assert step_result.metadata["attempt"] == 3

    @pytest.mark.asyncio
    async def test_step_retry_exhaust_retries(self, orchestrator):
        """Test step that fails after exhausting all retries."""
        workflow = orchestrator.create_workflow("retry_exhaust_test")

        attempt_counter = {"count": 0}

        async def always_failing_step(context):
            attempt_counter["count"] += 1
            raise RuntimeError(f"Attempt {attempt_counter['count']} failed")

        workflow.add_step("always_failing_step", always_failing_step, retry_count=2, required=False)

        result = await orchestrator.execute_workflow(workflow)

        assert result.state == WorkflowState.COMPLETED  # Should complete due to required=False
        step_result = result.get_result("always_failing_step")
        assert step_result.success is False
        assert "Attempt 3 failed" in step_result.error  # 1 initial + 2 retries
        assert step_result.metadata["attempt"] == 3
        assert attempt_counter["count"] == 3

    @pytest.mark.asyncio
    async def test_step_timeout_with_retries(self, orchestrator):
        """Test step timeout behavior with retries."""
        workflow = orchestrator.create_workflow("timeout_retry_test")

        attempt_counter = {"count": 0}

        async def slow_step(context):
            attempt_counter["count"] += 1
            await asyncio.sleep(0.15)  # Longer than step timeout
            return f"Completed attempt {attempt_counter['count']}"

        workflow.add_step("slow_step", slow_step, timeout=0.1, retry_count=2, required=False)

        result = await orchestrator.execute_workflow(workflow)

        step_result = result.get_result("slow_step")
        assert step_result.success is False
        assert step_result.metadata.get("timeout") is True
        assert attempt_counter["count"] == 3  # Should retry on timeout

    @pytest.mark.asyncio
    async def test_retry_exponential_backoff(self, orchestrator):
        """Test that retries include exponential backoff delay."""
        workflow = orchestrator.create_workflow("backoff_test")

        attempt_times = []

        async def timing_step(context):
            attempt_times.append(asyncio.get_event_loop().time())
            if len(attempt_times) < 3:
                raise ValueError("Fail until third attempt")
            return "success"

        workflow.add_step("timing_step", timing_step, retry_count=2)

        start_time = asyncio.get_event_loop().time()
        result = await orchestrator.execute_workflow(workflow)

        assert result.state == WorkflowState.COMPLETED
        assert len(attempt_times) == 3

        # Check that there was increasing delay between attempts
        delay1 = attempt_times[1] - attempt_times[0]
        delay2 = attempt_times[2] - attempt_times[1]

        # Second delay should be longer than first (exponential backoff)
        assert delay2 > delay1


# Performance and stress tests
class TestWorkflowPerformance:
    """Test workflow performance characteristics."""

    @pytest.fixture
    def orchestrator(self):
        mock_app = AsyncMock()
        return Orchestrator(mock_app)

    @pytest.mark.asyncio
    async def test_large_workflow_execution(self, orchestrator):
        """Test execution of workflow with many steps."""
        workflow = orchestrator.create_workflow("large_workflow_test")

        # Create a workflow with many independent steps
        step_count = 50
        results = {}

        for i in range(step_count):

            async def step_func(context, step_id=i):
                await asyncio.sleep(0.001)  # Small delay
                results[step_id] = f"step_{step_id}_completed"
                return f"result_{step_id}"

            workflow.add_step(f"step_{i}", step_func)

        start_time = asyncio.get_event_loop().time()
        result = await orchestrator.execute_workflow(workflow)
        execution_time = asyncio.get_event_loop().time() - start_time

        assert result.state == WorkflowState.COMPLETED
        assert len(result.results) == step_count
        assert all(r.success for r in result.results)
        assert len(results) == step_count

        # Should complete in reasonable time (much less than sequential execution)
        assert execution_time < step_count * 0.001 * 0.5  # Allow for 50% of sequential time

    @pytest.mark.asyncio
    async def test_complex_dependency_graph(self, orchestrator):
        """Test workflow with complex dependency relationships."""
        workflow = orchestrator.create_workflow("complex_deps_test")

        execution_order = []

        # Create a diamond dependency pattern
        async def root_step(context):
            execution_order.append("root")
            return "root_data"

        async def branch_a(context):
            execution_order.append("branch_a")
            return "a_data"

        async def branch_b(context):
            execution_order.append("branch_b")
            return "b_data"

        async def merge_step(context):
            execution_order.append("merge")
            assert context["step_root_step_result"] == "root_data"
            assert context["step_branch_a_result"] == "a_data"
            assert context["step_branch_b_result"] == "b_data"
            return "merged_data"

        workflow.add_step("root_step", root_step)
        workflow.add_step("branch_a", branch_a, dependencies=["root_step"])
        workflow.add_step("branch_b", branch_b, dependencies=["root_step"])
        workflow.add_step("merge_step", merge_step, dependencies=["branch_a", "branch_b"])

        result = await orchestrator.execute_workflow(workflow)

        assert result.state == WorkflowState.COMPLETED
        assert len(result.results) == 4

        # Verify execution order respects dependencies
        root_index = execution_order.index("root")
        branch_a_index = execution_order.index("branch_a")
        branch_b_index = execution_order.index("branch_b")
        merge_index = execution_order.index("merge")

        assert root_index < branch_a_index
        assert root_index < branch_b_index
        assert branch_a_index < merge_index
        assert branch_b_index < merge_index
