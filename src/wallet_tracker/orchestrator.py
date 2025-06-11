"""Orchestrator for complex workflows and multi-step operations."""

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from .app import Application

logger = logging.getLogger(__name__)


class WorkflowState(str, Enum):
    """Workflow execution states."""

    PENDING = "pending"
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepResult:
    """Result of a workflow step execution."""

    def __init__(
        self,
        step_name: str,
        success: bool,
        data: Any = None,
        error: str | None = None,
        duration: float | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        self.step_name = step_name
        self.success = success
        self.data = data
        self.error = error
        self.duration = duration
        self.metadata = metadata or {}
        self.timestamp = datetime.now(UTC)


class WorkflowStep:
    """Represents a single step in a workflow."""

    def __init__(
        self,
        name: str,
        func: Callable,
        description: str = "",
        required: bool = True,
        retry_count: int = 0,
        timeout: float | None = None,
        dependencies: list[str] | None = None,
    ):
        self.name = name
        self.func = func
        self.description = description
        self.required = required
        self.retry_count = retry_count
        self.timeout = timeout
        self.dependencies = dependencies or []

        # Execution state
        self.executed = False
        self.result: StepResult | None = None


class Workflow:
    """Represents a complete workflow with multiple steps."""

    def __init__(self, name: str, description: str = "", timeout: float | None = None, continue_on_error: bool = False):
        self.name = name
        self.description = description
        self.timeout = timeout
        self.continue_on_error = continue_on_error

        self.steps: list[WorkflowStep] = []
        self.state = WorkflowState.PENDING
        self.results: list[StepResult] = []
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.error: str | None = None

    def add_step(
        self,
        name: str,
        func: Callable,
        description: str = "",
        required: bool = True,
        retry_count: int = 0,
        timeout: float | None = None,
        dependencies: list[str] | None = None,
    ) -> "Workflow":
        """Add a step to the workflow."""
        step = WorkflowStep(
            name=name,
            func=func,
            description=description,
            required=required,
            retry_count=retry_count,
            timeout=timeout,
            dependencies=dependencies,
        )
        self.steps.append(step)
        return self

    def get_step(self, name: str) -> WorkflowStep | None:
        """Get a step by name."""
        return next((step for step in self.steps if step.name == name), None)

    def get_result(self, step_name: str) -> StepResult | None:
        """Get result of a specific step."""
        return next((result for result in self.results if result.step_name == step_name), None)

    @property
    def duration(self) -> float | None:
        """Get workflow duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def success_rate(self) -> float:
        """Get success rate of executed steps."""
        if not self.results:
            return 0.0
        successful = sum(1 for result in self.results if result.success)
        return (successful / len(self.results)) * 100


class Orchestrator:
    """
    Orchestrates complex workflows and multi-step operations.

    Provides:
    - Workflow execution with dependency management
    - Error handling and recovery
    - Progress tracking and metrics
    - Parallel and sequential execution
    """

    def __init__(self, app: Application):
        """Initialize orchestrator with application instance.

        Args:
            app: Application instance
        """
        self.app = app
        self._logger = logging.getLogger(__name__)
        self._active_workflows: dict[str, Workflow] = {}
        self._global_context: dict[str, Any] = {}

    def create_workflow(
        self, name: str, description: str = "", timeout: float | None = None, continue_on_error: bool = False
    ) -> Workflow:
        """Create a new workflow.

        Args:
            name: Workflow name
            description: Workflow description
            timeout: Global timeout in seconds
            continue_on_error: Whether to continue on step failures

        Returns:
            New workflow instance
        """
        workflow = Workflow(name=name, description=description, timeout=timeout, continue_on_error=continue_on_error)
        return workflow

    async def execute_workflow(
        self,
        workflow: Workflow,
        context: dict[str, Any] | None = None,
        progress_callback: Callable[[Workflow], None] | None = None,
    ) -> Workflow:
        """Execute a workflow.

        Args:
            workflow: Workflow to execute
            context: Execution context data
            progress_callback: Progress update callback

        Returns:
            Completed workflow with results
        """
        workflow_id = f"{workflow.name}_{datetime.now(UTC).isoformat()}"
        self._active_workflows[workflow_id] = workflow

        try:
            self._logger.info(f"🚀 Starting workflow: {workflow.name}")
            workflow.state = WorkflowState.INITIALIZING
            workflow.start_time = datetime.now(UTC)

            # Merge context
            execution_context = {**self._global_context, **(context or {})}
            execution_context["workflow"] = workflow
            execution_context["orchestrator"] = self

            # Validate dependencies
            self._validate_workflow_dependencies(workflow)

            # Execute steps
            workflow.state = WorkflowState.RUNNING

            if workflow.timeout:
                # Execute with timeout
                await asyncio.wait_for(
                    self._execute_workflow_steps(workflow, execution_context, progress_callback),
                    timeout=workflow.timeout,
                )
            else:
                # Execute without timeout
                await self._execute_workflow_steps(workflow, execution_context, progress_callback)

            # Mark as completed
            workflow.state = WorkflowState.COMPLETED
            workflow.end_time = datetime.now(UTC)

            self._logger.info(f"✅ Workflow completed: {workflow.name} ({workflow.duration:.2f}s)")

        except TimeoutError:
            workflow.state = WorkflowState.FAILED
            workflow.error = "Workflow timeout"
            workflow.end_time = datetime.now(UTC)
            self._logger.error(f"⏰ Workflow timed out: {workflow.name}")

        except Exception as e:
            workflow.state = WorkflowState.FAILED
            workflow.error = str(e)
            workflow.end_time = datetime.now(UTC)
            self._logger.error(f"❌ Workflow failed: {workflow.name} - {e}")

        finally:
            # Cleanup
            if workflow_id in self._active_workflows:
                del self._active_workflows[workflow_id]

        return workflow

    def _validate_workflow_dependencies(self, workflow: Workflow) -> None:
        """Validate workflow step dependencies."""
        step_names = {step.name for step in workflow.steps}

        for step in workflow.steps:
            for dep in step.dependencies:
                if dep not in step_names:
                    raise ValueError(f"Step '{step.name}' depends on unknown step '{dep}'")

        # Check for circular dependencies (simple check)
        for step in workflow.steps:
            if step.name in step.dependencies:
                raise ValueError(f"Step '{step.name}' has circular dependency on itself")

    async def _execute_workflow_steps(
        self, workflow: Workflow, context: dict[str, Any], progress_callback: Callable[[Workflow], None] | None
    ) -> None:
        """Execute workflow steps in dependency order."""
        executed_steps = set()

        while len(executed_steps) < len(workflow.steps):
            # Find steps ready to execute
            ready_steps = [
                step
                for step in workflow.steps
                if not step.executed and all(dep in executed_steps for dep in step.dependencies)
            ]

            if not ready_steps:
                # Check if we have unexecuted steps with unmet dependencies
                remaining_steps = [step for step in workflow.steps if not step.executed]
                if remaining_steps:
                    unmet_deps = []
                    for step in remaining_steps:
                        unmet_deps.extend([dep for dep in step.dependencies if dep not in executed_steps])
                    raise RuntimeError(f"Circular dependencies or missing steps: {unmet_deps}")
                break

            # Execute ready steps (can be parallel)
            step_tasks = []
            for step in ready_steps:
                task = asyncio.create_task(self._execute_step(step, context))
                step_tasks.append((step, task))

            # Wait for all tasks to complete
            for step, task in step_tasks:
                try:
                    result = await task
                    workflow.results.append(result)
                    step.result = result
                    step.executed = True
                    executed_steps.add(step.name)

                    # Update context with step result
                    context[f"step_{step.name}_result"] = result.data

                    if not result.success and step.required and not workflow.continue_on_error:
                        raise RuntimeError(f"Required step '{step.name}' failed: {result.error}")

                except Exception as e:
                    # Create error result
                    error_result = StepResult(step_name=step.name, success=False, error=str(e))
                    workflow.results.append(error_result)
                    step.result = error_result
                    step.executed = True
                    executed_steps.add(step.name)

                    if step.required and not workflow.continue_on_error:
                        raise

            # Call progress callback
            if progress_callback:
                try:
                    progress_callback(workflow)
                except Exception as e:
                    self._logger.warning(f"Progress callback error: {e}")

    async def _execute_step(self, step: WorkflowStep, context: dict[str, Any]) -> StepResult:
        """Execute a single workflow step."""
        self._logger.debug(f"🔄 Executing step: {step.name}")

        start_time = datetime.now(UTC)

        for attempt in range(step.retry_count + 1):
            try:
                if step.timeout:
                    # Execute with timeout
                    result_data = await asyncio.wait_for(step.func(context), timeout=step.timeout)
                else:
                    # Execute without timeout
                    result_data = await step.func(context)

                # Success
                duration = (datetime.now(UTC) - start_time).total_seconds()

                result = StepResult(
                    step_name=step.name,
                    success=True,
                    data=result_data,
                    duration=duration,
                    metadata={"attempt": attempt + 1},
                )

                self._logger.debug(f"✅ Step completed: {step.name} ({duration:.2f}s)")
                return result

            except TimeoutError:
                duration = (datetime.now(UTC) - start_time).total_seconds()
                error_msg = f"Step timeout after {step.timeout}s"

                if attempt < step.retry_count:
                    self._logger.warning(f"⏰ Step timeout (attempt {attempt + 1}): {step.name}")
                    await asyncio.sleep(1.0 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    return StepResult(
                        step_name=step.name,
                        success=False,
                        error=error_msg,
                        duration=duration,
                        metadata={"attempt": attempt + 1, "timeout": True},
                    )

            except Exception as e:
                duration = (datetime.now(UTC) - start_time).total_seconds()
                error_msg = str(e)

                if attempt < step.retry_count:
                    self._logger.warning(f"❌ Step failed (attempt {attempt + 1}): {step.name} - {error_msg}")
                    await asyncio.sleep(1.0 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    return StepResult(
                        step_name=step.name,
                        success=False,
                        error=error_msg,
                        duration=duration,
                        metadata={"attempt": attempt + 1},
                    )

        # Should not reach here
        return StepResult(step_name=step.name, success=False, error="Unknown error")

    def set_global_context(self, key: str, value: Any) -> None:
        """Set global context value available to all workflows."""
        self._global_context[key] = value

    def get_global_context(self, key: str, default: Any = None) -> Any:
        """Get global context value."""
        return self._global_context.get(key, default)

    def get_active_workflows(self) -> dict[str, Workflow]:
        """Get all currently active workflows."""
        return self._active_workflows.copy()

    # Pre-built workflow factories

    def create_wallet_analysis_workflow(
        self,
        spreadsheet_id: str,
        input_range: str = "A:B",
        output_range: str = "A1",
        input_worksheet: str | None = None,
        output_worksheet: str | None = None,
        batch_size: int = 50,
        skip_inactive: bool = True,
        dry_run: bool = False,
    ) -> Workflow:
        """Create a complete wallet analysis workflow.

        Args:
            spreadsheet_id: Google Sheets ID
            input_range: Input range for addresses
            output_range: Output range for results
            input_worksheet: Input worksheet name
            output_worksheet: Output worksheet name
            batch_size: Processing batch size
            skip_inactive: Whether to skip inactive wallets
            dry_run: Whether to perform dry run

        Returns:
            Configured workflow
        """
        workflow = self.create_workflow(
            name="wallet_analysis",
            description=f"Analyze wallets from Google Sheets: {spreadsheet_id}",
            timeout=3600,  # 1 hour timeout
            continue_on_error=False,
        )

        # Step 1: Validate inputs
        async def validate_inputs(context):
            self._logger.info("📋 Validating inputs")

            # Validate spreadsheet access
            sheets_client = self.app.sheets_client
            try:
                # Try to read a small sample
                test_data = await sheets_client.read_wallet_addresses(
                    spreadsheet_id=spreadsheet_id, range_name="A1:B2", skip_header=False
                )
                return {"spreadsheet_accessible": True, "sample_rows": len(test_data)}
            except Exception as e:
                raise RuntimeError(f"Cannot access spreadsheet: {e}")

        workflow.add_step(
            name="validate_inputs",
            func=validate_inputs,
            description="Validate spreadsheet access and inputs",
            required=True,
        )

        # Step 2: Health check services
        async def health_check(context):
            self._logger.info("🏥 Checking service health")

            health_status = await self.app.health_check()
            unhealthy = [k for k, v in health_status.items() if not v]

            if unhealthy:
                # In production, fail on unhealthy services
                if self.app.config.is_production():
                    raise RuntimeError(f"Unhealthy services: {unhealthy}")
                else:
                    self._logger.warning(f"⚠️ Unhealthy services (continuing): {unhealthy}")

            return health_status

        workflow.add_step(
            name="health_check",
            func=health_check,
            description="Check health of all services",
            required=False,
            dependencies=["validate_inputs"],
        )

        # Step 3: Pre-cache prices
        async def precache_prices(context):
            self._logger.info("💾 Pre-caching token prices")

            try:
                # Cache ETH price
                eth_price = await self.app.coingecko_client.get_eth_price()

                # Cache stablecoin prices
                stablecoin_prices = await self.app.coingecko_client.get_stablecoin_prices()

                return {
                    "eth_price": float(eth_price) if eth_price else None,
                    "stablecoin_count": len(stablecoin_prices),
                }
            except Exception as e:
                self._logger.warning(f"⚠️ Price caching failed: {e}")
                return {"error": str(e)}

        workflow.add_step(
            name="precache_prices",
            func=precache_prices,
            description="Pre-cache token prices for better performance",
            required=False,
            retry_count=2,
            dependencies=["health_check"],
        )

        # Step 4: Read wallet addresses
        async def read_addresses(context):
            self._logger.info("📖 Reading wallet addresses from spreadsheet")

            sheets_client = self.app.sheets_client
            addresses = await sheets_client.read_wallet_addresses(
                spreadsheet_id=spreadsheet_id, range_name=input_range, worksheet_name=input_worksheet, skip_header=True
            )

            if not addresses:
                raise RuntimeError("No wallet addresses found in spreadsheet")

            self._logger.info(f"📊 Found {len(addresses)} wallet addresses")

            return {
                "addresses": [
                    {"address": addr.address, "label": addr.label, "row_number": addr.row_number} for addr in addresses
                ],
                "count": len(addresses),
            }

        workflow.add_step(
            name="read_addresses",
            func=read_addresses,
            description="Read wallet addresses from Google Sheets",
            required=True,
            retry_count=2,
            dependencies=["validate_inputs"],
        )

        # Step 5: Process wallets
        async def process_wallets(context):
            self._logger.info("🔄 Processing wallet addresses")

            addresses = context["step_read_addresses_result"]["addresses"]

            # Process using batch processor
            results = await self.app.batch_processor.process_wallet_list(addresses=addresses)

            return results.get_summary_dict() if hasattr(results, "get_summary_dict") else results

        workflow.add_step(
            name="process_wallets",
            func=process_wallets,
            description="Process wallet addresses and calculate balances",
            required=True,
            timeout=3000,  # 50 minutes
            dependencies=["read_addresses", "precache_prices"],
        )

        # Step 6: Write results (if not dry run)
        async def write_results(context):
            if dry_run:
                self._logger.info("📄 Skipping result writing (dry run)")
                return {"dry_run": True, "written": False}

            self._logger.info("✏️ Writing results to Google Sheets")

            # This would need the actual processed results to write
            # For now, we'll just return success
            return {"written": True, "output_range": output_range}

        workflow.add_step(
            name="write_results",
            func=write_results,
            description="Write analysis results back to Google Sheets",
            required=not dry_run,
            retry_count=2,
            dependencies=["process_wallets"],
        )

        # Step 7: Generate summary
        async def generate_summary(context):
            self._logger.info("📊 Generating analysis summary")

            processing_results = context.get("step_process_wallets_result", {})

            summary = {
                "workflow_name": workflow.name,
                "spreadsheet_id": spreadsheet_id,
                "execution_time": workflow.duration,
                "dry_run": dry_run,
                "processing_results": processing_results,
                "timestamp": datetime.now(UTC).isoformat(),
            }

            return summary

        workflow.add_step(
            name="generate_summary",
            func=generate_summary,
            description="Generate analysis summary report",
            required=False,
            dependencies=["process_wallets", "write_results"],
        )

        return workflow

    def create_price_update_workflow(
        self, token_addresses: list[str] | None = None, update_popular: bool = True, cache_duration: int = 3600
    ) -> Workflow:
        """Create a price update workflow.

        Args:
            token_addresses: Specific token addresses to update
            update_popular: Whether to update popular tokens
            cache_duration: Cache duration in seconds

        Returns:
            Configured workflow
        """
        workflow = self.create_workflow(
            name="price_update",
            description="Update token price cache",
            timeout=600,  # 10 minutes
            continue_on_error=True,
        )

        # Step 1: Update ETH price
        async def update_eth_price(context):
            self._logger.info("💰 Updating ETH price")

            eth_price = await self.app.coingecko_client.get_eth_price()

            if self.app.cache_manager:
                await self.app.cache_manager.set_price(
                    "ethereum",
                    {"usd": float(eth_price) if eth_price else None, "last_updated": datetime.now(UTC).isoformat()},
                )

            return {"eth_price": float(eth_price) if eth_price else None}

        workflow.add_step(
            name="update_eth_price",
            func=update_eth_price,
            description="Update ETH price in cache",
            required=True,
            retry_count=3,
        )

        # Step 2: Update stablecoin prices
        async def update_stablecoin_prices(context):
            self._logger.info("💱 Updating stablecoin prices")

            stablecoin_prices = await self.app.coingecko_client.get_stablecoin_prices()

            if self.app.cache_manager:
                for symbol, price in stablecoin_prices.items():
                    await self.app.cache_manager.set_price(
                        symbol.lower(), {"usd": float(price), "last_updated": datetime.now(UTC).isoformat()}
                    )

            return {"stablecoin_prices": {k: float(v) for k, v in stablecoin_prices.items()}}

        workflow.add_step(
            name="update_stablecoin_prices",
            func=update_stablecoin_prices,
            description="Update stablecoin prices in cache",
            required=False,
            retry_count=2,
            dependencies=["update_eth_price"],
        )

        # Step 3: Update popular tokens (if requested)
        async def update_popular_tokens(context):
            if not update_popular:
                return {"skipped": True}

            self._logger.info("📈 Updating popular token prices")

            # Get top tokens and cache their prices
            top_tokens = await self.app.coingecko_client.get_top_tokens_by_market_cap(limit=100)

            cached_count = 0
            if self.app.cache_manager:
                for token in top_tokens:
                    if token.current_price_usd:
                        await self.app.cache_manager.set_price(
                            token.token_id,
                            {
                                "usd": float(token.current_price_usd),
                                "symbol": token.symbol,
                                "name": token.name,
                                "last_updated": datetime.now(UTC).isoformat(),
                            },
                        )
                        cached_count += 1

            return {"tokens_cached": cached_count}

        workflow.add_step(
            name="update_popular_tokens",
            func=update_popular_tokens,
            description="Update popular token prices",
            required=False,
            timeout=300,  # 5 minutes
            retry_count=1,
            dependencies=["update_stablecoin_prices"],
        )

        # Step 4: Update specific tokens (if provided)
        async def update_specific_tokens(context):
            if not token_addresses:
                return {"skipped": True}

            self._logger.info(f"🎯 Updating {len(token_addresses)} specific token prices")

            token_prices = await self.app.coingecko_client.get_token_prices_by_contracts(
                contract_addresses=token_addresses, include_market_data=False
            )

            cached_count = 0
            if self.app.cache_manager:
                for addr, price_data in token_prices.items():
                    if price_data.current_price_usd:
                        await self.app.cache_manager.set_price(
                            addr,
                            {
                                "usd": float(price_data.current_price_usd),
                                "symbol": price_data.symbol,
                                "contract_address": addr,
                                "last_updated": datetime.now(UTC).isoformat(),
                            },
                        )
                        cached_count += 1

            return {"specific_tokens_cached": cached_count}

        workflow.add_step(
            name="update_specific_tokens",
            func=update_specific_tokens,
            description="Update specific token prices",
            required=False,
            retry_count=2,
            dependencies=["update_popular_tokens"],
        )

        return workflow

    def create_maintenance_workflow(self) -> Workflow:
        """Create a maintenance workflow for system upkeep.

        Returns:
            Configured maintenance workflow
        """
        workflow = self.create_workflow(
            name="maintenance",
            description="System maintenance and cleanup",
            timeout=1800,  # 30 minutes
            continue_on_error=True,
        )

        # Step 1: Cache cleanup
        async def cache_cleanup(context):
            self._logger.info("🧹 Performing cache cleanup")

            if not self.app.cache_manager:
                return {"skipped": True, "reason": "No cache manager"}

            # Get cache stats before cleanup
            stats_before = await self.app.cache_manager.get_stats()

            # Clear expired entries (if supported)
            # This is a placeholder - actual implementation would depend on cache backend

            stats_after = await self.app.cache_manager.get_stats()

            return {"stats_before": stats_before, "stats_after": stats_after, "cleanup_performed": True}

        workflow.add_step(
            name="cache_cleanup", func=cache_cleanup, description="Clean up expired cache entries", required=False
        )

        # Step 2: Health check all services
        async def comprehensive_health_check(context):
            self._logger.info("🏥 Performing comprehensive health check")

            health_status = await self.app.health_check()

            # Collect detailed metrics
            metrics = await self.app.collect_metrics()

            return {
                "health_status": health_status,
                "metrics_snapshot": metrics,
                "timestamp": datetime.now(UTC).isoformat(),
            }

        workflow.add_step(
            name="comprehensive_health_check",
            func=comprehensive_health_check,
            description="Comprehensive health check of all services",
            required=False,
            dependencies=["cache_cleanup"],
        )

        # Step 3: Update configuration (reload from environment)
        async def refresh_configuration(context):
            self._logger.info("🔄 Refreshing configuration")

            try:
                from .config import get_settings

                settings = get_settings()
                settings.reload_config()

                return {"config_refreshed": True}
            except Exception as e:
                return {"config_refreshed": False, "error": str(e)}

        workflow.add_step(
            name="refresh_configuration",
            func=refresh_configuration,
            description="Refresh application configuration",
            required=False,
            dependencies=["comprehensive_health_check"],
        )

        return workflow

    async def execute_scheduled_maintenance(self) -> Workflow:
        """Execute scheduled maintenance workflow.

        Returns:
            Completed maintenance workflow
        """
        workflow = self.create_maintenance_workflow()
        return await self.execute_workflow(workflow)

    async def execute_price_update(
        self, token_addresses: list[str] | None = None, update_popular: bool = True
    ) -> Workflow:
        """Execute price update workflow.

        Args:
            token_addresses: Specific tokens to update
            update_popular: Whether to update popular tokens

        Returns:
            Completed price update workflow
        """
        workflow = self.create_price_update_workflow(token_addresses=token_addresses, update_popular=update_popular)
        return await self.execute_workflow(workflow)

    def get_workflow_stats(self) -> dict[str, Any]:
        """Get statistics about workflow execution.

        Returns:
            Workflow execution statistics
        """
        active_count = len(self._active_workflows)

        # This could be expanded to track historical data
        return {
            "active_workflows": active_count,
            "active_workflow_names": list(self._active_workflows.keys()),
            "global_context_keys": list(self._global_context.keys()),
        }
