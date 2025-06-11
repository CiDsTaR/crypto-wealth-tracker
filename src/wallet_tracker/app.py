"""Application class for dependency injection and component lifecycle management."""

import logging
from contextlib import AsyncExitStack
from typing import Any

from .clients import (
    CoinGeckoClient,
    EthereumClient,
    GoogleSheetsClient,
)
from .config import AppConfig, get_config
from .monitoring.health import HealthChecker
from .monitoring.metrics import MetricsCollector
from .processors import BatchProcessor, WalletProcessor
from .utils.cache import CacheManager

logger = logging.getLogger(__name__)


class ApplicationError(Exception):
    """Base exception for application errors."""

    pass


class InitializationError(ApplicationError):
    """Application initialization error."""

    pass


class ComponentNotFoundError(ApplicationError):
    """Component not found in application container."""

    pass


class Application:
    """
    Application class that manages component lifecycle and dependency injection.

    This class provides a structured approach to:
    - Component initialization and cleanup
    - Dependency injection
    - Service discovery
    - Graceful shutdown
    """

    def __init__(self, config: AppConfig | None = None):
        """Initialize application with optional configuration override.

        Args:
            config: Configuration override (uses default if None)
        """
        self.config = config or get_config()
        self._components: dict[str, Any] = {}
        self._initialized = False
        self._initializing = False  # Track initialization state
        self._exit_stack = AsyncExitStack()
        self._logger = logging.getLogger(__name__)

    @property
    def is_initialized(self) -> bool:
        """Check if application is initialized."""
        return self._initialized

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()

    async def initialize(self) -> None:
        """Initialize all application components in correct order."""
        if self._initialized:
            self._logger.warning("Application already initialized")
            return

        if self._initializing:
            self._logger.warning("Application initialization already in progress")
            return

        self._logger.info("🚀 Initializing application components")
        self._initializing = True

        try:
            # Initialize components in dependency order
            await self._initialize_cache_manager()
            await self._initialize_clients()
            await self._initialize_processors()
            await self._initialize_monitoring()

            self._initialized = True
            self._initializing = False
            self._logger.info("✅ Application initialization completed")

        except Exception as e:
            self._logger.error(f"❌ Application initialization failed: {e}")
            self._initializing = False
            await self.shutdown()
            raise InitializationError(f"Failed to initialize application: {e}") from e

    async def _initialize_cache_manager(self) -> None:
        """Initialize cache manager."""
        self._logger.debug("Initializing cache manager")

        try:
            cache_manager = CacheManager(self.config.cache)

            # Test cache health
            health_status = await cache_manager.health_check()
            if health_status:
                healthy_backends = [k for k, v in health_status.items() if v]
                if healthy_backends:
                    self._logger.info(f"💾 Cache manager initialized with: {', '.join(healthy_backends)}")
                else:
                    self._logger.warning("⚠️ Cache manager initialized but no backends are healthy")

            self._components["cache_manager"] = cache_manager

            # Register for cleanup
            self._exit_stack.push_async_callback(cache_manager.close)

        except Exception as e:
            # In development, we can continue without cache
            if self.config.is_development():
                self._logger.warning(f"⚠️ Cache initialization failed, continuing without cache: {e}")
                self._components["cache_manager"] = None
            else:
                raise

    async def _initialize_clients(self) -> None:
        """Initialize API clients with proper dependency injection."""
        self._logger.debug("Initializing API clients")

        cache_manager = self._components.get("cache_manager")

        # Initialize CoinGecko client first (dependency for Ethereum client)
        coingecko_client = CoinGeckoClient(config=self.config.coingecko, cache_manager=cache_manager)
        self._components["coingecko_client"] = coingecko_client
        self._exit_stack.push_async_callback(coingecko_client.close)

        # Initialize Ethereum client with CoinGecko client injected
        ethereum_client = EthereumClient(
            config=self.config.ethereum,
            cache_manager=cache_manager,
            coingecko_client=coingecko_client,  # Inject CoinGecko client
        )
        self._components["ethereum_client"] = ethereum_client
        self._exit_stack.push_async_callback(ethereum_client.close)

        # Initialize Google Sheets client
        sheets_client = GoogleSheetsClient(config=self.config.google_sheets, cache_manager=cache_manager)
        self._components["sheets_client"] = sheets_client
        self._exit_stack.push_async_callback(sheets_client.close)

        self._logger.info("🔌 API clients initialized with proper dependency injection")

    async def _initialize_processors(self) -> None:
        """Initialize processing components."""
        self._logger.debug("Initializing processors")

        # Get dependencies - use _get_component_internal to avoid initialization check
        ethereum_client = self._get_component_internal("ethereum_client")
        coingecko_client = self._get_component_internal("coingecko_client")
        sheets_client = self._get_component_internal("sheets_client")
        cache_manager = self._get_component_internal("cache_manager")

        # Initialize wallet processor
        wallet_processor = WalletProcessor(
            config=self.config,
            ethereum_client=ethereum_client,
            coingecko_client=coingecko_client,
            sheets_client=sheets_client,
            cache_manager=cache_manager,
        )
        self._components["wallet_processor"] = wallet_processor
        self._exit_stack.push_async_callback(wallet_processor.close)

        # Initialize batch processor
        batch_processor = BatchProcessor(
            config=self.config,
            ethereum_client=ethereum_client,
            coingecko_client=coingecko_client,
            cache_manager=cache_manager,
            sheets_client=sheets_client,
        )
        self._components["batch_processor"] = batch_processor
        self._exit_stack.push_async_callback(batch_processor.close)

        self._logger.info("⚙️ Processors initialized")

    async def _initialize_monitoring(self) -> None:
        """Initialize monitoring components."""
        self._logger.debug("Initializing monitoring")

        try:
            # Initialize health checker - use internal getter to avoid initialization check
            health_checker = HealthChecker(
                ethereum_client=self._get_component_internal("ethereum_client"),
                coingecko_client=self._get_component_internal("coingecko_client"),
                sheets_client=self._get_component_internal("sheets_client"),
                cache_manager=self._get_component_internal("cache_manager"),
            )
            self._components["health_checker"] = health_checker

            # Initialize metrics collector
            metrics_collector = MetricsCollector(config=self.config)
            self._components["metrics_collector"] = metrics_collector

            self._logger.info("📊 Monitoring initialized")

        except Exception as e:
            self._logger.warning(f"⚠️ Monitoring initialization failed: {e}")
            # Continue without monitoring
            self._components["health_checker"] = None
            self._components["metrics_collector"] = None

    def _get_component_internal(self, name: str) -> Any:
        """Internal method to get component without initialization check.

        Used during initialization phase when _initialized is still False.

        Args:
            name: Component name

        Returns:
            Component instance

        Raises:
            ComponentNotFoundError: If component not found
        """
        if name not in self._components:
            available = list(self._components.keys())
            raise ComponentNotFoundError(f"Component '{name}' not found. Available: {available}")

        return self._components[name]

    def get_component(self, name: str) -> Any:
        """Get a component by name.

        Args:
            name: Component name

        Returns:
            Component instance

        Raises:
            ComponentNotFoundError: If component not found
            ApplicationError: If application not initialized
        """
        if not self._initialized and not self._initializing:
            raise ApplicationError("Application not initialized")

        return self._get_component_internal(name)

    def get_optional_component(self, name: str) -> Any | None:
        """Get a component by name, returning None if not found.

        Args:
            name: Component name

        Returns:
            Component instance or None
        """
        try:
            return self.get_component(name)
        except ComponentNotFoundError:
            return None

    def has_component(self, name: str) -> bool:
        """Check if component exists.

        Args:
            name: Component name

        Returns:
            True if component exists
        """
        return name in self._components

    def list_components(self) -> list[str]:
        """List all available component names.

        Returns:
            List of component names
        """
        return list(self._components.keys())

    # Convenience methods for common components

    @property
    def cache_manager(self) -> CacheManager | None:
        """Get cache manager component."""
        return self.get_optional_component("cache_manager")

    @property
    def ethereum_client(self) -> EthereumClient:
        """Get Ethereum client component."""
        return self.get_component("ethereum_client")

    @property
    def coingecko_client(self) -> CoinGeckoClient:
        """Get CoinGecko client component."""
        return self.get_component("coingecko_client")

    @property
    def sheets_client(self) -> GoogleSheetsClient:
        """Get Google Sheets client component."""
        return self.get_component("sheets_client")

    @property
    def wallet_processor(self) -> WalletProcessor:
        """Get wallet processor component."""
        return self.get_component("wallet_processor")

    @property
    def batch_processor(self) -> BatchProcessor:
        """Get batch processor component."""
        return self.get_component("batch_processor")

    @property
    def health_checker(self) -> HealthChecker | None:
        """Get health checker component."""
        return self.get_optional_component("health_checker")

    @property
    def metrics_collector(self) -> MetricsCollector | None:
        """Get metrics collector component."""
        return self.get_optional_component("metrics_collector")

    async def health_check(self) -> dict[str, bool]:
        """Perform health check on all services.

        Returns:
            Dictionary mapping service names to health status
        """
        if not self._initialized:
            raise ApplicationError("Application not initialized")

        health_checker = self.health_checker
        if not health_checker:
            return {"error": "Health checker not available"}

        try:
            return await health_checker.check_all_services()
        except Exception as e:
            self._logger.error(f"❌ Health check failed: {e}")
            return {"error": str(e)}

    async def collect_metrics(self) -> dict[str, Any]:
        """Collect metrics from all components.

        Returns:
            Dictionary with metrics from all components
        """
        if not self._initialized:
            raise ApplicationError("Application not initialized")

        metrics = {}

        try:
            # Collect client metrics
            if self.ethereum_client:
                metrics["ethereum_client"] = self.ethereum_client.get_stats()

            if self.coingecko_client:
                metrics["coingecko_client"] = self.coingecko_client.get_stats()

            if self.sheets_client:
                metrics["sheets_client"] = self.sheets_client.get_stats()

            # Collect cache metrics
            if self.cache_manager:
                metrics["cache"] = await self.cache_manager.get_stats()

            # Collect processor metrics
            if self.batch_processor:
                metrics["batch_processor"] = self.batch_processor.get_stats()

            # Collect application metrics
            if self.metrics_collector:
                app_metrics = await self.metrics_collector.get_current_metrics()
                metrics["application"] = app_metrics

        except Exception as e:
            self._logger.error(f"❌ Failed to collect metrics: {e}")
            metrics["error"] = str(e)

        return metrics

    async def process_wallets_from_sheets(
        self,
        spreadsheet_id: str,
        input_range: str = "A:B",
        output_range: str = "A1",
        input_worksheet: str | None = None,
        output_worksheet: str | None = None,
        dry_run: bool = False,
        **kwargs,
    ) -> Any:
        """Process wallets from Google Sheets.

        Args:
            spreadsheet_id: Google Sheets ID
            input_range: Input range for addresses
            output_range: Output range for results
            input_worksheet: Input worksheet name
            output_worksheet: Output worksheet name
            dry_run: Whether to perform dry run
            **kwargs: Additional arguments

        Returns:
            Processing results
        """
        if not self._initialized:
            raise ApplicationError("Application not initialized")

        processor = self.batch_processor

        return await processor.process_wallets_from_sheets(
            spreadsheet_id=spreadsheet_id,
            input_range=input_range,
            output_range=output_range if not dry_run else None,
            input_worksheet=input_worksheet,
            output_worksheet=output_worksheet if not dry_run else None,
            **kwargs,
        )

    async def process_wallet_list(self, addresses: list[dict], **kwargs) -> Any:
        """Process a list of wallet addresses.

        Args:
            addresses: List of wallet address dictionaries
            **kwargs: Additional arguments

        Returns:
            Processing results
        """
        if not self._initialized:
            raise ApplicationError("Application not initialized")

        processor = self.batch_processor

        return await processor.process_wallet_list(addresses=addresses, **kwargs)

    async def shutdown(self) -> None:
        """Shutdown application and cleanup all resources."""
        if not self._initialized and not self._initializing:
            return

        self._logger.info("🛑 Shutting down application")

        try:
            # Close all components in reverse order
            await self._exit_stack.aclose()

            # Clear components
            self._components.clear()

            self._initialized = False
            self._initializing = False
            self._logger.info("✅ Application shutdown completed")

        except Exception as e:
            self._logger.error(f"❌ Error during shutdown: {e}")
            self._initialized = False
            self._initializing = False
            raise

    def get_component_status(self) -> dict[str, dict[str, Any]]:
        """Get status information for all components.

        Returns:
            Dictionary with component status information
        """
        status = {}

        for name, component in self._components.items():
            try:
                component_info = {
                    "initialized": component is not None,
                    "type": type(component).__name__ if component else None,
                }

                # Add component-specific status if available
                if hasattr(component, "get_stats"):
                    try:
                        component_info["stats"] = component.get_stats()
                    except Exception:
                        component_info["stats"] = "unavailable"

                status[name] = component_info

            except Exception as e:
                status[name] = {"initialized": False, "error": str(e)}

        return status

    async def warm_up(self) -> None:
        """Warm up application by performing initial operations.

        This method can be used to:
        - Pre-cache common data
        - Validate connections
        - Initialize expensive resources
        """
        if not self._initialized:
            raise ApplicationError("Application not initialized")

        self._logger.info("🔥 Warming up application")

        try:
            # Perform health checks
            health_status = await self.health_check()
            unhealthy_services = [k for k, v in health_status.items() if not v]

            if unhealthy_services:
                self._logger.warning(f"⚠️ Some services are unhealthy: {unhealthy_services}")

            # Pre-cache popular token prices if CoinGecko client is available
            if self.coingecko_client:
                try:
                    # Cache ETH price
                    eth_price = await self.coingecko_client.get_eth_price()
                    if eth_price:
                        self._logger.debug(f"💰 ETH price cached: ${eth_price}")

                    # Cache stablecoin prices
                    stablecoin_prices = await self.coingecko_client.get_stablecoin_prices()
                    if stablecoin_prices:
                        self._logger.debug(f"💱 Cached {len(stablecoin_prices)} stablecoin prices")

                except Exception as e:
                    self._logger.warning(f"⚠️ Failed to pre-cache prices: {e}")

            self._logger.info("✅ Application warm-up completed")

        except Exception as e:
            self._logger.warning(f"⚠️ Application warm-up failed: {e}")
            # Don't fail on warm-up errors


# Factory function for easier application creation
def create_application(config: AppConfig | None = None) -> Application:
    """Create and return a new Application instance.

    Args:
        config: Optional configuration override

    Returns:
        New Application instance
    """
    return Application(config=config)


# Context manager for temporary application instances
class TemporaryApplication:
    """Context manager for temporary application instances."""

    def __init__(self, config: AppConfig | None = None):
        """Initialize temporary application.

        Args:
            config: Optional configuration override
        """
        self._config = config
        self._app: Application | None = None

    async def __aenter__(self) -> Application:
        """Create and initialize temporary application."""
        self._app = create_application(self._config)
        await self._app.initialize()
        return self._app

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Shutdown temporary application."""
        if self._app:
            await self._app.shutdown()
            self._app = None


# Utility function for one-off operations
async def with_application(func, config: AppConfig | None = None, warm_up: bool = False):
    """Execute a function with a temporary application instance.

    Args:
        func: Async function to execute (receives app as first argument)
        config: Optional configuration override
        warm_up: Whether to warm up the application

    Returns:
        Function result
    """
    async with TemporaryApplication(config) as app:
        if warm_up:
            await app.warm_up()

        return await func(app)
