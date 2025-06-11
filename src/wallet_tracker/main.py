"""Main entry point for the Crypto Wealth Tracker application."""

import asyncio
import logging
import signal
import sys
import os
from pathlib import Path
from typing import Optional

# Fix Windows console encoding for Unicode support
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())
    # Set console to UTF-8 if possible
    try:
        os.system("chcp 65001 >nul 2>&1")
    except:
        pass

from .config import get_config, SettingsError
from .clients import (
    CoinGeckoClient,
    EthereumClient,
    GoogleSheetsClient,
    CoinGeckoClientError,
    EthereumClientError,
    GoogleSheetsClientError,
)
from .processors import WalletProcessor, BatchProcessor
from .utils import CacheManager, CacheFactory
from .monitoring.health import HealthChecker
from .monitoring.metrics import MetricsCollector


class UnicodeFilter(logging.Filter):
    """Filter to replace Unicode characters in log messages for Windows compatibility."""

    def filter(self, record):
        if sys.platform == "win32":
            # Replace common emoji with text equivalents
            emoji_replacements = {
                "🚀": "[INIT]",
                "📋": "[LOG]",
                "💾": "[CACHE]",
                "🔌": "[API]",
                "✅": "[OK]",
                "⚙️": "[PROC]",
                "📊": "[MONITOR]",
                "🏥": "[HEALTH]",
                "❌": "[ERROR]",
                "⚠️": "[WARN]",
                "🎮": "[INTERACTIVE]",
                "🔄": "[PROCESSING]",
                "📖": "[READ]",
                "✏️": "[WRITE]",
                "💰": "[PRICE]",
                "🧹": "[CLEANUP]",
                "🔍": "[ANALYZE]",
                "📈": "[STATS]",
                "⏭️": "[SKIP]",
                "🎯": "[TARGET]",
                "💱": "[EXCHANGE]",
                "🪙": "[TOKEN]",
            }

            if hasattr(record, 'msg'):
                message = str(record.msg)
                for emoji, replacement in emoji_replacements.items():
                    message = message.replace(emoji, replacement)
                record.msg = message

        return True


class WalletTrackerApp:
    """Main application class for Crypto Wealth Tracker."""

    def __init__(self):
        """Initialize the application."""
        self.config = None
        self.cache_manager: Optional[CacheManager] = None
        self.ethereum_client: Optional[EthereumClient] = None
        self.coingecko_client: Optional[CoinGeckoClient] = None
        self.sheets_client: Optional[GoogleSheetsClient] = None
        self.wallet_processor: Optional[WalletProcessor] = None
        self.batch_processor: Optional[BatchProcessor] = None
        self.health_checker: Optional[HealthChecker] = None
        self.metrics_collector: Optional[MetricsCollector] = None

        self._shutdown_requested = False
        self._logger = logging.getLogger(__name__)

    async def initialize(self) -> None:
        """Initialize all application components."""
        try:
            self._logger.info("🚀 Starting Crypto Wealth Tracker initialization")

            # Step 1: Load and validate configuration
            await self._load_configuration()

            # Step 2: Setup logging
            self._setup_logging()

            # Step 3: Initialize caching system
            await self._initialize_cache()

            # Step 4: Initialize API clients
            await self._initialize_clients()

            # Step 5: Initialize processors
            await self._initialize_processors()

            # Step 6: Initialize monitoring
            await self._initialize_monitoring()

            # Step 7: Perform health checks
            await self._perform_initial_health_checks()

            self._logger.info("✅ Application initialized successfully")

        except Exception as e:
            self._logger.error(f"❌ Failed to initialize application: {e}")
            await self.cleanup()
            raise

    async def _load_configuration(self) -> None:
        """Load and validate application configuration."""
        try:
            from .config import get_settings
            settings = get_settings()
            self.config = settings.load_config()

            # Validate configuration
            validation_result = settings.validate_config()

            if not validation_result["valid"]:
                error_msg = "Configuration validation failed:\n" + "\n".join(validation_result["issues"])
                raise SettingsError(error_msg)

            if validation_result["warnings"]:
                for warning in validation_result["warnings"]:
                    self._logger.warning(f"⚠️ Configuration warning: {warning}")

            self._logger.info(f"📝 Configuration loaded for environment: {self.config.environment.value}")

        except Exception as e:
            raise SettingsError(f"Failed to load configuration: {e}") from e

    def _setup_logging(self) -> None:
        """Setup application logging."""
        log_config = self.config.logging

        # Create log directory if it doesn't exist
        if log_config.file:
            log_config.file.parent.mkdir(parents=True, exist_ok=True)

        # Configure logging
        handlers = []

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_config.level))

        # Add Unicode filter for Windows
        unicode_filter = UnicodeFilter()
        console_handler.addFilter(unicode_filter)

        console_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_format)
        handlers.append(console_handler)

        # File handler - keep Unicode for file logs
        if log_config.file:
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                log_config.file,
                maxBytes=log_config.max_size_mb * 1024 * 1024,
                backupCount=log_config.backup_count,
                encoding='utf-8'  # Explicitly set UTF-8 encoding for file
            )
            file_handler.setLevel(getattr(logging, log_config.level))
            file_format = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(file_format)
            handlers.append(file_handler)

        # Configure root logger
        logging.basicConfig(
            level=getattr(logging, log_config.level),
            handlers=handlers,
            force=True
        )

        # Reduce noise from external libraries
        logging.getLogger("aiohttp").setLevel(logging.WARNING)
        logging.getLogger("gspread").setLevel(logging.WARNING)

        self._logger = logging.getLogger(__name__)
        self._logger.info(f"📋 Logging configured: level={log_config.level}, file={log_config.file}")

    async def _initialize_cache(self) -> None:
        """Initialize caching system."""
        try:
            self._logger.info("💾 Initializing cache system")

            cache_factory = CacheFactory()
            cache_interface = cache_factory.create_cache(self.config.cache)

            self.cache_manager = CacheManager(self.config.cache)

            # Test cache connectivity
            health_status = await self.cache_manager.health_check()
            if not any(health_status.values()):
                self._logger.warning("⚠️ No cache backends are healthy, continuing without cache")
            else:
                healthy_backends = [k for k, v in health_status.items() if v]
                self._logger.info(f"✅ Cache initialized with backends: {', '.join(healthy_backends)}")

        except Exception as e:
            self._logger.error(f"❌ Cache initialization failed: {e}")
            # Continue without cache in development, fail in production
            if self.config.is_production():
                raise
            else:
                self._logger.warning("⚠️ Continuing without cache in development mode")
                self.cache_manager = None

    async def _initialize_clients(self) -> None:
        """Initialize API clients."""
        self._logger.info("🔌 Initializing API clients")

        try:
            # Initialize Ethereum client
            self.ethereum_client = EthereumClient(
                config=self.config.ethereum,
                cache_manager=self.cache_manager
            )

            # Initialize CoinGecko client
            self.coingecko_client = CoinGeckoClient(
                config=self.config.coingecko,
                cache_manager=self.cache_manager
            )

            # Initialize Google Sheets client
            self.sheets_client = GoogleSheetsClient(
                config=self.config.google_sheets,
                cache_manager=self.cache_manager
            )

            self._logger.info("✅ API clients initialized successfully")

        except Exception as e:
            self._logger.error(f"❌ Failed to initialize API clients: {e}")
            raise

    async def _initialize_processors(self) -> None:
        """Initialize processing components."""
        self._logger.info("⚙️ Initializing processors")

        try:
            # Initialize wallet processor
            self.wallet_processor = WalletProcessor(
                config=self.config,
                ethereum_client=self.ethereum_client,
                coingecko_client=self.coingecko_client,
                sheets_client=self.sheets_client,
                cache_manager=self.cache_manager
            )

            # Initialize batch processor
            self.batch_processor = BatchProcessor(
                config=self.config,
                ethereum_client=self.ethereum_client,
                coingecko_client=self.coingecko_client,
                cache_manager=self.cache_manager,
                sheets_client=self.sheets_client
            )

            self._logger.info("✅ Processors initialized successfully")

        except Exception as e:
            self._logger.error(f"❌ Failed to initialize processors: {e}")
            raise

    async def _initialize_monitoring(self) -> None:
        """Initialize monitoring and health checking."""
        try:
            self._logger.info("📊 Initializing monitoring")

            # Initialize health checker
            self.health_checker = HealthChecker(
                ethereum_client=self.ethereum_client,
                coingecko_client=self.coingecko_client,
                sheets_client=self.sheets_client,
                cache_manager=self.cache_manager
            )

            # Initialize metrics collector
            self.metrics_collector = MetricsCollector(
                config=self.config
            )

            self._logger.info("✅ Monitoring initialized successfully")

        except Exception as e:
            self._logger.warning(f"⚠️ Monitoring initialization failed: {e}")
            # Continue without monitoring
            self.health_checker = None
            self.metrics_collector = None

    async def _perform_initial_health_checks(self) -> None:
        """Perform initial health checks on all services."""
        if not self.health_checker:
            self._logger.warning("⚠️ Skipping health checks - monitoring not available")
            return

        self._logger.info("🏥 Performing initial health checks")

        try:
            health_status = await self.health_checker.check_all_services()

            # Log health status
            for service, is_healthy in health_status.items():
                status_text = "Healthy" if is_healthy else "Unhealthy"
                status_emoji = "✅" if is_healthy else "❌"
                self._logger.info(f"  {status_emoji} {service}: {status_text}")

            # Determine if we can continue
            critical_services = ["ethereum_client", "coingecko_client"]
            critical_health = [health_status.get(svc, False) for svc in critical_services]

            if not any(critical_health):
                raise RuntimeError("Critical services are unhealthy - cannot continue")

            if not all(critical_health):
                unhealthy = [svc for svc in critical_services if not health_status.get(svc, False)]
                self._logger.warning(f"⚠️ Some critical services are unhealthy: {unhealthy}")

        except Exception as e:
            self._logger.error(f"❌ Health checks failed: {e}")
            if self.config.is_production():
                raise
            else:
                self._logger.warning("⚠️ Continuing despite health check failures in development mode")

    async def process_wallets_from_sheets(
        self,
        spreadsheet_id: str,
        input_range: str = "A:B",
        output_range: str = "A1",
        input_worksheet: Optional[str] = None,
        output_worksheet: Optional[str] = None,
        dry_run: bool = False
    ) -> dict:
        """Process wallets from Google Sheets.

        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            input_range: Range to read wallet addresses from
            output_range: Starting cell for writing results
            input_worksheet: Input worksheet name
            output_worksheet: Output worksheet name
            dry_run: If True, don't write results back to sheets

        Returns:
            Processing results dictionary
        """
        if not self.wallet_processor:
            raise RuntimeError("Application not properly initialized")

        self._logger.info(f"📊 Starting wallet processing from Google Sheets")
        self._logger.info(f"   Spreadsheet: {spreadsheet_id}")
        self._logger.info(f"   Input range: {input_range}")
        self._logger.info(f"   Dry run: {dry_run}")

        try:
            # Use batch processor for better performance
            results = await self.batch_processor.process_wallets_from_sheets(
                spreadsheet_id=spreadsheet_id,
                input_range=input_range,
                output_range=output_range if not dry_run else None,
                input_worksheet=input_worksheet,
                output_worksheet=output_worksheet if not dry_run else None,
            )

            # Collect metrics
            if self.metrics_collector:
                await self.metrics_collector.record_processing_run(results)

            return results.get_summary_dict() if hasattr(results, 'get_summary_dict') else results

        except Exception as e:
            self._logger.error(f"❌ Wallet processing failed: {e}")
            raise

    async def process_wallet_list(
        self,
        addresses: list[dict],
        dry_run: bool = False
    ) -> dict:
        """Process a list of wallet addresses.

        Args:
            addresses: List of wallet address dictionaries
            dry_run: If True, only analyze without side effects

        Returns:
            Processing results dictionary
        """
        if not self.batch_processor:
            raise RuntimeError("Application not properly initialized")

        self._logger.info(f"🔄 Starting batch processing of {len(addresses)} wallets")

        try:
            results = await self.batch_processor.process_wallet_list(
                addresses=addresses
            )

            # Collect metrics
            if self.metrics_collector:
                await self.metrics_collector.record_processing_run(results)

            return results.get_summary_dict() if hasattr(results, 'get_summary_dict') else results

        except Exception as e:
            self._logger.error(f"❌ Batch processing failed: {e}")
            raise

    async def get_health_status(self) -> dict:
        """Get current health status of all services."""
        if not self.health_checker:
            return {"error": "Health checking not available"}

        try:
            return await self.health_checker.check_all_services()
        except Exception as e:
            self._logger.error(f"❌ Health check failed: {e}")
            return {"error": str(e)}

    async def get_metrics(self) -> dict:
        """Get current application metrics."""
        metrics = {}

        try:
            # Collect client stats
            if self.ethereum_client:
                metrics["ethereum_client"] = self.ethereum_client.get_stats()
            if self.coingecko_client:
                metrics["coingecko_client"] = self.coingecko_client.get_stats()
            if self.sheets_client:
                metrics["sheets_client"] = self.sheets_client.get_stats()

            # Collect cache stats
            if self.cache_manager:
                metrics["cache"] = await self.cache_manager.get_stats()

            # Collect processor stats
            if self.batch_processor:
                metrics["batch_processor"] = self.batch_processor.get_stats()

            # Collect application metrics
            if self.metrics_collector:
                metrics["application"] = await self.metrics_collector.get_current_metrics()

        except Exception as e:
            self._logger.error(f"❌ Failed to collect metrics: {e}")
            metrics["error"] = str(e)

        return metrics

    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self._logger.info(f"📡 Received signal {signum}, initiating graceful shutdown...")
            self._shutdown_requested = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        while not self._shutdown_requested:
            await asyncio.sleep(0.1)

    async def cleanup(self) -> None:
        """Clean up all resources."""
        self._logger.info("🧹 Cleaning up application resources")

        cleanup_tasks = []

        # Close processors
        if self.wallet_processor:
            cleanup_tasks.append(self.wallet_processor.close())
        if self.batch_processor:
            cleanup_tasks.append(self.batch_processor.close())

        # Close clients
        if self.ethereum_client:
            cleanup_tasks.append(self.ethereum_client.close())
        if self.coingecko_client:
            cleanup_tasks.append(self.coingecko_client.close())
        if self.sheets_client:
            cleanup_tasks.append(self.sheets_client.close())

        # Close cache
        if self.cache_manager:
            cleanup_tasks.append(self.cache_manager.close())

        # Execute all cleanup tasks
        if cleanup_tasks:
            try:
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            except Exception as e:
                self._logger.error(f"❌ Error during cleanup: {e}")

        self._logger.info("✅ Application cleanup completed")

    async def run_interactive_mode(self) -> None:
        """Run application in interactive mode."""
        self._logger.info("🎮 Starting interactive mode")

        print("\n" + "="*50)
        print("🏦 Crypto Wealth Tracker - Interactive Mode")
        print("="*50)

        while not self._shutdown_requested:
            try:
                print("\nAvailable commands:")
                print("1. Analyze wallets from Google Sheets")
                print("2. Check health status")
                print("3. View metrics")
                print("4. Test CoinGecko integration")
                print("5. Test Ethereum integration")
                print("6. Warm up application")
                print("7. Exit")

                choice = input("\nEnter your choice (1-7): ").strip()

                if choice == "1":
                    await self._interactive_analyze_sheets()
                elif choice == "2":
                    await self._interactive_health_check()
                elif choice == "3":
                    await self._interactive_metrics()
                elif choice == "4":
                    await self._interactive_test_coingecko()
                elif choice == "5":
                    await self._interactive_test_ethereum()
                elif choice == "6":
                    await self._interactive_warm_up()
                elif choice == "7":
                    print("👋 Goodbye!")
                    break
                else:
                    print("❌ Invalid choice. Please enter 1-7.")

            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")

    async def _interactive_analyze_sheets(self) -> None:
        """Interactive Google Sheets analysis."""
        try:
            spreadsheet_id = input("Enter Google Sheets ID: ").strip()
            if not spreadsheet_id:
                print("❌ Spreadsheet ID cannot be empty")
                return

            input_range = input("Enter input range (default: A:B): ").strip() or "A:B"
            output_range = input("Enter output range (default: A1): ").strip() or "A1"

            dry_run_input = input("Dry run? (y/N): ").strip().lower()
            dry_run = dry_run_input in ['y', 'yes']

            print(f"\n🚀 Starting analysis...")
            print(f"   Spreadsheet: {spreadsheet_id}")
            print(f"   Input range: {input_range}")
            print(f"   Output range: {output_range}")
            print(f"   Dry run: {dry_run}")

            results = await self.process_wallets_from_sheets(
                spreadsheet_id=spreadsheet_id,
                input_range=input_range,
                output_range=output_range,
                dry_run=dry_run
            )

            print("\n✅ Analysis completed!")
            print(f"   Wallets processed: {results.get('results', {}).get('processed', 0)}")
            print(f"   Total value: ${results.get('portfolio_values', {}).get('total_usd', 0):,.2f}")

        except Exception as e:
            print(f"❌ Analysis failed: {e}")

    async def _interactive_health_check(self) -> None:
        """Interactive health check."""
        print("\n🏥 Checking service health...")

        health_status = await self.get_health_status()

        if "error" in health_status:
            print(f"❌ Health check failed: {health_status['error']}")
            return

        print("\nService Health Status:")
        for service, is_healthy in health_status.items():
            status_text = "Healthy" if is_healthy else "Unhealthy"
            status_emoji = "✅" if is_healthy else "❌"
            print(f"  {status_emoji} {service}: {status_text}")

    async def _interactive_metrics(self) -> None:
        """Interactive metrics display."""
        print("\n📊 Collecting metrics...")

        metrics = await self.get_metrics()

        if "error" in metrics:
            print(f"❌ Failed to collect metrics: {metrics['error']}")
            return

        print("\nApplication Metrics:")
        for component, stats in metrics.items():
            if isinstance(stats, dict):
                print(f"\n{component.replace('_', ' ').title()}:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")

    async def _interactive_test_coingecko(self) -> None:
        """Interactive CoinGecko testing."""
        print("\n💰 Testing CoinGecko integration...")

        try:
            # Test ETH price
            eth_price = await self.coingecko_client.get_eth_price()
            if eth_price:
                print(f"✅ ETH Price: ${eth_price}")
            else:
                print("❌ Failed to get ETH price")

            # Test stablecoin prices
            stablecoin_prices = await self.coingecko_client.get_stablecoin_prices()
            if stablecoin_prices:
                print("✅ Stablecoin Prices:")
                for symbol, price in stablecoin_prices.items():
                    print(f"  💱 {symbol}: ${price}")
            else:
                print("❌ Failed to get stablecoin prices")

        except Exception as e:
            print(f"❌ CoinGecko test failed: {e}")

    async def _interactive_test_ethereum(self) -> None:
        """Interactive Ethereum testing."""
        print("\nTesting Ethereum integration...")

        try:
            # Test with a well-known address
            test_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"  # vitalik.eth
            print(f"Testing with address: {test_address}")

            portfolio = await self.ethereum_client.get_wallet_portfolio(
                wallet_address=test_address,
                include_metadata=True,
                include_prices=True
            )

            print(f"ETH Balance: {portfolio.eth_balance.balance_eth}")
            print(f"Token Count: {len(portfolio.token_balances)}")
            print(f"Total Value: ${portfolio.total_value_usd}")

        except Exception as e:
            print(f"Ethereum test failed: {e}")

    async def _interactive_warm_up(self) -> None:
        """Interactive application warm-up."""
        print("\nWarming up application...")

        try:
            # Pre-cache prices
            if self.coingecko_client:
                print("Caching token prices...")
                eth_price = await self.coingecko_client.get_eth_price()
                stablecoin_prices = await self.coingecko_client.get_stablecoin_prices()
                print(f"Cached ETH price and {len(stablecoin_prices)} stablecoin prices")

            print("Warm-up completed!")

        except Exception as e:
            print(f"Warm-up failed: {e}")


# Global application instance
_app_instance: Optional[WalletTrackerApp] = None


def get_app() -> WalletTrackerApp:
    """Get or create global application instance."""
    global _app_instance
    if _app_instance is None:
        _app_instance = WalletTrackerApp()
    return _app_instance


async def main() -> None:
    """Main application entry point."""
    app = get_app()

    try:
        # Setup signal handlers
        app.setup_signal_handlers()

        # Initialize application
        await app.initialize()

        # Check if we should run in interactive mode
        import sys
        if len(sys.argv) == 1 or "--interactive" in sys.argv:
            await app.run_interactive_mode()
        else:
            # CLI mode will be handled by cli.py
            print("Use --interactive for interactive mode or run with CLI commands")

    except KeyboardInterrupt:
        print("\nApplication interrupted by user")
    except Exception as e:
        logging.getLogger(__name__).error(f"Application failed: {e}")
        return 1
    finally:
        await app.cleanup()

    return 0


def run() -> None:
    """Run the application."""
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nApplication stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()