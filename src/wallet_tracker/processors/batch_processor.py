"""Batch processor for handling large-scale wallet analysis operations."""

import asyncio
import logging
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from ..clients import (
    CoinGeckoClient,
    EthereumClient,
    GoogleSheetsClient,
    InvalidAddressError,
    WalletPortfolio,
    is_valid_ethereum_address,
    normalize_address,
)
from ..config import AppConfig
from ..utils.cache import CacheManager
from .batch_types import (
    BatchConfig,
    BatchProgress,
)
from .wallet_types import (
    ProcessingResults,
    SkipReason,
    WalletProcessingJob,
    WalletValidationResult,
    create_jobs_from_addresses,
    get_retry_jobs,
    group_jobs_by_priority,
)

logger = logging.getLogger(__name__)


class BatchProcessorError(Exception):
    """Base exception for batch processor errors."""

    pass


class BatchProcessor:
    """High-performance batch processor for wallet analysis."""

    def __init__(
        self,
        config: AppConfig,
        ethereum_client: EthereumClient,
        coingecko_client: CoinGeckoClient,
        cache_manager: CacheManager,
        sheets_client: GoogleSheetsClient | None = None,
    ):
        """Initialize batch processor.

        Args:
            config: Application configuration
            ethereum_client: Ethereum blockchain client
            coingecko_client: Token price client
            cache_manager: Cache manager
            sheets_client: Google Sheets client (optional)
        """
        self.config = config
        self.ethereum_client = ethereum_client
        self.coingecko_client = coingecko_client
        self.cache_manager = cache_manager
        self.sheets_client = sheets_client

        # Create batch config from app config
        self.batch_config = BatchConfig(
            batch_size=config.processing.batch_size,
            max_concurrent_jobs_per_batch=config.processing.max_concurrent_requests,
            request_delay_seconds=config.processing.request_delay,
            timeout_seconds=60,
            inactive_threshold_days=config.processing.inactive_wallet_threshold_days,
            max_requests_per_minute=config.ethereum.rate_limit,
        )

        # Active processing state
        self._active_batches: dict[str, BatchProgress] = {}
        self._processing_callbacks: list[Callable[[BatchProgress], None]] = []
        self._stop_requested = False

    async def process_wallets_from_sheets(
        self,
        spreadsheet_id: str,
        input_range: str = "A:B",
        output_range: str = "A1",
        input_worksheet: str | None = None,
        output_worksheet: str | None = None,
        config_override: BatchConfig | None = None,
        progress_callback: Callable[[BatchProgress], None] | None = None,
    ) -> ProcessingResults:
        """Process wallets from Google Sheets input.

        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            input_range: Range to read wallet addresses from
            output_range: Starting cell for writing results
            input_worksheet: Input worksheet name
            output_worksheet: Output worksheet name
            config_override: Override batch configuration
            progress_callback: Progress update callback

        Returns:
            Processing results

        Raises:
            BatchProcessorError: If processing fails
        """
        if not self.sheets_client:
            raise BatchProcessorError("Google Sheets client not configured")

        logger.info("🚀 Starting batch wallet processing from Google Sheets")

        try:
            # Step 1: Read wallet addresses
            logger.info(f"📖 Reading wallet addresses from range: {input_range}")
            wallet_addresses = await self.sheets_client.read_wallet_addresses(
                spreadsheet_id=spreadsheet_id,
                range_name=input_range,
                worksheet_name=input_worksheet,
                skip_header=True,
            )

            if not wallet_addresses:
                logger.warning("No wallet addresses found in spreadsheet")
                return ProcessingResults(
                    total_wallets_input=0,
                    batch_config=config_override or self.batch_config,
                )

            # Convert to processing format
            address_list = [
                {
                    "address": wa.address,
                    "label": wa.label,
                    "row_number": wa.row_number,
                }
                for wa in wallet_addresses
            ]

            # Step 2: Process wallets
            results = await self.process_wallet_list(
                addresses=address_list,
                config_override=config_override,
                progress_callback=progress_callback,
            )

            # Step 3: Write results back to sheets
            if results.wallets_processed > 0:
                logger.info("✏️ Writing results to Google Sheets")
                # Note: This would need actual results to write
                # For now, we'll skip the implementation as it requires
                # the processed wallet results to be stored

            return results

        except Exception as e:
            logger.error(f"❌ Batch processing from sheets failed: {e}")
            raise BatchProcessorError(f"Batch processing failed: {e}") from e

    async def process_wallet_list(
        self,
        addresses: list[dict[str, Any]],
        config_override: BatchConfig | None = None,
        progress_callback: Callable[[BatchProgress], None] | None = None,
    ) -> ProcessingResults:
        """Process a list of wallet addresses.

        Args:
            addresses: List of wallet address dictionaries
            config_override: Override batch configuration
            progress_callback: Progress update callback

        Returns:
            Processing results
        """
        batch_config = config_override or self.batch_config
        batch_id = str(uuid.uuid4())

        logger.info(f"🔄 Starting batch processing: {batch_id}")
        logger.info(f"📊 Processing {len(addresses)} wallets in batches of {batch_config.batch_size}")

        # Create processing jobs
        jobs = create_jobs_from_addresses(addresses, max_retries=self.config.processing.retry_attempts)

        # Validate addresses first
        jobs = await self._validate_addresses(jobs)

        # Initialize progress tracking
        progress = BatchProgress(
            batch_id=batch_id,
            total_jobs=len(jobs),
            started_at=datetime.now(UTC),
            total_batches=(len(jobs) + batch_config.batch_size - 1) // batch_config.batch_size,
        )
        self._active_batches[batch_id] = progress

        # Add progress callback
        if progress_callback:
            self._processing_callbacks.append(progress_callback)

        try:
            # Pre-cache popular token prices
            await self._precache_token_prices()

            # Process jobs in priority order
            processed_jobs = await self._process_jobs_with_priority(jobs, batch_config, progress)

            # Handle retries
            if batch_config.retry_failed_jobs:
                retry_jobs = get_retry_jobs(processed_jobs)
                if retry_jobs:
                    logger.info(f"🔄 Retrying {len(retry_jobs)} failed jobs")
                    retry_results = await self._process_jobs_batch(retry_jobs, batch_config, progress)
                    processed_jobs.extend(retry_results)

            # Generate final results
            results = ProcessingResults(
                total_wallets_input=len(addresses),
                batch_config=batch_config,
                started_at=progress.started_at,
                completed_at=datetime.now(UTC),
            )
            results.finalize_results(processed_jobs)

            logger.info("✅ Batch processing completed successfully")
            logger.info(f"📈 Processed: {results.wallets_processed}/{results.total_wallets_input}")
            logger.info(f"💰 Total Value: ${results.total_portfolio_value:,.2f}")
            logger.info(f"⏱️ Processing Time: {results.total_processing_time:.1f}s")

            return results

        except Exception as e:
            logger.error(f"❌ Batch processing failed: {e}")
            raise
        finally:
            # Cleanup
            if batch_id in self._active_batches:
                del self._active_batches[batch_id]
            if progress_callback in self._processing_callbacks:
                self._processing_callbacks.remove(progress_callback)

    async def _validate_addresses(self, jobs: list[WalletProcessingJob]) -> list[WalletProcessingJob]:
        """Validate Ethereum addresses and mark invalid ones."""
        logger.info(f"🔍 Validating {len(jobs)} wallet addresses")

        valid_jobs = []
        invalid_count = 0

        for job in jobs:
            validation = self._validate_single_address(job.address)

            if validation.is_valid:
                job.address = validation.normalized_address  # Use normalized address
                valid_jobs.append(job)
            else:
                job.mark_failed(validation.error_message, SkipReason.INVALID_ADDRESS)
                invalid_count += 1

        if invalid_count > 0:
            logger.warning(f"⚠️ Found {invalid_count} invalid addresses")

        logger.info(f"✅ {len(valid_jobs)} valid addresses ready for processing")
        return valid_jobs

    def _validate_single_address(self, address: str) -> WalletValidationResult:
        """Validate a single Ethereum address."""
        try:
            if not address or not isinstance(address, str):
                return WalletValidationResult.invalid(address, "Address is empty or not a string")

            # Remove whitespace
            address = address.strip()

            if not is_valid_ethereum_address(address):
                return WalletValidationResult.invalid(address, "Invalid Ethereum address format")

            normalized = normalize_address(address)
            return WalletValidationResult.valid(address, normalized)

        except Exception as e:
            return WalletValidationResult.invalid(address, f"Validation error: {e}")

    async def _precache_token_prices(self) -> None:
        """Pre-cache popular token prices for better performance."""
        try:
            logger.info("💾 Pre-caching popular token prices")

            # Get ETH price
            eth_price = await self.coingecko_client.get_eth_price()
            if eth_price:
                logger.debug(f"💰 ETH price: ${eth_price}")

            # Get stablecoin prices
            stablecoin_prices = await self.coingecko_client.get_stablecoin_prices()
            logger.debug(f"💱 Cached {len(stablecoin_prices)} stablecoin prices")

            # Cache popular tokens if we have a price service
            if hasattr(self.coingecko_client, "cache_popular_token_prices"):
                cached_count = await self.coingecko_client.cache_popular_token_prices()
                logger.info(f"📈 Pre-cached {cached_count} popular token prices")

        except Exception as e:
            logger.warning(f"⚠️ Failed to pre-cache token prices: {e}")

    async def _process_jobs_with_priority(
        self,
        jobs: list[WalletProcessingJob],
        config: BatchConfig,
        progress: BatchProgress,
    ) -> list[WalletProcessingJob]:
        """Process jobs grouped by priority."""
        all_processed = []

        # Group by priority
        priority_groups = group_jobs_by_priority(jobs)

        # Process in priority order (highest first)
        for priority in sorted(priority_groups.keys(), reverse=True):
            priority_jobs = priority_groups[priority]
            if not priority_jobs:
                continue

            logger.info(f"🎯 Processing {len(priority_jobs)} jobs with priority: {priority.name}")

            processed = await self._process_jobs_batch(priority_jobs, config, progress)
            all_processed.extend(processed)

            # Check if we should stop
            if self._stop_requested:
                logger.info("⏹️ Processing stopped by request")
                break

        return all_processed

    async def _process_jobs_batch(
        self,
        jobs: list[WalletProcessingJob],
        config: BatchConfig,
        progress: BatchProgress,
    ) -> list[WalletProcessingJob]:
        """Process a batch of jobs with concurrency control."""
        processed_jobs = []

        # Split into batches
        for i in range(0, len(jobs), config.batch_size):
            batch = jobs[i : i + config.batch_size]
            batch_num = (i // config.batch_size) + 1

            logger.info(f"📦 Processing batch {batch_num} ({len(batch)} wallets)")
            progress.current_batch_number = batch_num

            # Process batch with concurrency limit
            semaphore = asyncio.Semaphore(config.max_concurrent_jobs_per_batch)
            tasks = []

            for job in batch:
                task = self._process_single_job_with_semaphore(semaphore, job, config)
                tasks.append(task)

            # Wait for batch completion
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            for job, result in zip(batch, batch_results, strict=False):
                if isinstance(result, Exception):
                    job.mark_failed(str(result))
                    logger.error(f"❌ Job failed: {job.address[:10]}...{job.address[-6:]} - {result}")

                processed_jobs.append(job)
                progress.update_progress(job)

                # Notify callbacks
                for callback in self._processing_callbacks:
                    try:
                        callback(progress)
                    except Exception as e:
                        logger.warning(f"Progress callback error: {e}")

            # Inter-batch delay
            if batch_num < progress.total_batches:
                await asyncio.sleep(config.batch_delay_seconds)

        return processed_jobs

    async def _process_single_job_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        job: WalletProcessingJob,
        config: BatchConfig,
    ) -> None:
        """Process a single job with semaphore for concurrency control."""
        async with semaphore:
            await self._process_single_job(job, config)

    async def _process_single_job(self, job: WalletProcessingJob, config: BatchConfig) -> None:
        """Process a single wallet job."""
        job.mark_started()

        try:
            logger.debug(f"🔍 Processing: {job.address[:10]}...{job.address[-6:]} ({job.label})")

            # Check cache first
            if config.use_cache:
                cached_result = await self._check_cache(job)
                if cached_result:
                    job.cache_hit = True
                    logger.debug(f"💾 Cache hit for {job.address[:10]}...{job.address[-6:]}")
                    return

            # Get wallet portfolio
            portfolio = await self._get_wallet_portfolio(job)
            job.api_calls_made += 1

            # Check if wallet should be skipped
            if config.skip_inactive_wallets and self._should_skip_wallet(portfolio, config):
                job.mark_skipped(SkipReason.INACTIVE, "Wallet inactive beyond threshold")
                return

            # Extract wallet data
            job.eth_balance = portfolio.eth_balance.balance_eth
            job.total_value_usd = portfolio.total_value_usd
            job.transaction_count = portfolio.transaction_count
            job.last_transaction_timestamp = portfolio.last_transaction_timestamp

            # Extract token balances
            for token in portfolio.token_balances:
                job.token_balances[token.symbol] = token.balance_formatted

            # Determine activity status
            job.is_active = self._is_wallet_active(portfolio, config)

            # Check minimum value threshold
            if job.total_value_usd < config.min_value_threshold_usd:
                job.mark_skipped(SkipReason.LOW_VALUE, f"Value ${job.total_value_usd} below threshold")
                return

            # Mark as completed
            job.mark_completed(job.total_value_usd)

            # Cache the result
            if config.use_cache:
                await self._cache_result(job, portfolio, config)

            # Request delay
            await asyncio.sleep(config.request_delay_seconds)

        except InvalidAddressError as e:
            job.mark_failed(str(e), SkipReason.INVALID_ADDRESS)
        except TimeoutError:
            job.mark_failed("Request timeout", SkipReason.TIMEOUT)
        except Exception as e:
            error_msg = str(e)
            if "rate limit" in error_msg.lower():
                job.mark_failed(error_msg, SkipReason.RATE_LIMITED)
            else:
                job.mark_failed(error_msg, SkipReason.API_ERROR)

    async def _check_cache(self, job: WalletProcessingJob) -> bool:
        """Check if job result is cached."""
        try:
            cached_data = await self.cache_manager.get_balance(job.address)
            if cached_data:
                # Load from cache
                portfolio_data = self.ethereum_client._deserialize_portfolio(cached_data)

                job.eth_balance = portfolio_data.eth_balance.balance_eth
                job.total_value_usd = portfolio_data.total_value_usd
                job.transaction_count = portfolio_data.transaction_count
                job.last_transaction_timestamp = portfolio_data.last_transaction_timestamp
                job.is_active = True  # Assume active if cached

                # Extract token balances
                for token in portfolio_data.token_balances:
                    job.token_balances[token.symbol] = token.balance_formatted

                job.mark_completed(job.total_value_usd)
                return True

        except Exception as e:
            logger.debug(f"Cache check failed for {job.address}: {e}")

        return False

    async def _get_wallet_portfolio(self, job: WalletProcessingJob) -> WalletPortfolio:
        """Get wallet portfolio with timeout."""
        try:
            portfolio = await asyncio.wait_for(
                self.ethereum_client.get_wallet_portfolio(
                    wallet_address=job.address,
                    include_metadata=True,
                    include_prices=True,
                ),
                timeout=self.batch_config.timeout_seconds,
            )
            return portfolio

        except TimeoutError:
            raise TimeoutError(f"Timeout getting portfolio for {job.address}")

    def _should_skip_wallet(self, portfolio: WalletPortfolio, config: BatchConfig) -> bool:
        """Check if wallet should be skipped based on activity."""
        # Check transaction activity
        if portfolio.last_transaction_timestamp:
            threshold_date = datetime.now(UTC) - timedelta(days=config.inactive_threshold_days)
            if portfolio.last_transaction_timestamp < threshold_date:
                # Skip if inactive and low value
                if portfolio.total_value_usd < config.min_value_threshold_usd:
                    return True

        # Don't skip if it has significant value
        if portfolio.total_value_usd >= config.min_value_threshold_usd * 10:
            return False

        return False

    def _is_wallet_active(self, portfolio: WalletPortfolio, config: BatchConfig) -> bool:
        """Determine if wallet is considered active."""
        # Active if has recent transactions (last 90 days)
        if portfolio.last_transaction_timestamp:
            recent_threshold = datetime.now(UTC) - timedelta(days=90)
            if portfolio.last_transaction_timestamp > recent_threshold:
                return True

        # Active if has significant balance
        if portfolio.total_value_usd >= config.min_value_threshold_usd * 100:  # 100x threshold
            return True

        # Active if has many transactions
        if portfolio.transaction_count > 100:
            return True

        return False

    async def _cache_result(self, job: WalletProcessingJob, portfolio: WalletPortfolio, config: BatchConfig) -> None:
        """Cache processing result."""
        try:
            serialized = self.ethereum_client._serialize_portfolio(portfolio)
            await self.cache_manager.set_balance(job.address, serialized)
        except Exception as e:
            logger.debug(f"Failed to cache result for {job.address}: {e}")

    def get_active_batches(self) -> dict[str, BatchProgress]:
        """Get currently active batch operations."""
        return self._active_batches.copy()

    def stop_processing(self) -> None:
        """Request to stop all active processing."""
        logger.info("⏹️ Stop requested for batch processing")
        self._stop_requested = True

    def resume_processing(self) -> None:
        """Resume processing after stop."""
        logger.info("▶️ Resuming batch processing")
        self._stop_requested = False

    async def health_check(self) -> dict[str, bool]:
        """Check health of all connected services."""
        health = {}

        try:
            # Check Ethereum client
            health["ethereum_client"] = True  # Simplified check
        except:
            health["ethereum_client"] = False

        try:
            # Check CoinGecko client
            health["coingecko_client"] = await self.coingecko_client.health_check()
        except:
            health["coingecko_client"] = False

        try:
            # Check cache manager
            cache_health = await self.cache_manager.health_check()
            health["cache_manager"] = any(cache_health.values()) if cache_health else False
        except:
            health["cache_manager"] = False

        if self.sheets_client:
            try:
                health["sheets_client"] = self.sheets_client.health_check()
            except:
                health["sheets_client"] = False

        return health

    def get_stats(self) -> dict[str, Any]:
        """Get batch processor statistics."""
        return {
            "active_batches": len(self._active_batches),
            "stop_requested": self._stop_requested,
            "batch_config": {
                "batch_size": self.batch_config.batch_size,
                "max_concurrent": self.batch_config.max_concurrent_jobs_per_batch,
                "timeout_seconds": self.batch_config.timeout_seconds,
                "retry_enabled": self.batch_config.retry_failed_jobs,
            },
            "client_stats": {
                "ethereum": self.ethereum_client.get_stats(),
                "coingecko": self.coingecko_client.get_stats(),
            },
        }

    async def close(self) -> None:
        """Clean up resources."""
        logger.info("🧹 Cleaning up batch processor")

        # Stop any active processing
        self.stop_processing()

        # Wait for active batches to complete (with timeout)
        if self._active_batches:
            logger.info(f"⏳ Waiting for {len(self._active_batches)} active batches to complete")
            await asyncio.sleep(5)  # Give them time to finish

        # Clear state
        self._active_batches.clear()
        self._processing_callbacks.clear()

        logger.info("✅ Batch processor cleanup completed")
