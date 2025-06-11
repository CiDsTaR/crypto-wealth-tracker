"""Wallet processor for analyzing Ethereum wallets and calculating on-chain wealth."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from ..clients import (
    CoinGeckoClient,
    EthereumClient,
    GoogleSheetsClient,
    WalletPortfolio,
    create_wallet_result_from_portfolio,
)
from ..config import AppConfig
from ..utils import CacheManager

logger = logging.getLogger(__name__)


class WalletProcessorError(Exception):
    """Base exception for wallet processor errors."""

    pass


class WalletProcessor:
    """Core wallet analysis processor."""

    def __init__(
        self,
        config: AppConfig,
        ethereum_client: EthereumClient,
        coingecko_client: CoinGeckoClient,
        sheets_client: GoogleSheetsClient,
        cache_manager: CacheManager,
    ):
        """Initialize wallet processor.

        Args:
            config: Application configuration
            ethereum_client: Ethereum blockchain client
            coingecko_client: CoinGecko price client
            sheets_client: Google Sheets client
            cache_manager: Cache manager
        """
        self.config = config
        self.ethereum_client = ethereum_client
        self.coingecko_client = coingecko_client
        self.sheets_client = sheets_client
        self.cache_manager = cache_manager

        # Processing statistics
        self._stats = {
            "wallets_processed": 0,
            "wallets_skipped": 0,
            "wallets_failed": 0,
            "active_wallets": 0,
            "inactive_wallets": 0,
            "total_value_usd": Decimal("0"),
            "processing_start_time": None,
            "processing_end_time": None,
        }

    async def process_wallets_from_sheets(
        self,
        spreadsheet_id: str,
        input_range: str = "A:B",
        output_range: str = "A1",
        input_worksheet: str | None = None,
        output_worksheet: str | None = None,
        skip_header: bool = True,
        include_summary: bool = True,
    ) -> dict[str, Any]:
        """Process wallets from Google Sheets input.

        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            input_range: Range to read wallet addresses from
            output_range: Starting cell for writing results
            input_worksheet: Input worksheet name (None for first sheet)
            output_worksheet: Output worksheet name (None for input sheet)
            skip_header: Whether to skip header row in input
            include_summary: Whether to create summary sheet

        Returns:
            Dictionary with processing results and statistics

        Raises:
            WalletProcessorError: If processing fails
        """
        logger.info("🚀 Starting wallet processing from Google Sheets")
        self._stats["processing_start_time"] = datetime.now(UTC)

        try:
            # Step 1: Read wallet addresses from Google Sheets
            logger.info(f"📖 Reading wallet addresses from range: {input_range}")
            wallet_addresses = await self.sheets_client.read_wallet_addresses(
                spreadsheet_id=spreadsheet_id,
                range_name=input_range,
                worksheet_name=input_worksheet,
                skip_header=skip_header,
            )

            if not wallet_addresses:
                logger.warning("No wallet addresses found in spreadsheet")
                return {
                    "success": True,
                    "message": "No wallet addresses to process",
                    "stats": self._stats,
                    "results": [],
                }

            logger.info(f"📊 Found {len(wallet_addresses)} wallet addresses to process")

            # Step 2: Process wallets
            wallet_results = await self.process_wallet_list(wallet_addresses)

            # Step 3: Write results to Google Sheets
            if wallet_results:
                logger.info(f"✏️ Writing {len(wallet_results)} results to Google Sheets")

                # Convert WalletResult objects to dictionaries for sheets client
                results_for_sheets = []
                for result in wallet_results:
                    result_dict = {
                        "address": result.address,
                        "label": result.label,
                        "eth_balance": result.eth_balance,
                        "eth_value_usd": result.eth_value_usd,
                        "usdc_balance": result.usdc_balance,
                        "usdt_balance": result.usdt_balance,
                        "dai_balance": result.dai_balance,
                        "aave_balance": result.aave_balance,
                        "uni_balance": result.uni_balance,
                        "link_balance": result.link_balance,
                        "other_tokens_value_usd": result.other_tokens_value_usd,
                        "total_value_usd": result.total_value_usd,
                        "last_updated": result.last_updated,
                        "transaction_count": result.transaction_count,
                        "is_active": result.is_active,
                    }
                    results_for_sheets.append(result_dict)

                success = self.sheets_client.write_wallet_results(
                    spreadsheet_id=spreadsheet_id,
                    wallet_results=results_for_sheets,
                    range_start=output_range,
                    worksheet_name=output_worksheet,
                    include_header=True,
                    clear_existing=True,
                )

                if not success:
                    raise WalletProcessorError("Failed to write results to Google Sheets")

                # Step 4: Create summary sheet if requested
                if include_summary:
                    logger.info("📊 Creating summary sheet")
                    from ..clients.google_sheets_types import create_summary_from_results

                    processing_time = self._get_processing_time()
                    summary = create_summary_from_results(wallet_results, processing_time)

                    summary_success = self.sheets_client.create_summary_sheet(
                        spreadsheet_id=spreadsheet_id,
                        summary_data=summary.__dict__,
                        worksheet_name="Analysis_Summary",
                    )

                    if not summary_success:
                        logger.warning("Failed to create summary sheet")

            # Finalize processing
            self._stats["processing_end_time"] = datetime.now(UTC)

            logger.info("✅ Wallet processing completed successfully")
            logger.info(f"📈 Processed: {self._stats['wallets_processed']} wallets")
            logger.info(f"🎯 Active: {self._stats['active_wallets']} wallets")
            logger.info(f"💰 Total Value: ${self._stats['total_value_usd']:,.2f}")
            logger.info(f"⏱️ Processing Time: {self._get_processing_time()}")

            return {
                "success": True,
                "message": f"Successfully processed {len(wallet_results)} wallets",
                "stats": self._stats,
                "results": wallet_results,
                "processing_time": self._get_processing_time(),
            }

        except Exception as e:
            self._stats["processing_end_time"] = datetime.now(UTC)
            logger.error(f"❌ Wallet processing failed: {e}")
            raise WalletProcessorError(f"Wallet processing failed: {e}") from e

    async def process_wallet_list(
        self, wallet_addresses: list[dict[str, str]]
    ) -> list[Any]:  # Should be list[WalletResult] but importing would cause circular import
        """Process a list of wallet addresses.

        Args:
            wallet_addresses: List of wallet address dictionaries

        Returns:
            List of WalletResult objects
        """
        logger.info(f"🔄 Processing {len(wallet_addresses)} wallets")

        results = []
        batch_size = self.config.processing.batch_size

        # Process wallets in batches
        for i in range(0, len(wallet_addresses), batch_size):
            batch = wallet_addresses[i : i + batch_size]
            batch_number = (i // batch_size) + 1
            total_batches = (len(wallet_addresses) + batch_size - 1) // batch_size

            logger.info(f"📦 Processing batch {batch_number}/{total_batches} ({len(batch)} wallets)")

            batch_results = await self._process_wallet_batch(batch)
            results.extend(batch_results)

            # Progress logging
            progress = (i + len(batch)) / len(wallet_addresses) * 100
            logger.info(f"📊 Progress: {progress:.1f}% ({len(results)} processed)")

        return results

    async def _process_wallet_batch(
        self, wallet_batch: list[dict[str, str]]
    ) -> list[Any]:  # Should be list[WalletResult]
        """Process a batch of wallets concurrently.

        Args:
            wallet_batch: Batch of wallet address dictionaries

        Returns:
            List of WalletResult objects
        """
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.config.processing.max_concurrent_requests)

        # Process wallets concurrently within the batch
        tasks = []
        for wallet_data in wallet_batch:
            task = self._process_single_wallet_with_semaphore(semaphore, wallet_data)
            tasks.append(task)

        # Wait for all tasks to complete
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and None results
        valid_results = []
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"Wallet processing error: {result}")
                self._stats["wallets_failed"] += 1
            elif result is not None:
                valid_results.append(result)

        return valid_results

    async def _process_single_wallet_with_semaphore(
        self, semaphore: asyncio.Semaphore, wallet_data: dict[str, str]
    ) -> Any | None:  # Should be WalletResult | None
        """Process a single wallet with semaphore for concurrency control."""
        async with semaphore:
            return await self._process_single_wallet(wallet_data)

    async def _process_single_wallet(self, wallet_data: dict[str, str]) -> Any | None:  # Should be WalletResult | None
        """Process a single wallet address.

        Args:
            wallet_data: Dictionary with 'address' and 'label' keys

        Returns:
            WalletResult object or None if skipped/failed
        """
        address = wallet_data["address"]
        label = wallet_data["label"]

        try:
            logger.debug(f"🔍 Processing wallet: {address[:10]}...{address[-6:]} ({label})")

            # Step 1: Get wallet portfolio from Ethereum client
            portfolio = await self.ethereum_client.get_wallet_portfolio(
                wallet_address=address,
                include_metadata=True,
                include_prices=True,
            )

            # Step 2: Check if wallet should be skipped (inactive)
            if self._should_skip_wallet(portfolio):
                logger.debug(f"⏭️ Skipping inactive wallet: {address[:10]}...{address[-6:]}")
                self._stats["wallets_skipped"] += 1
                self._stats["inactive_wallets"] += 1
                return None

            # Step 3: Determine if wallet is active
            is_active = self._is_wallet_active(portfolio)
            if is_active:
                self._stats["active_wallets"] += 1
            else:
                self._stats["inactive_wallets"] += 1

            # Step 4: Convert to WalletResult
            result = create_wallet_result_from_portfolio(
                address=address,
                label=label,
                portfolio=portfolio,
                is_active=is_active,
            )

            # Step 5: Update statistics
            self._stats["wallets_processed"] += 1
            self._stats["total_value_usd"] += result.total_value_usd

            logger.debug(f"✅ Processed wallet: {address[:10]}...{address[-6:]} (${result.total_value_usd:,.2f})")

            # Add delay to respect rate limits
            await asyncio.sleep(self.config.processing.request_delay)

            return result

        except Exception as e:
            logger.error(f"❌ Failed to process wallet {address}: {e}")
            self._stats["wallets_failed"] += 1
            return None

    def _should_skip_wallet(self, portfolio: WalletPortfolio) -> bool:
        """Determine if a wallet should be skipped based on activity."""
        # Check if wallet has any activity in the threshold period
        threshold_days = self.config.processing.inactive_wallet_threshold_days
        threshold_date = datetime.now(UTC) - timedelta(days=threshold_days)

        # If we have transaction data, check last transaction
        if portfolio.last_transaction_timestamp:
            return portfolio.last_transaction_timestamp < threshold_date

        # If no transaction data but wallet has significant value, don't skip
        if portfolio.total_value_usd > Decimal("100"):  # $100 threshold
            return False

        # If wallet has very low value and no recent transaction data, skip
        if portfolio.total_value_usd < Decimal("1"):  # $1 threshold
            return True

        return False

    def _is_wallet_active(self, portfolio: WalletPortfolio) -> bool:
        """Determine if a wallet is considered active."""
        # Active if has recent transactions
        if portfolio.last_transaction_timestamp:
            threshold_date = datetime.now(UTC) - timedelta(days=90)  # 3 months
            if portfolio.last_transaction_timestamp > threshold_date:
                return True

        # Active if has significant balance
        if portfolio.total_value_usd > Decimal("1000"):  # $1000 threshold
            return True

        # Active if has many transactions
        if portfolio.transaction_count > 50:
            return True

        return False

    def _get_processing_time(self) -> str:
        """Get human-readable processing time."""
        if not self._stats["processing_start_time"]:
            return "Unknown"

        end_time = self._stats["processing_end_time"] or datetime.now(UTC)
        duration = end_time - self._stats["processing_start_time"]

        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"

    def get_stats(self) -> dict[str, Any]:
        """Get processing statistics."""
        stats = self._stats.copy()

        # Add derived statistics
        total_processed = stats["wallets_processed"] + stats["wallets_skipped"]
        if total_processed > 0:
            stats["success_rate"] = (stats["wallets_processed"] / total_processed) * 100
            stats["skip_rate"] = (stats["wallets_skipped"] / total_processed) * 100
            stats["failure_rate"] = (stats["wallets_failed"] / total_processed) * 100

        stats["processing_time"] = self._get_processing_time()

        return stats

    async def health_check(self) -> dict[str, bool]:
        """Check health of all connected services."""
        health_status = {}

        # Check Ethereum client
        try:
            # Test with a simple call
            health_status["ethereum_client"] = True  # Simplified for now
        except Exception:
            health_status["ethereum_client"] = False

        # Check CoinGecko client
        try:
            health_status["coingecko_client"] = await self.coingecko_client.health_check()
        except Exception:
            health_status["coingecko_client"] = False

        # Check Google Sheets client
        try:
            health_status["sheets_client"] = self.sheets_client.health_check()
        except Exception:
            health_status["sheets_client"] = False

        # Check cache manager
        try:
            cache_health = await self.cache_manager.health_check()
            health_status["cache_manager"] = any(cache_health.values()) if cache_health else False
        except Exception:
            health_status["cache_manager"] = False

        return health_status

    async def close(self) -> None:
        """Clean up resources."""
        logger.info("🧹 Cleaning up wallet processor resources")

        # Close clients
        if hasattr(self.ethereum_client, "close"):
            logger.info("... closing ethereum client")
            await self.ethereum_client.close()

        if hasattr(self.coingecko_client, "close"):
            logger.info("... closing coingecko client")
            await self.coingecko_client.close()

        # Close cache manager
        if hasattr(self.cache_manager, "close"):
            logger.info("... closing cache_manager client")
            await self.cache_manager.close()

        logger.info("✅ Wallet processor cleanup completed")
