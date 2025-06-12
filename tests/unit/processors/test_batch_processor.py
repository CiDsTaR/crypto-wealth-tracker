"""Tests for batch processor."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from wallet_tracker.clients import (
    CoinGeckoClient,
    EthBalance,
    EthereumClient,
    GoogleSheetsClient,
    InvalidAddressError,
    TokenBalance,
    WalletPortfolio,
)
from wallet_tracker.config import AppConfig, CoinGeckoConfig, EthereumConfig, GoogleSheetsConfig
from wallet_tracker.processors import BatchProcessor, BatchProcessorError
from wallet_tracker.processors.batch_types import BatchConfig
from wallet_tracker.processors.wallet_types import (
    ProcessingResults,
    SkipReason,
    WalletProcessingJob,
    WalletStatus,
)
from wallet_tracker.utils.cache import CacheManager


class TestBatchProcessor:
    """Test BatchProcessor class."""

    @pytest.fixture
    def app_config(self, tmp_path) -> AppConfig:
        """Create application configuration for testing."""
        # Create dummy credentials file
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text('{"type": "service_account"}')

        return AppConfig(
            ethereum=EthereumConfig(alchemy_api_key="test_key", rpc_url="https://test.com", rate_limit=100),
            coingecko=CoinGeckoConfig(),
            google_sheets=GoogleSheetsConfig(credentials_file=creds_file),
            processing=MagicMock(
                batch_size=10,
                max_concurrent_requests=5,
                request_delay=0.01,
                retry_attempts=2,
                inactive_wallet_threshold_days=365,
            ),
        )

    @pytest.fixture
    def mock_cache_manager(self) -> AsyncMock:
        """Create mock cache manager."""
        cache_manager = AsyncMock(spec=CacheManager)
        cache_manager.get_balance = AsyncMock(return_value=None)
        cache_manager.set_balance = AsyncMock(return_value=True)
        cache_manager.health_check = AsyncMock(return_value={"cache": True})
        cache_manager.get_stats = AsyncMock(return_value={"hits": 0, "misses": 0})
        return cache_manager

    @pytest.fixture
    def mock_ethereum_client(self) -> AsyncMock:
        """Create mock Ethereum client."""
        client = AsyncMock(spec=EthereumClient)

        # Mock portfolio response
        portfolio = WalletPortfolio(
            address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            eth_balance=EthBalance(
                balance_wei="1000000000000000000",
                balance_eth=Decimal("1.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("2000.0"),
            ),
            token_balances=[
                TokenBalance(
                    contract_address="0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0",
                    symbol="USDC",
                    name="USD Coin",
                    decimals=6,
                    balance_raw="1000000",
                    balance_formatted=Decimal("1.0"),
                    price_usd=Decimal("1.0"),
                    value_usd=Decimal("1.0"),
                    is_verified=True,
                )
            ],
            total_value_usd=Decimal("2001.0"),
            last_updated=datetime.now(UTC),
            transaction_count=100,
        )

        client.get_wallet_portfolio = AsyncMock(return_value=portfolio)
        client._serialize_portfolio = MagicMock(return_value={"test": "data"})
        client._deserialize_portfolio = MagicMock(return_value=portfolio)
        client.get_stats = MagicMock(return_value={"requests": 0})
        client.close = AsyncMock()

        return client

    @pytest.fixture
    def mock_coingecko_client(self) -> AsyncMock:
        """Create mock CoinGecko client."""
        client = AsyncMock(spec=CoinGeckoClient)
        client.get_eth_price = AsyncMock(return_value=Decimal("2000.0"))
        client.get_stablecoin_prices = AsyncMock(return_value={"USDC": Decimal("1.0")})
        client.health_check = AsyncMock(return_value=True)
        client.get_stats = MagicMock(return_value={"requests": 0})
        client.close = AsyncMock()
        return client

    @pytest.fixture
    def mock_sheets_client(self) -> AsyncMock:
        """Create mock Google Sheets client."""
        client = AsyncMock(spec=GoogleSheetsClient)

        # Mock wallet addresses
        from wallet_tracker.clients.google_sheets_types import WalletAddress

        wallet_addresses = [
            WalletAddress(address="0x123", label="Wallet 1", row_number=2),
            WalletAddress(address="0x456", label="Wallet 2", row_number=3),
        ]
        client.read_wallet_addresses = AsyncMock(return_value=wallet_addresses)
        client.health_check = MagicMock(return_value=True)
        client.get_stats = MagicMock(return_value={"reads": 0})
        client.close = AsyncMock()

        return client

    @pytest.fixture
    def batch_processor(
        self, app_config, mock_ethereum_client, mock_coingecko_client, mock_cache_manager, mock_sheets_client
    ) -> BatchProcessor:
        """Create batch processor for testing."""
        return BatchProcessor(
            config=app_config,
            ethereum_client=mock_ethereum_client,
            coingecko_client=mock_coingecko_client,
            cache_manager=mock_cache_manager,
            sheets_client=mock_sheets_client,
        )

    @pytest.mark.asyncio
    async def test_processor_initialization(self, batch_processor):
        """Test batch processor initialization."""
        assert batch_processor.config is not None
        assert batch_processor.ethereum_client is not None
        assert batch_processor.coingecko_client is not None
        assert batch_processor.cache_manager is not None
        assert batch_processor.sheets_client is not None
        assert batch_processor.batch_config is not None
        assert batch_processor._active_batches == {}
        assert batch_processor._stop_requested is False

    @pytest.mark.asyncio
    async def test_process_wallets_from_sheets_success(self, batch_processor, mock_sheets_client):
        """Test successful processing from Google Sheets."""
        spreadsheet_id = "test_sheet_id"

        result = await batch_processor.process_wallets_from_sheets(
            spreadsheet_id=spreadsheet_id, input_range="A:B", output_range="A1"
        )

        assert isinstance(result, ProcessingResults)
        assert result.total_wallets_input == 2
        mock_sheets_client.read_wallet_addresses.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_wallets_from_sheets_no_addresses(self, batch_processor, mock_sheets_client):
        """Test processing when no addresses found."""
        mock_sheets_client.read_wallet_addresses.return_value = []

        result = await batch_processor.process_wallets_from_sheets(spreadsheet_id="test_sheet_id")

        assert result.total_wallets_input == 0

    @pytest.mark.asyncio
    async def test_process_wallets_from_sheets_no_client(
        self, app_config, mock_ethereum_client, mock_coingecko_client, mock_cache_manager
    ):
        """Test processing without sheets client."""
        processor = BatchProcessor(
            config=app_config,
            ethereum_client=mock_ethereum_client,
            coingecko_client=mock_coingecko_client,
            cache_manager=mock_cache_manager,
            sheets_client=None,
        )

        with pytest.raises(BatchProcessorError, match="Google Sheets client not configured"):
            await processor.process_wallets_from_sheets("test_sheet_id")

    @pytest.mark.asyncio
    async def test_process_wallet_list_success(self, batch_processor):
        """Test successful wallet list processing."""
        addresses = [
            {"address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86", "label": "Test 1", "row_number": 1},
            {"address": "0x123", "label": "Test 2", "row_number": 2},
        ]

        result = await batch_processor.process_wallet_list(addresses=addresses)

        assert isinstance(result, ProcessingResults)
        assert result.total_wallets_input == 2

    @pytest.mark.asyncio
    async def test_validate_addresses(self, batch_processor):
        """Test address validation."""
        jobs = [
            WalletProcessingJob("0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86", "Valid", 1),
            WalletProcessingJob("invalid_address", "Invalid", 2),
            WalletProcessingJob("", "Empty", 3),
        ]

        valid_jobs = await batch_processor._validate_addresses(jobs)

        # Should have only 1 valid job
        assert len(valid_jobs) == 1
        assert valid_jobs[0].address.startswith("0x742d35cc")  # Normalized

        # Check that invalid jobs were marked as failed
        invalid_jobs = [job for job in jobs if job.status == WalletStatus.FAILED]
        assert len(invalid_jobs) == 2

    @pytest.mark.asyncio
    async def test_precache_token_prices(self, batch_processor, mock_coingecko_client):
        """Test pre-caching token prices."""
        await batch_processor._precache_token_prices()

        mock_coingecko_client.get_eth_price.assert_called_once()
        mock_coingecko_client.get_stablecoin_prices.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_single_job_success(self, batch_processor):
        """Test successful single job processing."""
        test_job = WalletProcessingJob("0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86", "Test", 1)
        config = BatchConfig()

        await batch_processor._process_single_job(test_job, config)

        assert test_job.status == WalletStatus.COMPLETED
        assert test_job.total_value_usd == Decimal("2001.0")
        assert test_job.eth_balance == Decimal("1.0")
        assert "USDC" in test_job.token_balances

    @pytest.mark.asyncio
    async def test_process_single_job_invalid_address(self, batch_processor, mock_ethereum_client):
        """Test processing job with invalid address."""
        mock_ethereum_client.get_wallet_portfolio.side_effect = InvalidAddressError("Invalid address")

        test_job = WalletProcessingJob("invalid", "Test", 1)
        config = BatchConfig()

        await batch_processor._process_single_job(test_job, config)

        assert test_job.status == WalletStatus.FAILED
        assert test_job.skip_reason == SkipReason.INVALID_ADDRESS

    @pytest.mark.asyncio
    async def test_process_single_job_timeout(self, batch_processor, mock_ethereum_client):
        """Test processing job with timeout."""

        # Mock timeout
        async def slow_response(*args, **kwargs):
            await asyncio.sleep(0.1)
            return MagicMock()

        mock_ethereum_client.get_wallet_portfolio.side_effect = slow_response

        test_job = WalletProcessingJob("0x123", "Test", 1)
        config = BatchConfig(timeout_seconds=0.01)  # Very short timeout

        await batch_processor._process_single_job(test_job, config)

        assert test_job.status == WalletStatus.FAILED
        assert test_job.skip_reason == SkipReason.TIMEOUT

    @pytest.mark.asyncio
    async def test_check_cache_hit(self, batch_processor, mock_cache_manager):
        """Test cache hit scenario."""
        # Mock cache hit
        cached_data = {
            "address": "0x123",
            "eth_balance": {"balance_eth": "1.0"},
            "total_value_usd": "1000.0",
            "transaction_count": 50,
            "token_balances": [],
        }
        mock_cache_manager.get_balance.return_value = cached_data

        test_job = WalletProcessingJob("0x123", "Test", 1)

        # Mock deserialization
        portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="1000000000000000000",
                balance_eth=Decimal("1.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("2000.0"),
            ),
            token_balances=[],
            total_value_usd=Decimal("1000.0"),
            last_updated=datetime.now(UTC),
            transaction_count=50,
        )
        batch_processor.ethereum_client._deserialize_portfolio.return_value = portfolio

        result = await batch_processor._check_cache(test_job)

        assert result is True
        assert test_job.cache_hit is True
        assert test_job.status == WalletStatus.COMPLETED
        assert test_job.total_value_usd == Decimal("1000.0")

    @pytest.mark.asyncio
    async def test_cache_result(self, batch_processor, mock_cache_manager):
        """Test caching processing result."""
        test_job = WalletProcessingJob("0x123", "Test", 1)
        portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="1000000000000000000",
                balance_eth=Decimal("1.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("2000.0"),
            ),
            token_balances=[],
            total_value_usd=Decimal("2000.0"),
            last_updated=datetime.now(UTC),
            transaction_count=50,
        )
        config = BatchConfig()

        await batch_processor._cache_result(test_job, portfolio, config)

        batch_processor.ethereum_client._serialize_portfolio.assert_called_once_with(portfolio)
        mock_cache_manager.set_balance.assert_called_once()

    @pytest.mark.asyncio
    async def test_should_skip_wallet_inactive(self, batch_processor):
        """Test skipping inactive wallet."""
        # Create portfolio with old timestamp
        old_timestamp = datetime.now(UTC) - timedelta(days=400)
        portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="1000000000000000",  # Very small amount
                balance_eth=Decimal("0.001"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("2.0"),
            ),
            token_balances=[],
            total_value_usd=Decimal("2.0"),  # Low value
            last_updated=datetime.now(UTC),
            transaction_count=5,
            last_transaction_timestamp=old_timestamp,
        )

        config = BatchConfig(inactive_threshold_days=365, min_value_threshold_usd=Decimal("1.0"))

        should_skip = batch_processor._should_skip_wallet(portfolio, config)
        assert should_skip is True

    @pytest.mark.asyncio
    async def test_should_not_skip_wallet_high_value(self, batch_processor):
        """Test not skipping wallet with high value."""
        # Create portfolio with high value
        portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="10000000000000000000",  # 10 ETH
                balance_eth=Decimal("10.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("20000.0"),
            ),
            token_balances=[],
            total_value_usd=Decimal("20000.0"),  # High value
            last_updated=datetime.now(UTC),
            transaction_count=5,
        )

        config = BatchConfig(min_value_threshold_usd=Decimal("1.0"))

        should_skip = batch_processor._should_skip_wallet(portfolio, config)
        assert should_skip is False

    @pytest.mark.asyncio
    async def test_health_check(self, batch_processor, mock_coingecko_client, mock_cache_manager, mock_sheets_client):
        """Test health check of all services."""
        health = await batch_processor.health_check()

        assert isinstance(health, dict)
        assert "ethereum_client" in health
        assert "coingecko_client" in health
        assert "cache_manager" in health
        assert "sheets_client" in health

        mock_coingecko_client.health_check.assert_called_once()
        mock_cache_manager.health_check.assert_called_once()
        mock_sheets_client.health_check.assert_called_once()

    def test_get_stats(self, batch_processor):
        """Test getting processor statistics."""
        stats = batch_processor.get_stats()

        assert isinstance(stats, dict)
        assert "active_batches" in stats
        assert "stop_requested" in stats
        assert "batch_config" in stats
        assert "client_stats" in stats

    @pytest.mark.asyncio
    async def test_close(self, batch_processor):
        """Test closing batch processor."""
        # Add some active batches
        batch_processor._active_batches = {"test": MagicMock()}
        batch_processor._processing_callbacks = [MagicMock()]

        await batch_processor.close()

        assert batch_processor._stop_requested is True
        assert len(batch_processor._active_batches) == 0
        assert len(batch_processor._processing_callbacks) == 0

    @pytest.mark.asyncio
    async def test_cache_error_handling(self):
        """Test handling cache errors."""
        minimal_config = AppConfig(
            ethereum=EthereumConfig(alchemy_api_key="test", rpc_url="https://test.com"),
            coingecko=CoinGeckoConfig(),
            google_sheets=GoogleSheetsConfig(credentials_file=Path("/tmp/fake.json")),
            processing=MagicMock(
                batch_size=1,
                max_concurrent_requests=1,
                request_delay=0,
                retry_attempts=0,
                inactive_wallet_threshold_days=365,
            ),
        )

        mock_cache = AsyncMock()
        mock_cache.get_balance.side_effect = Exception("Cache error")
        mock_cache.set_balance.side_effect = Exception("Cache error")
        mock_cache.health_check.return_value = {"cache": False}

        # Mock ethereum client to return a valid portfolio
        mock_ethereum = AsyncMock()
        portfolio = WalletPortfolio(
            address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            eth_balance=EthBalance(
                balance_wei="1000000000000000000",
                balance_eth=Decimal("1.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("2000.0"),
            ),
            token_balances=[],
            total_value_usd=Decimal("2000.0"),
            last_updated=datetime.now(UTC),
            transaction_count=50,
        )
        mock_ethereum.get_wallet_portfolio.return_value = portfolio
        mock_ethereum._serialize_portfolio.return_value = {"test": "data"}

        processor = BatchProcessor(
            config=minimal_config, ethereum_client=mock_ethereum, coingecko_client=AsyncMock(), cache_manager=mock_cache
        )

        addresses = [{"address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86", "label": "Test", "row_number": 1}]

        # Should not fail even if cache operations fail
        result = await processor.process_wallet_list(addresses=addresses)

        assert isinstance(result, ProcessingResults)
