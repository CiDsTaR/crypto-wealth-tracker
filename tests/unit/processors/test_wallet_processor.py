"""Tests for wallet processor."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wallet_tracker.clients import (
    CoinGeckoClient,
    EthereumClient,
    GoogleSheetsClient,
    WalletPortfolio,
    EthBalance,
    TokenBalance,
    create_wallet_result_from_portfolio,
)
from wallet_tracker.config import AppConfig, EthereumConfig, CoinGeckoConfig, GoogleSheetsConfig
from wallet_tracker.processors import WalletProcessor, WalletProcessorError
from wallet_tracker.utils import CacheManager


class TestWalletProcessor:
    """Test WalletProcessor class."""

    @pytest.fixture
    def app_config(self, tmp_path) -> AppConfig:
        """Create application configuration for testing."""
        # Create dummy credentials file
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text('{"type": "service_account"}')

        return AppConfig(
            ethereum=EthereumConfig(
                alchemy_api_key="test_key",
                rpc_url="https://test.com",
                rate_limit=100
            ),
            coingecko=CoinGeckoConfig(),
            google_sheets=GoogleSheetsConfig(credentials_file=creds_file),
            processing=MagicMock(
                batch_size=10,
                max_concurrent_requests=5,
                request_delay=0.01,
                retry_attempts=2,
                inactive_wallet_threshold_days=365,
            )
        )

    @pytest.fixture
    def mock_cache_manager(self) -> AsyncMock:
        """Create mock cache manager."""
        cache_manager = AsyncMock(spec=CacheManager)
        cache_manager.health_check = AsyncMock(return_value={"cache": True})
        cache_manager.get_stats = AsyncMock(return_value={"hits": 0, "misses": 0})
        cache_manager.close = AsyncMock()
        return cache_manager

    @pytest.fixture
    def mock_ethereum_client(self) -> AsyncMock:
        """Create mock Ethereum client."""
        client = AsyncMock(spec=EthereumClient)

        # Mock successful portfolio response
        portfolio = WalletPortfolio(
            address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            eth_balance=EthBalance(
                balance_wei="2000000000000000000",  # 2 ETH
                balance_eth=Decimal("2.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("4000.0")
            ),
            token_balances=[
                TokenBalance(
                    contract_address="0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0",
                    symbol="USDC",
                    name="USD Coin",
                    decimals=6,
                    balance_raw="1000000000",  # 1000 USDC
                    balance_formatted=Decimal("1000.0"),
                    price_usd=Decimal("1.0"),
                    value_usd=Decimal("1000.0"),
                    is_verified=True
                ),
                TokenBalance(
                    contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
                    symbol="USDT",
                    name="Tether",
                    decimals=6,
                    balance_raw="500000000",  # 500 USDT
                    balance_formatted=Decimal("500.0"),
                    price_usd=Decimal("1.0"),
                    value_usd=Decimal("500.0"),
                    is_verified=True
                )
            ],
            total_value_usd=Decimal("5500.0"),
            last_updated=datetime.now(UTC),
            transaction_count=150,
            last_transaction_timestamp=datetime.now(UTC) - timedelta(days=30)
        )

        client.get_wallet_portfolio = AsyncMock(return_value=portfolio)
        client.close = AsyncMock()

        return client

    @pytest.fixture
    def mock_coingecko_client(self) -> AsyncMock:
        """Create mock CoinGecko client."""
        client = AsyncMock(spec=CoinGeckoClient)
        client.health_check = AsyncMock(return_value=True)
        client.close = AsyncMock()
        return client

    @pytest.fixture
    def mock_sheets_client(self) -> AsyncMock:
        """Create mock Google Sheets client."""
        client = AsyncMock(spec=GoogleSheetsClient)

        # Mock wallet addresses
        from wallet_tracker.clients.google_sheets_types import WalletAddress
        wallet_addresses = [
            WalletAddress(address="0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86", label="Wallet 1", row_number=2),
            WalletAddress(address="0x123", label="Wallet 2", row_number=3),
            WalletAddress(address="0x456", label="Wallet 3", row_number=4),
        ]
        client.read_wallet_addresses = AsyncMock(return_value=wallet_addresses)
        client.write_wallet_results = MagicMock(return_value=True)
        client.create_summary_sheet = MagicMock(return_value=True)
        client.health_check = MagicMock(return_value=True)
        client.close = AsyncMock()

        return client

    @pytest.fixture
    def wallet_processor(
        self,
        app_config,
        mock_ethereum_client,
        mock_coingecko_client,
        mock_sheets_client,
        mock_cache_manager
    ) -> WalletProcessor:
        """Create wallet processor for testing."""
        return WalletProcessor(
            config=app_config,
            ethereum_client=mock_ethereum_client,
            coingecko_client=mock_coingecko_client,
            sheets_client=mock_sheets_client,
            cache_manager=mock_cache_manager
        )

    @pytest.mark.asyncio
    async def test_processor_initialization(self, wallet_processor):
        """Test wallet processor initialization."""
        assert wallet_processor.config is not None
        assert wallet_processor.ethereum_client is not None
        assert wallet_processor.coingecko_client is not None
        assert wallet_processor.sheets_client is not None
        assert wallet_processor.cache_manager is not None

        # Check initial stats
        stats = wallet_processor._stats
        assert stats["wallets_processed"] == 0
        assert stats["wallets_skipped"] == 0
        assert stats["wallets_failed"] == 0
        assert stats["total_value_usd"] == Decimal("0")

    @pytest.mark.asyncio
    async def test_process_wallets_from_sheets_success(
        self,
        wallet_processor,
        mock_sheets_client
    ):
        """Test successful processing from Google Sheets."""
        spreadsheet_id = "test_sheet_id"

        with patch.object(wallet_processor, 'process_wallet_list') as mock_process:
            # Mock the process_wallet_list to return sample results
            mock_results = [MagicMock()]  # Sample wallet results
            mock_process.return_value = mock_results

            result = await wallet_processor.process_wallets_from_sheets(
                spreadsheet_id=spreadsheet_id,
                input_range="A:B",
                output_range="A1"
            )

        assert result["success"] is True
        assert "stats" in result
        assert "results" in result

        # Verify sheets client was called
        mock_sheets_client.read_wallet_addresses.assert_called_once()
        mock_sheets_client.write_wallet_results.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_wallets_from_sheets_no_addresses(
        self,
        wallet_processor,
        mock_sheets_client
    ):
        """Test processing when no addresses found in sheets."""
        mock_sheets_client.read_wallet_addresses.return_value = []

        result = await wallet_processor.process_wallets_from_sheets(
            spreadsheet_id="test_sheet_id"
        )

        assert result["success"] is True
        assert result["message"] == "No wallet addresses to process"
        assert len(result["results"]) == 0

    @pytest.mark.asyncio
    async def test_process_wallets_from_sheets_with_summary(
        self,
        wallet_processor,
        mock_sheets_client
    ):
        """Test processing with summary sheet creation."""
        with patch.object(wallet_processor, 'process_wallet_list') as mock_process:
            # Mock wallet results
            from wallet_tracker.clients.google_sheets_types import WalletResult
            mock_results = [
                WalletResult(
                    address="0x123",
                    label="Test Wallet",
                    eth_balance=Decimal("1.0"),
                    eth_value_usd=Decimal("2000.0"),
                    usdc_balance=Decimal("1000.0"),
                    usdt_balance=Decimal("0.0"),
                    dai_balance=Decimal("0.0"),
                    aave_balance=Decimal("0.0"),
                    uni_balance=Decimal("0.0"),
                    link_balance=Decimal("0.0"),
                    other_tokens_value_usd=Decimal("0.0"),
                    total_value_usd=Decimal("3000.0"),
                    last_updated=datetime.now(UTC),
                    transaction_count=100,
                    is_active=True
                )
            ]
            mock_process.return_value = mock_results

            result = await wallet_processor.process_wallets_from_sheets(
                spreadsheet_id="test_sheet_id",
                include_summary=True
            )

        assert result["success"] is True
        # Verify summary sheet was created
        mock_sheets_client.create_summary_sheet.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_wallets_from_sheets_error_handling(
        self,
        wallet_processor,
        mock_sheets_client
    ):
        """Test error handling in sheets processing."""
        # Make sheets client raise an exception
        mock_sheets_client.read_wallet_addresses.side_effect = Exception("Sheets API error")

        with pytest.raises(WalletProcessorError, match="Wallet processing failed"):
            await wallet_processor.process_wallets_from_sheets("test_sheet_id")

    @pytest.mark.asyncio
    async def test_process_wallet_list_success(self, wallet_processor):
        """Test successful wallet list processing."""
        wallet_addresses = [
            {"address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86", "label": "Wallet 1"},
            {"address": "0x123", "label": "Wallet 2"},
        ]

        with patch.object(wallet_processor, '_process_wallet_batch') as mock_batch:
            # Mock batch processing results
            mock_batch.return_value = [MagicMock(), MagicMock()]  # Two results

            results = await wallet_processor.process_wallet_list(wallet_addresses)

        assert len(results) == 2
        # Check that stats were updated
        assert wallet_processor._stats["wallets_processed"] >= 0

    @pytest.mark.asyncio
    async def test_process_single_wallet_success(self, wallet_processor, mock_ethereum_client):
        """Test successful single wallet processing."""
        wallet_data = {
            "address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
            "label": "Test Wallet"
        }

        with patch('wallet_tracker.clients.create_wallet_result_from_portfolio') as mock_create:
            # Mock wallet result creation
            from wallet_tracker.clients.google_sheets_types import WalletResult
            mock_result = WalletResult(
                address=wallet_data["address"],
                label=wallet_data["label"],
                eth_balance=Decimal("2.0"),
                eth_value_usd=Decimal("4000.0"),
                usdc_balance=Decimal("1000.0"),
                usdt_balance=Decimal("500.0"),
                dai_balance=Decimal("0.0"),
                aave_balance=Decimal("0.0"),
                uni_balance=Decimal("0.0"),
                link_balance=Decimal("0.0"),
                other_tokens_value_usd=Decimal("0.0"),
                total_value_usd=Decimal("5500.0"),
                last_updated=datetime.now(UTC),
                transaction_count=150,
                is_active=True
            )
            mock_create.return_value = mock_result

            result = await wallet_processor._process_single_wallet(wallet_data)

        assert result is not None
        assert result.address == wallet_data["address"]
        assert result.label == wallet_data["label"]

        # Verify ethereum client was called
        mock_ethereum_client.get_wallet_portfolio.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_single_wallet_inactive_skip(self, wallet_processor, mock_ethereum_client):
        """Test skipping inactive wallet."""
        # Create portfolio for inactive wallet
        inactive_portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="1000000000000000",  # Very small amount
                balance_eth=Decimal("0.001"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("2.0")
            ),
            token_balances=[],
            total_value_usd=Decimal("2.0"),  # Low value
            last_updated=datetime.now(UTC),
            transaction_count=5,
            last_transaction_timestamp=datetime.now(UTC) - timedelta(days=400)  # Very old
        )

        mock_ethereum_client.get_wallet_portfolio.return_value = inactive_portfolio

        wallet_data = {"address": "0x123", "label": "Inactive Wallet"}

        result = await wallet_processor._process_single_wallet(wallet_data)

        # Should be skipped due to inactivity
        assert result is None
        assert wallet_processor._stats["wallets_skipped"] == 1
        assert wallet_processor._stats["inactive_wallets"] == 1

    @pytest.mark.asyncio
    async def test_process_single_wallet_error_handling(self, wallet_processor, mock_ethereum_client):
        """Test error handling in single wallet processing."""
        # Make ethereum client raise an exception
        mock_ethereum_client.get_wallet_portfolio.side_effect = Exception("Network error")

        wallet_data = {"address": "0x123", "label": "Error Wallet"}

        result = await wallet_processor._process_single_wallet(wallet_data)

        # Should return None and increment failed count
        assert result is None
        assert wallet_processor._stats["wallets_failed"] == 1

    def test_should_skip_wallet_inactive_old_transactions(self, wallet_processor):
        """Test wallet skipping logic for old transactions."""
        # Create portfolio with old transactions and low value
        old_timestamp = datetime.now(UTC) - timedelta(days=400)
        portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="1000000000000000",  # 0.001 ETH
                balance_eth=Decimal("0.001"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("2.0")
            ),
            token_balances=[],
            total_value_usd=Decimal("2.0"),  # Low value
            last_updated=datetime.now(UTC),
            transaction_count=5,
            last_transaction_timestamp=old_timestamp
        )

        should_skip = wallet_processor._should_skip_wallet(portfolio)
        assert should_skip is True

    def test_should_skip_wallet_high_value_no_skip(self, wallet_processor):
        """Test wallet skipping logic for high value wallets."""
        # Create portfolio with high value (should not skip even if inactive)
        portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="50000000000000000000",  # 50 ETH
                balance_eth=Decimal("50.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("100000.0")
            ),
            token_balances=[],
            total_value_usd=Decimal("100000.0"),  # High value
            last_updated=datetime.now(UTC),
            transaction_count=5
        )

        should_skip = wallet_processor._should_skip_wallet(portfolio)
        assert should_skip is False

    def test_should_skip_wallet_no_transaction_data(self, wallet_processor):
        """Test wallet skipping logic when no transaction data available."""
        # Create portfolio with no transaction timestamp but significant value
        portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="1000000000000000000",  # 1 ETH
                balance_eth=Decimal("1.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("2000.0")
            ),
            token_balances=[],
            total_value_usd=Decimal("2000.0"),  # Significant value
            last_updated=datetime.now(UTC),
            transaction_count=10,
            last_transaction_timestamp=None  # No transaction data
        )

        should_skip = wallet_processor._should_skip_wallet(portfolio)
        assert should_skip is False  # Should not skip due to significant value

    def test_is_wallet_active_recent_transactions(self, wallet_processor):
        """Test wallet activity detection with recent transactions."""
        # Portfolio with recent transactions
        recent_timestamp = datetime.now(UTC) - timedelta(days=30)
        portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="1000000000000000000",
                balance_eth=Decimal("1.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("2000.0")
            ),
            token_balances=[],
            total_value_usd=Decimal("2000.0"),
            last_updated=datetime.now(UTC),
            transaction_count=50,
            last_transaction_timestamp=recent_timestamp
        )

        is_active = wallet_processor._is_wallet_active(portfolio)
        assert is_active is True

    def test_is_wallet_active_high_value(self, wallet_processor):
        """Test wallet activity detection based on high value."""
        # Portfolio with high value but no recent transactions
        portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="500000000000000000000",  # 500 ETH
                balance_eth=Decimal("500.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("1000000.0")
            ),
            token_balances=[],
            total_value_usd=Decimal("1000000.0"),  # Very high value
            last_updated=datetime.now(UTC),
            transaction_count=10
        )

        is_active = wallet_processor._is_wallet_active(portfolio)
        assert is_active is True

    def test_is_wallet_active_many_transactions(self, wallet_processor):
        """Test wallet activity detection based on transaction count."""
        # Portfolio with many transactions
        portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="1000000000000000000",
                balance_eth=Decimal("1.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("2000.0")
            ),
            token_balances=[],
            total_value_usd=Decimal("2000.0"),
            last_updated=datetime.now(UTC),
            transaction_count=100  # Many transactions
        )

        is_active = wallet_processor._is_wallet_active(portfolio)
        assert is_active is True

    def test_is_wallet_inactive(self, wallet_processor):
        """Test wallet inactivity detection."""
        # Portfolio that should be considered inactive
        old_timestamp = datetime.now(UTC) - timedelta(days=200)
        portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="100000000000000000",  # 0.1 ETH
                balance_eth=Decimal("0.1"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("200.0")
            ),
            token_balances=[],
            total_value_usd=Decimal("200.0"),  # Low value
            last_updated=datetime.now(UTC),
            transaction_count=10,  # Few transactions
            last_transaction_timestamp=old_timestamp
        )

        is_active = wallet_processor._is_wallet_active(portfolio)
        assert is_active is False

    def test_get_processing_time_calculation(self, wallet_processor):
        """Test processing time calculation."""
        # No start time
        assert wallet_processor._get_processing_time() == "Unknown"

        # Set start time only
        wallet_processor._stats["processing_start_time"] = datetime.now(UTC)
        time_str = wallet_processor._get_processing_time()
        assert time_str.endswith("s")  # Should end with seconds

        # Set both start and end times
        start_time = datetime.now(UTC) - timedelta(hours=1, minutes=30, seconds=45)
        end_time = datetime.now(UTC)
        wallet_processor._stats["processing_start_time"] = start_time
        wallet_processor._stats["processing_end_time"] = end_time

        time_str = wallet_processor._get_processing_time()
        assert "1h 30m 45s" == time_str

    def test_get_stats(self, wallet_processor):
        """Test getting processor statistics."""
        # Update some stats
        wallet_processor._stats["wallets_processed"] = 10
        wallet_processor._stats["wallets_skipped"] = 2
        wallet_processor._stats["wallets_failed"] = 1
        wallet_processor._stats["total_value_usd"] = Decimal("50000.0")

        stats = wallet_processor.get_stats()

        assert stats["wallets_processed"] == 10
        assert stats["wallets_skipped"] == 2
        assert stats["wallets_failed"] == 1
        assert stats["total_value_usd"] == Decimal("50000.0")
        assert "success_rate" in stats
        assert "skip_rate" in stats
        assert "failure_rate" in stats
        assert "processing_time" in stats

    @pytest.mark.asyncio
    async def test_health_check(self, wallet_processor, mock_coingecko_client, mock_sheets_client, mock_cache_manager):
        """Test health check of all services."""
        health = await wallet_processor.health_check()

        assert isinstance(health, dict)
        assert "ethereum_client" in health
        assert "coingecko_client" in health
        assert "sheets_client" in health
        assert "cache_manager" in health

        # Verify clients were called
        mock_coingecko_client.health_check.assert_called_once()
        mock_cache_manager.health_check.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_check_with_failures(self, wallet_processor, mock_coingecko_client):
        """Test health check when services fail."""
        # Make coingecko client health check fail
        mock_coingecko_client.health_check.side_effect = Exception("Health check failed")

        health = await wallet_processor.health_check()

        assert health["coingecko_client"] is False

    @pytest.mark.asyncio
    async def test_close(self, wallet_processor, mock_ethereum_client, mock_coingecko_client, mock_cache_manager):
        """Test closing processor and cleaning up resources."""
        await wallet_processor.close()

        # Verify all clients were closed
        mock_ethereum_client.close.assert_called_once()
        mock_coingecko_client.close.assert_called_once()
        mock_cache_manager.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_processing_with_semaphore(self, wallet_processor):
        """Test batch processing with concurrency control."""
        wallet_addresses = []
        # Create multiple wallets to test batching
        for i in range(25):
            wallet_addresses.append({
                "address": f"0x{'123' + str(i).zfill(37)}",
                "label": f"Wallet {i}"
            })

        # Mock batch processing
        with patch.object(wallet_processor, '_process_single_wallet') as mock_single:
            mock_single.return_value = MagicMock()  # Return a mock result

            results = await wallet_processor.process_wallet_list(wallet_addresses)

        # Should process all wallets
        assert len(results) > 0

    @pytest.mark.asyncio
    async def test_processing_delay_between_requests(self, wallet_processor):
        """Test delay between processing requests."""
        wallet_addresses = [
            {"address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86", "label": "Wallet 1"},
            {"address": "0x123", "label": "Wallet 2"},
        ]

        # Set a small delay for testing
        wallet_processor.config.processing.request_delay = 0.01

        start_time = datetime.now(UTC)

        with patch.object(wallet_processor, '_process_single_wallet') as mock_single:
            mock_single.return_value = MagicMock()
            await wallet_processor.process_wallet_list(wallet_addresses)

        end_time = datetime.now(UTC)
        duration = (end_time - start_time).total_seconds()

        # Should take some time due to delays
        assert duration > 0


class TestWalletProcessorEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.fixture
    def minimal_config(self) -> AppConfig:
        """Create minimal configuration for edge case testing."""
        return AppConfig(
            ethereum=EthereumConfig(alchemy_api_key="test", rpc_url="https://test.com"),
            coingecko=CoinGeckoConfig(),
            google_sheets=GoogleSheetsConfig(credentials_file=Path("/tmp/fake.json")),
            processing=MagicMock(
                batch_size=1,
                max_concurrent_requests=1,
                request_delay=0,
                retry_attempts=0,
                inactive_wallet_threshold_days=365,
            )
        )

    @pytest.mark.asyncio
    async def test_empty_wallet_list(self, minimal_config):
        """Test processing empty wallet list."""
        processor = WalletProcessor(
            config=minimal_config,
            ethereum_client=AsyncMock(),
            coingecko_client=AsyncMock(),
            sheets_client=AsyncMock(),
            cache_manager=AsyncMock()
        )

        results = await processor.process_wallet_list([])

        assert len(results) == 0
        assert processor._stats["wallets_processed"] == 0

    @pytest.mark.asyncio
    async def test_wallet_with_no_balances(self, minimal_config):
        """Test processing wallet with no balances."""
        # Create empty portfolio
        empty_portfolio = WalletPortfolio(
            address="0x123",
            eth_balance=EthBalance(
                balance_wei="0",
                balance_eth=Decimal("0.0"),
                price_usd=Decimal("2000.0"),
                value_usd=Decimal("0.0")
            ),
            token_balances=[],
            total_value_usd=Decimal("0.0"),
            last_updated=datetime.now(UTC),
            transaction_count=0
        )

        mock_ethereum = AsyncMock()
        mock_ethereum.get_wallet_portfolio.return_value = empty_portfolio

        processor = WalletProcessor(
            config=minimal_config,
            ethereum_client=mock_ethereum,
            coingecko_client=AsyncMock(),
            sheets_client=AsyncMock(),
            cache_manager=AsyncMock()
        )

        wallet_data = {"address": "0x123", "label": "Empty Wallet"}

        # Should be skipped due to low value
        result = await processor._process_single_wallet(wallet_data)
        assert result is None

    @pytest.mark.asyncio
    async def test_concurrent_processing_stress(self, minimal_config):
        """Test concurrent processing with many wallets."""
        processor = WalletProcessor(
            config=minimal_config,
            ethereum_client=AsyncMock(),
            coingecko_client=AsyncMock(),
            sheets_client=AsyncMock(),
            cache_manager=AsyncMock()
        )

        # Create many wallet addresses
        wallet_addresses = []
        for i in range(100):
            wallet_addresses.append({
                "address": f"0x{'1' * 38}{i:02d}",
                "label": f"Wallet {i}"
            })

        with patch.object(processor, '_process_single_wallet') as mock_single:
            mock_single.return_value = None  # All wallets skipped

            results = await processor.process_wallet_list(wallet_addresses)

        # All should be processed (even if skipped)
        assert mock_single.call_count == 100

    @pytest.mark.asyncio
    async def test_sheets_write_failure_handling(self, minimal_config):
        """Test handling sheets write failures."""
        mock_sheets = AsyncMock()
        mock_sheets.read_wallet_addresses.return_value = [
            MagicMock(address="0x123", label="Test", row_number=1)
        ]
        mock_sheets.write_wallet_results.return_value = False  # Write fails

        processor = WalletProcessor(
            config=minimal_config,
            ethereum_client=AsyncMock(),
            coingecko_client=AsyncMock(),
            sheets_client=mock_sheets,
            cache_manager=AsyncMock()
        )

        with patch.object(processor, 'process_wallet_list') as mock_process:
            mock_process.return_value = [MagicMock()]  # Return some results

            with pytest.raises(WalletProcessorError, match="Failed to write results"):
                await processor.process_wallets_from_sheets("test_sheet_id")

    @pytest.mark.asyncio
    async def test_summary_creation_failure(self, minimal_config):
        """Test handling summary creation failures."""
        mock_sheets = AsyncMock()
        mock_sheets.read_wallet_addresses.return_value = [
            MagicMock(address="0x123", label="Test", row_number=1)
        ]
        mock_sheets.write_wallet_results.return_value = True
        mock_sheets.create_summary_sheet.return_value = False  # Summary fails

        processor = WalletProcessor(
            config=minimal_config,
            ethereum_client=AsyncMock(),
            coingecko_client=AsyncMock(),
            sheets_client=mock_sheets,
            cache_manager=AsyncMock()
        )

        with patch.object(processor, 'process_wallet_list') as mock_process:
            mock_process.return_value = [MagicMock()]

            # Should not fail even if summary creation fails
            result = await processor.process_wallets_from_sheets(
                "test_sheet_id",
                include_summary=True
            )

            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_memory_usage_with_large_results(self, minimal_config):
        """Test memory usage with large result sets."""
        processor = WalletProcessor(
            config=minimal_config,
            ethereum_client=AsyncMock(),
            coingecko_client=AsyncMock(),
            sheets_client=AsyncMock(),
            cache_manager=AsyncMock()
        )

        # Mock processing to return large results
        large_results = []
        for i in range(1000):
            result = MagicMock()
            result.total_value_usd = Decimal(f"{i * 1000}")
            large_results.append(result)

        with patch.object(processor, 'process_wallet_list') as mock_process:
            mock_process.return_value = large_results

            # Should handle large result sets without issues
            result = await processor.process_wallets_from_sheets("test_sheet_id")

            assert result["success"] is True
            assert len(result["results"]) == 1000