"""Tests for Google Sheets client."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wallet_tracker.clients import (
    GoogleSheetsClient,
    SheetRange,
    SheetsAPIError,
    SheetsAuthenticationError,
    SheetsPermissionError,
    WalletResult,
    create_summary_from_results,
)
from wallet_tracker.config import GoogleSheetsConfig
from wallet_tracker.utils import CacheManager


class TestSheetRange:
    """Test SheetRange utility class."""

    def test_column_to_letter(self) -> None:
        """Test column number to letter conversion."""
        assert SheetRange.column_to_letter(1) == "A"
        assert SheetRange.column_to_letter(26) == "Z"
        assert SheetRange.column_to_letter(27) == "AA"
        assert SheetRange.column_to_letter(52) == "AZ"
        assert SheetRange.column_to_letter(53) == "BA"

    def test_letter_to_column(self) -> None:
        """Test letter to column number conversion."""
        assert SheetRange.letter_to_column("A") == 1
        assert SheetRange.letter_to_column("Z") == 26
        assert SheetRange.letter_to_column("AA") == 27
        assert SheetRange.letter_to_column("AZ") == 52
        assert SheetRange.letter_to_column("BA") == 53

    def test_parse_range(self) -> None:
        """Test range parsing."""
        # Single cell
        start_col, start_row, end_col, end_row = SheetRange.parse_range("A1")
        assert start_col == "A"
        assert start_row == 1
        assert end_col == "A"
        assert end_row == 1

        # Range
        start_col, start_row, end_col, end_row = SheetRange.parse_range("A1:C10")
        assert start_col == "A"
        assert start_row == 1
        assert end_col == "C"
        assert end_row == 10

    def test_build_range(self) -> None:
        """Test range building."""
        range_str = SheetRange.build_range("A", 1, "C", 10)
        assert range_str == "A1:C10"

    def test_expand_range(self) -> None:
        """Test range expansion."""
        expanded = SheetRange.expand_range("A1:C10", 5, 2)
        assert expanded == "A1:E15"

        # Expand only rows
        expanded = SheetRange.expand_range("A1:C10", 3)
        assert expanded == "A1:C13"


class TestGoogleSheetsClient:
    """Test Google Sheets client."""

    @pytest.fixture
    def sheets_config(self, tmp_path) -> GoogleSheetsConfig:
        """Create Google Sheets configuration for testing."""
        # Create a dummy credentials file
        creds_file = tmp_path / "test_credentials.json"
        creds_file.write_text('{"type": "service_account", "project_id": "test"}')

        return GoogleSheetsConfig(
            credentials_file=creds_file,
            scope="https://www.googleapis.com/auth/spreadsheets",
        )

    @pytest.fixture
    def mock_cache_manager(self) -> AsyncMock:
        """Create mock cache manager."""
        cache_manager = AsyncMock(spec=CacheManager)

        # Mock general cache
        general_cache = AsyncMock()
        general_cache.get = AsyncMock(return_value=None)
        general_cache.set = AsyncMock(return_value=True)
        cache_manager.get_general_cache = MagicMock(return_value=general_cache)

        return cache_manager

    @pytest.fixture
    def sheets_client(self, sheets_config, mock_cache_manager) -> GoogleSheetsClient:
        """Create Google Sheets client for testing."""
        return GoogleSheetsClient(
            config=sheets_config,
            cache_manager=mock_cache_manager,
        )

    @pytest.mark.asyncio
    async def test_authentication_success(self, sheets_client) -> None:
        """Test successful authentication."""
        with patch("gspread.authorize") as mock_authorize:
            mock_client = MagicMock()
            mock_authorize.return_value = mock_client

            client = sheets_client._get_client()
            assert client == mock_client

    def test_authentication_file_not_found(self, mock_cache_manager) -> None:
        """Test authentication with missing credentials file."""
        config = GoogleSheetsConfig(
            credentials_file=Path("/nonexistent/credentials.json"),
            scope="https://www.googleapis.com/auth/spreadsheets",
        )

        client = GoogleSheetsClient(config=config, cache_manager=mock_cache_manager)

        with pytest.raises(SheetsAuthenticationError):
            client._get_client()

    @pytest.mark.asyncio
    async def test_read_wallet_addresses_success(self, sheets_client) -> None:
        """Test successful wallet address reading."""
        mock_worksheet_data = [
            ["Address", "Label"],  # Header
            ["0x123abc", "Wallet 1"],
            ["0x456def", "Wallet 2"],
            ["0x789ghi", ""],  # No label
        ]

        with patch.object(sheets_client, "_get_worksheet") as mock_get_worksheet:
            mock_worksheet = MagicMock()
            mock_worksheet.get.return_value = mock_worksheet_data
            mock_get_worksheet.return_value = mock_worksheet

            wallet_data = await sheets_client.read_wallet_addresses(
                spreadsheet_id="test_sheet_id",
                range_name="A:B",
                skip_header=True,
            )

            assert len(wallet_data) == 3
            assert wallet_data[0].address == "0x123abc"
            assert wallet_data[0].label == "Wallet 1"
            assert wallet_data[2].label == "Wallet 3"  # Auto-generated label

    @pytest.mark.asyncio
    async def test_read_wallet_addresses_cached(self, sheets_client, mock_cache_manager) -> None:
        """Test wallet address reading with cache hit."""
        cached_data = [{"address": "0x123abc", "label": "Cached Wallet", "row_number": 2}]

        mock_cache_manager.get_general_cache().get.return_value = cached_data

        wallet_data = await sheets_client.read_wallet_addresses(
            spreadsheet_id="test_sheet_id",
            range_name="A:B",
        )

        assert wallet_data == cached_data
        assert sheets_client._stats["cache_hits"] == 1

    @pytest.mark.asyncio
    async def test_write_wallet_results_success(self, sheets_client) -> None:
        """Test successful wallet results writing."""
        wallet_results = [
            {
                "address": "0x123abc",
                "label": "Test Wallet",
                "eth_balance": Decimal("1.5"),
                "eth_value_usd": Decimal("3000.0"),
                "usdc_balance": Decimal("1000.0"),
                "usdt_balance": Decimal("500.0"),
                "dai_balance": Decimal("0.0"),
                "aave_balance": Decimal("10.0"),
                "uni_balance": Decimal("5.0"),
                "link_balance": Decimal("20.0"),
                "other_tokens_value_usd": Decimal("100.0"),
                "total_value_usd": Decimal("4600.0"),
                "last_updated": datetime.now(UTC),
                "transaction_count": 150,
                "is_active": True,
            }
        ]

        with patch.object(sheets_client, "_get_worksheet") as mock_get_worksheet:
            mock_worksheet = MagicMock()
            mock_get_worksheet.return_value = mock_worksheet

            success = sheets_client.write_wallet_results(
                spreadsheet_id="test_sheet_id",
                wallet_results=wallet_results,
                include_header=True,
                clear_existing=True,
            )

            assert success is True
            mock_worksheet.batch_clear.assert_called_once()
            mock_worksheet.update.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_batch_results_success(self, sheets_client) -> None:
        """Test successful batch results writing."""
        batch_results = [
            [
                {
                    "address": "0x123abc",
                    "label": "Wallet 1",
                    "total_value_usd": Decimal("1000.0"),
                    "is_active": True,
                }
            ],
            [
                {
                    "address": "0x456def",
                    "label": "Wallet 2",
                    "total_value_usd": Decimal("2000.0"),
                    "is_active": False,
                }
            ],
        ]

        with patch.object(sheets_client, "write_wallet_results") as mock_write:
            mock_write.return_value = True

            success = sheets_client.write_batch_results(
                spreadsheet_id="test_sheet_id",
                batch_results=batch_results,
                batch_size=1,
            )

            assert success is True
            assert mock_write.call_count == 2

    @pytest.mark.asyncio
    async def test_create_summary_sheet_success(self, sheets_client) -> None:
        """Test successful summary sheet creation."""
        summary_data = {
            "total_wallets": 100,
            "active_wallets": 80,
            "inactive_wallets": 20,
            "total_value_usd": Decimal("1000000.0"),
            "average_value_usd": Decimal("10000.0"),
            "median_value_usd": Decimal("5000.0"),
            "eth_total_value": Decimal("500000.0"),
            "eth_holders": 75,
            "processing_time": "5 minutes",
        }

        with patch.object(sheets_client, "_get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_spreadsheet = MagicMock()
            mock_worksheet = MagicMock()

            mock_client.open_by_key.return_value = mock_spreadsheet
            mock_spreadsheet.add_worksheet.return_value = mock_worksheet
            mock_get_client.return_value = mock_client

            # Simulate worksheet not found (new sheet)
            mock_spreadsheet.worksheet.side_effect = Exception("WorksheetNotFound")

            with patch.object(sheets_client, "_format_summary_sheet"):
                success = sheets_client.create_summary_sheet(
                    spreadsheet_id="test_sheet_id",
                    summary_data=summary_data,
                    worksheet_name="Test Summary",
                )

                assert success is True
                mock_worksheet.update.assert_called_once()

    def test_format_balance(self, sheets_client) -> None:
        """Test balance formatting."""
        assert sheets_client._format_balance(Decimal("0")) == "0"
        assert sheets_client._format_balance(Decimal("0.0001")) == "0.000100"
        assert sheets_client._format_balance(Decimal("0.1234")) == "0.1234"
        assert sheets_client._format_balance(Decimal("123.456")) == "123.46"

    def test_format_usd_value(self, sheets_client) -> None:
        """Test USD value formatting."""
        assert sheets_client._format_usd_value(Decimal("0")) == "$0.00"
        assert sheets_client._format_usd_value(Decimal("0.001")) == "$0.0010"
        assert sheets_client._format_usd_value(Decimal("1234.56")) == "$1,234.56"

    def test_get_stats(self, sheets_client) -> None:
        """Test statistics retrieval."""
        stats = sheets_client.get_stats()

        assert "read_operations" in stats
        assert "write_operations" in stats
        assert "batch_operations" in stats
        assert "api_errors" in stats
        assert "cache_hits" in stats
        assert "authenticated" in stats

    @pytest.mark.asyncio
    async def test_health_check_success(self, sheets_client) -> None:
        """Test successful health check."""
        with patch.object(sheets_client, "_get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            health = sheets_client.health_check()
            assert health is True

    @pytest.mark.asyncio
    async def test_health_check_failure(self, sheets_client) -> None:
        """Test health check failure."""
        with patch.object(sheets_client, "_get_client") as mock_get_client:
            mock_get_client.side_effect = Exception("Connection failed")

            health = sheets_client.health_check()
            assert health is False


class TestWalletResultCreation:
    """Test wallet result creation utilities."""

    def test_create_wallet_result_from_portfolio(self) -> None:
        """Test creating WalletResult from WalletPortfolio."""
        # This would need to be implemented with mock portfolio data
        # For now, we'll test the structure
        pass

    def test_create_summary_from_results(self) -> None:
        """Test creating SummaryData from WalletResults."""
        wallet_results = [
            WalletResult(
                address="0x123",
                label="Wallet 1",
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
                is_active=True,
            ),
            WalletResult(
                address="0x456",
                label="Wallet 2",
                eth_balance=Decimal("0.5"),
                eth_value_usd=Decimal("1000.0"),
                usdc_balance=Decimal("0.0"),
                usdt_balance=Decimal("500.0"),
                dai_balance=Decimal("0.0"),
                aave_balance=Decimal("0.0"),
                uni_balance=Decimal("0.0"),
                link_balance=Decimal("0.0"),
                other_tokens_value_usd=Decimal("0.0"),
                total_value_usd=Decimal("1500.0"),
                last_updated=datetime.now(UTC),
                transaction_count=50,
                is_active=False,
            ),
        ]

        summary = create_summary_from_results(wallet_results, "2 minutes")

        assert summary.total_wallets == 2
        assert summary.active_wallets == 1
        assert summary.inactive_wallets == 1
        assert summary.total_value_usd == Decimal("4500.0")
        assert summary.average_value_usd == Decimal("2250.0")
        assert summary.median_value_usd == Decimal("2250.0")
        assert summary.eth_holders == 2
        assert summary.usdc_holders == 1
        assert summary.usdt_holders == 1
        assert summary.processing_time == "2 minutes"

    def test_create_summary_from_empty_results(self) -> None:
        """Test creating SummaryData from empty results."""
        summary = create_summary_from_results([], "0 seconds")

        assert summary.total_wallets == 0
        assert summary.active_wallets == 0
        assert summary.inactive_wallets == 0
        assert summary.total_value_usd == Decimal("0")
        assert summary.average_value_usd == Decimal("0")
        assert summary.median_value_usd == Decimal("0")


class TestErrorHandling:
    """Test error handling scenarios."""

    @pytest.fixture
    def sheets_client_bad_config(self, mock_cache_manager) -> GoogleSheetsClient:
        """Create client with bad configuration."""
        config = GoogleSheetsConfig(
            credentials_file=Path("/nonexistent/file.json"),
            scope="invalid_scope",
        )
        return GoogleSheetsClient(config=config, cache_manager=mock_cache_manager)

    def test_sheets_not_found_error(self, sheets_client) -> None:
        """Test spreadsheet not found error."""
        with patch.object(sheets_client, "_get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_client.open_by_key.side_effect = Exception("SpreadsheetNotFound")
            mock_get_client.return_value = mock_client

            with pytest.raises(SheetsAPIError):
                sheets_client._get_worksheet("invalid_id")

    @pytest.mark.asyncio
    async def test_permission_denied_error(self, sheets_client) -> None:
        """Test permission denied error."""
        with patch.object(sheets_client, "_get_worksheet") as mock_get_worksheet:
            mock_get_worksheet.side_effect = SheetsPermissionError("Permission denied")

            with pytest.raises(SheetsPermissionError):
                sheets_client.read_wallet_addresses("restricted_sheet_id")

    @pytest.mark.asyncio
    async def test_api_error_handling(self, sheets_client) -> None:
        """Test general API error handling."""
        with patch.object(sheets_client, "_get_worksheet") as mock_get_worksheet:
            mock_get_worksheet.side_effect = Exception("General API error")

            with pytest.raises(SheetsAPIError):
                sheets_client.read_wallet_addresses("error_sheet_id")


# Integration test placeholder
class TestGoogleSheetsIntegration:
    """Integration tests for Google Sheets client."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.skipif(
        True,  # Skip by default since it requires real credentials
        reason="Integration test requires real Google Sheets credentials",
    )
    async def test_real_sheets_integration(self) -> None:
        """Test integration with real Google Sheets API."""
        # This would test with real credentials and a test spreadsheet
        # Implementation would be similar to the mocked tests above
        pass
