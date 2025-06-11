"""Tests for Google Sheets types and utility functions."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

from wallet_tracker.clients.google_sheets_types import (
    WALLET_RESULT_COLUMNS,
    WALLET_RESULT_HEADERS,
    SheetConfig,
    SheetRange,
    SummaryData,
    WalletAddress,
    WalletResult,
    create_summary_from_results,
    create_wallet_result_from_portfolio,
)


class TestWalletAddress:
    """Test WalletAddress dataclass."""

    def test_wallet_address_creation(self):
        """Test creating WalletAddress instance."""
        wallet_addr = WalletAddress(
            address="0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
            label="Vitalik's Wallet",
            row_number=2,
        )

        assert wallet_addr.address == "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"
        assert wallet_addr.label == "Vitalik's Wallet"
        assert wallet_addr.row_number == 2

    def test_wallet_address_minimal(self):
        """Test WalletAddress with minimal data."""
        wallet_addr = WalletAddress(
            address="0x123",
            label="Test",
            row_number=1,
        )

        assert len(wallet_addr.address) > 0
        assert len(wallet_addr.label) > 0
        assert wallet_addr.row_number >= 1


class TestWalletResult:
    """Test WalletResult dataclass."""

    def test_wallet_result_creation_full(self):
        """Test creating WalletResult with all data."""
        now = datetime.now(UTC)

        result = WalletResult(
            address="0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
            label="Test Wallet",
            eth_balance=Decimal("2.5"),
            eth_value_usd=Decimal("5000.0"),
            usdc_balance=Decimal("1000.0"),
            usdt_balance=Decimal("500.0"),
            dai_balance=Decimal("250.0"),
            aave_balance=Decimal("10.0"),
            uni_balance=Decimal("20.0"),
            link_balance=Decimal("100.0"),
            other_tokens_value_usd=Decimal("750.0"),
            total_value_usd=Decimal("6500.0"),
            last_updated=now,
            transaction_count=150,
            is_active=True,
        )

        assert result.address == "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"
        assert result.label == "Test Wallet"
        assert result.eth_balance == Decimal("2.5")
        assert result.eth_value_usd == Decimal("5000.0")
        assert result.usdc_balance == Decimal("1000.0")
        assert result.usdt_balance == Decimal("500.0")
        assert result.dai_balance == Decimal("250.0")
        assert result.aave_balance == Decimal("10.0")
        assert result.uni_balance == Decimal("20.0")
        assert result.link_balance == Decimal("100.0")
        assert result.other_tokens_value_usd == Decimal("750.0")
        assert result.total_value_usd == Decimal("6500.0")
        assert result.last_updated == now
        assert result.transaction_count == 150
        assert result.is_active is True

    def test_wallet_result_creation_minimal(self):
        """Test creating WalletResult with minimal data."""
        now = datetime.now(UTC)

        result = WalletResult(
            address="0x123",
            label="Empty Wallet",
            eth_balance=Decimal("0"),
            eth_value_usd=Decimal("0"),
            usdc_balance=Decimal("0"),
            usdt_balance=Decimal("0"),
            dai_balance=Decimal("0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("0"),
            total_value_usd=Decimal("0"),
            last_updated=now,
            transaction_count=0,
            is_active=False,
        )

        assert result.total_value_usd == Decimal("0")
        assert result.transaction_count == 0
        assert result.is_active is False

    def test_wallet_result_calculations(self):
        """Test wallet result value calculations."""
        now = datetime.now(UTC)

        result = WalletResult(
            address="0x123",
            label="Test Wallet",
            eth_balance=Decimal("1.0"),
            eth_value_usd=Decimal("2000.0"),
            usdc_balance=Decimal("1000.0"),
            usdt_balance=Decimal("0"),
            dai_balance=Decimal("0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("500.0"),
            total_value_usd=Decimal("2500.0"),  # ETH + USDC (assuming $1) + other
            last_updated=now,
            transaction_count=50,
            is_active=True,
        )

        # Total should be sum of ETH value + stablecoin values + other tokens
        expected_total = result.eth_value_usd + result.usdc_balance + result.other_tokens_value_usd
        assert result.total_value_usd == expected_total


class TestSummaryData:
    """Test SummaryData dataclass."""

    def test_summary_data_creation(self):
        """Test creating SummaryData instance."""
        now = datetime.now(UTC)

        summary = SummaryData(
            total_wallets=100,
            active_wallets=80,
            inactive_wallets=20,
            total_value_usd=Decimal("1000000.0"),
            average_value_usd=Decimal("10000.0"),
            median_value_usd=Decimal("5000.0"),
            eth_total_value=Decimal("500000.0"),
            eth_holders=75,
            usdc_total_value=Decimal("300000.0"),
            usdc_holders=60,
            usdt_total_value=Decimal("150000.0"),
            usdt_holders=40,
            dai_total_value=Decimal("50000.0"),
            dai_holders=25,
            analysis_time=now,
            processing_time="5 minutes 30 seconds",
        )

        assert summary.total_wallets == 100
        assert summary.active_wallets == 80
        assert summary.inactive_wallets == 20
        assert summary.total_value_usd == Decimal("1000000.0")
        assert summary.average_value_usd == Decimal("10000.0")
        assert summary.median_value_usd == Decimal("5000.0")
        assert summary.eth_total_value == Decimal("500000.0")
        assert summary.eth_holders == 75
        assert summary.processing_time == "5 minutes 30 seconds"

    def test_summary_data_consistency(self):
        """Test SummaryData consistency checks."""
        now = datetime.now(UTC)

        summary = SummaryData(
            total_wallets=100,
            active_wallets=70,
            inactive_wallets=30,
            total_value_usd=Decimal("500000.0"),
            average_value_usd=Decimal("5000.0"),
            median_value_usd=Decimal("3000.0"),
            eth_total_value=Decimal("300000.0"),
            eth_holders=80,
            usdc_total_value=Decimal("150000.0"),
            usdc_holders=50,
            usdt_total_value=Decimal("50000.0"),
            usdt_holders=30,
            dai_total_value=Decimal("0"),
            dai_holders=0,
            analysis_time=now,
            processing_time="3 minutes",
        )

        # Active + inactive should equal total
        assert summary.active_wallets + summary.inactive_wallets == summary.total_wallets

        # Average should be total / count (approximately)
        expected_average = summary.total_value_usd / summary.total_wallets
        assert summary.average_value_usd == expected_average


class TestSheetConfig:
    """Test SheetConfig dataclass."""

    def test_sheet_config_defaults(self):
        """Test SheetConfig with default values."""
        config = SheetConfig(spreadsheet_id="test_id")

        assert config.spreadsheet_id == "test_id"
        assert config.input_worksheet is None
        assert config.output_worksheet is None
        assert config.input_range == "A:B"
        assert config.output_range == "A1"
        assert config.skip_header is True
        assert config.include_header is True
        assert config.clear_existing is True
        assert config.batch_size == 100

    def test_sheet_config_custom_values(self):
        """Test SheetConfig with custom values."""
        config = SheetConfig(
            spreadsheet_id="custom_id",
            input_worksheet="Input",
            output_worksheet="Output",
            input_range="B:C",
            output_range="B2",
            skip_header=False,
            include_header=False,
            clear_existing=False,
            batch_size=50,
        )

        assert config.spreadsheet_id == "custom_id"
        assert config.input_worksheet == "Input"
        assert config.output_worksheet == "Output"
        assert config.input_range == "B:C"
        assert config.output_range == "B2"
        assert config.skip_header is False
        assert config.include_header is False
        assert config.clear_existing is False
        assert config.batch_size == 50


class TestSheetRange:
    """Test SheetRange utility class."""

    def test_column_to_letter_single_letters(self):
        """Test column number to letter conversion for single letters."""
        assert SheetRange.column_to_letter(1) == "A"
        assert SheetRange.column_to_letter(2) == "B"
        assert SheetRange.column_to_letter(26) == "Z"

    def test_column_to_letter_double_letters(self):
        """Test column number to letter conversion for double letters."""
        assert SheetRange.column_to_letter(27) == "AA"
        assert SheetRange.column_to_letter(28) == "AB"
        assert SheetRange.column_to_letter(52) == "AZ"
        assert SheetRange.column_to_letter(53) == "BA"

    def test_column_to_letter_triple_letters(self):
        """Test column number to letter conversion for triple letters."""
        assert SheetRange.column_to_letter(702) == "ZZ"
        assert SheetRange.column_to_letter(703) == "AAA"

    def test_letter_to_column_single_letters(self):
        """Test letter to column number conversion for single letters."""
        assert SheetRange.letter_to_column("A") == 1
        assert SheetRange.letter_to_column("B") == 2
        assert SheetRange.letter_to_column("Z") == 26

    def test_letter_to_column_double_letters(self):
        """Test letter to column number conversion for double letters."""
        assert SheetRange.letter_to_column("AA") == 27
        assert SheetRange.letter_to_column("AB") == 28
        assert SheetRange.letter_to_column("AZ") == 52
        assert SheetRange.letter_to_column("BA") == 53

    def test_letter_to_column_case_insensitive(self):
        """Test letter to column conversion is case insensitive."""
        assert SheetRange.letter_to_column("a") == 1
        assert SheetRange.letter_to_column("aa") == 27
        assert SheetRange.letter_to_column("aA") == 27

    def test_column_letter_roundtrip(self):
        """Test that column number to letter and back is consistent."""
        for i in range(1, 1000):
            letter = SheetRange.column_to_letter(i)
            number = SheetRange.letter_to_column(letter)
            assert number == i

    def test_parse_range_single_cell(self):
        """Test parsing single cell range."""
        start_col, start_row, end_col, end_row = SheetRange.parse_range("A1")
        assert start_col == "A"
        assert start_row == 1
        assert end_col == "A"
        assert end_row == 1

        start_col, start_row, end_col, end_row = SheetRange.parse_range("Z100")
        assert start_col == "Z"
        assert start_row == 100
        assert end_col == "Z"
        assert end_row == 100

    def test_parse_range_cell_range(self):
        """Test parsing cell range."""
        start_col, start_row, end_col, end_row = SheetRange.parse_range("A1:C10")
        assert start_col == "A"
        assert start_row == 1
        assert end_col == "C"
        assert end_row == 10

        start_col, start_row, end_col, end_row = SheetRange.parse_range("B5:Z100")
        assert start_col == "B"
        assert start_row == 5
        assert end_col == "Z"
        assert end_row == 100

    def test_parse_range_complex(self):
        """Test parsing complex ranges."""
        start_col, start_row, end_col, end_row = SheetRange.parse_range("AA10:ZZ999")
        assert start_col == "AA"
        assert start_row == 10
        assert end_col == "ZZ"
        assert end_row == 999

    def test_build_range(self):
        """Test building range strings."""
        range_str = SheetRange.build_range("A", 1, "C", 10)
        assert range_str == "A1:C10"

        range_str = SheetRange.build_range("AA", 5, "ZZ", 100)
        assert range_str == "AA5:ZZ100"

    def test_expand_range_rows_only(self):
        """Test expanding range by rows only."""
        expanded = SheetRange.expand_range("A1:C10", 5)
        assert expanded == "A1:C15"

        expanded = SheetRange.expand_range("B2:D5", 10)
        assert expanded == "B2:D15"

    def test_expand_range_rows_and_cols(self):
        """Test expanding range by rows and columns."""
        expanded = SheetRange.expand_range("A1:C10", 5, 2)
        assert expanded == "A1:E15"

        expanded = SheetRange.expand_range("B2:D5", 3, 1)
        assert expanded == "B2:E8"

    def test_expand_range_single_cell(self):
        """Test expanding single cell range."""
        expanded = SheetRange.expand_range("A1", 5, 2)
        assert expanded == "A1:C6"

    def test_parse_build_roundtrip(self):
        """Test that parse and build are consistent."""
        original_ranges = [
            "A1:C10",
            "B5:Z100",
            "AA10:ZZ999",
            "A1",
            "Z100",
        ]

        for original in original_ranges:
            start_col, start_row, end_col, end_row = SheetRange.parse_range(original)
            rebuilt = SheetRange.build_range(start_col, start_row, end_col, end_row)
            assert rebuilt == original


class TestWalletResultColumns:
    """Test wallet result column mappings."""

    def test_wallet_result_columns_completeness(self):
        """Test that all expected columns are defined."""
        expected_columns = [
            "address",
            "label",
            "eth_balance",
            "eth_value_usd",
            "usdc_balance",
            "usdt_balance",
            "dai_balance",
            "aave_balance",
            "uni_balance",
            "link_balance",
            "other_tokens_value_usd",
            "total_value_usd",
            "last_updated",
            "transaction_count",
            "is_active",
        ]

        for column in expected_columns:
            assert column in WALLET_RESULT_COLUMNS
            assert isinstance(WALLET_RESULT_COLUMNS[column], str)
            assert len(WALLET_RESULT_COLUMNS[column]) == 1  # Should be single letter

    def test_wallet_result_columns_unique(self):
        """Test that column letters are unique."""
        columns = list(WALLET_RESULT_COLUMNS.values())
        assert len(columns) == len(set(columns))

    def test_wallet_result_columns_sequential(self):
        """Test that columns are in sequential order."""
        columns = list(WALLET_RESULT_COLUMNS.values())
        expected_sequence = [chr(ord("A") + i) for i in range(len(columns))]
        assert columns == expected_sequence

    def test_wallet_result_headers_count(self):
        """Test that headers match columns count."""
        assert len(WALLET_RESULT_HEADERS) == len(WALLET_RESULT_COLUMNS)

    def test_wallet_result_headers_content(self):
        """Test wallet result headers content."""
        expected_headers = [
            "Address",
            "Label",
            "ETH Balance",
            "ETH Value (USD)",
            "USDC Balance",
            "USDT Balance",
            "DAI Balance",
            "AAVE Balance",
            "UNI Balance",
            "LINK Balance",
            "Other Tokens Value (USD)",
            "Total Value (USD)",
            "Last Updated",
            "Transaction Count",
            "Is Active",
        ]

        assert expected_headers == WALLET_RESULT_HEADERS

    def test_wallet_result_headers_no_empty(self):
        """Test that no headers are empty."""
        for header in WALLET_RESULT_HEADERS:
            assert isinstance(header, str)
            assert len(header.strip()) > 0


class TestCreateWalletResultFromPortfolio:
    """Test create_wallet_result_from_portfolio function."""

    def test_create_wallet_result_from_portfolio_basic(self):
        """Test creating wallet result from basic portfolio."""
        # Mock portfolio
        mock_portfolio = MagicMock()
        mock_portfolio.eth_balance.balance_eth = Decimal("2.0")
        mock_portfolio.eth_balance.value_usd = Decimal("4000.0")
        mock_portfolio.total_value_usd = Decimal("5000.0")
        mock_portfolio.last_updated = datetime.now(UTC)
        mock_portfolio.transaction_count = 100
        mock_portfolio.token_balances = []

        result = create_wallet_result_from_portfolio(
            address="0x123", label="Test Wallet", portfolio=mock_portfolio, is_active=True
        )

        assert result.address == "0x123"
        assert result.label == "Test Wallet"
        assert result.eth_balance == Decimal("2.0")
        assert result.eth_value_usd == Decimal("4000.0")
        assert result.total_value_usd == Decimal("5000.0")
        assert result.transaction_count == 100
        assert result.is_active is True

    def test_create_wallet_result_with_tokens(self):
        """Test creating wallet result with token balances."""
        # Mock portfolio with tokens
        mock_portfolio = MagicMock()
        mock_portfolio.eth_balance.balance_eth = Decimal("1.0")
        mock_portfolio.eth_balance.value_usd = Decimal("2000.0")
        mock_portfolio.total_value_usd = Decimal("4000.0")
        mock_portfolio.last_updated = datetime.now(UTC)
        mock_portfolio.transaction_count = 50

        # Mock token balances
        usdc_token = MagicMock()
        usdc_token.symbol = "USDC"
        usdc_token.balance_formatted = Decimal("1000.0")
        usdc_token.value_usd = Decimal("1000.0")

        aave_token = MagicMock()
        aave_token.symbol = "AAVE"
        aave_token.balance_formatted = Decimal("5.0")
        aave_token.value_usd = Decimal("500.0")

        other_token = MagicMock()
        other_token.symbol = "OTHER"
        other_token.balance_formatted = Decimal("100.0")
        other_token.value_usd = Decimal("500.0")

        mock_portfolio.token_balances = [usdc_token, aave_token, other_token]

        result = create_wallet_result_from_portfolio(
            address="0x123", label="Rich Wallet", portfolio=mock_portfolio, is_active=True
        )

        assert result.usdc_balance == Decimal("1000.0")
        assert result.aave_balance == Decimal("5.0")
        assert result.other_tokens_value_usd == Decimal("500.0")  # OTHER token value

    def test_create_wallet_result_missing_token_values(self):
        """Test creating wallet result with tokens missing values."""
        mock_portfolio = MagicMock()
        mock_portfolio.eth_balance.balance_eth = Decimal("1.0")
        mock_portfolio.eth_balance.value_usd = None  # Missing value
        mock_portfolio.total_value_usd = Decimal("1000.0")
        mock_portfolio.last_updated = datetime.now(UTC)
        mock_portfolio.transaction_count = 25

        # Token without value
        token_without_value = MagicMock()
        token_without_value.symbol = "UNKNOWN"
        token_without_value.balance_formatted = Decimal("100.0")
        token_without_value.value_usd = None

        mock_portfolio.token_balances = [token_without_value]

        result = create_wallet_result_from_portfolio(
            address="0x123", label="Test Wallet", portfolio=mock_portfolio, is_active=False
        )

        assert result.eth_value_usd == Decimal("0")  # Should default to 0
        assert result.other_tokens_value_usd == Decimal("0")  # No value to add


class TestCreateSummaryFromResults:
    """Test create_summary_from_results function."""

    def test_create_summary_from_empty_results(self):
        """Test creating summary from empty results."""
        summary = create_summary_from_results([], "0 seconds")

        assert summary.total_wallets == 0
        assert summary.active_wallets == 0
        assert summary.inactive_wallets == 0
        assert summary.total_value_usd == Decimal("0")
        assert summary.average_value_usd == Decimal("0")
        assert summary.median_value_usd == Decimal("0")
        assert summary.eth_total_value == Decimal("0")
        assert summary.eth_holders == 0
        assert summary.processing_time == "0 seconds"

    def test_create_summary_from_single_result(self):
        """Test creating summary from single wallet result."""
        now = datetime.now(UTC)

        wallet_result = WalletResult(
            address="0x123",
            label="Single Wallet",
            eth_balance=Decimal("2.0"),
            eth_value_usd=Decimal("4000.0"),
            usdc_balance=Decimal("1000.0"),
            usdt_balance=Decimal("0"),
            dai_balance=Decimal("0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("0"),
            total_value_usd=Decimal("5000.0"),
            last_updated=now,
            transaction_count=100,
            is_active=True,
        )

        summary = create_summary_from_results([wallet_result], "30 seconds")

        assert summary.total_wallets == 1
        assert summary.active_wallets == 1
        assert summary.inactive_wallets == 0
        assert summary.total_value_usd == Decimal("5000.0")
        assert summary.average_value_usd == Decimal("5000.0")
        assert summary.median_value_usd == Decimal("5000.0")
        assert summary.eth_total_value == Decimal("4000.0")
        assert summary.eth_holders == 1
        assert summary.usdc_holders == 1
        assert summary.processing_time == "30 seconds"

    def test_create_summary_from_multiple_results(self):
        """Test creating summary from multiple wallet results."""
        now = datetime.now(UTC)

        wallet1 = WalletResult(
            address="0x123",
            label="Wallet 1",
            eth_balance=Decimal("2.0"),
            eth_value_usd=Decimal("4000.0"),
            usdc_balance=Decimal("1000.0"),
            usdt_balance=Decimal("0"),
            dai_balance=Decimal("500.0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("0"),
            total_value_usd=Decimal("5500.0"),
            last_updated=now,
            transaction_count=100,
            is_active=True,
        )

        wallet2 = WalletResult(
            address="0x456",
            label="Wallet 2",
            eth_balance=Decimal("0.5"),
            eth_value_usd=Decimal("1000.0"),
            usdc_balance=Decimal("0"),
            usdt_balance=Decimal("2000.0"),
            dai_balance=Decimal("0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("500.0"),
            total_value_usd=Decimal("3500.0"),
            last_updated=now,
            transaction_count=50,
            is_active=False,
        )

        wallet3 = WalletResult(
            address="0x789",
            label="Wallet 3",
            eth_balance=Decimal("1.0"),
            eth_value_usd=Decimal("2000.0"),
            usdc_balance=Decimal("500.0"),
            usdt_balance=Decimal("0"),
            dai_balance=Decimal("0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("0"),
            total_value_usd=Decimal("2500.0"),
            last_updated=now,
            transaction_count=75,
            is_active=True,
        )

        results = [wallet1, wallet2, wallet3]
        summary = create_summary_from_results(results, "2 minutes")

        assert summary.total_wallets == 3
        assert summary.active_wallets == 2
        assert summary.inactive_wallets == 1
        assert summary.total_value_usd == Decimal("11500.0")  # 5500 + 3500 + 2500
        assert summary.average_value_usd == Decimal("3833.333333333333333333333333")  # 11500 / 3
        assert summary.median_value_usd == Decimal("3500.0")  # Middle value when sorted: 2500, 3500, 5500
        assert summary.eth_total_value == Decimal("7000.0")  # 4000 + 1000 + 2000
        assert summary.eth_holders == 3  # All have ETH > 0
        assert summary.usdc_holders == 2  # wallet1 and wallet3
        assert summary.usdt_holders == 1  # wallet2 only
        assert summary.dai_holders == 1  # wallet1 only

    def test_create_summary_median_calculation_even_count(self):
        """Test median calculation with even number of wallets."""
        now = datetime.now(UTC)

        # Create 4 wallets with values: 1000, 2000, 3000, 4000
        # Median should be (2000 + 3000) / 2 = 2500
        wallets = []
        for i in range(4):
            value = Decimal(str((i + 1) * 1000))
            wallet = WalletResult(
                address=f"0x{i}",
                label=f"Wallet {i}",
                eth_balance=Decimal("1.0"),
                eth_value_usd=value,
                usdc_balance=Decimal("0"),
                usdt_balance=Decimal("0"),
                dai_balance=Decimal("0"),
                aave_balance=Decimal("0"),
                uni_balance=Decimal("0"),
                link_balance=Decimal("0"),
                other_tokens_value_usd=Decimal("0"),
                total_value_usd=value,
                last_updated=now,
                transaction_count=10,
                is_active=True,
            )
            wallets.append(wallet)

        summary = create_summary_from_results(wallets, "1 minute")

        assert summary.median_value_usd == Decimal("2500")  # (2000 + 3000) / 2

    def test_create_summary_stablecoin_assumptions(self):
        """Test that summary uses $1 assumption for stablecoin values."""
        now = datetime.now(UTC)

        wallet = WalletResult(
            address="0x123",
            label="Stablecoin Wallet",
            eth_balance=Decimal("0"),
            eth_value_usd=Decimal("0"),
            usdc_balance=Decimal("1000.0"),
            usdt_balance=Decimal("2000.0"),
            dai_balance=Decimal("500.0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("0"),
            total_value_usd=Decimal("3500.0"),
            last_updated=now,
            transaction_count=20,
            is_active=True,
        )

        summary = create_summary_from_results([wallet], "15 seconds")

        # Each stablecoin should be valued at $1 for summary calculations
        assert summary.usdc_total_value == Decimal("1000.0")  # 1000 * $1
        assert summary.usdt_total_value == Decimal("2000.0")  # 2000 * $1
        assert summary.dai_total_value == Decimal("500.0")  # 500 * $1


class TestTypeValidation:
    """Test type validation and edge cases."""

    def test_decimal_precision_handling(self):
        """Test handling of high precision decimals."""
        now = datetime.now(UTC)

        # Very precise decimal values
        result = WalletResult(
            address="0x123",
            label="Precise Wallet",
            eth_balance=Decimal("1.123456789012345678"),
            eth_value_usd=Decimal("2246.913578024691358"),
            usdc_balance=Decimal("999.999999"),
            usdt_balance=Decimal("0"),
            dai_balance=Decimal("0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("0"),
            total_value_usd=Decimal("3246.913578024691358"),
            last_updated=now,
            transaction_count=1,
            is_active=True,
        )

        # Should preserve precision
        assert isinstance(result.eth_balance, Decimal)
        assert isinstance(result.eth_value_usd, Decimal)
        assert result.eth_balance.as_tuple().exponent == -18
        assert result.usdc_balance.as_tuple().exponent == -6

    def test_zero_values_handling(self):
        """Test handling of zero values in calculations."""
        now = datetime.now(UTC)

        # All zero wallet
        zero_wallet = WalletResult(
            address="0x000",
            label="Empty Wallet",
            eth_balance=Decimal("0"),
            eth_value_usd=Decimal("0"),
            usdc_balance=Decimal("0"),
            usdt_balance=Decimal("0"),
            dai_balance=Decimal("0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("0"),
            total_value_usd=Decimal("0"),
            last_updated=now,
            transaction_count=0,
            is_active=False,
        )

        summary = create_summary_from_results([zero_wallet], "5 seconds")

        assert summary.total_value_usd == Decimal("0")
        assert summary.average_value_usd == Decimal("0")
        assert summary.median_value_usd == Decimal("0")
        assert summary.eth_holders == 0
        assert summary.usdc_holders == 0

    def test_datetime_timezone_handling(self):
        """Test datetime timezone handling."""
        # UTC datetime
        utc_time = datetime.now(UTC)

        result = WalletResult(
            address="0x123",
            label="Test",
            eth_balance=Decimal("1"),
            eth_value_usd=Decimal("2000"),
            usdc_balance=Decimal("0"),
            usdt_balance=Decimal("0"),
            dai_balance=Decimal("0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("0"),
            total_value_usd=Decimal("2000"),
            last_updated=utc_time,
            transaction_count=1,
            is_active=True,
        )

        # Should preserve timezone info
        assert result.last_updated.tzinfo is not None
        assert result.last_updated.tzinfo == UTC

    def test_large_numbers_handling(self):
        """Test handling of very large numbers."""
        now = datetime.now(UTC)

        # Very large values (whale wallet)
        whale_wallet = WalletResult(
            address="0xwhale",
            label="Whale Wallet",
            eth_balance=Decimal("100000"),  # 100k ETH
            eth_value_usd=Decimal("200000000"),  # $200M
            usdc_balance=Decimal("50000000"),  # $50M USDC
            usdt_balance=Decimal("0"),
            dai_balance=Decimal("0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("0"),
            total_value_usd=Decimal("250000000"),  # $250M total
            last_updated=now,
            transaction_count=10000,
            is_active=True,
        )

        summary = create_summary_from_results([whale_wallet], "10 minutes")

        assert summary.total_value_usd == Decimal("250000000")
        assert summary.average_value_usd == Decimal("250000000")
        assert summary.eth_total_value == Decimal("200000000")

    def test_string_field_validation(self):
        """Test string field validation and edge cases."""
        now = datetime.now(UTC)

        # Test with various string inputs
        result = WalletResult(
            address="0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",  # Mixed case
            label="Test Wallet with Special Chars !@#$%",
            eth_balance=Decimal("1"),
            eth_value_usd=Decimal("2000"),
            usdc_balance=Decimal("0"),
            usdt_balance=Decimal("0"),
            dai_balance=Decimal("0"),
            aave_balance=Decimal("0"),
            uni_balance=Decimal("0"),
            link_balance=Decimal("0"),
            other_tokens_value_usd=Decimal("0"),
            total_value_usd=Decimal("2000"),
            last_updated=now,
            transaction_count=1,
            is_active=True,
        )

        assert len(result.address) == 42  # Standard Ethereum address length
        assert result.address.startswith("0x")
        assert len(result.label) > 0
