"""Type definitions for Google Sheets data structures."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass
class WalletAddress:
    """Wallet address information from Google Sheets."""

    address: str
    label: str
    row_number: int


@dataclass
class WalletResult:
    """Wallet analysis result for Google Sheets output."""

    address: str
    label: str
    eth_balance: Decimal
    eth_value_usd: Decimal
    usdc_balance: Decimal
    usdt_balance: Decimal
    dai_balance: Decimal
    aave_balance: Decimal
    uni_balance: Decimal
    link_balance: Decimal
    other_tokens_value_usd: Decimal
    total_value_usd: Decimal
    last_updated: datetime
    transaction_count: int
    is_active: bool


@dataclass
class SummaryData:
    """Summary statistics for Google Sheets summary sheet."""

    total_wallets: int
    active_wallets: int
    inactive_wallets: int
    total_value_usd: Decimal
    average_value_usd: Decimal
    median_value_usd: Decimal
    eth_total_value: Decimal
    eth_holders: int
    usdc_total_value: Decimal
    usdc_holders: int
    usdt_total_value: Decimal
    usdt_holders: int
    dai_total_value: Decimal
    dai_holders: int
    analysis_time: datetime
    processing_time: str


@dataclass
class SheetConfig:
    """Configuration for Google Sheets operations."""

    spreadsheet_id: str
    input_worksheet: str | None = None
    output_worksheet: str | None = None
    input_range: str = "A:B"
    output_range: str = "A1"
    skip_header: bool = True
    include_header: bool = True
    clear_existing: bool = True
    batch_size: int = 100


# Sheet range utilities
class SheetRange:
    """Utility class for working with Google Sheets ranges."""

    @staticmethod
    def column_to_letter(column_number: int) -> str:
        """Convert column number to letter (1 -> A, 26 -> Z, 27 -> AA, etc.)."""
        result = ""
        while column_number > 0:
            column_number -= 1
            result = chr(column_number % 26 + ord("A")) + result
            column_number //= 26
        return result

    @staticmethod
    def letter_to_column(column_letter: str) -> int:
        """Convert column letter to number (A -> 1, Z -> 26, AA -> 27, etc.)."""
        result = 0
        for char in column_letter.upper():
            result = result * 26 + (ord(char) - ord("A") + 1)
        return result

    @staticmethod
    def parse_range(range_str: str) -> tuple[str, int, str, int]:
        """Parse A1 notation range into components.

        Args:
            range_str: Range in A1 notation (e.g., "A1:C10")

        Returns:
            Tuple of (start_col, start_row, end_col, end_row)
        """
        if ":" not in range_str:
            # Single cell
            col = ""
            row = ""
            for char in range_str:
                if char.isalpha():
                    col += char
                else:
                    row += char
            return col, int(row), col, int(row)

        start_cell, end_cell = range_str.split(":")

        # Parse start cell
        start_col = ""
        start_row = ""
        for char in start_cell:
            if char.isalpha():
                start_col += char
            else:
                start_row += char

        # Parse end cell
        end_col = ""
        end_row = ""
        for char in end_cell:
            if char.isalpha():
                end_col += char
            else:
                end_row += char

        return start_col, int(start_row), end_col, int(end_row)

    @staticmethod
    def build_range(start_col: str, start_row: int, end_col: str, end_row: int) -> str:
        """Build A1 notation range from components."""
        return f"{start_col}{start_row}:{end_col}{end_row}"

    @staticmethod
    def expand_range(range_str: str, rows: int, cols: int = 0) -> str:
        """Expand a range by the specified number of rows and columns."""
        start_col, start_row, end_col, end_row = SheetRange.parse_range(range_str)

        new_end_row = end_row + rows
        if cols > 0:
            end_col_num = SheetRange.letter_to_column(end_col)
            new_end_col = SheetRange.column_to_letter(end_col_num + cols)
        else:
            new_end_col = end_col

        return SheetRange.build_range(start_col, start_row, new_end_col, new_end_row)


# Standard column mappings for wallet results
WALLET_RESULT_COLUMNS = {
    "address": "A",
    "label": "B",
    "eth_balance": "C",
    "eth_value_usd": "D",
    "usdc_balance": "E",
    "usdt_balance": "F",
    "dai_balance": "G",
    "aave_balance": "H",
    "uni_balance": "I",
    "link_balance": "J",
    "other_tokens_value_usd": "K",
    "total_value_usd": "L",
    "last_updated": "M",
    "transaction_count": "N",
    "is_active": "O",
}

WALLET_RESULT_HEADERS = [
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


def create_wallet_result_from_portfolio(
    address: str,
    label: str,
    portfolio: Any,  # WalletPortfolio from ethereum_types
    is_active: bool = True,
) -> WalletResult:
    """Create WalletResult from WalletPortfolio.

    Args:
        address: Wallet address
        label: Wallet label
        portfolio: WalletPortfolio object
        is_active: Whether wallet is considered active

    Returns:
        WalletResult object
    """

    # Extract token balances by symbol
    token_balances = {}
    other_tokens_value = Decimal("0")

    for token in portfolio.token_balances:
        symbol = token.symbol.upper()
        if symbol in ["USDC", "USDT", "DAI", "AAVE", "UNI", "LINK"]:
            token_balances[symbol] = token.balance_formatted
        else:
            # Add to other tokens value
            if token.value_usd:
                other_tokens_value += token.value_usd

    return WalletResult(
        address=address,
        label=label,
        eth_balance=portfolio.eth_balance.balance_eth,
        eth_value_usd=portfolio.eth_balance.value_usd or Decimal("0"),
        usdc_balance=token_balances.get("USDC", Decimal("0")),
        usdt_balance=token_balances.get("USDT", Decimal("0")),
        dai_balance=token_balances.get("DAI", Decimal("0")),
        aave_balance=token_balances.get("AAVE", Decimal("0")),
        uni_balance=token_balances.get("UNI", Decimal("0")),
        link_balance=token_balances.get("LINK", Decimal("0")),
        other_tokens_value_usd=other_tokens_value,
        total_value_usd=portfolio.total_value_usd,
        last_updated=portfolio.last_updated,
        transaction_count=portfolio.transaction_count,
        is_active=is_active,
    )


def create_summary_from_results(wallet_results: list[WalletResult], processing_time: str) -> SummaryData:
    """Create SummaryData from list of WalletResults.

    Args:
        wallet_results: List of wallet analysis results
        processing_time: Human-readable processing time

    Returns:
        SummaryData object
    """
    if not wallet_results:
        return SummaryData(
            total_wallets=0,
            active_wallets=0,
            inactive_wallets=0,
            total_value_usd=Decimal("0"),
            average_value_usd=Decimal("0"),
            median_value_usd=Decimal("0"),
            eth_total_value=Decimal("0"),
            eth_holders=0,
            usdc_total_value=Decimal("0"),
            usdc_holders=0,
            usdt_total_value=Decimal("0"),
            usdt_holders=0,
            dai_total_value=Decimal("0"),
            dai_holders=0,
            analysis_time=datetime.now(),
            processing_time=processing_time,
        )

    # Calculate basic statistics
    total_wallets = len(wallet_results)
    active_wallets = sum(1 for result in wallet_results if result.is_active)
    inactive_wallets = total_wallets - active_wallets

    # Calculate value statistics
    total_values = [result.total_value_usd for result in wallet_results]
    total_value_usd = sum(total_values)
    average_value_usd = total_value_usd / total_wallets if total_wallets > 0 else Decimal("0")

    # Calculate median
    sorted_values = sorted(total_values)
    n = len(sorted_values)
    if n == 0:
        median_value_usd = Decimal("0")
    elif n % 2 == 0:
        median_value_usd = (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
    else:
        median_value_usd = sorted_values[n // 2]

    # Calculate token-specific statistics
    eth_total_value = sum(result.eth_value_usd for result in wallet_results)
    eth_holders = sum(1 for result in wallet_results if result.eth_balance > 0)

    usdc_total_value = sum(
        result.usdc_balance * Decimal("1.0") for result in wallet_results if result.usdc_balance > 0
    )  # Assume USDC = $1 for summary
    usdc_holders = sum(1 for result in wallet_results if result.usdc_balance > 0)

    usdt_total_value = sum(
        result.usdt_balance * Decimal("1.0") for result in wallet_results if result.usdt_balance > 0
    )  # Assume USDT = $1 for summary
    usdt_holders = sum(1 for result in wallet_results if result.usdt_balance > 0)

    dai_total_value = sum(
        result.dai_balance * Decimal("1.0") for result in wallet_results if result.dai_balance > 0
    )  # Assume DAI = $1 for summary
    dai_holders = sum(1 for result in wallet_results if result.dai_balance > 0)

    return SummaryData(
        total_wallets=total_wallets,
        active_wallets=active_wallets,
        inactive_wallets=inactive_wallets,
        total_value_usd=total_value_usd,
        average_value_usd=average_value_usd,
        median_value_usd=median_value_usd,
        eth_total_value=eth_total_value,
        eth_holders=eth_holders,
        usdc_total_value=usdc_total_value,
        usdc_holders=usdc_holders,
        usdt_total_value=usdt_total_value,
        usdt_holders=usdt_holders,
        dai_total_value=dai_total_value,
        dai_holders=dai_holders,
        analysis_time=datetime.now(),
        processing_time=processing_time,
    )
