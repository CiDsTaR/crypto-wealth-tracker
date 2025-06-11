"""API clients package for the Ethereum Wallet Tracker.

This package provides clients for interacting with various APIs:
- Ethereum blockchain (via Alchemy)
- CoinGecko for token prices
- Google Sheets for data input/output
"""

from .coingecko_client import (
    APIError as CoinGeckoAPIError,
)
from .coingecko_client import (
    CoinGeckoClient,
    CoinGeckoClientError,
    CoinGeckoPriceService,
    RateLimitError,
)
from .coingecko_types import (
    ContractPriceResponse,
    TokenPrice,
    TokenSearchResult,
    get_coingecko_id,
    is_stablecoin,
    normalize_coingecko_price_data,
)
from .ethereum_client import APIError as EthereumAPIError
from .ethereum_client import EthereumClient, EthereumClientError, InvalidAddressError
from .ethereum_types import (
    EthBalance,
    TokenBalance,
    TokenMetadata,
    TransactionInfo,
    WalletActivity,
    WalletPortfolio,
    calculate_token_value,
    format_token_amount,
    is_valid_ethereum_address,
    normalize_address,
    wei_to_eth,
)
from .google_sheets_client import (
    GoogleSheetsClient,
    GoogleSheetsClientError,
    SheetsAPIError,
    SheetsAuthenticationError,
    SheetsNotFoundError,
    SheetsPermissionError,
)
from .google_sheets_types import (
    SheetConfig,
    SheetRange,
    SummaryData,
    WalletAddress,
    WalletResult,
    WALLET_RESULT_COLUMNS,
    WALLET_RESULT_HEADERS,
    create_summary_from_results,
    create_wallet_result_from_portfolio,
)

__all__ = [
    # Ethereum client
    "EthereumClient",
    "EthereumClientError",
    "InvalidAddressError",
    "EthereumAPIError",
    # CoinGecko client
    "CoinGeckoClient",
    "CoinGeckoPriceService",
    "CoinGeckoClientError",
    "CoinGeckoAPIError",
    "RateLimitError",
    # Google Sheets client
    "GoogleSheetsClient",
    "GoogleSheetsClientError",
    "SheetsAPIError",
    "SheetsAuthenticationError",
    "SheetsNotFoundError",
    "SheetsPermissionError",
    # Ethereum data types
    "TokenBalance",
    "EthBalance",
    "WalletPortfolio",
    "TokenMetadata",
    "TransactionInfo",
    "WalletActivity",
    # CoinGecko data types
    "TokenPrice",
    "TokenSearchResult",
    "ContractPriceResponse",
    # Google Sheets data types
    "WalletAddress",
    "WalletResult",
    "SummaryData",
    "SheetConfig",
    "SheetRange",
    "WALLET_RESULT_COLUMNS",
    "WALLET_RESULT_HEADERS",
    # Utility functions
    "normalize_address",
    "is_valid_ethereum_address",
    "wei_to_eth",
    "format_token_amount",
    "calculate_token_value",
    "get_coingecko_id",
    "normalize_coingecko_price_data",
    "is_stablecoin",
    "create_wallet_result_from_portfolio",
    "create_summary_from_results",
]