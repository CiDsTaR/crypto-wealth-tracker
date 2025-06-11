"""Type definitions for Ethereum client data structures."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass
class TokenBalance:
    """Token balance information."""

    contract_address: str
    symbol: str
    name: str
    decimals: int
    balance_raw: str
    balance_formatted: Decimal
    price_usd: Decimal | None = None
    value_usd: Decimal | None = None
    logo_url: str | None = None
    is_verified: bool = False


@dataclass
class EthBalance:
    """ETH balance information."""

    balance_wei: str
    balance_eth: Decimal
    price_usd: Decimal | None = None
    value_usd: Decimal | None = None


@dataclass
class WalletPortfolio:
    """Complete wallet portfolio information."""

    address: str
    eth_balance: EthBalance
    token_balances: list[TokenBalance]
    total_value_usd: Decimal
    last_updated: datetime
    transaction_count: int
    last_transaction_hash: str | None = None
    last_transaction_timestamp: datetime | None = None


@dataclass
class TokenMetadata:
    """Token metadata information."""

    contract_address: str
    symbol: str
    name: str
    decimals: int
    logo_url: str | None = None
    is_verified: bool = False
    price_usd: Decimal | None = None
    market_cap_usd: Decimal | None = None
    volume_24h_usd: Decimal | None = None


@dataclass
class TransactionInfo:
    """Transaction information."""

    hash: str
    block_number: int
    timestamp: datetime
    from_address: str
    to_address: str
    value_wei: str
    value_eth: Decimal
    gas_used: int
    gas_price_wei: str
    status: str  # "success" or "failed"


@dataclass
class WalletActivity:
    """Wallet activity summary."""

    address: str
    first_transaction: TransactionInfo | None
    last_transaction: TransactionInfo | None
    transaction_count: int
    total_gas_used: int
    is_active: bool  # Active in last X days based on threshold
    days_since_last_transaction: int | None = None


@dataclass
class AlchemyPortfolioResponse:
    """Raw response from Alchemy Portfolio API."""

    address: str
    tokenBalances: list[dict[str, Any]]
    pageKey: str | None = None


@dataclass
class AlchemyTokenMetadataResponse:
    """Raw response from Alchemy Token Metadata API."""

    tokens: list[dict[str, Any]]


@dataclass
class AlchemyPriceResponse:
    """Raw response from Alchemy Token Prices API."""

    data: list[dict[str, Any]]


# Constants for well-known tokens
WELL_KNOWN_TOKENS = {
    # Stablecoins
    "0xA0b86a33E6441e94bB0a8d0F7E5F8D69E2C0e5a0": {  # USDC
        "symbol": "USDC",
        "name": "USD Coin",
        "decimals": 6,
        "is_verified": True,
    },
    "0xdAC17F958D2ee523a2206206994597C13D831ec7": {  # USDT
        "symbol": "USDT",
        "name": "Tether USD",
        "decimals": 6,
        "is_verified": True,
    },
    "0x6B175474E89094C44Da98b954EedeAC495271d0F": {  # DAI
        "symbol": "DAI",
        "name": "Dai Stablecoin",
        "decimals": 18,
        "is_verified": True,
    },
    # DeFi Tokens
    "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9": {  # AAVE
        "symbol": "AAVE",
        "name": "Aave Token",
        "decimals": 18,
        "is_verified": True,
    },
    "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984": {  # UNI
        "symbol": "UNI",
        "name": "Uniswap",
        "decimals": 18,
        "is_verified": True,
    },
    "0x514910771AF9Ca656af840dff83E8264EcF986CA": {  # LINK
        "symbol": "LINK",
        "name": "ChainLink Token",
        "decimals": 18,
        "is_verified": True,
    },
    # Wrapped ETH
    "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": {  # WETH
        "symbol": "WETH",
        "name": "Wrapped Ether",
        "decimals": 18,
        "is_verified": True,
    },
}


def normalize_address(address: str) -> str:
    """Normalize Ethereum address to checksum format."""
    # Remove 0x prefix if present, convert to lowercase
    clean_address = address.replace("0x", "").lower()

    # Add 0x prefix back
    return f"0x{clean_address}"


def is_valid_ethereum_address(address: str) -> bool:
    """Check if string is a valid Ethereum address."""
    try:
        # Handle None and non-string inputs
        if not isinstance(address, str):
            return False

        # Remove 0x prefix
        clean_address = address.replace("0x", "")

        # Check length (40 hex characters)
        if len(clean_address) != 40:
            return False

        # Check if all characters are hex
        int(clean_address, 16)
        return True

    except (ValueError, TypeError):
        return False


def wei_to_eth(wei_amount: str) -> Decimal:
    """Convert Wei to ETH."""
    # Handle hex values (from Ethereum API responses)
    if wei_amount.startswith("0x"):
        wei_int = int(wei_amount, 16)
    else:
        wei_int = int(wei_amount)

    return Decimal(wei_int) / Decimal(10**18)


def format_token_amount(raw_amount: str, decimals: int) -> Decimal:
    """Format raw token amount using decimals."""
    return Decimal(raw_amount) / Decimal(10**decimals)


def calculate_token_value(balance: Decimal, price_usd: Decimal | None) -> Decimal | None:
    """Calculate USD value of token balance."""
    if price_usd is None:
        return None

    return balance * price_usd
