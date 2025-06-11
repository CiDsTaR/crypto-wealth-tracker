"""Type definitions for CoinGecko API data structures."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass
class TokenPrice:
    """Token price information from CoinGecko."""

    token_id: str  # CoinGecko ID (e.g., "ethereum", "usd-coin")
    contract_address: str | None = None  # Ethereum contract address
    symbol: str = ""
    name: str = ""
    current_price_usd: Decimal | None = None
    market_cap_usd: Decimal | None = None
    volume_24h_usd: Decimal | None = None
    price_change_24h_percent: Decimal | None = None
    price_change_7d_percent: Decimal | None = None
    last_updated: datetime | None = None


@dataclass
class TokenSearchResult:
    """Token search result from CoinGecko."""

    id: str  # CoinGecko ID
    symbol: str
    name: str
    contract_address: str | None = None
    thumb: str | None = None  # Small image URL
    large: str | None = None  # Large image URL


@dataclass
class SimplePriceResponse:
    """Response from CoinGecko simple price API."""

    prices: dict[str, dict[str, Decimal]]  # token_id -> {currency: price}


@dataclass
class ContractPriceResponse:
    """Response from CoinGecko contract price API."""

    contract_address: str
    price_usd: Decimal | None = None
    market_cap_usd: Decimal | None = None
    volume_24h_usd: Decimal | None = None


# CoinGecko ID mappings for well-known tokens
COINGECKO_TOKEN_IDS = {
    # Native tokens
    "ethereum": {
        "id": "ethereum",
        "symbol": "eth",
        "name": "Ethereum",
        "contract_address": None,  # Native token
    },
    # Stablecoins
    "0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0": {  # USDC
        "id": "usd-coin",
        "symbol": "usdc",
        "name": "USD Coin",
        "contract_address": "0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0",
    },
    "0xdac17f958d2ee523a2206206994597c13d831ec7": {  # USDT
        "id": "tether",
        "symbol": "usdt",
        "name": "Tether",
        "contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    },
    "0x6b175474e89094c44da98b954eedeac495271d0f": {  # DAI
        "id": "dai",
        "symbol": "dai",
        "name": "Dai Stablecoin",
        "contract_address": "0x6b175474e89094c44da98b954eedeac495271d0f",
    },
    # DeFi Tokens
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": {  # AAVE
        "id": "aave",
        "symbol": "aave",
        "name": "Aave",
        "contract_address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
    },
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": {  # UNI
        "id": "uniswap",
        "symbol": "uni",
        "name": "Uniswap",
        "contract_address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
    },
    "0x514910771af9ca656af840dff83e8264ecf986ca": {  # LINK
        "id": "chainlink",
        "symbol": "link",
        "name": "Chainlink",
        "contract_address": "0x514910771af9ca656af840dff83e8264ecf986ca",
    },
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {  # WETH
        "id": "weth",
        "symbol": "weth",
        "name": "Wrapped Ether",
        "contract_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    },
    # Other major tokens
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {  # WBTC
        "id": "wrapped-bitcoin",
        "symbol": "wbtc",
        "name": "Wrapped BTC",
        "contract_address": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
    },
    "0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b": {  # CRO
        "id": "crypto-com-chain",
        "symbol": "cro",
        "name": "Cronos",
        "contract_address": "0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b",
    },
}


def get_coingecko_id(contract_address: str | None = None, symbol: str | None = None) -> str | None:
    """Get CoinGecko ID for a token.

    Args:
        contract_address: Ethereum contract address
        symbol: Token symbol

    Returns:
        CoinGecko ID if found, None otherwise
    """
    if contract_address:
        # Normalize address to lowercase
        normalized_address = contract_address.lower()
        token_info = COINGECKO_TOKEN_IDS.get(normalized_address)
        if token_info:
            return token_info["id"]

    if symbol:
        # Search by symbol (less reliable)
        symbol_lower = symbol.lower()
        for token_info in COINGECKO_TOKEN_IDS.values():
            if token_info["symbol"] == symbol_lower:
                return token_info["id"]

    return None


def get_contract_address_from_coingecko_id(coingecko_id: str) -> str | None:
    """Get contract address from CoinGecko ID.

    Args:
        coingecko_id: CoinGecko token ID

    Returns:
        Contract address if found, None otherwise
    """
    for token_info in COINGECKO_TOKEN_IDS.values():
        if token_info["id"] == coingecko_id:
            return token_info.get("contract_address")

    return None


def normalize_coingecko_price_data(data: dict[str, Any]) -> TokenPrice:
    """Normalize CoinGecko API response to TokenPrice.

    Args:
        data: Raw CoinGecko API response data

    Returns:
        Normalized TokenPrice object
    """
    token_id = data.get("id", "")

    # Handle different response formats
    current_price = data.get("current_price") or data.get("usd")
    market_cap = data.get("market_cap") or data.get("usd_market_cap")
    volume_24h = data.get("total_volume") or data.get("usd_24h_vol")

    # Convert to Decimal for precision
    current_price_decimal = Decimal(str(current_price)) if current_price else None
    market_cap_decimal = Decimal(str(market_cap)) if market_cap else None
    volume_24h_decimal = Decimal(str(volume_24h)) if volume_24h else None

    # Price changes
    price_change_24h = data.get("price_change_percentage_24h")
    price_change_7d = data.get("price_change_percentage_7d")

    price_change_24h_decimal = Decimal(str(price_change_24h)) if price_change_24h else None
    price_change_7d_decimal = Decimal(str(price_change_7d)) if price_change_7d else None

    # Last updated
    last_updated = None
    if "last_updated" in data:
        try:
            last_updated = datetime.fromisoformat(data["last_updated"].replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass

    # Get contract address if available
    contract_address = get_contract_address_from_coingecko_id(token_id)

    return TokenPrice(
        token_id=token_id,
        contract_address=contract_address,
        symbol=data.get("symbol", ""),
        name=data.get("name", ""),
        current_price_usd=current_price_decimal,
        market_cap_usd=market_cap_decimal,
        volume_24h_usd=volume_24h_decimal,
        price_change_24h_percent=price_change_24h_decimal,
        price_change_7d_percent=price_change_7d_decimal,
        last_updated=last_updated,
    )


def create_batch_ids_string(token_ids: list[str], max_length: int = 2000) -> list[str]:
    """Create batched ID strings for CoinGecko API.

    CoinGecko API has URL length limits, so we need to batch large requests.

    Args:
        token_ids: List of CoinGecko token IDs
        max_length: Maximum length for each batch string

    Returns:
        List of comma-separated ID strings
    """
    if not token_ids:
        return []

    batches = []
    current_batch = []
    current_length = 0

    for token_id in token_ids:
        # Account for comma separator
        addition_length = len(token_id) + (1 if current_batch else 0)

        if current_length + addition_length > max_length and current_batch:
            # Start new batch
            batches.append(",".join(current_batch))
            current_batch = [token_id]
            current_length = len(token_id)
        else:
            current_batch.append(token_id)
            current_length += addition_length

    # Add final batch
    if current_batch:
        batches.append(",".join(current_batch))

    return batches


def is_stablecoin(symbol: str) -> bool:
    """Check if token is likely a stablecoin based on symbol.

    Args:
        symbol: Token symbol

    Returns:
        True if likely a stablecoin
    """
    stablecoin_symbols = {"usdc", "usdt", "dai", "busd", "tusd", "gusd", "usdd", "frax", "lusd"}
    return symbol.lower() in stablecoin_symbols
