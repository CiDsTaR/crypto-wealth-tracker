"""CoinGecko client for token price data."""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import aiohttp
from asyncio_throttle import Throttler

from ..config import CoinGeckoConfig
from ..utils import CacheManager
from .coingecko_types import (
    TokenPrice,
    TokenSearchResult,
    create_batch_ids_string,
    get_coingecko_id,
    is_stablecoin,
    normalize_coingecko_price_data,
)

logger = logging.getLogger(__name__)


class CoinGeckoClientError(Exception):
    """Base exception for CoinGecko client errors."""

    pass


class APIError(CoinGeckoClientError):
    """API request error."""

    pass


class RateLimitError(CoinGeckoClientError):
    """Rate limit exceeded error."""

    pass


class CoinGeckoClient:
    """CoinGecko API client for token prices and market data."""

    def __init__(
        self,
        config: CoinGeckoConfig,
        cache_manager: CacheManager | None = None,
        session: aiohttp.ClientSession | None = None,
    ):
        """Initialize CoinGecko client.

        Args:
            config: CoinGecko configuration
            cache_manager: Cache manager for caching responses
            session: HTTP session (optional, will create if not provided)
        """
        self.config = config
        self.cache_manager = cache_manager
        self._session = session
        self._own_session = session is None

        # Rate limiting (different for free vs pro)
        self.throttler = Throttler(rate_limit=config.rate_limit, period=60)

        # API endpoints
        self.base_url = str(config.base_url).rstrip("/")
        self.simple_price_url = f"{self.base_url}/simple/price"
        self.coins_url = f"{self.base_url}/coins"
        self.contract_url = f"{self.base_url}/simple/token_price/ethereum"
        self.search_url = f"{self.base_url}/search"

        # Stats
        self._stats = {
            "price_requests": 0,
            "batch_requests": 0,
            "contract_requests": 0,
            "search_requests": 0,
            "cache_hits": 0,
            "api_errors": 0,
            "rate_limit_errors": 0,
        }

    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure HTTP session is available."""
        if self._session is None:
            headers = {
                "Accept": "application/json",
                "User-Agent": "EthereumWalletTracker/1.0",
            }

            # Add API key if provided
            if self.config.api_key:
                if self.config.is_pro:
                    headers["x-cg-pro-api-key"] = self.config.api_key
                else:
                    headers["x-cg-demo-api-key"] = self.config.api_key

            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
            )
        return self._session

    async def _make_request(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        cache_key: str | None = None,
        cache_ttl: int | None = None,
    ) -> dict[str, Any]:
        """Make HTTP request with rate limiting and caching.

        Args:
            url: Request URL
            params: Request parameters
            cache_key: Cache key for response
            cache_ttl: Cache TTL in seconds

        Returns:
            Response data

        Raises:
            APIError: If request fails
            RateLimitError: If rate limited
        """
        # Check cache first
        if cache_key and self.cache_manager:
            cached_response = await self.cache_manager.get_price_cache().get(cache_key)
            if cached_response:
                self._stats["cache_hits"] += 1
                logger.debug(f"Cache hit for key: {cache_key}")
                return cached_response

        # Rate limiting
        async with self.throttler:
            session = await self._ensure_session()

            try:
                async with session.get(url, params=params) as response:
                    response_data = await self._handle_response(response)

                # Cache successful response
                if cache_key and self.cache_manager and cache_ttl:
                    await self.cache_manager.get_price_cache().set(cache_key, response_data, ttl=cache_ttl)

                return response_data

            except RateLimitError:
                self._stats["rate_limit_errors"] += 1
                raise
            except Exception as e:
                self._stats["api_errors"] += 1
                logger.error(f"CoinGecko API request failed: {e}")
                raise APIError(f"Request to {url} failed: {e}") from e

    async def _handle_response(self, response: aiohttp.ClientResponse) -> dict[str, Any]:
        """Handle HTTP response and extract data."""
        if response.status == 200:
            return await response.json()

        elif response.status == 429:
            # Rate limited
            retry_after = response.headers.get("Retry-After", "60")
            raise RateLimitError(f"Rate limited, retry after {retry_after} seconds")

        elif response.status == 404:
            # Not found - return empty result instead of error
            logger.warning(f"CoinGecko endpoint not found: {response.url}")
            return {}

        else:
            error_text = await response.text()
            raise APIError(f"HTTP {response.status}: {error_text}")

    async def get_token_price(self, token_id: str, include_market_data: bool = False) -> TokenPrice | None:
        """Get price for a single token by CoinGecko ID.

        Args:
            token_id: CoinGecko token ID (e.g., "ethereum", "usd-coin")
            include_market_data: Whether to include market cap, volume, etc.

        Returns:
            Token price data or None if not found
        """
        cache_key = f"price_single:{token_id}:{include_market_data}"

        try:
            if include_market_data:
                # Use detailed coins endpoint
                url = f"{self.coins_url}/{token_id}"
                params = {
                    "localization": "false",
                    "tickers": "false",
                    "market_data": "true",
                    "community_data": "false",
                    "developer_data": "false",
                    "sparkline": "false",
                }

                response = await self._make_request(
                    url,
                    params=params,
                    cache_key=cache_key,
                    cache_ttl=300,  # 5 minutes
                )

                if not response:
                    return None

                # Extract market data
                market_data = response.get("market_data", {})
                price_data = {
                    "id": response.get("id"),
                    "symbol": response.get("symbol"),
                    "name": response.get("name"),
                    "current_price": market_data.get("current_price", {}).get("usd"),
                    "market_cap": market_data.get("market_cap", {}).get("usd"),
                    "total_volume": market_data.get("total_volume", {}).get("usd"),
                    "price_change_percentage_24h": market_data.get("price_change_percentage_24h"),
                    "price_change_percentage_7d": market_data.get("price_change_percentage_7d"),
                    "last_updated": response.get("last_updated"),
                }

            else:
                # Use simple price endpoint
                params = {
                    "ids": token_id,
                    "vs_currencies": "usd",
                    "include_last_updated_at": "true",
                }

                response = await self._make_request(
                    self.simple_price_url,
                    params=params,
                    cache_key=cache_key,
                    cache_ttl=300,  # 5 minutes
                )

                if not response or token_id not in response:
                    return None

                token_data = response[token_id]
                price_data = {
                    "id": token_id,
                    "usd": token_data.get("usd"),
                    "last_updated": datetime.fromtimestamp(token_data["last_updated_at"]).isoformat()
                    if "last_updated_at" in token_data
                    else None,
                }

            self._stats["price_requests"] += 1
            return normalize_coingecko_price_data(price_data)

        except Exception as e:
            logger.warning(f"Failed to get price for token {token_id}: {e}")
            return None

    async def get_token_prices_batch(
        self, token_ids: list[str], include_market_data: bool = False
    ) -> dict[str, TokenPrice]:
        """Get prices for multiple tokens in batch.

        Args:
            token_ids: List of CoinGecko token IDs
            include_market_data: Whether to include market cap, volume, etc.

        Returns:
            Dictionary mapping token IDs to price data
        """
        if not token_ids:
            return {}

        # Remove duplicates while preserving order
        unique_ids = list(dict.fromkeys(token_ids))

        # Create batches for large requests
        id_batches = create_batch_ids_string(unique_ids, max_length=1500)

        all_prices = {}

        for batch_ids in id_batches:
            cache_key = f"price_batch:{hash(batch_ids)}:{include_market_data}"

            try:
                if include_market_data:
                    # Use markets endpoint for detailed data
                    params = {
                        "ids": batch_ids,
                        "vs_currency": "usd",
                        "include_market_cap": "true",
                        "include_24hr_vol": "true",
                        "include_24hr_change": "true",
                        "include_last_updated_at": "true",
                    }

                    url = f"{self.base_url}/coins/markets"

                else:
                    # Use simple price endpoint
                    params = {
                        "ids": batch_ids,
                        "vs_currencies": "usd",
                        "include_last_updated_at": "true",
                    }

                    url = self.simple_price_url

                response = await self._make_request(
                    url,
                    params=params,
                    cache_key=cache_key,
                    cache_ttl=300,  # 5 minutes
                )

                if response:
                    if include_market_data and isinstance(response, list):
                        # Markets endpoint returns array
                        for token_data in response:
                            if token_data.get("id"):
                                price = normalize_coingecko_price_data(token_data)
                                all_prices[price.token_id] = price

                    elif not include_market_data and isinstance(response, dict):
                        # Simple price endpoint returns dict
                        for token_id, token_data in response.items():
                            price_data = {
                                "id": token_id,
                                "usd": token_data.get("usd"),
                                "last_updated": datetime.fromtimestamp(token_data["last_updated_at"]).isoformat()
                                if "last_updated_at" in token_data
                                else None,
                            }
                            price = normalize_coingecko_price_data(price_data)
                            all_prices[token_id] = price

                self._stats["batch_requests"] += 1

            except Exception as e:
                logger.warning(f"Failed to get batch prices for {batch_ids}: {e}")
                continue

        return all_prices

    async def get_token_price_by_contract(
        self, contract_address: str, include_market_data: bool = False
    ) -> TokenPrice | None:
        """Get token price by Ethereum contract address.

        Args:
            contract_address: Ethereum contract address
            include_market_data: Whether to include market cap, volume, etc.

        Returns:
            Token price data or None if not found
        """
        normalized_address = contract_address.lower()

        # Check if we have a known CoinGecko ID for this contract
        coingecko_id = get_coingecko_id(contract_address=normalized_address)
        if coingecko_id:
            return await self.get_token_price(coingecko_id, include_market_data)

        # Fallback to contract price endpoint
        cache_key = f"contract_price:{normalized_address}:{include_market_data}"

        try:
            params = {
                "contract_addresses": normalized_address,
                "vs_currencies": "usd",
            }

            if include_market_data:
                params.update(
                    {
                        "include_market_cap": "true",
                        "include_24hr_vol": "true",
                        "include_24hr_change": "true",
                        "include_last_updated_at": "true",
                    }
                )

            response = await self._make_request(
                self.contract_url,
                params=params,
                cache_key=cache_key,
                cache_ttl=300,  # 5 minutes
            )

            if not response or normalized_address not in response:
                return None

            contract_data = response[normalized_address]

            # Create price data
            price_data = {
                "id": f"contract_{normalized_address}",
                "contract_address": normalized_address,
                "usd": contract_data.get("usd"),
                "usd_market_cap": contract_data.get("usd_market_cap") if include_market_data else None,
                "usd_24h_vol": contract_data.get("usd_24h_vol") if include_market_data else None,
                "usd_24h_change": contract_data.get("usd_24h_change") if include_market_data else None,
                "last_updated_at": contract_data.get("last_updated_at"),
            }

            self._stats["contract_requests"] += 1
            return normalize_coingecko_price_data(price_data)

        except Exception as e:
            logger.warning(f"Failed to get price for contract {contract_address}: {e}")
            return None

    async def get_token_prices_by_contracts(
        self, contract_addresses: list[str], include_market_data: bool = False
    ) -> dict[str, TokenPrice]:
        """Get prices for multiple tokens by contract addresses.

        Args:
            contract_addresses: List of Ethereum contract addresses
            include_market_data: Whether to include market cap, volume, etc.

        Returns:
            Dictionary mapping contract addresses to price data
        """
        if not contract_addresses:
            return {}

        # Normalize addresses
        normalized_addresses = [addr.lower() for addr in contract_addresses]

        # Split into known and unknown tokens
        known_tokens = {}  # coingecko_id -> contract_address
        unknown_addresses = []

        for addr in normalized_addresses:
            coingecko_id = get_coingecko_id(contract_address=addr)
            if coingecko_id:
                known_tokens[coingecko_id] = addr
            else:
                unknown_addresses.append(addr)

        all_prices = {}

        # Get prices for known tokens (more efficient)
        if known_tokens:
            known_prices = await self.get_token_prices_batch(list(known_tokens.keys()), include_market_data)

            for coingecko_id, price in known_prices.items():
                contract_addr = known_tokens[coingecko_id]
                # Update price with contract address
                price.contract_address = contract_addr
                all_prices[contract_addr] = price

        # Get prices for unknown tokens (less efficient)
        if unknown_addresses:
            # Batch contract requests (CoinGecko supports up to 100 at once)
            batch_size = 50  # Conservative batch size

            for i in range(0, len(unknown_addresses), batch_size):
                batch = unknown_addresses[i : i + batch_size]
                batch_str = ",".join(batch)

                cache_key = f"contract_batch:{hash(batch_str)}:{include_market_data}"

                try:
                    params = {
                        "contract_addresses": batch_str,
                        "vs_currencies": "usd",
                    }

                    if include_market_data:
                        params.update(
                            {
                                "include_market_cap": "true",
                                "include_24hr_vol": "true",
                                "include_24hr_change": "true",
                                "include_last_updated_at": "true",
                            }
                        )

                    response = await self._make_request(
                        self.contract_url,
                        params=params,
                        cache_key=cache_key,
                        cache_ttl=300,  # 5 minutes
                    )

                    if response:
                        for addr, contract_data in response.items():
                            if contract_data.get("usd"):
                                price_data = {
                                    "id": f"contract_{addr}",
                                    "contract_address": addr,
                                    "usd": contract_data.get("usd"),
                                    "usd_market_cap": contract_data.get("usd_market_cap")
                                    if include_market_data
                                    else None,
                                    "usd_24h_vol": contract_data.get("usd_24h_vol") if include_market_data else None,
                                    "usd_24h_change": contract_data.get("usd_24h_change")
                                    if include_market_data
                                    else None,
                                    "last_updated_at": contract_data.get("last_updated_at"),
                                }

                                price = normalize_coingecko_price_data(price_data)
                                all_prices[addr] = price

                    self._stats["contract_requests"] += 1

                except Exception as e:
                    logger.warning(f"Failed to get batch contract prices: {e}")
                    continue

        return all_prices

    async def search_tokens(self, query: str, limit: int = 10) -> list[TokenSearchResult]:
        """Search for tokens by name or symbol.

        Args:
            query: Search query (token name or symbol)
            limit: Maximum number of results

        Returns:
            List of search results
        """
        cache_key = f"search:{query.lower()}:{limit}"

        try:
            params = {"query": query}

            response = await self._make_request(
                self.search_url,
                params=params,
                cache_key=cache_key,
                cache_ttl=3600,  # 1 hour (search results change less frequently)
            )

            if not response or "coins" not in response:
                return []

            results = []
            coins = response["coins"][:limit]  # Limit results

            for coin in coins:
                result = TokenSearchResult(
                    id=coin.get("id", ""),
                    symbol=coin.get("symbol", ""),
                    name=coin.get("name", ""),
                    thumb=coin.get("thumb"),
                    large=coin.get("large"),
                )
                results.append(result)

            self._stats["search_requests"] += 1
            return results

        except Exception as e:
            logger.warning(f"Failed to search tokens for query '{query}': {e}")
            return []

    async def get_eth_price(self) -> Decimal | None:
        """Get current ETH price in USD.

        Returns:
            ETH price in USD or None if not available
        """
        eth_price = await self.get_token_price("ethereum", include_market_data=False)
        return eth_price.current_price_usd if eth_price else None

    async def get_stablecoin_prices(self) -> dict[str, Decimal]:
        """Get prices for major stablecoins.

        Returns:
            Dictionary mapping stablecoin symbols to USD prices
        """
        stablecoin_ids = ["usd-coin", "tether", "dai", "binance-usd", "true-usd"]

        prices = await self.get_token_prices_batch(stablecoin_ids, include_market_data=False)

        stablecoin_prices = {}
        for token_id, price in prices.items():
            if price.current_price_usd:
                stablecoin_prices[price.symbol.upper()] = price.current_price_usd

        return stablecoin_prices

    async def get_top_tokens_by_market_cap(self, limit: int = 100) -> list[TokenPrice]:
        """Get top tokens by market capitalization.

        Args:
            limit: Number of tokens to return (max 250)

        Returns:
            List of top tokens by market cap
        """
        cache_key = f"top_tokens:{limit}"

        try:
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": min(limit, 250),
                "page": 1,
                "sparkline": "false",
                "locale": "en",
            }

            url = f"{self.base_url}/coins/markets"

            response = await self._make_request(
                url,
                params=params,
                cache_key=cache_key,
                cache_ttl=600,  # 10 minutes
            )

            if not response:
                return []

            top_tokens = []
            for token_data in response:
                price = normalize_coingecko_price_data(token_data)
                top_tokens.append(price)

            return top_tokens

        except Exception as e:
            logger.warning(f"Failed to get top tokens: {e}")
            return []

    def get_stats(self) -> dict[str, Any]:
        """Get client statistics."""
        return {
            "price_requests": self._stats["price_requests"],
            "batch_requests": self._stats["batch_requests"],
            "contract_requests": self._stats["contract_requests"],
            "search_requests": self._stats["search_requests"],
            "cache_hits": self._stats["cache_hits"],
            "api_errors": self._stats["api_errors"],
            "rate_limit_errors": self._stats["rate_limit_errors"],
            "rate_limit": self.config.rate_limit,
            "has_pro_api": bool(self.config.is_pro),
        }

    async def health_check(self) -> bool:
        """Check if CoinGecko API is accessible.

        Returns:
            True if API is healthy, False otherwise
        """
        try:
            # Simple ping endpoint
            url = f"{self.base_url}/ping"

            session = await self._ensure_session()
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("gecko_says") == "(V3) To the Moon!"

        except Exception as e:
            logger.error(f"CoinGecko health check failed: {e}")

        return False

    async def close(self) -> None:
        """Close HTTP session."""
        if self._session and self._own_session:
            await self._session.close()
            self._session = None
            logger.info("CoinGecko client session closed")


class CoinGeckoPriceService:
    """High-level service for token price management."""

    def __init__(
        self,
        coingecko_client: CoinGeckoClient,
        cache_manager: CacheManager | None = None,
    ):
        """Initialize price service.

        Args:
            coingecko_client: CoinGecko client instance
            cache_manager: Cache manager for extended caching
        """
        self.client = coingecko_client
        self.cache_manager = cache_manager

    async def get_wallet_token_prices(
        self,
        contract_addresses: list[str],
        include_eth: bool = True,
    ) -> dict[str, Decimal]:
        """Get prices for all tokens in a wallet.

        Args:
            contract_addresses: List of token contract addresses
            include_eth: Whether to include ETH price

        Returns:
            Dictionary mapping addresses/symbols to USD prices
        """
        prices = {}

        # Get ETH price if requested
        if include_eth:
            eth_price = await self.client.get_eth_price()
            if eth_price:
                prices["ETH"] = eth_price

        # Get token prices by contract
        if contract_addresses:
            token_prices = await self.client.get_token_prices_by_contracts(
                contract_addresses, include_market_data=False
            )

            for addr, price_data in token_prices.items():
                if price_data.current_price_usd:
                    prices[addr] = price_data.current_price_usd

        return prices

    async def cache_popular_token_prices(self) -> int:
        """Pre-cache prices for popular tokens.

        Returns:
            Number of tokens cached
        """
        if not self.cache_manager:
            return 0

        # Get top 100 tokens
        top_tokens = await self.client.get_top_tokens_by_market_cap(limit=100)

        cached_count = 0
        for token in top_tokens:
            if token.current_price_usd:
                cache_key = f"popular_price:{token.token_id}"
                await self.cache_manager.set_price(
                    token.token_id,
                    {
                        "usd": str(token.current_price_usd),
                        "symbol": token.symbol,
                        "name": token.name,
                        "last_updated": datetime.now(UTC).isoformat(),
                    },
                )
                cached_count += 1

        logger.info(f"Cached prices for {cached_count} popular tokens")
        return cached_count

    async def get_price_with_fallback(
        self,
        contract_address: str | None = None,
        symbol: str | None = None,
        coingecko_id: str | None = None,
    ) -> Decimal | None:
        """Get token price with multiple fallback methods.

        Args:
            contract_address: Token contract address
            symbol: Token symbol
            coingecko_id: CoinGecko token ID

        Returns:
            Token price in USD or None if not found
        """
        # Try CoinGecko ID first (most reliable)
        if coingecko_id:
            price = await self.client.get_token_price(coingecko_id)
            if price and price.current_price_usd:
                return price.current_price_usd

        # Try contract address
        if contract_address:
            price = await self.client.get_token_price_by_contract(contract_address)
            if price and price.current_price_usd:
                return price.current_price_usd

        # Try searching by symbol (least reliable)
        if symbol:
            search_results = await self.client.search_tokens(symbol, limit=1)
            if search_results:
                price = await self.client.get_token_price(search_results[0].id)
                if price and price.current_price_usd:
                    return price.current_price_usd

        # Handle stablecoins with fallback price
        if symbol and is_stablecoin(symbol):
            logger.info(f"Using fallback price $1.00 for stablecoin {symbol}")
            return Decimal("1.00")

        return None
