"""Enhanced Ethereum client with better error handling and address validation."""

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import aiohttp
from asyncio_throttle import Throttler

from ..config import EthereumConfig
from ..utils.cache import CacheManager
from .ethereum_types import (
    WELL_KNOWN_TOKENS,
    EthBalance,
    TokenBalance,
    WalletActivity,
    WalletPortfolio,
    calculate_token_value,
    format_token_amount,
    is_valid_ethereum_address,
    normalize_address,
    wei_to_eth,
)

logger = logging.getLogger(__name__)


class EthereumClientError(Exception):
    """Base exception for Ethereum client errors."""

    pass


class InvalidAddressError(EthereumClientError):
    """Invalid Ethereum address error."""

    pass


class APIError(EthereumClientError):
    """API request error."""

    pass


class RateLimitError(EthereumClientError):
    """Rate limit error."""

    pass


class ServiceUnavailableError(EthereumClientError):
    """Service temporarily unavailable error."""

    pass


class EthereumClient:
    """Enhanced Ethereum client using Alchemy Portfolio API with better error handling."""

    def __init__(
        self,
        config: EthereumConfig,
        cache_manager: CacheManager | None = None,
        session: aiohttp.ClientSession | None = None,
        coingecko_client=None,  # Accept CoinGecko client for price data
    ):
        """Initialize Ethereum client."""
        self.config = config
        self.cache_manager = cache_manager
        self._session = session
        self._own_session = session is None
        self.coingecko_client = coingecko_client

        # Rate limiting
        self.throttler = Throttler(rate_limit=config.rate_limit, period=60)

        # API endpoints
        self.base_url = config.rpc_url
        self.portfolio_url = config.rpc_url
        self.metadata_url = config.rpc_url
        self.transaction_url = config.rpc_url
        self.eth_balance_url = config.rpc_url

        # Stats
        self._stats = {
            "portfolio_requests": 0,
            "metadata_requests": 0,
            "transaction_requests": 0,
            "cache_hits": 0,
            "api_errors": 0,
            "rate_limit_errors": 0,
            "service_unavailable_errors": 0,
        }

        # Error handling configuration
        self.max_retries = 3
        self.retry_delay = 1.0  # seconds
        self.backoff_multiplier = 2.0

        # API batch size configuration
        self.metadata_batch_size = 1  # Conservative: 1 address per request
        self.max_metadata_batch_size = 5  # Maximum to try if we want to be more aggressive

    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure HTTP session is available with proper SSL configuration."""
        if self._session is None:
            # Configure SSL context to be more tolerant
            import ssl

            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE  # Only for development/testing

            connector = aiohttp.TCPConnector(
                ssl=ssl_context,
                limit=100,  # Connection pool limit
                limit_per_host=30,  # Per-host limit
                ttl_dns_cache=300,  # DNS cache TTL
                use_dns_cache=True,
                keepalive_timeout=30,  # Keep connections alive
                enable_cleanup_closed=True,  # Clean up closed connections
            )

            timeout = aiohttp.ClientTimeout(
                total=60,  # Increased total timeout
                connect=15,  # Connection timeout
                sock_read=30,  # Socket read timeout
            )

            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "EthereumWalletTracker/1.0",
                    "Connection": "keep-alive",
                },
                # Add retry configuration
                raise_for_status=False,  # Handle status codes manually
            )
        return self._session

    async def _make_request_with_retry(
        self,
        method: str,
        url: str,
        data: dict[str, Any] | None = None,
        cache_key: str | None = None,
        cache_ttl: int | None = None,
        max_retries: int | None = None,
    ) -> dict[str, Any]:
        """Make HTTP request with retry logic and better error handling."""
        # Check cache first
        if cache_key and self.cache_manager:
            cached_response = await self.cache_manager.get_general_cache().get(cache_key)
            if cached_response:
                self._stats["cache_hits"] += 1
                logger.debug(f"💾 Cache hit for key: {cache_key}")
                return cached_response

        retry_count = max_retries if max_retries is not None else self.max_retries
        last_exception = None

        for attempt in range(retry_count + 1):
            try:
                # Rate limiting
                async with self.throttler:
                    response_data = await self._make_single_request(method, url, data)

                # Cache successful response
                if cache_key and self.cache_manager and cache_ttl:
                    await self.cache_manager.get_general_cache().set(cache_key, response_data, ttl=cache_ttl)

                return response_data

            except ServiceUnavailableError as e:
                last_exception = e
                self._stats["service_unavailable_errors"] += 1

                if attempt < retry_count:
                    delay = self.retry_delay * (self.backoff_multiplier**attempt)
                    logger.warning(
                        f"⚠️ Service unavailable (attempt {attempt + 1}/{retry_count + 1}), retrying in {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"❌ Service unavailable after {retry_count + 1} attempts")
                    raise

            except RateLimitError as e:
                last_exception = e
                self._stats["rate_limit_errors"] += 1

                if attempt < retry_count:
                    delay = 60  # Wait 1 minute for rate limits
                    logger.warning(f"⚠️ Rate limited (attempt {attempt + 1}/{retry_count + 1}), waiting {delay}s")
                    await asyncio.sleep(delay)
                    continue
                else:
                    raise

            except APIError as e:
                # Don't retry on general API errors unless it's a temporary issue
                if "unable to complete" in str(e).lower() and attempt < retry_count:
                    delay = self.retry_delay * (self.backoff_multiplier**attempt)
                    logger.warning(
                        f"⚠️ Temporary API error (attempt {attempt + 1}/{retry_count + 1}), retrying in {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    raise

        # If we get here, all retries failed
        if last_exception:
            raise last_exception
        else:
            raise APIError("All retry attempts failed")

    async def _make_single_request(
        self,
        method: str,
        url: str,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make a single HTTP request."""
        session = await self._ensure_session()

        try:
            if method.upper() == "POST":
                async with session.post(url, json=data) as response:
                    return await self._handle_response(response)
            else:
                async with session.get(url, params=data) as response:
                    return await self._handle_response(response)

        except TimeoutError as e:
            raise APIError(f"Request timeout: {e}")
        except Exception as e:
            self._stats["api_errors"] += 1
            logger.error(f"❌ API request failed: {e}")
            raise APIError(f"Request to {url} failed: {e}") from e

    async def _handle_response(self, response: aiohttp.ClientResponse) -> dict[str, Any]:
        """Handle HTTP response and extract data with better error classification."""
        if response.status == 200:
            try:
                data = await response.json()
            except Exception as e:
                raise APIError(f"Failed to parse JSON response: {e}")

            # Check for Alchemy API errors
            if "error" in data:
                error_code = data["error"].get("code", 0)
                error_msg = data["error"].get("message", "Unknown API error")

                # Classify different types of errors
                if error_code == -32000 and "unable to complete" in error_msg.lower():
                    raise ServiceUnavailableError(f"Alchemy service temporarily unavailable: {error_msg}")
                elif error_code == -32602:
                    raise APIError(f"Invalid parameters: {error_msg}")
                elif error_code == -32601:
                    raise APIError(f"Method not found: {error_msg}")
                else:
                    raise APIError(f"Alchemy API error (code {error_code}): {error_msg}")

            return data

        elif response.status == 429:
            raise RateLimitError("Rate limited by Alchemy API")

        elif response.status == 503:
            raise ServiceUnavailableError("Alchemy service temporarily unavailable")

        elif response.status == 400:
            error_text = await response.text()
            raise APIError(f"Bad request (400): {error_text}")

        elif response.status == 401:
            raise APIError("Unauthorized (401): Check your API key")

        elif response.status == 403:
            raise APIError("Forbidden (403): API key may lack required permissions")

        else:
            error_text = await response.text()
            raise APIError(f"HTTP {response.status}: {error_text}")

    def _validate_and_filter_addresses(self, addresses: list[str]) -> list[str]:
        """Validate and filter a list of addresses, removing invalid ones."""
        valid_addresses = []

        for i, addr in enumerate(addresses):
            if not addr or not isinstance(addr, str):
                logger.warning(f"⚠️ Skipping invalid address at index {i} (not a string): {repr(addr)}")
                continue

            addr = addr.strip()
            if not addr:
                logger.warning(f"⚠️ Skipping empty address at index {i}")
                continue

            # Check if address is complete (42 characters including 0x)
            if len(addr) != 42:
                logger.warning(f"⚠️ Skipping incomplete address at index {i} (length {len(addr)}): {addr}")
                continue

            # Check if it starts with 0x
            if not addr.startswith("0x"):
                logger.warning(f"⚠️ Skipping address without 0x prefix at index {i}: {addr}")
                continue

            # Check if the rest are hex characters
            try:
                int(addr[2:], 16)  # This will fail if not valid hex
            except ValueError:
                logger.warning(f"⚠️ Skipping address with invalid hex characters at index {i}: {addr}")
                continue

            if not is_valid_ethereum_address(addr):
                logger.warning(f"⚠️ Skipping invalid address format at index {i}: {addr}")
                continue

            valid_addresses.append(normalize_address(addr))

        if len(valid_addresses) != len(addresses):
            removed_count = len(addresses) - len(valid_addresses)
            logger.info(
                f"🔍 Address validation: {len(valid_addresses)} valid, {removed_count} invalid/incomplete removed"
            )

        return valid_addresses

    async def get_wallet_portfolio(
        self,
        wallet_address: str,
        include_metadata: bool = True,
        include_prices: bool = True,
    ) -> WalletPortfolio:
        """Get complete wallet portfolio with enhanced error handling."""
        # Validate address
        if not is_valid_ethereum_address(wallet_address):
            raise InvalidAddressError(f"Invalid Ethereum address: {wallet_address}")

        normalized_address = normalize_address(wallet_address)

        # Check if address is complete
        if len(normalized_address) != 42:
            raise InvalidAddressError(
                f"Incomplete Ethereum address: {wallet_address} (length: {len(normalized_address)})"
            )

        # Check cache first
        cache_key = f"portfolio:{normalized_address}"
        if self.cache_manager:
            cached_portfolio = await self.cache_manager.get_balance(normalized_address)
            if cached_portfolio:
                self._stats["cache_hits"] += 1
                logger.debug(f"💾 Portfolio cache hit for {normalized_address}")
                return self._deserialize_portfolio(cached_portfolio)

        logger.info(f"🔍 Fetching portfolio for wallet: {normalized_address}")

        try:
            # Get ETH balance
            eth_balance = await self._get_eth_balance(normalized_address)

            # Get token balances using Portfolio API
            token_balances = await self._get_token_balances(normalized_address, include_metadata, include_prices)

            # Get wallet activity
            activity = await self._get_wallet_activity(normalized_address)

            # Calculate total value
            total_value_usd = eth_balance.value_usd or Decimal("0")
            for token in token_balances:
                if token.value_usd:
                    total_value_usd += token.value_usd

            # Create portfolio
            portfolio = WalletPortfolio(
                address=normalized_address,
                eth_balance=eth_balance,
                token_balances=token_balances,
                total_value_usd=total_value_usd,
                last_updated=datetime.now(UTC),
                transaction_count=activity.transaction_count,
                last_transaction_hash=activity.last_transaction.hash if activity.last_transaction else None,
                last_transaction_timestamp=activity.last_transaction.timestamp if activity.last_transaction else None,
            )

            # Cache the portfolio
            if self.cache_manager:
                serialized = self._serialize_portfolio(portfolio)
                await self.cache_manager.set_balance(normalized_address, serialized)

            self._stats["portfolio_requests"] += 1
            return portfolio

        except (ServiceUnavailableError, RateLimitError) as e:
            logger.error(f"❌ Failed to fetch portfolio for {normalized_address}: {e}")
            # Return a minimal portfolio with just ETH balance as fallback
            return await self._create_fallback_portfolio(normalized_address)

    async def _create_fallback_portfolio(self, wallet_address: str) -> WalletPortfolio:
        """Create a minimal portfolio when full data is unavailable."""
        logger.info(f"🔄 Creating fallback portfolio for {wallet_address}")

        try:
            # Try to get just ETH balance
            eth_balance = await self._get_eth_balance(wallet_address)
        except Exception:
            # Even ETH balance failed, create minimal response
            eth_balance = EthBalance(balance_wei="0x0", balance_eth=Decimal("0"), price_usd=None, value_usd=None)

        # Create minimal activity
        activity = WalletActivity(
            address=wallet_address,
            first_transaction=None,
            last_transaction=None,
            transaction_count=0,
            total_gas_used=0,
            is_active=False,
        )

        return WalletPortfolio(
            address=wallet_address,
            eth_balance=eth_balance,
            token_balances=[],  # Empty token list
            total_value_usd=eth_balance.value_usd or Decimal("0"),
            last_updated=datetime.now(UTC),
            transaction_count=0,
            last_transaction_hash=None,
            last_transaction_timestamp=None,
        )

    async def _get_eth_balance(self, wallet_address: str) -> EthBalance:
        """Get ETH balance for wallet with error handling."""
        data = {"id": 1, "jsonrpc": "2.0", "method": "eth_getBalance", "params": [wallet_address, "latest"]}

        cache_key = f"eth_balance:{wallet_address}"

        try:
            response = await self._make_request_with_retry(
                "POST",
                self.eth_balance_url,
                data=data,
                cache_key=cache_key,
                cache_ttl=300,  # 5 minutes
            )

            balance_wei = response["result"]
            balance_eth = wei_to_eth(balance_wei)

            # Get ETH price with fallback
            eth_price_usd = await self._get_eth_price_with_fallback()
            value_usd = balance_eth * eth_price_usd if eth_price_usd else None

            return EthBalance(
                balance_wei=balance_wei,
                balance_eth=balance_eth,
                price_usd=eth_price_usd,
                value_usd=value_usd,
            )

        except Exception as e:
            logger.warning(f"⚠️ Failed to get ETH balance for {wallet_address}: {e}")
            # Return zero balance as fallback
            return EthBalance(
                balance_wei="0x0",
                balance_eth=Decimal("0"),
                price_usd=None,
                value_usd=None,
            )

    async def _get_token_balances(
        self,
        wallet_address: str,
        include_metadata: bool,
        include_prices: bool,
    ) -> list[TokenBalance]:
        """Get token balances with enhanced error handling."""
        data = {"id": 1, "jsonrpc": "2.0", "method": "alchemy_getTokenBalances", "params": [wallet_address]}

        cache_key = f"token_balances:{wallet_address}"

        try:
            response = await self._make_request_with_retry(
                "POST",
                self.portfolio_url,
                data=data,
                cache_key=cache_key,
                cache_ttl=600,  # 10 minutes
            )

            token_balances = []
            token_data = response.get("result", {}).get("tokenBalances", [])

            if not token_data:
                logger.info(f"💰 No tokens found for wallet {wallet_address}")
                return token_balances

            # Get metadata for all tokens if requested
            metadata_map = {}
            if include_metadata and token_data:
                # Filter out zero balances and invalid addresses
                non_zero_tokens = [
                    token
                    for token in token_data
                    if token.get("tokenBalance", "0x0") != "0x0"
                    and is_valid_ethereum_address(token.get("contractAddress", ""))
                ]

                if non_zero_tokens:
                    contract_addresses = [token["contractAddress"] for token in non_zero_tokens]
                    metadata_map = await self._get_tokens_metadata_safe(contract_addresses)

            # Get prices for all tokens if requested
            price_map = {}
            if include_prices and metadata_map:
                price_map = await self._get_tokens_prices_safe(list(metadata_map.keys()))

            # Process tokens
            for token_info in token_data:
                # Skip zero balances
                if token_info.get("tokenBalance", "0x0") == "0x0":
                    continue

                contract_address_raw = token_info.get("contractAddress")
                if not contract_address_raw or not is_valid_ethereum_address(contract_address_raw):
                    logger.warning(f"⚠️ Skipping token with invalid contract address: {contract_address_raw}")
                    continue

                contract_address = normalize_address(contract_address_raw)
                raw_balance = str(int(token_info["tokenBalance"], 16))

                # Get metadata
                metadata = metadata_map.get(contract_address) or WELL_KNOWN_TOKENS.get(contract_address, {})
                if not metadata:
                    logger.debug(f"🔍 No metadata for token {contract_address}, skipping")
                    continue

                # Format balance
                decimals = metadata.get("decimals", 18)
                try:
                    formatted_balance = format_token_amount(raw_balance, decimals)
                except Exception as e:
                    logger.warning(f"⚠️ Failed to format balance for token {contract_address}: {e}")
                    continue

                # Calculate value
                price_usd = price_map.get(contract_address)
                value_usd = calculate_token_value(formatted_balance, price_usd)

                token_balance = TokenBalance(
                    contract_address=contract_address,
                    symbol=metadata.get("symbol", "UNKNOWN"),
                    name=metadata.get("name", "Unknown Token"),
                    decimals=decimals,
                    balance_raw=raw_balance,
                    balance_formatted=formatted_balance,
                    price_usd=price_usd,
                    value_usd=value_usd,
                    logo_url=metadata.get("logo"),
                    is_verified=metadata.get("is_verified", False),
                )

                token_balances.append(token_balance)

            logger.info(f"🪙 Found {len(token_balances)} tokens for wallet {wallet_address}")
            return token_balances

        except Exception as e:
            logger.warning(f"⚠️ Failed to get token balances for {wallet_address}: {e}")
            return []

    async def _get_tokens_metadata_safe(self, contract_addresses: list[str]) -> dict[str, dict[str, Any]]:
        """Get metadata for multiple tokens with error handling.

        Note: alchemy_getTokenMetadata accepts multiple contract addresses in one call.
        """
        if not contract_addresses:
            return {}

        # Validate and filter addresses
        valid_addresses = self._validate_and_filter_addresses(contract_addresses)
        if not valid_addresses:
            logger.warning("⚠️ No valid addresses for metadata lookup")
            return {}

        # Check cache first
        metadata_map = {}
        uncached_addresses = []

        if self.cache_manager:
            for address in valid_addresses:
                cached_metadata = await self.cache_manager.get_token_metadata(address)
                if cached_metadata:
                    metadata_map[address] = cached_metadata
                else:
                    uncached_addresses.append(address)
        else:
            uncached_addresses = valid_addresses

        # Fetch uncached metadata in smaller batches
        # alchemy_getTokenMetadata DOES accept multiple addresses
        if uncached_addresses:
            batch_size = 1  # Smaller batches to avoid issues

            for i in range(0, len(uncached_addresses), batch_size):
                batch = uncached_addresses[i : i + batch_size]

                try:
                    logger.debug(f"🔍 Fetching metadata for {len(batch)} tokens")
                    # This is correct - alchemy_getTokenMetadata accepts array of addresses
                    data = {
                        "id": 1,
                        "jsonrpc": "2.0",
                        "method": "alchemy_getTokenMetadata",
                        "params": batch,  # Array of contract addresses
                    }

                    response = await self._make_request_with_retry("POST", self.metadata_url, data=data, max_retries=2)

                    self._stats["metadata_requests"] += 1

                    # Process response - should be array matching the input array
                    result = response.get("result", [])
                    if isinstance(result, list):
                        for j, token_metadata in enumerate(result):
                            if j < len(batch) and token_metadata:
                                address = normalize_address(batch[j])
                                metadata = {
                                    "symbol": token_metadata.get("symbol"),
                                    "name": token_metadata.get("name"),
                                    "decimals": token_metadata.get("decimals", 18),
                                    "logo": token_metadata.get("logo"),
                                }
                                metadata_map[address] = metadata

                                # Cache metadata
                                if self.cache_manager:
                                    await self.cache_manager.set_token_metadata(address, metadata)

                except Exception as e:
                    logger.warning(f"⚠️ Failed to fetch metadata for batch: {e}")
                    continue

        return metadata_map

    async def _get_tokens_prices_safe(self, contract_addresses: list[str]) -> dict[str, Decimal]:
        """Get prices for multiple tokens with CoinGecko integration."""
        if not contract_addresses:
            return {}

        if not self.coingecko_client:
            logger.debug("💰 CoinGecko client not available, skipping price lookup")
            return {}

        try:
            logger.debug(f"💰 Fetching prices for {len(contract_addresses)} tokens")

            # Use CoinGecko client to get prices by contract addresses
            price_data = await self.coingecko_client.get_token_prices_by_contracts(
                contract_addresses=contract_addresses, include_market_data=False
            )

            # Convert to the format expected by the rest of the code
            prices = {}
            for address, token_price in price_data.items():
                if token_price and token_price.current_price_usd:
                    prices[address] = token_price.current_price_usd

            logger.info(f"💰 Retrieved prices for {len(prices)}/{len(contract_addresses)} tokens")
            return prices

        except Exception as e:
            logger.warning(f"⚠️ Failed to get token prices from CoinGecko: {e}")
            return {}

    async def _get_eth_price_with_fallback(self) -> Decimal | None:
        """Get current ETH price with CoinGecko integration."""
        try:
            # Try to get from CoinGecko client if available
            if self.coingecko_client:
                eth_price = await self.coingecko_client.get_eth_price()
                if eth_price:
                    logger.debug(f"💰 ETH price from CoinGecko: ${eth_price}")
                    return eth_price

            # Fallback price if CoinGecko is unavailable
            logger.warning("⚠️ CoinGecko client not available, using fallback ETH price")
            return Decimal("2000.0")  # Conservative fallback price

        except Exception as e:
            logger.warning(f"⚠️ Failed to get ETH price from CoinGecko: {e}")
            # Use fallback price
            return Decimal("2000.0")

    async def _get_wallet_activity(self, wallet_address: str) -> WalletActivity:
        """Get wallet activity summary with error handling."""
        # Check cache first
        if self.cache_manager:
            try:
                cached_activity = await self.cache_manager.get_wallet_activity(wallet_address)
                if cached_activity:
                    return self._deserialize_activity(cached_activity)
            except Exception:
                pass

        # Create minimal activity for now
        activity = WalletActivity(
            address=wallet_address,
            first_transaction=None,
            last_transaction=None,
            transaction_count=0,
            total_gas_used=0,
            is_active=True,
        )

        # Cache activity
        if self.cache_manager:
            try:
                serialized = self._serialize_activity(activity)
                await self.cache_manager.set_wallet_activity(wallet_address, serialized)
            except Exception:
                pass

        return activity

    def _serialize_portfolio(self, portfolio: WalletPortfolio) -> dict[str, Any]:
        """Serialize portfolio for caching."""
        return {
            "address": portfolio.address,
            "eth_balance": {
                "balance_wei": portfolio.eth_balance.balance_wei,
                "balance_eth": str(portfolio.eth_balance.balance_eth),
                "price_usd": str(portfolio.eth_balance.price_usd) if portfolio.eth_balance.price_usd else None,
                "value_usd": str(portfolio.eth_balance.value_usd) if portfolio.eth_balance.value_usd else None,
            },
            "token_balances": [
                {
                    "contract_address": token.contract_address,
                    "symbol": token.symbol,
                    "name": token.name,
                    "decimals": token.decimals,
                    "balance_raw": token.balance_raw,
                    "balance_formatted": str(token.balance_formatted),
                    "price_usd": str(token.price_usd) if token.price_usd else None,
                    "value_usd": str(token.value_usd) if token.value_usd else None,
                    "logo_url": token.logo_url,
                    "is_verified": token.is_verified,
                }
                for token in portfolio.token_balances
            ],
            "total_value_usd": str(portfolio.total_value_usd),
            "last_updated": portfolio.last_updated.isoformat(),
            "transaction_count": portfolio.transaction_count,
        }

    def _deserialize_portfolio(self, data: dict[str, Any]) -> WalletPortfolio:
        """Deserialize portfolio from cache."""
        eth_data = data["eth_balance"]
        eth_balance = EthBalance(
            balance_wei=eth_data["balance_wei"],
            balance_eth=Decimal(eth_data["balance_eth"]),
            price_usd=Decimal(eth_data["price_usd"]) if eth_data["price_usd"] else None,
            value_usd=Decimal(eth_data["value_usd"]) if eth_data["value_usd"] else None,
        )

        token_balances = []
        for token_data in data["token_balances"]:
            token_balance = TokenBalance(
                contract_address=token_data["contract_address"],
                symbol=token_data["symbol"],
                name=token_data["name"],
                decimals=token_data["decimals"],
                balance_raw=token_data["balance_raw"],
                balance_formatted=Decimal(token_data["balance_formatted"]),
                price_usd=Decimal(token_data["price_usd"]) if token_data["price_usd"] else None,
                value_usd=Decimal(token_data["value_usd"]) if token_data["value_usd"] else None,
                logo_url=token_data["logo_url"],
                is_verified=token_data["is_verified"],
            )
            token_balances.append(token_balance)

        return WalletPortfolio(
            address=data["address"],
            eth_balance=eth_balance,
            token_balances=token_balances,
            total_value_usd=Decimal(data["total_value_usd"]),
            last_updated=datetime.fromisoformat(data["last_updated"]),
            transaction_count=data["transaction_count"],
        )

    def _serialize_activity(self, activity: WalletActivity) -> dict[str, Any]:
        """Serialize activity for caching."""
        return {
            "address": activity.address,
            "transaction_count": activity.transaction_count,
            "total_gas_used": activity.total_gas_used,
            "is_active": activity.is_active,
            "days_since_last_transaction": activity.days_since_last_transaction,
        }

    def _deserialize_activity(self, data: dict[str, Any]) -> WalletActivity:
        """Deserialize activity from cache."""
        return WalletActivity(
            address=data["address"],
            first_transaction=None,
            last_transaction=None,
            transaction_count=data["transaction_count"],
            total_gas_used=data["total_gas_used"],
            is_active=data["is_active"],
            days_since_last_transaction=data.get("days_since_last_transaction"),
        )

    def configure_batch_sizes(self, metadata_batch_size: int = 1):
        """Configure batch sizes for API calls.

        Args:
            metadata_batch_size: Number of addresses to send per metadata request (1-5 recommended)
        """
        if metadata_batch_size < 1 or metadata_batch_size > 20:
            raise ValueError("metadata_batch_size must be between 1 and 20")

        self.metadata_batch_size = metadata_batch_size
        logger.info(f"⚙️ Configured metadata batch size: {metadata_batch_size}")

    def get_stats(self) -> dict[str, Any]:
        """Get client statistics."""
        return {
            "portfolio_requests": self._stats["portfolio_requests"],
            "metadata_requests": self._stats["metadata_requests"],
            "transaction_requests": self._stats["transaction_requests"],
            "cache_hits": self._stats["cache_hits"],
            "api_errors": self._stats["api_errors"],
            "rate_limit_errors": self._stats["rate_limit_errors"],
            "service_unavailable_errors": self._stats["service_unavailable_errors"],
            "rate_limit": self.config.rate_limit,
            "metadata_batch_size": self.metadata_batch_size,
        }

    async def close(self) -> None:
        """Close HTTP session."""
        if self._session and self._own_session:
            await self._session.close()
            self._session = None
            logger.info("🔌 Ethereum client session closed")
