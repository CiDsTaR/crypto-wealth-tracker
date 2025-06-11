"""Tests for Ethereum client."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from wallet_tracker.clients import (
    APIError,
    EthBalance,
    EthereumClient,
    InvalidAddressError,
    TokenBalance,
    WalletPortfolio,
    calculate_token_value,
    format_token_amount,
    is_valid_ethereum_address,
    normalize_address,
    wei_to_eth,
)
from wallet_tracker.config import EthereumConfig
from wallet_tracker.utils import CacheManager


class TestEthereumTypes:
    """Test Ethereum data types and utility functions."""

    def test_normalize_address(self) -> None:
        """Test address normalization."""
        # Test with 0x prefix
        addr1 = "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"
        normalized1 = normalize_address(addr1)
        assert normalized1 == "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"

        # Test without 0x prefix
        addr2 = "742d35Cc6634C0532925a3b8D40e3f337ABC7b86"
        normalized2 = normalize_address(addr2)
        assert normalized2 == "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"

    def test_is_valid_ethereum_address(self) -> None:
        """Test Ethereum address validation."""
        # Valid addresses
        assert is_valid_ethereum_address("0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86") is True
        assert is_valid_ethereum_address("742d35Cc6634C0532925a3b8D40e3f337ABC7b86") is True

        # Invalid addresses
        assert is_valid_ethereum_address("0x123") is False  # Too short
        assert is_valid_ethereum_address("not_an_address") is False  # Not hex
        assert is_valid_ethereum_address("") is False  # Empty
        assert is_valid_ethereum_address(None) is False  # None

    def test_wei_to_eth(self) -> None:
        """Test Wei to ETH conversion."""
        # 1 ETH = 1e18 Wei
        assert wei_to_eth("1000000000000000000") == Decimal("1")
        assert wei_to_eth("500000000000000000") == Decimal("0.5")
        assert wei_to_eth("0") == Decimal("0")

    def test_format_token_amount(self) -> None:
        """Test token amount formatting."""
        # USDC (6 decimals)
        assert format_token_amount("1000000", 6) == Decimal("1")
        assert format_token_amount("500000", 6) == Decimal("0.5")

        # Standard ERC20 (18 decimals)
        assert format_token_amount("1000000000000000000", 18) == Decimal("1")

    def test_calculate_token_value(self) -> None:
        """Test token value calculation."""
        # With price
        value = calculate_token_value(Decimal("100"), Decimal("2.50"))
        assert value == Decimal("250")

        # Without price
        value = calculate_token_value(Decimal("100"), None)
        assert value is None


class TestEthereumClient:
    """Test Ethereum client."""

    @pytest.fixture
    def ethereum_config(self) -> EthereumConfig:
        """Create Ethereum configuration for testing."""
        return EthereumConfig(
            alchemy_api_key="test_api_key",
            rpc_url="https://eth-mainnet.g.alchemy.com/v2/test_api_key",
            rate_limit=100,
        )

    @pytest.fixture
    def mock_cache_manager(self) -> AsyncMock:
        """Create mock cache manager."""
        cache_manager = AsyncMock(spec=CacheManager)

        # Mock cache methods
        cache_manager.get_balance = AsyncMock(return_value=None)
        cache_manager.set_balance = AsyncMock(return_value=True)
        cache_manager.get_wallet_activity = AsyncMock(return_value=None)
        cache_manager.set_wallet_activity = AsyncMock(return_value=True)
        cache_manager.get_token_metadata = AsyncMock(return_value=None)
        cache_manager.set_token_metadata = AsyncMock(return_value=True)

        # Mock general cache
        general_cache = AsyncMock()
        general_cache.get = AsyncMock(return_value=None)
        general_cache.set = AsyncMock(return_value=True)
        cache_manager.get_general_cache = MagicMock(return_value=general_cache)

        return cache_manager

    @pytest.fixture
    def ethereum_client(self, ethereum_config, mock_cache_manager) -> EthereumClient:
        """Create Ethereum client for testing."""
        return EthereumClient(
            config=ethereum_config,
            cache_manager=mock_cache_manager,
        )

    @pytest.mark.asyncio
    async def test_client_context_manager(self, ethereum_client) -> None:
        """Test client as async context manager."""
        async with ethereum_client as client:
            assert client is not None
            assert client._session is not None

    @pytest.mark.asyncio
    async def test_invalid_address_error(self, ethereum_client) -> None:
        """Test invalid address handling."""
        with pytest.raises(InvalidAddressError):
            await ethereum_client.get_wallet_portfolio("invalid_address")

    @pytest.mark.asyncio
    async def test_get_wallet_portfolio_success(self, ethereum_client) -> None:
        """Test successful wallet portfolio retrieval."""
        wallet_address = "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"

        # Mock API responses
        eth_balance_response = {
            "result": "0x1bc16d674ec80000"  # 2 ETH in hex
        }

        token_balances_response = {
            "result": {
                "tokenBalances": [
                    {
                        "contractAddress": "0xA0b86a33E6441e94bB0a8d0F7E5F8D69E2C0e5a0",
                        "tokenBalance": "0xF4240",  # 1000000 in hex (1 USDC)
                    }
                ]
            }
        }

        metadata_response = {
            "result": [{"symbol": "USDC", "name": "USD Coin", "decimals": 6, "logo": "https://example.com/usdc.png"}]
        }

        with patch.object(ethereum_client, "_make_request") as mock_request:
            # Set up mock responses in order
            mock_request.side_effect = [
                eth_balance_response,  # ETH balance
                token_balances_response,  # Token balances
                metadata_response,  # Token metadata
            ]

            # Mock ETH price
            with patch.object(ethereum_client, "_get_eth_price", return_value=Decimal("2000")):
                portfolio = await ethereum_client.get_wallet_portfolio(wallet_address)

                # Verify portfolio structure
                assert portfolio.address == normalize_address(wallet_address)
                assert portfolio.eth_balance.balance_eth == Decimal("2")
                assert portfolio.eth_balance.value_usd == Decimal("4000")
                assert len(portfolio.token_balances) == 1

                # Verify token balance
                usdc_balance = portfolio.token_balances[0]
                assert usdc_balance.symbol == "USDC"
                assert usdc_balance.balance_formatted == Decimal("1")
                assert usdc_balance.decimals == 6

    @pytest.mark.asyncio
    async def test_get_wallet_portfolio_cached(self, ethereum_client, mock_cache_manager) -> None:
        """Test wallet portfolio retrieval from cache."""
        wallet_address = "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"

        # Mock cached portfolio data
        cached_portfolio = {
            "address": normalize_address(wallet_address),
            "eth_balance": {
                "balance_wei": "1000000000000000000",
                "balance_eth": "1.0",
                "price_usd": "2000.0",
                "value_usd": "2000.0",
            },
            "token_balances": [],
            "total_value_usd": "2000.0",
            "last_updated": datetime.now(UTC).isoformat(),
            "transaction_count": 10,
        }

        mock_cache_manager.get_balance.return_value = cached_portfolio

        portfolio = await ethereum_client.get_wallet_portfolio(wallet_address)

        # Verify cache was used
        mock_cache_manager.get_balance.assert_called_once_with(normalize_address(wallet_address))
        assert portfolio.address == normalize_address(wallet_address)
        assert portfolio.eth_balance.balance_eth == Decimal("1.0")

    @pytest.mark.asyncio
    async def test_api_error_handling(self, ethereum_client) -> None:
        """Test API error handling."""
        wallet_address = "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"

        with patch.object(ethereum_client, "_make_request") as mock_request:
            mock_request.side_effect = APIError("API request failed")

            with pytest.raises(APIError):
                await ethereum_client.get_wallet_portfolio(wallet_address)

    @pytest.mark.asyncio
    async def test_rate_limiting(self, ethereum_client) -> None:
        """Test rate limiting functionality."""
        # The throttler should limit requests per minute
        assert ethereum_client.throttler.rate_limit == 100
        assert ethereum_client.throttler.period == 60

    def test_get_stats(self, ethereum_client) -> None:
        """Test statistics retrieval."""
        stats = ethereum_client.get_stats()

        assert "portfolio_requests" in stats
        assert "metadata_requests" in stats
        assert "cache_hits" in stats
        assert "api_errors" in stats
        assert stats["rate_limit"] == 100

    @pytest.mark.asyncio
    async def test_close_session(self, ethereum_client) -> None:
        """Test session cleanup."""
        # Initialize session
        await ethereum_client._ensure_session()
        assert ethereum_client._session is not None

        # Close session
        await ethereum_client.close()
        assert ethereum_client._session is None

    @pytest.mark.asyncio
    async def test_serialization_deserialization(self, ethereum_client) -> None:
        """Test portfolio serialization and deserialization."""
        # Create sample portfolio
        eth_balance = EthBalance(
            balance_wei="1000000000000000000",
            balance_eth=Decimal("1.0"),
            price_usd=Decimal("2000.0"),
            value_usd=Decimal("2000.0"),
        )

        token_balance = TokenBalance(
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

        portfolio = WalletPortfolio(
            address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            eth_balance=eth_balance,
            token_balances=[token_balance],
            total_value_usd=Decimal("2001.0"),
            last_updated=datetime.now(UTC),
            transaction_count=10,
        )

        # Test serialization
        serialized = ethereum_client._serialize_portfolio(portfolio)
        assert serialized["address"] == portfolio.address
        assert serialized["total_value_usd"] == "2001.0"
        assert len(serialized["token_balances"]) == 1

        # Test deserialization
        deserialized = ethereum_client._deserialize_portfolio(serialized)
        assert deserialized.address == portfolio.address
        assert deserialized.total_value_usd == portfolio.total_value_usd
        assert len(deserialized.token_balances) == 1
        assert deserialized.token_balances[0].symbol == "USDC"


class TestEthereumClientIntegration:
    """Integration tests for Ethereum client."""

    @pytest.fixture
    def real_ethereum_config(self) -> EthereumConfig:
        """Create real Ethereum configuration for integration tests."""
        return EthereumConfig(
            alchemy_api_key="demo",  # Use demo key for integration tests
            rpc_url="https://eth-mainnet.g.alchemy.com/v2/demo",
            rate_limit=5,  # Lower rate limit for demo
        )

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_real_api_integration(self, real_ethereum_config) -> None:
        """Test integration with real Alchemy API (requires network)."""
        # This test requires actual network access and should be marked as integration
        ethereum_client = EthereumClient(config=real_ethereum_config)

        try:
            async with ethereum_client:
                # Test with a known wallet (Vitalik's public address)
                vitalik_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

                # This might fail with demo key, but tests the integration
                try:
                    portfolio = await ethereum_client.get_wallet_portfolio(
                        vitalik_address,
                        include_metadata=False,  # Skip metadata to reduce API calls
                        include_prices=False,  # Skip prices to reduce API calls
                    )

                    # Basic validation
                    assert portfolio.address == normalize_address(vitalik_address)
                    assert isinstance(portfolio.eth_balance.balance_eth, Decimal)

                except APIError as e:
                    # Expected with demo key - just ensure error handling works
                    assert "demo" in str(e) or "rate" in str(e).lower()

        finally:
            await ethereum_client.close()


class TestMockHTTPSession:
    """Test HTTP session mocking."""

    @pytest.mark.asyncio
    async def test_mock_http_response(self) -> None:
        """Test mocking HTTP responses."""
        config = EthereumConfig(
            alchemy_api_key="test_key",
            rpc_url="https://test.com/v2/test_key",
            rate_limit=100,
        )

        # Create mock session
        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"result": "0x0"})

        # Configure mock
        mock_session.post.return_value.__aenter__.return_value = mock_response

        client = EthereumClient(config=config, session=mock_session)

        try:
            # Test request
            response = await client._make_request("POST", "https://test.com", {"test": "data"})
            assert response == {"result": "0x0"}

            # Verify session was called
            mock_session.post.assert_called_once()

        finally:
            await client.close()


# Pytest markers for different test types
# Only mark async tests, not all tests
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "integration: mark test as integration test requiring network access")
