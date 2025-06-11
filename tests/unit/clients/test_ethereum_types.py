"""Tests for Ethereum types and utility functions."""

from datetime import UTC, datetime
from decimal import Decimal

from wallet_tracker.clients.ethereum_types import (
    WELL_KNOWN_TOKENS,
    AlchemyPortfolioResponse,
    AlchemyPriceResponse,
    AlchemyTokenMetadataResponse,
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


class TestTokenBalance:
    """Test TokenBalance dataclass."""

    def test_token_balance_creation(self):
        """Test creating TokenBalance instance."""
        token_balance = TokenBalance(
            contract_address="0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0",
            symbol="USDC",
            name="USD Coin",
            decimals=6,
            balance_raw="1000000",
            balance_formatted=Decimal("1.0"),
            price_usd=Decimal("1.001"),
            value_usd=Decimal("1.001"),
            logo_url="https://example.com/usdc.png",
            is_verified=True,
        )

        assert token_balance.contract_address == "0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0"
        assert token_balance.symbol == "USDC"
        assert token_balance.name == "USD Coin"
        assert token_balance.decimals == 6
        assert token_balance.balance_raw == "1000000"
        assert token_balance.balance_formatted == Decimal("1.0")
        assert token_balance.price_usd == Decimal("1.001")
        assert token_balance.value_usd == Decimal("1.001")
        assert token_balance.logo_url == "https://example.com/usdc.png"
        assert token_balance.is_verified is True

    def test_token_balance_optional_fields(self):
        """Test TokenBalance with optional fields."""
        token_balance = TokenBalance(
            contract_address="0x123",
            symbol="TEST",
            name="Test Token",
            decimals=18,
            balance_raw="1000000000000000000",
            balance_formatted=Decimal("1.0"),
        )

        assert token_balance.price_usd is None
        assert token_balance.value_usd is None
        assert token_balance.logo_url is None
        assert token_balance.is_verified is False


class TestEthBalance:
    """Test EthBalance dataclass."""

    def test_eth_balance_creation(self):
        """Test creating EthBalance instance."""
        eth_balance = EthBalance(
            balance_wei="1000000000000000000",
            balance_eth=Decimal("1.0"),
            price_usd=Decimal("2000.0"),
            value_usd=Decimal("2000.0"),
        )

        assert eth_balance.balance_wei == "1000000000000000000"
        assert eth_balance.balance_eth == Decimal("1.0")
        assert eth_balance.price_usd == Decimal("2000.0")
        assert eth_balance.value_usd == Decimal("2000.0")

    def test_eth_balance_optional_fields(self):
        """Test EthBalance with optional fields."""
        eth_balance = EthBalance(
            balance_wei="500000000000000000",
            balance_eth=Decimal("0.5"),
        )

        assert eth_balance.price_usd is None
        assert eth_balance.value_usd is None


class TestWalletPortfolio:
    """Test WalletPortfolio dataclass."""

    def test_wallet_portfolio_creation(self):
        """Test creating WalletPortfolio instance."""
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
            balance_formatted=Decimal("1000.0"),
            price_usd=Decimal("1.0"),
            value_usd=Decimal("1000.0"),
        )

        now = datetime.now(UTC)
        portfolio = WalletPortfolio(
            address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            eth_balance=eth_balance,
            token_balances=[token_balance],
            total_value_usd=Decimal("3000.0"),
            last_updated=now,
            transaction_count=150,
            last_transaction_hash="0xabc123",
            last_transaction_timestamp=now,
        )

        assert portfolio.address == "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"
        assert portfolio.eth_balance == eth_balance
        assert len(portfolio.token_balances) == 1
        assert portfolio.token_balances[0] == token_balance
        assert portfolio.total_value_usd == Decimal("3000.0")
        assert portfolio.last_updated == now
        assert portfolio.transaction_count == 150
        assert portfolio.last_transaction_hash == "0xabc123"
        assert portfolio.last_transaction_timestamp == now

    def test_wallet_portfolio_optional_fields(self):
        """Test WalletPortfolio with optional fields."""
        eth_balance = EthBalance(
            balance_wei="1000000000000000000",
            balance_eth=Decimal("1.0"),
        )

        portfolio = WalletPortfolio(
            address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            eth_balance=eth_balance,
            token_balances=[],
            total_value_usd=Decimal("0.0"),
            last_updated=datetime.now(UTC),
            transaction_count=0,
        )

        assert portfolio.last_transaction_hash is None
        assert portfolio.last_transaction_timestamp is None


class TestTokenMetadata:
    """Test TokenMetadata dataclass."""

    def test_token_metadata_creation(self):
        """Test creating TokenMetadata instance."""
        metadata = TokenMetadata(
            contract_address="0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0",
            symbol="USDC",
            name="USD Coin",
            decimals=6,
            logo_url="https://example.com/usdc.png",
            is_verified=True,
            price_usd=Decimal("1.001"),
            market_cap_usd=Decimal("50000000000"),
            volume_24h_usd=Decimal("5000000000"),
        )

        assert metadata.contract_address == "0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0"
        assert metadata.symbol == "USDC"
        assert metadata.name == "USD Coin"
        assert metadata.decimals == 6
        assert metadata.logo_url == "https://example.com/usdc.png"
        assert metadata.is_verified is True
        assert metadata.price_usd == Decimal("1.001")
        assert metadata.market_cap_usd == Decimal("50000000000")
        assert metadata.volume_24h_usd == Decimal("5000000000")


class TestTransactionInfo:
    """Test TransactionInfo dataclass."""

    def test_transaction_info_creation(self):
        """Test creating TransactionInfo instance."""
        now = datetime.now(UTC)
        tx_info = TransactionInfo(
            hash="0xabc123def456",
            block_number=18500000,
            timestamp=now,
            from_address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            to_address="0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0",
            value_wei="1000000000000000000",
            value_eth=Decimal("1.0"),
            gas_used=21000,
            gas_price_wei="20000000000",
            status="success",
        )

        assert tx_info.hash == "0xabc123def456"
        assert tx_info.block_number == 18500000
        assert tx_info.timestamp == now
        assert tx_info.from_address == "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"
        assert tx_info.to_address == "0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0"
        assert tx_info.value_wei == "1000000000000000000"
        assert tx_info.value_eth == Decimal("1.0")
        assert tx_info.gas_used == 21000
        assert tx_info.gas_price_wei == "20000000000"
        assert tx_info.status == "success"


class TestWalletActivity:
    """Test WalletActivity dataclass."""

    def test_wallet_activity_creation(self):
        """Test creating WalletActivity instance."""
        now = datetime.now(UTC)
        first_tx = TransactionInfo(
            hash="0xfirst",
            block_number=18000000,
            timestamp=now,
            from_address="0x0",
            to_address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            value_wei="1000000000000000000",
            value_eth=Decimal("1.0"),
            gas_used=21000,
            gas_price_wei="20000000000",
            status="success",
        )

        last_tx = TransactionInfo(
            hash="0xlast",
            block_number=18500000,
            timestamp=now,
            from_address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            to_address="0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0",
            value_wei="500000000000000000",
            value_eth=Decimal("0.5"),
            gas_used=21000,
            gas_price_wei="25000000000",
            status="success",
        )

        activity = WalletActivity(
            address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            first_transaction=first_tx,
            last_transaction=last_tx,
            transaction_count=150,
            total_gas_used=3150000,
            is_active=True,
            days_since_last_transaction=5,
        )

        assert activity.address == "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"
        assert activity.first_transaction == first_tx
        assert activity.last_transaction == last_tx
        assert activity.transaction_count == 150
        assert activity.total_gas_used == 3150000
        assert activity.is_active is True
        assert activity.days_since_last_transaction == 5

    def test_wallet_activity_optional_fields(self):
        """Test WalletActivity with optional fields."""
        activity = WalletActivity(
            address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            first_transaction=None,
            last_transaction=None,
            transaction_count=0,
            total_gas_used=0,
            is_active=False,
        )

        assert activity.first_transaction is None
        assert activity.last_transaction is None
        assert activity.days_since_last_transaction is None


class TestAlchemyResponses:
    """Test Alchemy API response dataclasses."""

    def test_alchemy_portfolio_response(self):
        """Test AlchemyPortfolioResponse creation."""
        response = AlchemyPortfolioResponse(
            address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            tokenBalances=[
                {"contractAddress": "0xabc", "tokenBalance": "0x123"},
                {"contractAddress": "0xdef", "tokenBalance": "0x456"},
            ],
            pageKey="next_page_key",
        )

        assert response.address == "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"
        assert len(response.tokenBalances) == 2
        assert response.pageKey == "next_page_key"

    def test_alchemy_token_metadata_response(self):
        """Test AlchemyTokenMetadataResponse creation."""
        response = AlchemyTokenMetadataResponse(
            tokens=[
                {"symbol": "USDC", "name": "USD Coin", "decimals": 6},
                {"symbol": "USDT", "name": "Tether", "decimals": 6},
            ]
        )

        assert len(response.tokens) == 2
        assert response.tokens[0]["symbol"] == "USDC"

    def test_alchemy_price_response(self):
        """Test AlchemyPriceResponse creation."""
        response = AlchemyPriceResponse(
            data=[
                {"address": "0xabc", "price": 1.001},
                {"address": "0xdef", "price": 0.999},
            ]
        )

        assert len(response.data) == 2
        assert response.data[0]["price"] == 1.001


class TestAddressValidation:
    """Test Ethereum address validation functions."""

    def test_is_valid_ethereum_address_valid(self):
        """Test valid Ethereum addresses."""
        # Standard address with 0x prefix
        assert is_valid_ethereum_address("0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86") is True

        # Address without 0x prefix
        assert is_valid_ethereum_address("742d35Cc6634C0532925a3b8D40e3f337ABC7b86") is True

        # All lowercase
        assert is_valid_ethereum_address("0x742d35cc6634c0532925a3b8d40e3f337abc7b86") is True

        # All uppercase
        assert is_valid_ethereum_address("0x742D35CC6634C0532925A3B8D40E3F337ABC7B86") is True

    def test_is_valid_ethereum_address_invalid(self):
        """Test invalid Ethereum addresses."""
        # Too short
        assert is_valid_ethereum_address("0x123") is False

        # Too long
        assert is_valid_ethereum_address("0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86abc") is False

        # Non-hex characters
        assert is_valid_ethereum_address("0x742d35Gc6634C0532925a3b8D40e3f337ABC7b86") is False

        # Empty string
        assert is_valid_ethereum_address("") is False

        # None
        assert is_valid_ethereum_address(None) is False

        # Non-string type
        assert is_valid_ethereum_address(123) is False

        # Just 0x
        assert is_valid_ethereum_address("0x") is False

    def test_normalize_address(self):
        """Test address normalization."""
        # With 0x prefix
        addr1 = "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"
        normalized1 = normalize_address(addr1)
        assert normalized1 == "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"

        # Without 0x prefix
        addr2 = "742d35Cc6634C0532925a3b8D40e3f337ABC7b86"
        normalized2 = normalize_address(addr2)
        assert normalized2 == "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"

        # Already normalized
        addr3 = "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"
        normalized3 = normalize_address(addr3)
        assert normalized3 == addr3


class TestWeiConversion:
    """Test Wei to ETH conversion functions."""

    def test_wei_to_eth_standard_values(self):
        """Test standard Wei to ETH conversions."""
        # 1 ETH = 1e18 Wei
        assert wei_to_eth("1000000000000000000") == Decimal("1")

        # 0.5 ETH = 5e17 Wei
        assert wei_to_eth("500000000000000000") == Decimal("0.5")

        # 2.5 ETH = 2.5e18 Wei
        assert wei_to_eth("2500000000000000000") == Decimal("2.5")

        # 0 ETH
        assert wei_to_eth("0") == Decimal("0")

    def test_wei_to_eth_hex_values(self):
        """Test Wei to ETH conversion with hex input."""
        # 1 ETH in hex
        assert wei_to_eth("0xde0b6b3a7640000") == Decimal("1")

        # 0.5 ETH in hex
        assert wei_to_eth("0x6f05b59d3b20000") == Decimal("0.5")

    def test_wei_to_eth_precision(self):
        """Test Wei to ETH conversion precision."""
        # 1 Wei = 1e-18 ETH
        assert wei_to_eth("1") == Decimal("0.000000000000000001")

        # Large value
        large_wei = "12345678901234567890"
        expected_eth = Decimal("12.34567890123456789")
        assert wei_to_eth(large_wei) == expected_eth


class TestTokenAmountFormatting:
    """Test token amount formatting functions."""

    def test_format_token_amount_standard_decimals(self):
        """Test token amount formatting with standard decimals."""
        # USDC (6 decimals)
        assert format_token_amount("1000000", 6) == Decimal("1")
        assert format_token_amount("1500000", 6) == Decimal("1.5")
        assert format_token_amount("1234567", 6) == Decimal("1.234567")

        # Standard ERC20 (18 decimals)
        assert format_token_amount("1000000000000000000", 18) == Decimal("1")
        assert format_token_amount("1500000000000000000", 18) == Decimal("1.5")

    def test_format_token_amount_edge_cases(self):
        """Test token amount formatting edge cases."""
        # Zero amount
        assert format_token_amount("0", 18) == Decimal("0")
        assert format_token_amount("0", 6) == Decimal("0")

        # Very small amounts
        assert format_token_amount("1", 18) == Decimal("0.000000000000000001")
        assert format_token_amount("1", 6) == Decimal("0.000001")

        # No decimals
        assert format_token_amount("123", 0) == Decimal("123")

    def test_format_token_amount_precision(self):
        """Test token amount formatting precision."""
        # High precision token
        raw_amount = "123456789012345678"
        decimals = 18
        expected = Decimal("0.123456789012345678")
        assert format_token_amount(raw_amount, decimals) == expected


class TestTokenValueCalculation:
    """Test token value calculation functions."""

    def test_calculate_token_value_with_price(self):
        """Test token value calculation with price."""
        # Simple case
        balance = Decimal("100")
        price = Decimal("2.50")
        expected_value = Decimal("250")
        assert calculate_token_value(balance, price) == expected_value

        # Fractional amounts
        balance = Decimal("0.5")
        price = Decimal("1000")
        expected_value = Decimal("500")
        assert calculate_token_value(balance, price) == expected_value

        # High precision
        balance = Decimal("1.234567890123456789")
        price = Decimal("1.001")
        expected_value = balance * price
        assert calculate_token_value(balance, price) == expected_value

    def test_calculate_token_value_without_price(self):
        """Test token value calculation without price."""
        balance = Decimal("100")
        assert calculate_token_value(balance, None) is None

    def test_calculate_token_value_zero_values(self):
        """Test token value calculation with zero values."""
        # Zero balance
        assert calculate_token_value(Decimal("0"), Decimal("100")) == Decimal("0")

        # Zero price
        assert calculate_token_value(Decimal("100"), Decimal("0")) == Decimal("0")

        # Both zero
        assert calculate_token_value(Decimal("0"), Decimal("0")) == Decimal("0")


class TestWellKnownTokens:
    """Test well-known tokens constants."""

    def test_well_known_tokens_structure(self):
        """Test well-known tokens data structure."""
        assert isinstance(WELL_KNOWN_TOKENS, dict)
        assert len(WELL_KNOWN_TOKENS) > 0

        # Check USDC
        usdc_addr = "0xA0b86a33E6441e94bB0a8d0F7E5F8D69E2C0e5a0"
        assert usdc_addr in WELL_KNOWN_TOKENS
        usdc_data = WELL_KNOWN_TOKENS[usdc_addr]
        assert usdc_data["symbol"] == "USDC"
        assert usdc_data["name"] == "USD Coin"
        assert usdc_data["decimals"] == 6
        assert usdc_data["is_verified"] is True

    def test_well_known_tokens_stablecoins(self):
        """Test well-known stablecoins."""
        # USDC
        usdc_addr = "0xA0b86a33E6441e94bB0a8d0F7E5F8D69E2C0e5a0"
        assert usdc_addr in WELL_KNOWN_TOKENS

        # USDT
        usdt_addr = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
        assert usdt_addr in WELL_KNOWN_TOKENS

        # DAI
        dai_addr = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
        assert dai_addr in WELL_KNOWN_TOKENS

    def test_well_known_tokens_defi(self):
        """Test well-known DeFi tokens."""
        # AAVE
        aave_addr = "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9"
        assert aave_addr in WELL_KNOWN_TOKENS

        # UNI
        uni_addr = "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984"
        assert uni_addr in WELL_KNOWN_TOKENS

        # LINK
        link_addr = "0x514910771AF9Ca656af840dff83E8264EcF986CA"
        assert link_addr in WELL_KNOWN_TOKENS

    def test_well_known_tokens_weth(self):
        """Test WETH token."""
        weth_addr = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        assert weth_addr in WELL_KNOWN_TOKENS
        weth_data = WELL_KNOWN_TOKENS[weth_addr]
        assert weth_data["symbol"] == "WETH"
        assert weth_data["name"] == "Wrapped Ether"
        assert weth_data["decimals"] == 18

    def test_well_known_tokens_data_integrity(self):
        """Test data integrity of well-known tokens."""
        required_fields = ["symbol", "name", "decimals", "is_verified"]

        for address, token_data in WELL_KNOWN_TOKENS.items():
            # Check address format
            assert isinstance(address, str)
            assert len(address) == 42  # 0x + 40 hex chars
            assert address.startswith("0x")

            # Check required fields
            for field in required_fields:
                assert field in token_data

            # Check data types
            assert isinstance(token_data["symbol"], str)
            assert isinstance(token_data["name"], str)
            assert isinstance(token_data["decimals"], int)
            assert isinstance(token_data["is_verified"], bool)

            # Check value ranges
            assert 0 <= token_data["decimals"] <= 18
            assert len(token_data["symbol"]) > 0
            assert len(token_data["name"]) > 0


class TestTypeValidation:
    """Test type validation and edge cases."""

    def test_decimal_precision(self):
        """Test Decimal precision handling."""
        # High precision decimal
        high_precision = Decimal("1.123456789012345678901234567890")
        assert isinstance(high_precision, Decimal)

        # Conversion from string
        from_string = Decimal("123.456")
        assert from_string == Decimal("123.456")

    def test_datetime_handling(self):
        """Test datetime handling in dataclasses."""
        now = datetime.now(UTC)

        # Ensure UTC timezone
        assert now.tzinfo is not None

        # Test in portfolio
        eth_balance = EthBalance(
            balance_wei="1000000000000000000",
            balance_eth=Decimal("1.0"),
        )

        portfolio = WalletPortfolio(
            address="0x742d35cc6634c0532925a3b8d40e3f337abc7b86",
            eth_balance=eth_balance,
            token_balances=[],
            total_value_usd=Decimal("0.0"),
            last_updated=now,
            transaction_count=0,
        )

        assert portfolio.last_updated.tzinfo is not None

    def test_string_address_handling(self):
        """Test string address handling edge cases."""
        # Mixed case
        mixed_case = "0xaAbBcCdDeEfF1234567890aBcDeF1234567890aB"
        normalized = normalize_address(mixed_case)
        assert normalized == mixed_case.lower()

        # Whitespace handling
        with_spaces = "  0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86  "
        # Note: Current implementation doesn't handle whitespace
        # This test documents the current behavior
        assert is_valid_ethereum_address(with_spaces) is False
