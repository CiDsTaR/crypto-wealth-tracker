"""Tests for validation utilities."""

from datetime import datetime
from decimal import Decimal

import pytest

# Note: Tests assume validation.py module will be implemented
# Currently the module is empty


class TestBasicValidators:
    """Test basic validation functions."""

    def test_is_valid_ethereum_address(self):
        """Test Ethereum address validation."""
        from wallet_tracker.utils.validation import is_valid_ethereum_address

        # Valid addresses
        assert is_valid_ethereum_address("0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86") is True
        assert is_valid_ethereum_address("0x0000000000000000000000000000000000000000") is True
        assert is_valid_ethereum_address("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF") is True

        # Invalid addresses
        assert is_valid_ethereum_address("742d35Cc6634C0532925a3b8D40e3f337ABC7b86") is False  # No 0x
        assert is_valid_ethereum_address("0x742d35Cc6634C0532925a3b8D40e3f337ABC7b") is False  # Too short
        assert is_valid_ethereum_address("0x742d35Cc6634C0532925a3b8D40e3f337ABC7b866") is False  # Too long
        assert is_valid_ethereum_address("0xGGGd35Cc6634C0532925a3b8D40e3f337ABC7b86") is False  # Invalid hex
        assert is_valid_ethereum_address("") is False  # Empty
        assert is_valid_ethereum_address(None) is False  # None

    def test_is_valid_token_symbol(self):
        """Test token symbol validation."""
        from wallet_tracker.utils.validation import is_valid_token_symbol

        # Valid symbols
        assert is_valid_token_symbol("ETH") is True
        assert is_valid_token_symbol("USDC") is True
        assert is_valid_token_symbol("BTC") is True
        assert is_valid_token_symbol("AAVE") is True
        assert is_valid_token_symbol("UNI") is True

        # Invalid symbols
        assert is_valid_token_symbol("") is False  # Empty
        assert is_valid_token_symbol("a") is False  # Too short
        assert is_valid_token_symbol("VERYLONGSYMBOL") is False  # Too long
        assert is_valid_token_symbol("eth") is False  # Lowercase
        assert is_valid_token_symbol("ET-H") is False  # Invalid characters
        assert is_valid_token_symbol("123") is False  # Numbers only
        assert is_valid_token_symbol(None) is False  # None

    def test_is_valid_decimal_amount(self):
        """Test decimal amount validation."""
        from wallet_tracker.utils.validation import is_valid_decimal_amount

        # Valid amounts
        assert is_valid_decimal_amount(Decimal("100.50")) is True
        assert is_valid_decimal_amount(Decimal("0")) is True
        assert is_valid_decimal_amount(Decimal("0.000001")) is True
        assert is_valid_decimal_amount(Decimal("999999999.999999")) is True

        # Invalid amounts
        assert is_valid_decimal_amount(Decimal("-10")) is False  # Negative
        assert is_valid_decimal_amount(None) is False  # None
        assert is_valid_decimal_amount("100.50") is False  # String, not Decimal

    def test_is_valid_url(self):
        """Test URL validation."""
        from wallet_tracker.utils.validation import is_valid_url

        # Valid URLs
        assert is_valid_url("https://example.com") is True
        assert is_valid_url("http://api.example.com/v1") is True
        assert is_valid_url("https://api.example.com:8080/path?param=value") is True

        # Invalid URLs
        assert is_valid_url("not-a-url") is False
        assert is_valid_url("ftp://example.com") is False  # Wrong protocol
        assert is_valid_url("") is False  # Empty
        assert is_valid_url(None) is False  # None

    def test_is_valid_api_key(self):
        """Test API key validation."""
        from wallet_tracker.utils.validation import is_valid_api_key

        # Valid API keys
        assert is_valid_api_key("abc123def456") is True
        assert is_valid_api_key("CG-123456789012345678901234567890123456") is True
        assert is_valid_api_key("sk_test_123456789012345678901234567890") is True

        # Invalid API keys
        assert is_valid_api_key("") is False  # Empty
        assert is_valid_api_key("123") is False  # Too short
        assert is_valid_api_key("a" * 200) is False  # Too long
        assert is_valid_api_key(None) is False  # None

    def test_is_valid_spreadsheet_id(self):
        """Test Google Sheets spreadsheet ID validation."""
        from wallet_tracker.utils.validation import is_valid_spreadsheet_id

        # Valid spreadsheet IDs
        assert is_valid_spreadsheet_id("1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms") is True
        assert is_valid_spreadsheet_id("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOP") is True

        # Invalid spreadsheet IDs
        assert is_valid_spreadsheet_id("") is False  # Empty
        assert is_valid_spreadsheet_id("short") is False  # Too short
        assert is_valid_spreadsheet_id("invalid-chars!@#") is False  # Invalid characters
        assert is_valid_spreadsheet_id(None) is False  # None


class TestAdvancedValidators:
    """Test advanced validation functions."""

    def test_validate_wallet_balance_data(self):
        """Test wallet balance data validation."""
        from wallet_tracker.utils.validation import ValidationError, validate_wallet_balance_data

        # Valid balance data
        valid_data = {
            "address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
            "eth_balance": Decimal("1.5"),
            "token_balances": [
                {
                    "symbol": "USDC",
                    "balance": Decimal("1000.0"),
                    "contract_address": "0xA0b86a33E6441e94bB0a8d0F7E5F8D69E2C0e5a0",
                }
            ],
            "total_value_usd": Decimal("3500.0"),
            "last_updated": datetime.now(),
        }

        # Should not raise exception
        validate_wallet_balance_data(valid_data)

        # Invalid data - missing required fields
        invalid_data = {
            "address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"
            # Missing other required fields
        }

        with pytest.raises(ValidationError):
            validate_wallet_balance_data(invalid_data)

    def test_validate_token_price_data(self):
        """Test token price data validation."""
        from wallet_tracker.utils.validation import ValidationError, validate_token_price_data

        # Valid price data
        valid_data = {
            "token_id": "ethereum",
            "symbol": "ETH",
            "current_price_usd": Decimal("2000.50"),
            "market_cap_usd": Decimal("240000000000"),
            "last_updated": datetime.now(),
        }

        validate_token_price_data(valid_data)

        # Invalid data - negative price
        invalid_data = {
            "token_id": "ethereum",
            "symbol": "ETH",
            "current_price_usd": Decimal("-100"),  # Invalid negative price
        }

        with pytest.raises(ValidationError):
            validate_token_price_data(invalid_data)

    def test_validate_configuration_data(self):
        """Test configuration data validation."""
        from wallet_tracker.utils.validation import ValidationError, validate_configuration_data

        # Valid configuration
        valid_config = {
            "ethereum": {
                "alchemy_api_key": "test_key_123456789012345678901234567890",
                "rpc_url": "https://eth-mainnet.g.alchemy.com/v2/test_key",
                "rate_limit": 100,
            },
            "coingecko": {
                "api_key": "CG-123456789012345678901234567890123456",
                "base_url": "https://api.coingecko.com/api/v3",
                "rate_limit": 30,
            },
        }

        validate_configuration_data(valid_config)

        # Invalid configuration - missing required fields
        invalid_config = {
            "ethereum": {
                "alchemy_api_key": "test_key"
                # Missing rpc_url
            }
        }

        with pytest.raises(ValidationError):
            validate_configuration_data(invalid_config)

    def test_validate_batch_processing_data(self):
        """Test batch processing data validation."""
        from wallet_tracker.utils.validation import ValidationError, validate_batch_processing_data

        # Valid batch data
        valid_batch = {
            "addresses": [
                {"address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86", "label": "Wallet 1"},
                {"address": "0x8ba1f109551bD432803012645Hac136c41AA563", "label": "Wallet 2"},
            ],
            "batch_size": 50,
            "max_concurrent": 10,
        }

        validate_batch_processing_data(valid_batch)

        # Invalid batch data - empty addresses
        invalid_batch = {
            "addresses": [],  # Empty list
            "batch_size": 50,
        }

        with pytest.raises(ValidationError):
            validate_batch_processing_data(invalid_batch)


class TestFieldValidators:
    """Test individual field validators."""

    def test_validate_required_fields(self):
        """Test required field validation."""
        from wallet_tracker.utils.validation import ValidationError, validate_required_fields

        data = {"name": "Test", "email": "test@example.com", "age": 25}

        required_fields = ["name", "email"]

        # Should pass - all required fields present
        validate_required_fields(data, required_fields)

        # Should fail - missing required field
        incomplete_data = {"name": "Test"}

        with pytest.raises(ValidationError) as exc_info:
            validate_required_fields(incomplete_data, required_fields)

        assert "email" in str(exc_info.value)

    def test_validate_field_types(self):
        """Test field type validation."""
        from wallet_tracker.utils.validation import ValidationError, validate_field_types

        data = {"name": "Test", "age": 25, "balance": Decimal("100.50"), "active": True}

        type_specs = {"name": str, "age": int, "balance": Decimal, "active": bool}

        # Should pass - all types correct
        validate_field_types(data, type_specs)

        # Should fail - wrong type
        wrong_type_data = {
            "name": "Test",
            "age": "25",  # Should be int, not str
            "balance": Decimal("100.50"),
            "active": True,
        }

        with pytest.raises(ValidationError):
            validate_field_types(wrong_type_data, type_specs)

    def test_validate_field_ranges(self):
        """Test field range validation."""
        from wallet_tracker.utils.validation import ValidationError, validate_field_ranges

        data = {"age": 25, "balance": Decimal("100.50"), "percentage": 85.5}

        range_specs = {
            "age": {"min": 0, "max": 120},
            "balance": {"min": Decimal("0"), "max": Decimal("1000000")},
            "percentage": {"min": 0.0, "max": 100.0},
        }

        # Should pass - all values in range
        validate_field_ranges(data, range_specs)

        # Should fail - value out of range
        out_of_range_data = {
            "age": 150,  # Over max
            "balance": Decimal("100.50"),
            "percentage": 85.5,
        }

        with pytest.raises(ValidationError):
            validate_field_ranges(out_of_range_data, range_specs)

    def test_validate_field_patterns(self):
        """Test field pattern validation."""
        from wallet_tracker.utils.validation import ValidationError, validate_field_patterns

        data = {"email": "user@example.com", "phone": "+1-555-123-4567", "token_symbol": "ETH"}

        pattern_specs = {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "phone": r"^\+\d{1,3}-\d{3}-\d{3}-\d{4}$",
            "token_symbol": r"^[A-Z]{2,10}$",
        }

        # Should pass - all patterns match
        validate_field_patterns(data, pattern_specs)

        # Should fail - pattern doesn't match
        invalid_pattern_data = {
            "email": "invalid-email",  # Doesn't match email pattern
            "phone": "+1-555-123-4567",
            "token_symbol": "ETH",
        }

        with pytest.raises(ValidationError):
            validate_field_patterns(invalid_pattern_data, pattern_specs)


class TestValidationDecorators:
    """Test validation decorators."""

    def test_validate_input_decorator(self):
        """Test input validation decorator."""
        from wallet_tracker.utils.validation import ValidationError, validate_input

        @validate_input(
            {
                "address": {"type": str, "pattern": r"^0x[a-fA-F0-9]{40}$"},
                "amount": {"type": Decimal, "min": Decimal("0")},
            }
        )
        def process_transaction(address: str, amount: Decimal):
            return f"Processing {amount} to {address}"

        # Valid input
        result = process_transaction(address="0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86", amount=Decimal("100.5"))
        assert "Processing" in result

        # Invalid input
        with pytest.raises(ValidationError):
            process_transaction(address="invalid-address", amount=Decimal("100.5"))

    def test_validate_output_decorator(self):
        """Test output validation decorator."""
        from wallet_tracker.utils.validation import ValidationError, validate_output

        @validate_output(
            {
                "status": {"type": str, "choices": ["success", "error", "pending"]},
                "value": {"type": Decimal, "min": Decimal("0")},
            }
        )
        def get_transaction_status():
            return {"status": "success", "value": Decimal("250.75")}

        # Should pass - valid output
        result = get_transaction_status()
        assert result["status"] == "success"

        # Invalid output would be caught by decorator
        @validate_output(
            {
                "status": {"type": str, "choices": ["success", "error"]},
            }
        )
        def invalid_output_function():
            return {"status": "invalid_status"}  # Not in choices

        with pytest.raises(ValidationError):
            invalid_output_function()

    def test_validate_schema_decorator(self):
        """Test schema validation decorator."""
        from wallet_tracker.utils.validation import validate_schema

        schema = {
            "type": "object",
            "properties": {
                "wallet_address": {"type": "string", "pattern": "^0x[a-fA-F0-9]{40}$"},
                "balance": {"type": "number", "minimum": 0},
            },
            "required": ["wallet_address", "balance"],
        }

        @validate_schema(schema)
        def process_wallet_data(data):
            return f"Processing wallet {data['wallet_address']}"

        # Valid data
        valid_data = {"wallet_address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86", "balance": 100.5}

        result = process_wallet_data(valid_data)
        assert "Processing wallet" in result


class TestCustomValidators:
    """Test custom validator classes."""

    def test_wallet_address_validator(self):
        """Test wallet address validator class."""
        from wallet_tracker.utils.validation import ValidationError, WalletAddressValidator

        validator = WalletAddressValidator()

        # Valid addresses
        validator.validate("0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86")
        validator.validate("0x0000000000000000000000000000000000000000")

        # Invalid addresses
        with pytest.raises(ValidationError):
            validator.validate("invalid-address")

        with pytest.raises(ValidationError):
            validator.validate(None)

    def test_token_data_validator(self):
        """Test token data validator class."""
        from wallet_tracker.utils.validation import TokenDataValidator, ValidationError

        validator = TokenDataValidator()

        valid_token_data = {
            "symbol": "ETH",
            "name": "Ethereum",
            "decimals": 18,
            "contract_address": "0x0000000000000000000000000000000000000000",
            "current_price_usd": Decimal("2000.50"),
        }

        # Should pass
        validator.validate(valid_token_data)

        # Invalid data
        invalid_token_data = {
            "symbol": "eth",  # Should be uppercase
            "name": "Ethereum",
            "decimals": -1,  # Invalid decimals
        }

        with pytest.raises(ValidationError):
            validator.validate(invalid_token_data)

    def test_configuration_validator(self):
        """Test configuration validator class."""
        from wallet_tracker.utils.validation import ConfigurationValidator, ValidationError

        validator = ConfigurationValidator()

        valid_config = {
            "ethereum": {
                "alchemy_api_key": "test_key_123456789012345678901234567890",
                "rpc_url": "https://eth-mainnet.g.alchemy.com/v2/test_key",
                "rate_limit": 100,
            },
            "cache": {"backend": "redis", "ttl_prices": 300, "ttl_balances": 150},
        }

        # Should pass
        validator.validate(valid_config)

        # Invalid config
        invalid_config = {
            "ethereum": {
                "alchemy_api_key": "",  # Empty API key
                "rpc_url": "invalid-url",  # Invalid URL
                "rate_limit": -1,  # Invalid rate limit
            }
        }

        with pytest.raises(ValidationError):
            validator.validate(invalid_config)


class TestValidationChains:
    """Test validation chains and composite validators."""

    def test_validation_chain(self):
        """Test chaining multiple validators."""
        from wallet_tracker.utils.validation import ValidationChain, ValidationError

        chain = ValidationChain()

        # Add validators to chain
        chain.add_validator("required_fields", ["address", "amount"])
        chain.add_validator("field_types", {"address": str, "amount": Decimal})
        chain.add_validator("field_patterns", {"address": r"^0x[a-fA-F0-9]{40}$"})
        chain.add_validator("field_ranges", {"amount": {"min": Decimal("0")}})

        # Valid data should pass all validators
        valid_data = {"address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86", "amount": Decimal("100.5")}

        chain.validate(valid_data)

        # Invalid data should fail at appropriate validator
        invalid_data = {
            "address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
            "amount": Decimal("-10"),  # Fails range validation
        }

        with pytest.raises(ValidationError) as exc_info:
            chain.validate(invalid_data)

        assert "range" in str(exc_info.value).lower()

    def test_conditional_validation(self):
        """Test conditional validation logic."""
        from wallet_tracker.utils.validation import ConditionalValidator

        def condition_func(data):
            return data.get("type") == "token_transfer"

        validator = ConditionalValidator(
            condition=condition_func,
            validators={
                "required_fields": ["token_address", "recipient"],
                "field_types": {"token_address": str, "recipient": str},
            },
        )

        # Should validate when condition is true
        token_data = {
            "type": "token_transfer",
            "token_address": "0xA0b86a33E6441e94bB0a8d0F7E5F8D69E2C0e5a0",
            "recipient": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
        }

        validator.validate(token_data)

        # Should skip validation when condition is false
        eth_data = {
            "type": "eth_transfer",
            "recipient": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
            # Missing token_address, but that's OK for eth_transfer
        }

        validator.validate(eth_data)  # Should not raise error

    def test_composite_validator(self):
        """Test composite validator with multiple validation strategies."""
        from wallet_tracker.utils.validation import CompositeValidator, ValidationError

        validator = CompositeValidator()

        # Add different types of validation
        validator.add_basic_validation("address", str, r"^0x[a-fA-F0-9]{40}$")
        validator.add_range_validation("amount", Decimal("0"), Decimal("1000000"))
        validator.add_choice_validation("currency", ["USD", "ETH", "BTC"])
        validator.add_custom_validation("timestamp", lambda x: isinstance(x, datetime))

        # Valid data
        valid_data = {
            "address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
            "amount": Decimal("100.5"),
            "currency": "USD",
            "timestamp": datetime.now(),
        }

        validator.validate(valid_data)

        # Invalid data
        invalid_data = {
            "address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
            "amount": Decimal("100.5"),
            "currency": "INVALID",  # Not in choices
            "timestamp": datetime.now(),
        }

        with pytest.raises(ValidationError):
            validator.validate(invalid_data)


class TestValidationUtils:
    """Test validation utility functions."""

    def test_sanitize_input(self):
        """Test input sanitization."""
        from wallet_tracker.utils.validation import sanitize_input

        # String sanitization
        assert sanitize_input("  test  ") == "test"
        assert sanitize_input("Test@#$%") == "Test"
        assert sanitize_input("<script>alert('xss')</script>") == "scriptalert('xss')/script"

        # Number sanitization
        assert sanitize_input("123.45") == "123.45"
        assert sanitize_input("$1,234.56") == "1234.56"

    def test_normalize_address(self):
        """Test address normalization."""
        from wallet_tracker.utils.validation import normalize_address

        # Should convert to lowercase and add 0x prefix
        assert (
            normalize_address("742d35Cc6634C0532925a3b8D40e3f337ABC7b86")
            == "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"
        )
        assert (
            normalize_address("0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86")
            == "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"
        )

        # Should handle None and empty strings
        assert normalize_address(None) is None
        assert normalize_address("") == ""

    def test_format_validation_error(self):
        """Test validation error formatting."""
        from wallet_tracker.utils.validation import ValidationError, format_validation_error

        error = ValidationError("Field 'address' is invalid")
        formatted = format_validation_error(error)

        assert "address" in formatted
        assert "invalid" in formatted

    def test_validate_and_convert(self):
        """Test validation with type conversion."""
        from wallet_tracker.utils.validation import validate_and_convert

        # String to Decimal conversion
        result = validate_and_convert("123.45", Decimal)
        assert result == Decimal("123.45")
        assert isinstance(result, Decimal)

        # String to int conversion
        result = validate_and_convert("42", int)
        assert result == 42
        assert isinstance(result, int)

        # Invalid conversion should raise error
        with pytest.raises(ValueError):
            validate_and_convert("not-a-number", int)


class TestValidationIntegration:
    """Integration tests for validation functionality."""

    def test_end_to_end_wallet_validation(self):
        """Test complete wallet data validation workflow."""
        from wallet_tracker.utils.validation import WalletDataValidator

        validator = WalletDataValidator()

        # Complete wallet data
        wallet_data = {
            "address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
            "label": "Test Wallet",
            "eth_balance": Decimal("1.5"),
            "token_balances": [
                {
                    "symbol": "USDC",
                    "contract_address": "0xA0b86a33E6441e94bB0a8d0F7E5F8D69E2C0e5a0",
                    "balance": Decimal("1000.0"),
                    "decimals": 6,
                },
                {
                    "symbol": "DAI",
                    "contract_address": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                    "balance": Decimal("500.0"),
                    "decimals": 18,
                },
            ],
            "total_value_usd": Decimal("3500.0"),
            "last_updated": datetime.now(),
            "transaction_count": 150,
            "is_active": True,
        }

        # Should pass validation
        validated_data = validator.validate_and_clean(wallet_data)

        assert validated_data["address"] == "0x742d35cc6634c0532925a3b8d40e3f337abc7b86"  # Normalized
        assert len(validated_data["token_balances"]) == 2
        assert validated_data["total_value_usd"] == Decimal("3500.0")

    def test_batch_validation(self):
        """Test batch validation of multiple items."""
        from wallet_tracker.utils.validation import BatchValidator

        validator = BatchValidator()

        wallet_addresses = [
            "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
            "0x8ba1f109551bD432803012645Hac136c41AA563",
            "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
        ]

        # Valid batch
        results = validator.validate_batch(wallet_addresses, "ethereum_address")
        assert len(results["valid"]) == 3
        assert len(results["invalid"]) == 0

        # Mixed valid/invalid batch
        mixed_addresses = [
            "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",  # Valid
            "invalid-address",  # Invalid
            "0x8ba1f109551bD432803012645Hac136c41AA563",  # Valid
        ]

        results = validator.validate_batch(mixed_addresses, "ethereum_address")
        assert len(results["valid"]) == 2
        assert len(results["invalid"]) == 1
        assert "invalid-address" in results["invalid"]

    def test_configuration_validation_workflow(self):
        """Test complete configuration validation workflow."""
        from wallet_tracker.utils.validation import ConfigurationValidator

        validator = ConfigurationValidator()

        # Test configuration from environment variables
        config_data = {
            "ethereum": {
                "alchemy_api_key": "test_key_123456789012345678901234567890",
                "rpc_url": "https://eth-mainnet.g.alchemy.com/v2/test_key",
                "rate_limit": 100,
            },
            "coingecko": {
                "api_key": "CG-123456789012345678901234567890123456",
                "base_url": "https://api.coingecko.com/api/v3",
                "rate_limit": 30,
            },
            "cache": {
                "backend": "redis",
                "redis_url": "redis://localhost:6379/0",
                "ttl_prices": 300,
                "ttl_balances": 150,
            },
            "processing": {"batch_size": 50, "max_concurrent": 10, "timeout_seconds": 30},
        }

        # Should validate successfully
        validation_result = validator.validate_configuration(config_data)

        assert validation_result["valid"] is True
        assert len(validation_result["errors"]) == 0
        assert "normalized_config" in validation_result

        # Test with missing required configuration
        incomplete_config = {
            "ethereum": {
                "alchemy_api_key": "test_key"
                # Missing rpc_url
            }
        }

        validation_result = validator.validate_configuration(incomplete_config)

        assert validation_result["valid"] is False
        assert len(validation_result["errors"]) > 0
        assert "rpc_url" in str(validation_result["errors"])


class TestValidationPerformance:
    """Test validation performance and optimization."""

    def test_cached_validation(self):
        """Test validation result caching."""
        from wallet_tracker.utils.validation import CachedValidator

        call_count = 0

        def expensive_validation(value):
            nonlocal call_count
            call_count += 1
            return len(value) > 5

        validator = CachedValidator(expensive_validation, max_cache_size=100)

        # First call should execute validation
        result1 = validator.validate("test_value")
        assert call_count == 1
        assert result1 is True

        # Second call with same value should use cache
        result2 = validator.validate("test_value")
        assert call_count == 1  # Should not increment
        assert result2 is True

        # Different value should execute validation
        result3 = validator.validate("short")
        assert call_count == 2
        assert result3 is False

    def test_lazy_validation(self):
        """Test lazy validation for performance."""
        from wallet_tracker.utils.validation import LazyValidator

        validation_calls = []

        def track_validation(field_name, value):
            validation_calls.append(field_name)
            return True

        validator = LazyValidator()
        validator.add_field_validator("expensive_field", track_validation)
        validator.add_field_validator("cheap_field", track_validation)

        data = {"cheap_field": "value1", "expensive_field": "value2"}

        # Only validate requested fields
        result = validator.validate_fields(data, ["cheap_field"])

        assert "cheap_field" in validation_calls
        assert "expensive_field" not in validation_calls

    def test_parallel_validation(self):
        """Test parallel validation for large datasets."""
        import asyncio

        from wallet_tracker.utils.validation import ParallelValidator

        async def async_validation(item):
            # Simulate async validation work
            await asyncio.sleep(0.001)
            return len(item) > 3

        validator = ParallelValidator(async_validation, max_workers=5)

        items = [f"item_{i}" for i in range(20)]

        # Should validate all items in parallel
        results = asyncio.run(validator.validate_batch(items))

        assert len(results) == 20
        assert all(results)  # All items should pass (length > 3)


class TestValidationErrorHandling:
    """Test validation error handling and reporting."""

    def test_validation_error_details(self):
        """Test detailed validation error information."""
        from wallet_tracker.utils.validation import DetailedValidationError, ValidationContext

        context = ValidationContext(
            field_path="user.wallet.address", data_type="ethereum_address", value="invalid-address"
        )

        error = DetailedValidationError(
            message="Invalid Ethereum address format",
            context=context,
            code="INVALID_ETH_ADDRESS",
            suggestions=["Ensure address starts with 0x", "Verify address length is 42 characters"],
        )

        assert error.field_path == "user.wallet.address"
        assert error.code == "INVALID_ETH_ADDRESS"
        assert len(error.suggestions) == 2
        assert "0x" in error.suggestions[0]

    def test_validation_error_collection(self):
        """Test collecting multiple validation errors."""
        from wallet_tracker.utils.validation import ValidationErrorCollector

        collector = ValidationErrorCollector()

        # Add multiple errors
        collector.add_error("address", "Invalid format")
        collector.add_error("amount", "Must be positive")
        collector.add_error("currency", "Invalid currency code")

        # Check error collection
        assert collector.has_errors() is True
        assert len(collector.get_errors()) == 3

        # Get formatted error report
        report = collector.get_error_report()

        assert "address" in report
        assert "amount" in report
        assert "currency" in report

    def test_validation_warning_system(self):
        """Test validation warning system."""
        from wallet_tracker.utils.validation import ValidationWarningCollector

        collector = ValidationWarningCollector()

        # Add warnings (non-critical issues)
        collector.add_warning("balance", "Balance seems unusually high", severity="medium")
        collector.add_warning("timestamp", "Timestamp is in the future", severity="low")

        warnings = collector.get_warnings()

        assert len(warnings) == 2
        assert warnings[0]["severity"] == "medium"
        assert warnings[1]["severity"] == "low"

    def test_validation_recovery_strategies(self):
        """Test validation error recovery strategies."""
        from wallet_tracker.utils.validation import RecoverableValidator

        def attempt_fix_address(value):
            if not value.startswith("0x"):
                return f"0x{value}"
            return value

        def attempt_fix_amount(value):
            if isinstance(value, str):
                return float(value.replace("$", "").replace(",", ""))
            return value

        validator = RecoverableValidator()
        validator.add_recovery_strategy("address", attempt_fix_address)
        validator.add_recovery_strategy("amount", attempt_fix_amount)

        # Test data with fixable issues
        data = {
            "address": "742d35Cc6634C0532925a3b8D40e3f337ABC7b86",  # Missing 0x
            "amount": "$1,234.56",  # String with formatting
        }

        result = validator.validate_and_recover(data)

        assert result["data"]["address"] == "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86"
        assert result["data"]["amount"] == 1234.56
        assert len(result["recoveries"]) == 2


class TestValidationExtensions:
    """Test validation system extensions and plugins."""

    def test_custom_validator_plugin(self):
        """Test custom validator plugin system."""
        from wallet_tracker.utils.validation import ValidationRegistry, ValidatorPlugin

        class TokenSymbolPlugin(ValidatorPlugin):
            def validate(self, value, **kwargs):
                if not isinstance(value, str):
                    return False, "Token symbol must be a string"

                if not value.isupper():
                    return False, "Token symbol must be uppercase"

                if len(value) < 2 or len(value) > 10:
                    return False, "Token symbol must be 2-10 characters"

                return True, None

        registry = ValidationRegistry()
        registry.register_plugin("token_symbol", TokenSymbolPlugin())

        # Valid token symbol
        valid, error = registry.validate("token_symbol", "ETH")
        assert valid is True
        assert error is None

        # Invalid token symbol
        valid, error = registry.validate("token_symbol", "eth")
        assert valid is False
        assert "uppercase" in error

    def test_validation_middleware(self):
        """Test validation middleware system."""
        from wallet_tracker.utils.validation import ValidationChain, ValidationMiddleware

        class LoggingMiddleware(ValidationMiddleware):
            def __init__(self):
                self.logged_validations = []

            def before_validation(self, field_name, value):
                self.logged_validations.append(f"Validating {field_name}: {value}")

            def after_validation(self, field_name, value, result):
                status = "PASS" if result else "FAIL"
                self.logged_validations.append(f"Result {field_name}: {status}")

        class SanitizationMiddleware(ValidationMiddleware):
            def before_validation(self, field_name, value):
                if isinstance(value, str):
                    return value.strip().lower()
                return value

        chain = ValidationChain()
        logging_middleware = LoggingMiddleware()
        sanitization_middleware = SanitizationMiddleware()

        chain.add_middleware(logging_middleware)
        chain.add_middleware(sanitization_middleware)

        # Test with middleware
        result = chain.validate_field("email", "  TEST@EXAMPLE.COM  ")

        # Check that logging occurred
        assert len(logging_middleware.logged_validations) >= 2
        assert "Validating email" in logging_middleware.logged_validations[0]

    def test_validation_rules_engine(self):
        """Test rules-based validation engine."""
        from wallet_tracker.utils.validation import ValidationRulesEngine

        # Define validation rules in JSON-like format
        rules = {
            "wallet_data": {
                "address": {
                    "required": True,
                    "type": "string",
                    "pattern": r"^0x[a-fA-F0-9]{40}$",
                    "error_message": "Invalid Ethereum address",
                },
                "balance": {
                    "required": True,
                    "type": "decimal",
                    "minimum": 0,
                    "error_message": "Balance must be non-negative",
                },
                "tokens": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "symbol": {"type": "string", "pattern": r"^[A-Z]{2,10}$"},
                            "amount": {"type": "decimal", "minimum": 0},
                        },
                    },
                },
            }
        }

        engine = ValidationRulesEngine(rules)

        # Valid data
        valid_data = {
            "address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
            "balance": Decimal("100.5"),
            "tokens": [{"symbol": "USDC", "amount": Decimal("1000")}, {"symbol": "DAI", "amount": Decimal("500")}],
        }

        result = engine.validate("wallet_data", valid_data)
        assert result.is_valid is True

        # Invalid data
        invalid_data = {
            "address": "invalid-address",
            "balance": Decimal("-10"),  # Negative balance
            "tokens": [
                {"symbol": "usdc", "amount": Decimal("1000")}  # Lowercase symbol
            ],
        }

        result = engine.validate("wallet_data", invalid_data)
        assert result.is_valid is False
        assert len(result.errors) >= 2


class TestValidationBenchmarks:
    """Test validation performance benchmarks."""

    def test_validation_performance_benchmark(self):
        """Test validation performance with large datasets."""
        import time

        from wallet_tracker.utils.validation import PerformanceValidator

        validator = PerformanceValidator()

        # Generate test data
        test_addresses = [f"0x{'1' * 39}{str(i).zfill(1)}" for i in range(1000)]

        # Benchmark validation
        start_time = time.time()

        results = validator.validate_batch(test_addresses, "ethereum_address")

        end_time = time.time()
        duration = end_time - start_time

        # Should validate 1000 addresses reasonably quickly
        assert duration < 1.0  # Less than 1 second
        assert len(results["valid"]) == 1000

        # Get performance metrics
        metrics = validator.get_performance_metrics()

        assert metrics["total_validations"] >= 1000
        assert metrics["average_validation_time"] > 0
        assert metrics["validations_per_second"] > 0

    def test_memory_efficient_validation(self):
        """Test memory-efficient validation for large datasets."""
        from wallet_tracker.utils.validation import StreamingValidator

        def address_generator():
            for i in range(10000):
                yield f"0x{'1' * 39}{str(i).zfill(1)}"

        validator = StreamingValidator()

        # Process in streaming fashion
        valid_count = 0
        invalid_count = 0

        for address in validator.validate_stream(address_generator(), "ethereum_address"):
            if address["valid"]:
                valid_count += 1
            else:
                invalid_count += 1

        assert valid_count == 10000
        assert invalid_count == 0

    def test_concurrent_validation(self):
        """Test concurrent validation performance."""
        import asyncio

        from wallet_tracker.utils.validation import ConcurrentValidator

        validator = ConcurrentValidator(max_workers=4)

        # Create large dataset
        test_data = [{"address": f"0x{'1' * 39}{str(i).zfill(1)}", "amount": Decimal(str(i))} for i in range(1000)]

        async def run_validation():
            results = await validator.validate_concurrent(test_data, "wallet_transaction")
            return results

        # Should handle concurrent validation efficiently
        results = asyncio.run(run_validation())

        assert len(results["valid"]) == 1000
        assert len(results["invalid"]) == 0


# Note: This test file assumes the validation.py module will be implemented
# with the following classes and functions:
#
# - Basic validators: is_valid_ethereum_address, is_valid_token_symbol, etc.
# - Advanced validators: validate_wallet_balance_data, validate_token_price_data, etc.
# - Field validators: validate_required_fields, validate_field_types, etc.
# - Decorator validators: validate_input, validate_output, validate_schema
# - Custom validator classes: WalletAddressValidator, TokenDataValidator, etc.
# - Validation chains and composite validators
# - Error handling: ValidationError, DetailedValidationError, etc.
# - Performance utilities: CachedValidator, ParallelValidator, etc.
# - Extension system: ValidatorPlugin, ValidationMiddleware, etc.
#
# The implementation should follow these specifications based on the tests above.
