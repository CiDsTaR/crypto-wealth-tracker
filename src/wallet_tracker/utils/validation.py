"""Validation utilities for the Crypto Wealth Tracker application."""

import json
import re
import statistics
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from datetime import datetime
from decimal import Decimal
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Pattern, Type, Union
from urllib.parse import urlparse

import jsonschema


class ValidationError(Exception):
    """Base validation error."""

    def __init__(self, message: str, field: Optional[str] = None):
        super().__init__(message)
        self.message = message
        self.field = field


class ValidationContext:
    """Context information for validation."""

    def __init__(
            self,
            field_path: str,
            data_type: str,
            value: Any
    ):
        self.field_path = field_path
        self.data_type = data_type
        self.value = value


class DetailedValidationError(ValidationError):
    """Detailed validation error with context and suggestions."""

    def __init__(
            self,
            message: str,
            context: ValidationContext,
            code: str,
            suggestions: Optional[List[str]] = None
    ):
        super().__init__(message, context.field_path)
        self.context = context
        self.code = code
        self.suggestions = suggestions or []


# Basic Validators

def is_valid_ethereum_address(address: Any) -> bool:
    """Validate Ethereum address format.

    Args:
        address: Address to validate

    Returns:
        True if valid Ethereum address
    """
    if not isinstance(address, str):
        return False

    if not address:
        return False

    # Must start with 0x and be 42 characters total
    if not address.startswith('0x') or len(address) != 42:
        return False

    # Check if the rest are valid hexadecimal characters
    hex_part = address[2:]
    try:
        int(hex_part, 16)
        return True
    except ValueError:
        return False


def is_valid_token_symbol(symbol: Any) -> bool:
    """Validate token symbol format.

    Args:
        symbol: Symbol to validate

    Returns:
        True if valid token symbol
    """
    if not isinstance(symbol, str):
        return False

    if not symbol:
        return False

    # Must be 2-10 characters, uppercase letters only
    if len(symbol) < 2 or len(symbol) > 10:
        return False

    if not symbol.isupper():
        return False

    if not symbol.isalpha():
        return False

    return True


def is_valid_decimal_amount(amount: Any) -> bool:
    """Validate decimal amount.

    Args:
        amount: Amount to validate

    Returns:
        True if valid decimal amount
    """
    if not isinstance(amount, Decimal):
        return False

    if amount < 0:
        return False

    return True


def is_valid_url(url: Any) -> bool:
    """Validate URL format.

    Args:
        url: URL to validate

    Returns:
        True if valid URL
    """
    if not isinstance(url, str):
        return False

    if not url:
        return False

    try:
        parsed = urlparse(url)
        return parsed.scheme in ['http', 'https'] and bool(parsed.netloc)
    except Exception:
        return False


def is_valid_api_key(api_key: Any) -> bool:
    """Validate API key format.

    Args:
        api_key: API key to validate

    Returns:
        True if valid API key
    """
    if not isinstance(api_key, str):
        return False

    if not api_key:
        return False

    # Must be at least 8 characters and no more than 128
    if len(api_key) < 8 or len(api_key) > 128:
        return False

    return True


def is_valid_spreadsheet_id(spreadsheet_id: Any) -> bool:
    """Validate Google Sheets spreadsheet ID.

    Args:
        spreadsheet_id: Spreadsheet ID to validate

    Returns:
        True if valid spreadsheet ID
    """
    if not isinstance(spreadsheet_id, str):
        return False

    if not spreadsheet_id:
        return False

    # Must be at least 20 characters (typical Google Sheet ID length)
    if len(spreadsheet_id) < 20:
        return False

    # Should only contain alphanumeric characters, hyphens, and underscores
    if not re.match(r'^[a-zA-Z0-9_-]+$', spreadsheet_id):
        return False

    return True


# Advanced Validators

def validate_wallet_balance_data(data: Dict[str, Any]) -> None:
    """Validate wallet balance data structure.

    Args:
        data: Wallet balance data to validate

    Raises:
        ValidationError: If data is invalid
    """
    required_fields = ["address", "eth_balance", "token_balances", "total_value_usd", "last_updated"]
    validate_required_fields(data, required_fields)

    # Validate address
    if not is_valid_ethereum_address(data["address"]):
        raise ValidationError("Invalid Ethereum address format", "address")

    # Validate balances
    if not is_valid_decimal_amount(data["eth_balance"]):
        raise ValidationError("Invalid ETH balance", "eth_balance")

    if not is_valid_decimal_amount(data["total_value_usd"]):
        raise ValidationError("Invalid total value USD", "total_value_usd")

    # Validate token balances
    if not isinstance(data["token_balances"], list):
        raise ValidationError("Token balances must be a list", "token_balances")

    for i, token_balance in enumerate(data["token_balances"]):
        if not isinstance(token_balance, dict):
            raise ValidationError(f"Token balance {i} must be a dict", f"token_balances[{i}]")

        token_required = ["symbol", "balance", "contract_address"]
        for field in token_required:
            if field not in token_balance:
                raise ValidationError(f"Missing required field: {field}", f"token_balances[{i}].{field}")

    # Validate timestamp
    if not isinstance(data["last_updated"], datetime):
        raise ValidationError("Invalid last_updated timestamp", "last_updated")


def validate_token_price_data(data: Dict[str, Any]) -> None:
    """Validate token price data structure.

    Args:
        data: Token price data to validate

    Raises:
        ValidationError: If data is invalid
    """
    required_fields = ["token_id", "symbol", "current_price_usd"]
    validate_required_fields(data, required_fields)

    # Validate token symbol
    if not is_valid_token_symbol(data["symbol"]):
        raise ValidationError("Invalid token symbol", "symbol")

    # Validate price
    if not is_valid_decimal_amount(data["current_price_usd"]):
        raise ValidationError("Invalid current price USD", "current_price_usd")

    # Check for negative price
    if data["current_price_usd"] < 0:
        raise ValidationError("Price cannot be negative", "current_price_usd")


def validate_configuration_data(data: Dict[str, Any]) -> None:
    """Validate configuration data structure.

    Args:
        data: Configuration data to validate

    Raises:
        ValidationError: If data is invalid
    """
    # Validate Ethereum config
    if "ethereum" in data:
        eth_config = data["ethereum"]
        eth_required = ["alchemy_api_key", "rpc_url"]
        validate_required_fields(eth_config, eth_required)

        if not is_valid_api_key(eth_config["alchemy_api_key"]):
            raise ValidationError("Invalid Alchemy API key", "ethereum.alchemy_api_key")

        if not is_valid_url(eth_config["rpc_url"]):
            raise ValidationError("Invalid RPC URL", "ethereum.rpc_url")

    # Validate CoinGecko config
    if "coingecko" in data:
        cg_config = data["coingecko"]
        if "api_key" in cg_config and not is_valid_api_key(cg_config["api_key"]):
            raise ValidationError("Invalid CoinGecko API key", "coingecko.api_key")

        if "base_url" in cg_config and not is_valid_url(cg_config["base_url"]):
            raise ValidationError("Invalid CoinGecko base URL", "coingecko.base_url")


def validate_batch_processing_data(data: Dict[str, Any]) -> None:
    """Validate batch processing data structure.

    Args:
        data: Batch processing data to validate

    Raises:
        ValidationError: If data is invalid
    """
    required_fields = ["addresses", "batch_size"]
    validate_required_fields(data, required_fields)

    # Validate addresses
    addresses = data["addresses"]
    if not isinstance(addresses, list):
        raise ValidationError("Addresses must be a list", "addresses")

    if not addresses:
        raise ValidationError("Addresses list cannot be empty", "addresses")

    for i, addr_data in enumerate(addresses):
        if not isinstance(addr_data, dict):
            raise ValidationError(f"Address {i} must be a dict", f"addresses[{i}]")

        if "address" not in addr_data:
            raise ValidationError(f"Missing address field", f"addresses[{i}].address")

        if not is_valid_ethereum_address(addr_data["address"]):
            raise ValidationError(f"Invalid Ethereum address", f"addresses[{i}].address")


# Field Validators

def validate_required_fields(data: Dict[str, Any], required_fields: List[str]) -> None:
    """Validate that all required fields are present.

    Args:
        data: Data to validate
        required_fields: List of required field names

    Raises:
        ValidationError: If any required field is missing
    """
    for field in required_fields:
        if field not in data:
            raise ValidationError(f"Missing required field: {field}", field)


def validate_field_types(data: Dict[str, Any], type_specs: Dict[str, Type]) -> None:
    """Validate field types.

    Args:
        data: Data to validate
        type_specs: Dictionary mapping field names to expected types

    Raises:
        ValidationError: If any field has wrong type
    """
    for field_name, expected_type in type_specs.items():
        if field_name in data:
            value = data[field_name]
            if not isinstance(value, expected_type):
                raise ValidationError(
                    f"Field '{field_name}' must be of type {expected_type.__name__}, got {type(value).__name__}",
                    field_name
                )


def validate_field_ranges(data: Dict[str, Any], range_specs: Dict[str, Dict[str, Any]]) -> None:
    """Validate field value ranges.

    Args:
        data: Data to validate
        range_specs: Dictionary mapping field names to range specifications

    Raises:
        ValidationError: If any field value is out of range
    """
    for field_name, range_spec in range_specs.items():
        if field_name in data:
            value = data[field_name]

            if "min" in range_spec and value < range_spec["min"]:
                raise ValidationError(
                    f"Field '{field_name}' value {value} is below minimum {range_spec['min']}",
                    field_name
                )

            if "max" in range_spec and value > range_spec["max"]:
                raise ValidationError(
                    f"Field '{field_name}' value {value} is above maximum {range_spec['max']}",
                    field_name
                )


def validate_field_patterns(data: Dict[str, Any], pattern_specs: Dict[str, str]) -> None:
    """Validate field patterns using regular expressions.

    Args:
        data: Data to validate
        pattern_specs: Dictionary mapping field names to regex patterns

    Raises:
        ValidationError: If any field value doesn't match pattern
    """
    for field_name, pattern in pattern_specs.items():
        if field_name in data:
            value = data[field_name]
            if isinstance(value, str):
                if not re.match(pattern, value):
                    raise ValidationError(
                        f"Field '{field_name}' value '{value}' doesn't match required pattern",
                        field_name
                    )


# Validation Decorators

def validate_input(field_specs: Dict[str, Dict[str, Any]]):
    """Decorator to validate function input parameters.

    Args:
        field_specs: Dictionary mapping parameter names to validation specs

    Returns:
        Decorated function
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get function signature and bind arguments
            import inspect
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            # Validate each specified parameter
            for param_name, spec in field_specs.items():
                if param_name in bound_args.arguments:
                    value = bound_args.arguments[param_name]

                    # Type validation
                    if "type" in spec:
                        if not isinstance(value, spec["type"]):
                            raise ValidationError(f"Parameter '{param_name}' must be of type {spec['type'].__name__}")

                    # Pattern validation
                    if "pattern" in spec and isinstance(value, str):
                        if not re.match(spec["pattern"], value):
                            raise ValidationError(f"Parameter '{param_name}' doesn't match required pattern")

                    # Range validation
                    if "min" in spec and value < spec["min"]:
                        raise ValidationError(f"Parameter '{param_name}' is below minimum {spec['min']}")

                    if "max" in spec and value > spec["max"]:
                        raise ValidationError(f"Parameter '{param_name}' is above maximum {spec['max']}")

            return func(*args, **kwargs)

        return wrapper

    return decorator


def validate_output(field_specs: Dict[str, Dict[str, Any]]):
    """Decorator to validate function output.

    Args:
        field_specs: Dictionary mapping output field names to validation specs

    Returns:
        Decorated function
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)

            if isinstance(result, dict):
                # Validate output fields
                for field_name, spec in field_specs.items():
                    if field_name in result:
                        value = result[field_name]

                        # Type validation
                        if "type" in spec:
                            if not isinstance(value, spec["type"]):
                                raise ValidationError(
                                    f"Output field '{field_name}' must be of type {spec['type'].__name__}")

                        # Choice validation
                        if "choices" in spec and value not in spec["choices"]:
                            raise ValidationError(f"Output field '{field_name}' must be one of {spec['choices']}")

                        # Range validation
                        if "min" in spec and value < spec["min"]:
                            raise ValidationError(f"Output field '{field_name}' is below minimum {spec['min']}")

            return result

        return wrapper

    return decorator


def validate_schema(schema: Dict[str, Any]):
    """Decorator to validate data against JSON schema.

    Args:
        schema: JSON schema for validation

    Returns:
        Decorated function
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Assume first argument is the data to validate
            if args:
                data = args[0]
                try:
                    jsonschema.validate(data, schema)
                except jsonschema.ValidationError as e:
                    raise ValidationError(f"Schema validation failed: {e.message}")

            return func(*args, **kwargs)

        return wrapper

    return decorator


# Custom Validator Classes

class WalletAddressValidator:
    """Validator for wallet addresses."""

    def validate(self, address: Any) -> None:
        """Validate wallet address.

        Args:
            address: Address to validate

        Raises:
            ValidationError: If address is invalid
        """
        if not is_valid_ethereum_address(address):
            raise ValidationError("Invalid Ethereum address format")


class TokenDataValidator:
    """Validator for token data."""

    def validate(self, token_data: Dict[str, Any]) -> None:
        """Validate token data.

        Args:
            token_data: Token data to validate

        Raises:
            ValidationError: If token data is invalid
        """
        required_fields = ["symbol", "name", "decimals"]
        validate_required_fields(token_data, required_fields)

        # Validate symbol
        if not is_valid_token_symbol(token_data["symbol"]):
            raise ValidationError("Invalid token symbol")

        # Validate decimals
        decimals = token_data["decimals"]
        if not isinstance(decimals, int) or decimals < 0 or decimals > 18:
            raise ValidationError("Invalid token decimals (must be 0-18)")

        # Validate price if present
        if "current_price_usd" in token_data:
            if not is_valid_decimal_amount(token_data["current_price_usd"]):
                raise ValidationError("Invalid current price USD")


class ConfigurationValidator:
    """Validator for configuration data."""

    def validate(self, config_data: Dict[str, Any]) -> None:
        """Validate configuration data.

        Args:
            config_data: Configuration data to validate

        Raises:
            ValidationError: If configuration is invalid
        """
        validate_configuration_data(config_data)

    def validate_configuration(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate configuration and return result.

        Args:
            config_data: Configuration data to validate

        Returns:
            Validation result dictionary
        """
        errors = []
        warnings = []

        try:
            self.validate(config_data)
            return {
                "valid": True,
                "errors": errors,
                "warnings": warnings,
                "normalized_config": config_data.copy()
            }
        except ValidationError as e:
            errors.append(str(e))
            return {
                "valid": False,
                "errors": errors,
                "warnings": warnings
            }


# Validation Chains and Composite Validators

class ValidationChain:
    """Chain multiple validators together."""

    def __init__(self):
        self.validators = []

    def add_validator(self, validator_type: str, config: Any) -> None:
        """Add validator to chain.

        Args:
            validator_type: Type of validator
            config: Validator configuration
        """
        self.validators.append((validator_type, config))

    def validate(self, data: Dict[str, Any]) -> None:
        """Validate data through all validators in chain.

        Args:
            data: Data to validate

        Raises:
            ValidationError: If any validator fails
        """
        for validator_type, config in self.validators:
            if validator_type == "required_fields":
                validate_required_fields(data, config)
            elif validator_type == "field_types":
                validate_field_types(data, config)
            elif validator_type == "field_patterns":
                validate_field_patterns(data, config)
            elif validator_type == "field_ranges":
                validate_field_ranges(data, config)


class ConditionalValidator:
    """Conditional validator that applies validation based on conditions."""

    def __init__(self, condition: Callable, validators: Dict[str, Any]):
        self.condition = condition
        self.validators = validators

    def validate(self, data: Dict[str, Any]) -> None:
        """Validate data conditionally.

        Args:
            data: Data to validate

        Raises:
            ValidationError: If validation fails
        """
        if self.condition(data):
            chain = ValidationChain()
            for validator_type, config in self.validators.items():
                chain.add_validator(validator_type, config)
            chain.validate(data)


class CompositeValidator:
    """Composite validator with multiple validation strategies."""

    def __init__(self):
        self.validations = []

    def add_basic_validation(self, field: str, field_type: Type, pattern: Optional[str] = None):
        """Add basic field validation.

        Args:
            field: Field name
            field_type: Expected field type
            pattern: Optional regex pattern
        """
        self.validations.append(("basic", field, field_type, pattern))

    def add_range_validation(self, field: str, min_val: Any, max_val: Any):
        """Add range validation.

        Args:
            field: Field name
            min_val: Minimum value
            max_val: Maximum value
        """
        self.validations.append(("range", field, min_val, max_val))

    def add_choice_validation(self, field: str, choices: List[Any]):
        """Add choice validation.

        Args:
            field: Field name
            choices: Valid choices
        """
        self.validations.append(("choice", field, choices))

    def add_custom_validation(self, field: str, validator_func: Callable):
        """Add custom validation.

        Args:
            field: Field name
            validator_func: Custom validator function
        """
        self.validations.append(("custom", field, validator_func))

    def validate(self, data: Dict[str, Any]) -> None:
        """Validate data using all configured validations.

        Args:
            data: Data to validate

        Raises:
            ValidationError: If any validation fails
        """
        for validation in self.validations:
            validation_type = validation[0]
            field = validation[1]

            if field not in data:
                continue

            value = data[field]

            if validation_type == "basic":
                field_type, pattern = validation[2], validation[3]
                if not isinstance(value, field_type):
                    raise ValidationError(f"Field '{field}' must be of type {field_type.__name__}")
                if pattern and isinstance(value, str) and not re.match(pattern, value):
                    raise ValidationError(f"Field '{field}' doesn't match required pattern")

            elif validation_type == "range":
                min_val, max_val = validation[2], validation[3]
                if value < min_val or value > max_val:
                    raise ValidationError(f"Field '{field}' must be between {min_val} and {max_val}")

            elif validation_type == "choice":
                choices = validation[2]
                if value not in choices:
                    raise ValidationError(f"Field '{field}' must be one of {choices}")

            elif validation_type == "custom":
                validator_func = validation[2]
                if not validator_func(value):
                    raise ValidationError(f"Field '{field}' failed custom validation")


# Validation Utilities

def sanitize_input(value: Any) -> Any:
    """Sanitize input value.

    Args:
        value: Value to sanitize

    Returns:
        Sanitized value
    """
    if isinstance(value, str):
        # Strip whitespace
        value = value.strip()

        # Remove non-alphanumeric characters except common symbols
        value = re.sub(r'[<>"\']', '', value)

        # Remove currency symbols and commas for numbers
        if re.match(r'^[\$,\d.]+$', value):
            value = re.sub(r'[\$,]', '', value)

    return value


def normalize_address(address: Optional[str]) -> Optional[str]:
    """Normalize Ethereum address.

    Args:
        address: Address to normalize

    Returns:
        Normalized address
    """
    if address is None or address == "":
        return address

    # Convert to lowercase
    address = address.lower()

    # Add 0x prefix if missing
    if not address.startswith('0x'):
        address = '0x' + address

    return address


def format_validation_error(error: ValidationError) -> str:
    """Format validation error for display.

    Args:
        error: Validation error to format

    Returns:
        Formatted error message
    """
    if error.field:
        return f"Validation error in field '{error.field}': {error.message}"
    else:
        return f"Validation error: {error.message}"


def validate_and_convert(value: Any, target_type: Type) -> Any:
    """Validate and convert value to target type.

    Args:
        value: Value to convert
        target_type: Target type

    Returns:
        Converted value

    Raises:
        ValueError: If conversion fails
    """
    try:
        return target_type(value)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot convert '{value}' to {target_type.__name__}: {e}")


# Error Handling and Collection

class ValidationErrorCollector:
    """Collector for multiple validation errors."""

    def __init__(self):
        self.errors = []

    def add_error(self, field: str, message: str) -> None:
        """Add validation error.

        Args:
            field: Field name
            message: Error message
        """
        self.errors.append({"field": field, "message": message})

    def has_errors(self) -> bool:
        """Check if there are any errors.

        Returns:
            True if there are errors
        """
        return len(self.errors) > 0

    def get_errors(self) -> List[Dict[str, str]]:
        """Get list of errors.

        Returns:
            List of error dictionaries
        """
        return self.errors.copy()

    def get_error_report(self) -> str:
        """Get formatted error report.

        Returns:
            Formatted error report
        """
        if not self.errors:
            return "No validation errors"

        lines = ["Validation errors:"]
        for error in self.errors:
            lines.append(f"  - {error['field']}: {error['message']}")

        return "\n".join(lines)


class ValidationWarningCollector:
    """Collector for validation warnings."""

    def __init__(self):
        self.warnings = []

    def add_warning(self, field: str, message: str, severity: str = "medium") -> None:
        """Add validation warning.

        Args:
            field: Field name
            message: Warning message
            severity: Warning severity
        """
        self.warnings.append({
            "field": field,
            "message": message,
            "severity": severity
        })

    def get_warnings(self) -> List[Dict[str, str]]:
        """Get list of warnings.

        Returns:
            List of warning dictionaries
        """
        return self.warnings.copy()


class RecoverableValidator:
    """Validator with error recovery strategies."""

    def __init__(self):
        self.recovery_strategies = {}

    def add_recovery_strategy(self, field: str, strategy_func: Callable) -> None:
        """Add recovery strategy for field.

        Args:
            field: Field name
            strategy_func: Recovery function
        """
        self.recovery_strategies[field] = strategy_func

    def validate_and_recover(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data and attempt recovery.

        Args:
            data: Data to validate

        Returns:
            Result with recovered data and recovery log
        """
        recovered_data = data.copy()
        recoveries = []

        for field, strategy_func in self.recovery_strategies.items():
            if field in recovered_data:
                try:
                    original_value = recovered_data[field]
                    recovered_value = strategy_func(original_value)

                    if recovered_value != original_value:
                        recovered_data[field] = recovered_value
                        recoveries.append({
                            "field": field,
                            "original": original_value,
                            "recovered": recovered_value
                        })
                except Exception as e:
                    # Recovery failed, keep original value
                    pass

        return {
            "data": recovered_data,
            "recoveries": recoveries
        }


# Validation Extensions and Plugins

class ValidatorPlugin(ABC):
    """Abstract base class for validator plugins."""

    @abstractmethod
    def validate(self, value: Any, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate value.

        Args:
            value: Value to validate
            **kwargs: Additional arguments

        Returns:
            Tuple of (is_valid, error_message)
        """
        pass


class ValidationRegistry:
    """Registry for validation plugins."""

    def __init__(self):
        self.plugins = {}

    def register_plugin(self, name: str, plugin: ValidatorPlugin) -> None:
        """Register validation plugin.

        Args:
            name: Plugin name
            plugin: Plugin instance
        """
        self.plugins[name] = plugin

    def validate(self, plugin_name: str, value: Any, **kwargs) -> tuple[bool, Optional[str]]:
        """Validate using plugin.

        Args:
            plugin_name: Name of plugin to use
            value: Value to validate
            **kwargs: Additional arguments

        Returns:
            Tuple of (is_valid, error_message)
        """
        if plugin_name not in self.plugins:
            return False, f"Unknown validation plugin: {plugin_name}"

        return self.plugins[plugin_name].validate(value, **kwargs)


class ValidationMiddleware(ABC):
    """Abstract base class for validation middleware."""

    def before_validation(self, field_name: str, value: Any) -> Any:
        """Called before validation.

        Args:
            field_name: Field name
            value: Field value

        Returns:
            Potentially modified value
        """
        return value

    def after_validation(self, field_name: str, value: Any, result: bool) -> None:
        """Called after validation.

        Args:
            field_name: Field name
            value: Field value
            result: Validation result
        """
        pass


# Comprehensive Validation Classes

class WalletDataValidator:
    """Comprehensive validator for wallet data."""

    def __init__(self):
        self.address_validator = WalletAddressValidator()
        self.token_validator = TokenDataValidator()

    def validate_and_clean(self, wallet_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean wallet data.

        Args:
            wallet_data: Wallet data to validate

        Returns:
            Cleaned and validated wallet data

        Raises:
            ValidationError: If validation fails
        """
        # Validate basic structure
        validate_wallet_balance_data(wallet_data)

        # Clean and normalize data
        cleaned_data = wallet_data.copy()

        # Normalize address
        cleaned_data["address"] = normalize_address(cleaned_data["address"])

        # Validate and clean token balances
        for token_balance in cleaned_data["token_balances"]:
            token_balance["contract_address"] = normalize_address(token_balance["contract_address"])

            # Validate token data
            self.token_validator.validate(token_balance)

        return cleaned_data


class BatchValidator:
    """Validator for batch operations."""

    def validate_batch(self, items: List[Any], validation_type: str) -> Dict[str, List[Any]]:
        """Validate batch of items.

        Args:
            items: Items to validate
            validation_type: Type of validation to perform

        Returns:
            Dictionary with 'valid' and 'invalid' lists
        """
        valid_items = []
        invalid_items = []

        for item in items:
            try:
                if validation_type == "ethereum_address":
                    if is_valid_ethereum_address(item):
                        valid_items.append(item)
                    else:
                        invalid_items.append(item)
                elif validation_type == "token_symbol":
                    if is_valid_token_symbol(item):
                        valid_items.append(item)
                    else:
                        invalid_items.append(item)
                else:
                    # Unknown validation type, consider invalid
                    invalid_items.append(item)

            except Exception:
                invalid_items.append(item)

        return {
            "valid": valid_items,
            "invalid": invalid_items
        }


# Performance and Optimization Classes

class CachedValidator:
    """Validator with caching for performance."""

    def __init__(self, validator_func: Callable, max_cache_size: int = 1000):
        self.validator_func = validator_func
        self.max_cache_size = max_cache_size
        self.cache = {}
        self.cache_order = deque()

    def validate(self, value: Any) -> bool:
        """Validate with caching.

        Args:
            value: Value to validate

        Returns:
            Validation result
        """
        # Create cache key
        cache_key = str(value)

        # Check cache
        if cache_key in self.cache:
            return self.cache[cache_key]

        # Validate
        result = self.validator_func(value)

        # Add to cache
        if len(self.cache) >= self.max_cache_size:
            # Remove oldest entry
            oldest_key = self.cache_order.popleft()
            del self.cache[oldest_key]

        self.cache[cache_key] = result
        self.cache_order.append(cache_key)

        return result


class LazyValidator:
    """Lazy validator that only validates requested fields."""

    def __init__(self):
        self.field_validators = {}

    def add_field_validator(self, field: str, validator_func: Callable) -> None:
        """Add field validator.

        Args:
            field: Field name
            validator_func: Validator function
        """
        self.field_validators[field] = validator_func

    def validate_fields(self, data: Dict[str, Any], fields: List[str]) -> Dict[str, bool]:
        """Validate only specified fields.

        Args:
            data: Data to validate
            fields: Fields to validate

        Returns:
            Validation results for each field
        """
        results = {}

        for field in fields:
            if field in self.field_validators and field in data:
                try:
                    result = self.field_validators[field](field, data[field])
                    results[field] = result
                except Exception:
                    results[field] = False
            else:
                results[field] = True  # Field not configured for validation

        return results


class ParallelValidator:
    """Parallel validator for large datasets."""

    def __init__(self, validator_func: Callable, max_workers: int = 5):
        self.validator_func = validator_func
        self.max_workers = max_workers

    async def validate_batch(self, items: List[Any]) -> List[bool]:
        """Validate items in parallel.

        Args:
            items: Items to validate

        Returns:
            List of validation results
        """
        import asyncio

        # Create tasks for parallel validation
        tasks = []
        for item in items:
            task = asyncio.create_task(self.validator_func(item))
            tasks.append(task)

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to False
        return [result if isinstance(result, bool) else False for result in results]


# Rules Engine

class ValidationRulesEngine:
    """Rules-based validation engine."""

    def __init__(self, rules: Dict[str, Any]):
        self.rules = rules

    def validate(self, rule_name: str, data: Dict[str, Any]) -> 'ValidationResult':
        """Validate data against rules.

        Args:
            rule_name: Name of rule set to use
            data: Data to validate

        Returns:
            ValidationResult object
        """
        if rule_name not in self.rules:
            return ValidationResult(False, [f"Unknown rule set: {rule_name}"])

        rule_set = self.rules[rule_name]
        errors = []

        # Validate each field in the rule set
        for field_name, field_rules in rule_set.items():
            if field_name not in data and field_rules.get("required", False):
                errors.append(f"Missing required field: {field_name}")
                continue

            if field_name not in data:
                continue

            value = data[field_name]

            # Type validation
            if "type" in field_rules:
                expected_type = field_rules["type"]
                if expected_type == "string" and not isinstance(value, str):
                    errors.append(f"Field '{field_name}' must be a string")
                elif expected_type == "decimal" and not isinstance(value, (int, float, Decimal)):
                    errors.append(f"Field '{field_name}' must be a number")
                elif expected_type == "array" and not isinstance(value, list):
                    errors.append(f"Field '{field_name}' must be an array")
                elif expected_type == "object" and not isinstance(value, dict):
                    errors.append(f"Field '{field_name}' must be an object")

            # Pattern validation
            if "pattern" in field_rules and isinstance(value, str):
                if not re.match(field_rules["pattern"], value):
                    error_msg = field_rules.get("error_message", f"Field '{field_name}' doesn't match required pattern")
                    errors.append(error_msg)

            # Range validation
            if "minimum" in field_rules and isinstance(value, (int, float, Decimal)):
                if value < field_rules["minimum"]:
                    error_msg = field_rules.get("error_message", f"Field '{field_name}' is below minimum")
                    errors.append(error_msg)

            # Array item validation
            if "items" in field_rules and isinstance(value, list):
                for i, item in enumerate(value):
                    if "type" in field_rules["items"]:
                        item_type = field_rules["items"]["type"]
                        if item_type == "object" and not isinstance(item, dict):
                            errors.append(f"Array item {i} in field '{field_name}' must be an object")

                        # Validate object properties
                        if item_type == "object" and isinstance(item, dict) and "properties" in field_rules["items"]:
                            for prop_name, prop_rules in field_rules["items"]["properties"].items():
                                if prop_name in item:
                                    prop_value = item[prop_name]

                                    if "type" in prop_rules:
                                        prop_type = prop_rules["type"]
                                        if prop_type == "string" and not isinstance(prop_value, str):
                                            errors.append(f"Property '{prop_name}' in array item {i} must be a string")
                                        elif prop_type == "decimal" and not isinstance(prop_value,
                                                                                       (int, float, Decimal)):
                                            errors.append(f"Property '{prop_name}' in array item {i} must be a number")

                                    if "pattern" in prop_rules and isinstance(prop_value, str):
                                        if not re.match(prop_rules["pattern"], prop_value):
                                            errors.append(
                                                f"Property '{prop_name}' in array item {i} doesn't match pattern")

                                    if "minimum" in prop_rules and isinstance(prop_value, (int, float, Decimal)):
                                        if prop_value < prop_rules["minimum"]:
                                            errors.append(f"Property '{prop_name}' in array item {i} is below minimum")

        return ValidationResult(len(errors) == 0, errors)


class ValidationResult:
    """Result of validation operation."""

    def __init__(self, is_valid: bool, errors: List[str]):
        self.is_valid = is_valid
        self.errors = errors


# Performance Monitoring

class PerformanceValidator:
    """Validator with performance monitoring."""

    def __init__(self):
        self.stats = {
            "total_validations": 0,
            "total_time": 0.0,
            "validation_times": []
        }

    def validate_batch(self, items: List[Any], validation_type: str) -> Dict[str, List[Any]]:
        """Validate batch with performance tracking.

        Args:
            items: Items to validate
            validation_type: Type of validation

        Returns:
            Validation results
        """
        import time

        start_time = time.time()

        valid_items = []

        for item in items:
            if validation_type == "ethereum_address":
                if is_valid_ethereum_address(item):
                    valid_items.append(item)

        end_time = time.time()
        duration = end_time - start_time

        # Update stats
        self.stats["total_validations"] += len(items)
        self.stats["total_time"] += duration
        self.stats["validation_times"].append(duration)

        return {"valid": valid_items}

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics.

        Returns:
            Performance metrics
        """
        if self.stats["total_validations"] > 0:
            average_time = self.stats["total_time"] / self.stats["total_validations"]
            validations_per_second = self.stats["total_validations"] / self.stats["total_time"] if self.stats[
                                                                                                       "total_time"] > 0 else 0
        else:
            average_time = 0
            validations_per_second = 0

        return {
            "total_validations": self.stats["total_validations"],
            "average_validation_time": average_time,
            "validations_per_second": validations_per_second,
            "total_time": self.stats["total_time"]
        }


class StreamingValidator:
    """Streaming validator for memory efficiency."""

    def validate_stream(self, item_generator, validation_type: str):
        """Validate items from a generator.

        Args:
            item_generator: Generator yielding items to validate
            validation_type: Type of validation

        Yields:
            Validation results
        """
        for item in item_generator:
            valid = False

            if validation_type == "ethereum_address":
                valid = is_valid_ethereum_address(item)
            elif validation_type == "token_symbol":
                valid = is_valid_token_symbol(item)

            yield {"item": item, "valid": valid}


class ConcurrentValidator:
    """Concurrent validator for high performance."""

    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers

    async def validate_concurrent(self, items: List[Dict[str, Any]], validation_type: str) -> Dict[str, List[Any]]:
        """Validate items concurrently.

        Args:
            items: Items to validate
            validation_type: Type of validation

        Returns:
            Validation results
        """
        import asyncio

        async def validate_item(item):
            # Simulate async validation
            await asyncio.sleep(0.001)  # Small delay to simulate work

            if validation_type == "wallet_transaction":
                # Validate wallet transaction structure
                required_fields = ["address", "amount"]
                if not all(field in item for field in required_fields):
                    return False

                if not is_valid_ethereum_address(item["address"]):
                    return False

                if not isinstance(item["amount"], (int, float, Decimal)) or item["amount"] < 0:
                    return False

                return True

            return False

        # Create tasks for concurrent validation
        tasks = [validate_item(item) for item in items]
        results = await asyncio.gather(*tasks)

        # Separate valid and invalid items
        valid_items = [items[i] for i, result in enumerate(results) if result]
        invalid_items = [items[i] for i, result in enumerate(results) if not result]

        return {
            "valid": valid_items,
            "invalid": invalid_items
        }