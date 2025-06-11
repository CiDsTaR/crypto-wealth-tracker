"""Tests for clients package __init__.py imports and exports."""

import inspect
from unittest.mock import MagicMock

import pytest

# Import the clients module
import wallet_tracker.clients as clients


class TestClientsPackageImports:
    """Test that all expected modules are importable from clients package."""

    def test_ethereum_client_imports(self):
        """Test Ethereum client imports."""
        # Main client class
        assert hasattr(clients, 'EthereumClient')
        assert inspect.isclass(clients.EthereumClient)

        # Exception classes
        assert hasattr(clients, 'EthereumClientError')
        assert hasattr(clients, 'InvalidAddressError')
        assert hasattr(clients, 'EthereumAPIError')

        # Check inheritance
        assert issubclass(clients.InvalidAddressError, clients.EthereumClientError)
        assert issubclass(clients.EthereumAPIError, clients.EthereumClientError)

    def test_coingecko_client_imports(self):
        """Test CoinGecko client imports."""
        # Main client classes
        assert hasattr(clients, 'CoinGeckoClient')
        assert hasattr(clients, 'CoinGeckoPriceService')
        assert inspect.isclass(clients.CoinGeckoClient)
        assert inspect.isclass(clients.CoinGeckoPriceService)

        # Exception classes
        assert hasattr(clients, 'CoinGeckoClientError')
        assert hasattr(clients, 'CoinGeckoAPIError')
        assert hasattr(clients, 'RateLimitError')

        # Check inheritance
        assert issubclass(clients.CoinGeckoAPIError, clients.CoinGeckoClientError)
        assert issubclass(clients.RateLimitError, clients.CoinGeckoClientError)

    def test_google_sheets_client_imports(self):
        """Test Google Sheets client imports."""
        # Main client class
        assert hasattr(clients, 'GoogleSheetsClient')
        assert inspect.isclass(clients.GoogleSheetsClient)

        # Exception classes
        assert hasattr(clients, 'GoogleSheetsClientError')
        assert hasattr(clients, 'SheetsAPIError')
        assert hasattr(clients, 'SheetsAuthenticationError')
        assert hasattr(clients, 'SheetsNotFoundError')
        assert hasattr(clients, 'SheetsPermissionError')

        # Check inheritance
        assert issubclass(clients.SheetsAPIError, clients.GoogleSheetsClientError)
        assert issubclass(clients.SheetsAuthenticationError, clients.GoogleSheetsClientError)
        assert issubclass(clients.SheetsNotFoundError, clients.GoogleSheetsClientError)
        assert issubclass(clients.SheetsPermissionError, clients.GoogleSheetsClientError)


class TestEthereumDataTypes:
    """Test Ethereum data type imports."""

    def test_balance_types(self):
        """Test balance-related type imports."""
        assert hasattr(clients, 'TokenBalance')
        assert hasattr(clients, 'EthBalance')
        assert inspect.isclass(clients.TokenBalance)
        assert inspect.isclass(clients.EthBalance)

    def test_portfolio_types(self):
        """Test portfolio-related type imports."""
        assert hasattr(clients, 'WalletPortfolio')
        assert hasattr(clients, 'WalletActivity')
        assert inspect.isclass(clients.WalletPortfolio)
        assert inspect.isclass(clients.WalletActivity)

    def test_metadata_types(self):
        """Test metadata type imports."""
        assert hasattr(clients, 'TokenMetadata')
        assert hasattr(clients, 'TransactionInfo')
        assert inspect.isclass(clients.TokenMetadata)
        assert inspect.isclass(clients.TransactionInfo)


class TestCoinGeckoDataTypes:
    """Test CoinGecko data type imports."""

    def test_price_types(self):
        """Test price-related type imports."""
        assert hasattr(clients, 'TokenPrice')
        assert hasattr(clients, 'TokenSearchResult')
        assert hasattr(clients, 'ContractPriceResponse')
        assert inspect.isclass(clients.TokenPrice)
        assert inspect.isclass(clients.TokenSearchResult)
        assert inspect.isclass(clients.ContractPriceResponse)


class TestGoogleSheetsDataTypes:
    """Test Google Sheets data type imports."""

    def test_sheets_types(self):
        """Test Google Sheets type imports."""
        assert hasattr(clients, 'WalletAddress')
        assert hasattr(clients, 'WalletResult')
        assert hasattr(clients, 'SummaryData')
        assert hasattr(clients, 'SheetConfig')
        assert hasattr(clients, 'SheetRange')
        assert inspect.isclass(clients.WalletAddress)
        assert inspect.isclass(clients.WalletResult)
        assert inspect.isclass(clients.SummaryData)
        assert inspect.isclass(clients.SheetConfig)
        assert inspect.isclass(clients.SheetRange)

    def test_sheets_constants(self):
        """Test Google Sheets constants imports."""
        assert hasattr(clients, 'WALLET_RESULT_COLUMNS')
        assert hasattr(clients, 'WALLET_RESULT_HEADERS')
        assert isinstance(clients.WALLET_RESULT_COLUMNS, dict)
        assert isinstance(clients.WALLET_RESULT_HEADERS, list)


class TestUtilityFunctions:
    """Test utility function imports."""

    def test_ethereum_utilities(self):
        """Test Ethereum utility function imports."""
        assert hasattr(clients, 'normalize_address')
        assert hasattr(clients, 'is_valid_ethereum_address')
        assert hasattr(clients, 'wei_to_eth')
        assert hasattr(clients, 'format_token_amount')
        assert hasattr(clients, 'calculate_token_value')

        # Test that they are callable
        assert callable(clients.normalize_address)
        assert callable(clients.is_valid_ethereum_address)
        assert callable(clients.wei_to_eth)
        assert callable(clients.format_token_amount)
        assert callable(clients.calculate_token_value)

    def test_coingecko_utilities(self):
        """Test CoinGecko utility function imports."""
        assert hasattr(clients, 'get_coingecko_id')
        assert hasattr(clients, 'normalize_coingecko_price_data')
        assert hasattr(clients, 'is_stablecoin')

        # Test that they are callable
        assert callable(clients.get_coingecko_id)
        assert callable(clients.normalize_coingecko_price_data)
        assert callable(clients.is_stablecoin)

    def test_google_sheets_utilities(self):
        """Test Google Sheets utility function imports."""
        assert hasattr(clients, 'create_wallet_result_from_portfolio')
        assert hasattr(clients, 'create_summary_from_results')

        # Test that they are callable
        assert callable(clients.create_wallet_result_from_portfolio)
        assert callable(clients.create_summary_from_results)


class TestModuleStructure:
    """Test the overall module structure."""

    def test_all_exports_defined(self):
        """Test that __all__ is properly defined and complete."""
        # The __all__ list should be defined in the clients module
        assert hasattr(clients, '__all__')
        assert isinstance(clients.__all__, list)
        assert len(clients.__all__) > 0

    def test_all_exports_importable(self):
        """Test that all items in __all__ are actually importable."""
        for item_name in clients.__all__:
            assert hasattr(clients, item_name), f"'{item_name}' in __all__ but not importable"

    def test_no_extra_exports(self):
        """Test that only intended items are exported."""
        # Get all public attributes (not starting with _)
        public_attrs = [attr for attr in dir(clients) if not attr.startswith('_')]

        # Check that all public attributes are in __all__
        for attr in public_attrs:
            if not attr.startswith('_'):  # Skip private attributes
                assert attr in clients.__all__, f"'{attr}' is public but not in __all__"

    def test_docstring_exists(self):
        """Test that the module has a docstring."""
        assert clients.__doc__ is not None
        assert len(clients.__doc__.strip()) > 0
        assert "API clients package" in clients.__doc__


class TestClientInstantiation:
    """Test that client classes can be instantiated (basic smoke tests)."""

    def test_ethereum_client_instantiable(self):
        """Test that EthereumClient can be imported and has expected interface."""
        EthereumClient = clients.EthereumClient

        # Check that it's a class
        assert inspect.isclass(EthereumClient)

        # Check that it has expected methods (without instantiating)
        expected_methods = [
            '__init__',
            'get_wallet_portfolio',
            'close',
            'get_stats',
        ]

        for method_name in expected_methods:
            assert hasattr(EthereumClient, method_name), f"EthereumClient missing {method_name} method"

    def test_coingecko_client_instantiable(self):
        """Test that CoinGeckoClient can be imported and has expected interface."""
        CoinGeckoClient = clients.CoinGeckoClient

        # Check that it's a class
        assert inspect.isclass(CoinGeckoClient)

        # Check that it has expected methods
        expected_methods = [
            '__init__',
            'get_token_price',
            'get_eth_price',
            'close',
            'get_stats',
        ]

        for method_name in expected_methods:
            assert hasattr(CoinGeckoClient, method_name), f"CoinGeckoClient missing {method_name} method"

    def test_google_sheets_client_instantiable(self):
        """Test that GoogleSheetsClient can be imported and has expected interface."""
        GoogleSheetsClient = clients.GoogleSheetsClient

        # Check that it's a class
        assert inspect.isclass(GoogleSheetsClient)

        # Check that it has expected methods
        expected_methods = [
            '__init__',
            'read_wallet_addresses',
            'write_wallet_results',
            'close',
            'get_stats',
        ]

        for method_name in expected_methods:
            assert hasattr(GoogleSheetsClient, method_name), f"GoogleSheetsClient missing {method_name} method"


class TestDataTypeStructures:
    """Test that data types have expected structure."""

    def test_token_balance_structure(self):
        """Test TokenBalance dataclass structure."""
        TokenBalance = clients.TokenBalance

        # Check that it's a dataclass
        assert hasattr(TokenBalance, '__dataclass_fields__')

        # Check expected fields
        expected_fields = [
            'contract_address',
            'symbol',
            'name',
            'decimals',
            'balance_raw',
            'balance_formatted',
            'price_usd',
            'value_usd',
            'logo_url',
            'is_verified'
        ]

        actual_fields = list(TokenBalance.__dataclass_fields__.keys())
        for field in expected_fields:
            assert field in actual_fields, f"TokenBalance missing field: {field}"

    def test_wallet_portfolio_structure(self):
        """Test WalletPortfolio dataclass structure."""
        WalletPortfolio = clients.WalletPortfolio

        # Check that it's a dataclass
        assert hasattr(WalletPortfolio, '__dataclass_fields__')

        # Check expected fields
        expected_fields = [
            'address',
            'eth_balance',
            'token_balances',
            'total_value_usd',
            'last_updated',
            'transaction_count'
        ]

        actual_fields = list(WalletPortfolio.__dataclass_fields__.keys())
        for field in expected_fields:
            assert field in actual_fields, f"WalletPortfolio missing field: {field}"

    def test_token_price_structure(self):
        """Test TokenPrice dataclass structure."""
        TokenPrice = clients.TokenPrice

        # Check that it's a dataclass
        assert hasattr(TokenPrice, '__dataclass_fields__')

        # Check expected fields
        expected_fields = [
            'token_id',
            'symbol',
            'name',
            'current_price_usd'
        ]

        actual_fields = list(TokenPrice.__dataclass_fields__.keys())
        for field in expected_fields:
            assert field in actual_fields, f"TokenPrice missing field: {field}"


class TestUtilityFunctionInterfaces:
    """Test utility function interfaces."""

    def test_ethereum_utility_interfaces(self):
        """Test Ethereum utility function interfaces."""
        # Test normalize_address
        assert callable(clients.normalize_address)
        # Should accept string parameter
        signature = inspect.signature(clients.normalize_address)
        assert len(signature.parameters) == 1

        # Test is_valid_ethereum_address
        assert callable(clients.is_valid_ethereum_address)
        signature = inspect.signature(clients.is_valid_ethereum_address)
        assert len(signature.parameters) == 1

        # Test wei_to_eth
        assert callable(clients.wei_to_eth)
        signature = inspect.signature(clients.wei_to_eth)
        assert len(signature.parameters) == 1

    def test_coingecko_utility_interfaces(self):
        """Test CoinGecko utility function interfaces."""
        # Test get_coingecko_id
        assert callable(clients.get_coingecko_id)
        signature = inspect.signature(clients.get_coingecko_id)
        # Should have optional contract_address and symbol parameters
        param_names = list(signature.parameters.keys())
        assert 'contract_address' in param_names or 'symbol' in param_names

        # Test is_stablecoin
        assert callable(clients.is_stablecoin)
        signature = inspect.signature(clients.is_stablecoin)
        assert len(signature.parameters) == 1


class TestErrorClassHierarchy:
    """Test that error classes have proper hierarchy."""

    def test_ethereum_error_hierarchy(self):
        """Test Ethereum error class hierarchy."""
        # Base error
        assert issubclass(clients.EthereumClientError, Exception)

        # Specific errors inherit from base
        assert issubclass(clients.InvalidAddressError, clients.EthereumClientError)
        assert issubclass(clients.EthereumAPIError, clients.EthereumClientError)

    def test_coingecko_error_hierarchy(self):
        """Test CoinGecko error class hierarchy."""
        # Base error
        assert issubclass(clients.CoinGeckoClientError, Exception)

        # Specific errors inherit from base
        assert issubclass(clients.CoinGeckoAPIError, clients.CoinGeckoClientError)
        assert issubclass(clients.RateLimitError, clients.CoinGeckoClientError)

    def test_google_sheets_error_hierarchy(self):
        """Test Google Sheets error class hierarchy."""
        # Base error
        assert issubclass(clients.GoogleSheetsClientError, Exception)

        # Specific errors inherit from base
        assert issubclass(clients.SheetsAPIError, clients.GoogleSheetsClientError)
        assert issubclass(clients.SheetsAuthenticationError, clients.GoogleSheetsClientError)
        assert issubclass(clients.SheetsNotFoundError, clients.GoogleSheetsClientError)
        assert issubclass(clients.SheetsPermissionError, clients.GoogleSheetsClientError)


class TestImportStability:
    """Test that imports are stable and don't cause side effects."""

    def test_import_does_not_raise(self):
        """Test that importing clients module doesn't raise exceptions."""
        # Re-import to ensure it's stable
        try:
            import wallet_tracker.clients
            # Should not raise any exceptions
        except Exception as e:
            pytest.fail(f"Importing clients module raised exception: {e}")

    def test_import_does_not_create_instances(self):
        """Test that importing doesn't create unwanted instances."""
        # Check that classes are classes, not instances
        assert inspect.isclass(clients.EthereumClient)
        assert inspect.isclass(clients.CoinGeckoClient)
        assert inspect.isclass(clients.GoogleSheetsClient)

        # Check that exceptions are classes, not instances
        assert inspect.isclass(clients.EthereumClientError)
        assert inspect.isclass(clients.CoinGeckoClientError)
        assert inspect.isclass(clients.GoogleSheetsClientError)

    def test_no_global_state_modification(self):
        """Test that importing doesn't modify global state."""
        # This is hard to test comprehensively, but we can check some basics
        import sys
        import os

        # Save current state
        original_path = sys.path[:]
        original_env = os.environ.copy()

        # Re-import clients
        import importlib
        importlib.reload(clients)

        # Check that global state wasn't modified
        assert sys.path == original_path
        # Environment might have some differences due to other operations, so we skip that check


class TestTypeHints:
    """Test that type hints are properly available."""

    def test_dataclass_type_hints(self):
        """Test that dataclass fields have type hints."""
        TokenBalance = clients.TokenBalance

        # Check that __annotations__ exists
        if hasattr(TokenBalance, '__annotations__'):
            annotations = TokenBalance.__annotations__
            assert len(annotations) > 0
        else:
            # For Python versions that might not preserve annotations
            # at least check that dataclass fields exist
            assert hasattr(TokenBalance, '__dataclass_fields__')

    def test_function_type_hints(self):
        """Test that functions have type hints where expected."""
        # Check some utility functions for type hints
        normalize_addr = clients.normalize_address

        if hasattr(normalize_addr, '__annotations__'):
            annotations = normalize_addr.__annotations__
            # Should have return type hint at minimum
            assert 'return' in annotations or len(annotations) > 0


class TestCompatibility:
    """Test compatibility with different Python versions and environments."""

    def test_python_version_compatibility(self):
        """Test that imports work with current Python version."""
        import sys

        # These tests assume Python 3.8+ based on the type hints used
        assert sys.version_info >= (3, 8), "Code requires Python 3.8+"

        # All imports should work
        assert hasattr(clients, 'EthereumClient')
        assert hasattr(clients, 'CoinGeckoClient')
        assert hasattr(clients, 'GoogleSheetsClient')

    def test_optional_dependencies_handling(self):
        """Test that the module handles optional dependencies gracefully."""
        # The clients module should import even if some optional deps are missing
        # This is more of a structural test - the actual dependency handling
        # would be tested in integration tests

        # At minimum, all the classes should be importable
        classes_to_test = [
            'EthereumClient',
            'CoinGeckoClient',
            'GoogleSheetsClient',
            'TokenBalance',
            'WalletPortfolio',
            'TokenPrice'
        ]

        for class_name in classes_to_test:
            assert hasattr(clients, class_name), f"Missing class: {class_name}"
            class_obj = getattr(clients, class_name)
            assert inspect.isclass(class_obj), f"{class_name} is not a class"