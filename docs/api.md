# API Documentation

## Overview

The Crypto Wealth Tracker provides a comprehensive Python API for analyzing Ethereum wallets, fetching token prices, and managing data through Google Sheets integration.

## Table of Contents

- [Core Components](#core-components)
- [Client APIs](#client-apis)
- [Configuration](#configuration)
- [Processing APIs](#processing-apis)
- [Utilities](#utilities)
- [Error Handling](#error-handling)
- [Examples](#examples)

## Core Components

### Application Class

The main entry point for the application with dependency injection and lifecycle management.

```python
from wallet_tracker.app import Application, create_application

# Create application instance
app = create_application()

# Initialize all components
await app.initialize()

# Use the application
results = await app.process_wallets_from_sheets(
    spreadsheet_id="your_sheet_id",
    input_range="A:B",
    output_range="A1"
)

# Clean up
await app.shutdown()
```

#### Context Manager Usage

```python
async with create_application() as app:
    # Application is automatically initialized
    results = await app.process_wallet_list(addresses)
    # Automatic cleanup on exit
```

### Configuration

Load and validate configuration from environment variables:

```python
from wallet_tracker.config import get_config, AppConfig

# Get current configuration
config = get_config()

# Check environment
if config.is_production():
    print("Running in production mode")

# Access specific configurations
print(f"Batch size: {config.processing.batch_size}")
print(f"Cache backend: {config.cache.backend}")
```

## Client APIs

### Ethereum Client

Interact with the Ethereum blockchain using Alchemy APIs.

```python
from wallet_tracker.clients import EthereumClient
from wallet_tracker.config import get_config

config = get_config()
cache_manager = app.cache_manager

ethereum_client = EthereumClient(
    config=config.ethereum,
    cache_manager=cache_manager
)

# Get wallet portfolio
portfolio = await ethereum_client.get_wallet_portfolio(
    wallet_address="0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
    include_metadata=True,
    include_prices=True
)

print(f"Total value: ${portfolio.total_value_usd}")
print(f"ETH balance: {portfolio.eth_balance.balance_eth}")
print(f"Token count: {len(portfolio.token_balances)}")
```

#### Available Methods

| Method | Description | Parameters |
|--------|-------------|------------|
| `get_wallet_portfolio()` | Get complete wallet portfolio | `wallet_address`, `include_metadata`, `include_prices` |
| `get_stats()` | Get client statistics | None |
| `close()` | Close client connections | None |

#### Portfolio Data Structure

```python
@dataclass
class WalletPortfolio:
    address: str
    eth_balance: EthBalance
    token_balances: list[TokenBalance]
    total_value_usd: Decimal
    last_updated: datetime
    transaction_count: int
    last_transaction_hash: str | None
    last_transaction_timestamp: datetime | None
```

### CoinGecko Client

Fetch token prices and market data.

```python
from wallet_tracker.clients import CoinGeckoClient

coingecko_client = CoinGeckoClient(
    config=config.coingecko,
    cache_manager=cache_manager
)

# Get single token price
eth_price = await coingecko_client.get_eth_price()
print(f"ETH price: ${eth_price}")

# Get multiple token prices
token_prices = await coingecko_client.get_token_prices_by_contracts([
    "0xa0b86a33e6441e94bb0a8d0f7e5f8d69e2c0e5a0",  # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7"   # USDT
])

for address, price_data in token_prices.items():
    print(f"{price_data.symbol}: ${price_data.current_price_usd}")
```

#### Available Methods

| Method | Description | Parameters |
|--------|-------------|------------|
| `get_eth_price()` | Get current ETH price | None |
| `get_token_price()` | Get price for single token | `token_id`, `include_market_data` |
| `get_token_prices_batch()` | Get prices for multiple tokens | `token_ids`, `include_market_data` |
| `get_token_price_by_contract()` | Get price by contract address | `contract_address`, `include_market_data` |
| `get_token_prices_by_contracts()` | Get prices for multiple contracts | `contract_addresses`, `include_market_data` |
| `search_tokens()` | Search tokens by name/symbol | `query`, `limit` |
| `health_check()` | Check API connectivity | None |

### Google Sheets Client

Read from and write to Google Sheets.

```python
from wallet_tracker.clients import GoogleSheetsClient

sheets_client = GoogleSheetsClient(
    config=config.google_sheets,
    cache_manager=cache_manager
)

# Read wallet addresses
addresses = await sheets_client.read_wallet_addresses(
    spreadsheet_id="your_sheet_id",
    range_name="A:B",
    skip_header=True
)

# Write results
success = await sheets_client.write_wallet_results(
    spreadsheet_id="your_sheet_id",
    wallet_results=results,
    range_start="A1",
    include_header=True
)
```

#### Available Methods

| Method | Description | Parameters |
|--------|-------------|------------|
| `read_wallet_addresses()` | Read wallet addresses from sheet | `spreadsheet_id`, `range_name`, `worksheet_name`, `skip_header` |
| `write_wallet_results()` | Write analysis results to sheet | `spreadsheet_id`, `wallet_results`, `range_start`, `worksheet_name`, `include_header`, `clear_existing` |
| `create_summary_sheet()` | Create summary statistics sheet | `spreadsheet_id`, `summary_data`, `worksheet_name` |
| `health_check()` | Test sheets connectivity | None |

## Configuration

### Environment Variables

Required environment variables for the application:

```bash
# Ethereum Configuration
ALCHEMY_API_KEY=your_alchemy_api_key_here
ETHEREUM_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/your_key
ALCHEMY_RATE_LIMIT=100

# CoinGecko Configuration (Optional)
COINGECKO_API_KEY=your_coingecko_api_key
COINGECKO_RATE_LIMIT=30

# Google Sheets Configuration
GOOGLE_SHEETS_CREDENTIALS_FILE=config/google_sheets_credentials.json

# Cache Configuration
CACHE_BACKEND=hybrid  # redis, file, hybrid
REDIS_URL=redis://localhost:6379/0
CACHE_TTL_PRICES=3600
CACHE_TTL_BALANCES=1800

# Processing Configuration
BATCH_SIZE=50
MAX_CONCURRENT_REQUESTS=10
INACTIVE_WALLET_THRESHOLD_DAYS=365

# Application Configuration
ENVIRONMENT=development  # development, staging, production
DEBUG=true
DRY_RUN=false
LOG_LEVEL=INFO
```

### Configuration Models

```python
from wallet_tracker.config import AppConfig

# Access configuration sections
config = get_config()

# Ethereum settings
print(f"RPC URL: {config.ethereum.rpc_url}")
print(f"Rate limit: {config.ethereum.rate_limit}")

# Processing settings
print(f"Batch size: {config.processing.batch_size}")
print(f"Max concurrent: {config.processing.max_concurrent_requests}")

# Cache settings
print(f"Backend: {config.cache.backend}")
print(f"Price TTL: {config.cache.ttl_prices}")
```

## Processing APIs

### Wallet Processor

High-level wallet processing with Google Sheets integration.

```python
from wallet_tracker.processors import WalletProcessor

processor = WalletProcessor(
    config=config,
    ethereum_client=ethereum_client,
    coingecko_client=coingecko_client,
    sheets_client=sheets_client,
    cache_manager=cache_manager
)

# Process from Google Sheets
results = await processor.process_wallets_from_sheets(
    spreadsheet_id="your_sheet_id",
    input_range="A:B",
    output_range="A1",
    skip_header=True,
    include_summary=True
)

print(f"Processed: {results['stats']['wallets_processed']}")
print(f"Total value: ${results['stats']['total_value_usd']}")
```

### Batch Processor

Advanced batch processing with progress tracking and error handling.

```python
from wallet_tracker.processors import BatchProcessor
from wallet_tracker.processors.batch_types import BatchConfig

# Custom batch configuration
batch_config = BatchConfig(
    batch_size=25,
    max_concurrent_jobs_per_batch=5,
    request_delay_seconds=0.2,
    skip_inactive_wallets=True,
    retry_failed_jobs=True,
    max_retries=3
)

batch_processor = BatchProcessor(
    config=config,
    ethereum_client=ethereum_client,
    coingecko_client=coingecko_client,
    cache_manager=cache_manager
)

# Process wallet list with custom config
addresses = [
    {"address": "0x...", "label": "Wallet 1", "row_number": 1},
    {"address": "0x...", "label": "Wallet 2", "row_number": 2}
]

results = await batch_processor.process_wallet_list(
    addresses=addresses,
    config_override=batch_config,
    progress_callback=lambda progress: print(f"Progress: {progress.get_progress_percentage():.1f}%")
)
```

#### Progress Tracking

```python
# Custom progress callback
def track_progress(batch_progress):
    print(f"Batch: {batch_progress.current_batch_number}/{batch_progress.total_batches}")
    print(f"Completed: {batch_progress.jobs_completed}")
    print(f"Failed: {batch_progress.jobs_failed}")
    print(f"Total Value: ${batch_progress.total_value_processed}")
    print(f"Progress: {batch_progress.get_progress_percentage():.1f}%")

results = await batch_processor.process_wallet_list(
    addresses=addresses,
    progress_callback=track_progress
)
```

## Utilities

### Cache Management

```python
from wallet_tracker.utils import CacheManager, CacheFactory

# Create cache manager
cache_manager = CacheManager(config.cache)

# Manual cache operations
await cache_manager.set_price("ethereum", {"usd": 2000.0})
eth_price = await cache_manager.get_price("ethereum")

# Set wallet balance
await cache_manager.set_balance("0x...", portfolio_data)
cached_portfolio = await cache_manager.get_balance("0x...")

# Health check
health = await cache_manager.health_check()
print(f"Cache backends: {health}")
```

### Rate Limiting and Throttling

```python
from wallet_tracker.utils import ThrottleManager, ThrottleConfig

# Create throttle manager
throttle_manager = ThrottleManager()

# Register throttle for Ethereum calls
ethereum_config = ThrottleConfig(
    mode=ThrottleMode.ADAPTIVE,
    requests_per_second=10.0,
    burst_size=20
)

throttle_manager.register_throttle("ethereum", ethereum_config)

# Use throttling
await throttle_manager.wait("ethereum")
# Make your API call here
await throttle_manager.report_success("ethereum")
```

## Error Handling

### Exception Hierarchy

```python
# Base exceptions
from wallet_tracker.clients import (
    EthereumClientError,
    InvalidAddressError,
    APIError
)

from wallet_tracker.clients import (
    CoinGeckoClientError,
    RateLimitError
)

from wallet_tracker.clients import (
    GoogleSheetsClientError,
    SheetsNotFoundError,
    SheetsPermissionError
)

# Error handling example
try:
    portfolio = await ethereum_client.get_wallet_portfolio(wallet_address)
except InvalidAddressError as e:
    print(f"Invalid address: {e}")
except APIError as e:
    print(f"API error: {e}")
except EthereumClientError as e:
    print(f"Ethereum client error: {e}")
```

### Retry Logic

```python
import asyncio
from wallet_tracker.clients import APIError

async def get_portfolio_with_retry(client, address, max_retries=3):
    """Get portfolio with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            return await client.get_wallet_portfolio(address)
        except APIError as e:
            if attempt == max_retries - 1:
                raise

            wait_time = 2 ** attempt  # Exponential backoff
            print(f"Retry {attempt + 1}/{max_retries} in {wait_time}s: {e}")
            await asyncio.sleep(wait_time)
```

## Health Monitoring

### Health Checks

```python
# Application-level health check
health_status = await app.health_check()
print(f"Ethereum client: {health_status['ethereum_client']}")
print(f"CoinGecko client: {health_status['coingecko_client']}")
print(f"Sheets client: {health_status['sheets_client']}")
print(f"Cache manager: {health_status['cache_manager']}")

# Individual client health checks
ethereum_healthy = await ethereum_client.health_check()
coingecko_healthy = await coingecko_client.health_check()
sheets_healthy = sheets_client.health_check()
```

### Metrics Collection

```python
# Get application metrics
metrics = await app.collect_metrics()

# Ethereum client metrics
eth_stats = metrics['ethereum_client']
print(f"Portfolio requests: {eth_stats['portfolio_requests']}")
print(f"Cache hits: {eth_stats['cache_hits']}")
print(f"API errors: {eth_stats['api_errors']}")

# CoinGecko client metrics
cg_stats = metrics['coingecko_client']
print(f"Price requests: {cg_stats['price_requests']}")
print(f"Rate limit errors: {cg_stats['rate_limit_errors']}")

# Cache metrics
cache_stats = metrics['cache']
print(f"Hit rate: {cache_stats['price_cache']['hit_rate_percent']}%")
```

## Examples

### Basic Wallet Analysis

```python
async def analyze_single_wallet():
    """Analyze a single wallet and print results."""
    async with create_application() as app:
        address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

        portfolio = await app.ethereum_client.get_wallet_portfolio(
            wallet_address=address,
            include_metadata=True,
            include_prices=True
        )

        print(f"Address: {portfolio.address}")
        print(f"ETH Balance: {portfolio.eth_balance.balance_eth:.4f} ETH")
        print(f"ETH Value: ${portfolio.eth_balance.value_usd:,.2f}")
        print(f"Total Value: ${portfolio.total_value_usd:,.2f}")
        print(f"Token Count: {len(portfolio.token_balances)}")

        for token in portfolio.token_balances[:5]:  # Top 5 tokens
            print(f"  {token.symbol}: {token.balance_formatted:.4f} (${token.value_usd:,.2f})")

# Run the example
await analyze_single_wallet()
```

### Batch Processing with Progress

```python
async def batch_analyze_wallets():
    """Process multiple wallets with progress tracking."""
    addresses = [
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Vitalik", "row_number": 1},
        {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "Whale", "row_number": 2},
        # Add more addresses...
    ]

    async with create_application() as app:
        def progress_callback(progress):
            percentage = progress.get_progress_percentage()
            print(f"Progress: {percentage:.1f}% - "
                  f"Completed: {progress.jobs_completed}, "
                  f"Failed: {progress.jobs_failed}, "
                  f"Value: ${progress.total_value_processed:,.2f}")

        results = await app.batch_processor.process_wallet_list(
            addresses=addresses,
            progress_callback=progress_callback
        )

        print(f"\nFinal Results:")
        print(f"Processed: {results.wallets_processed}")
        print(f"Total Value: ${results.total_portfolio_value:,.2f}")
        print(f"Average Value: ${results.average_portfolio_value:,.2f}")
        print(f"Processing Time: {results.total_processing_time:.1f}s")

await batch_analyze_wallets()
```

### Google Sheets Integration

```python
async def sheets_integration_example():
    """Complete Google Sheets integration example."""
    async with create_application() as app:
        spreadsheet_id = "your_spreadsheet_id_here"

        # Read addresses from sheets
        addresses = await app.sheets_client.read_wallet_addresses(
            spreadsheet_id=spreadsheet_id,
            range_name="A:B",
            skip_header=True
        )

        print(f"Found {len(addresses)} addresses in sheet")

        # Process wallets
        results = await app.process_wallets_from_sheets(
            spreadsheet_id=spreadsheet_id,
            input_range="A:B",
            output_range="A1",
            dry_run=False  # Set to True to test without writing
        )

        print(f"Results written to sheet: {spreadsheet_id}")
        print(f"Summary: {results.get_summary_dict()}")

await sheets_integration_example()
```

### Custom Cache Implementation

```python
async def cache_example():
    """Example of custom cache usage."""
    async with create_application() as app:
        cache_manager = app.cache_manager

        # Cache token prices manually
        await cache_manager.set_price("ethereum", {
            "usd": 2000.0,
            "last_updated": datetime.now().isoformat()
        })

        # Retrieve cached price
        cached_price = await cache_manager.get_price("ethereum")
        print(f"Cached ETH price: ${cached_price['usd']}")

        # Cache wallet portfolio
        address = "0x..."
        portfolio_data = {"total_value": 50000, "token_count": 5}
        await cache_manager.set_balance(address, portfolio_data)

        # Get cache statistics
        stats = await cache_manager.get_stats()
        for backend, backend_stats in stats.items():
            if isinstance(backend_stats, dict):
                print(f"{backend}: {backend_stats.get('hit_rate_percent', 0):.1f}% hit rate")

await cache_example()
```

## Rate Limits and Best Practices

### API Rate Limits

| Service | Free Tier | Pro Tier | Recommendations |
|---------|-----------|----------|-----------------|
| Alchemy | 100 req/s | 1000 req/s | Use caching, batch requests |
| CoinGecko | 30 req/min | 500 req/min | Cache prices, batch token queries |
| Google Sheets | 300 req/min | 600 req/min | Batch read/write operations |

### Performance Optimization

1. **Enable Caching**: Use Redis or hybrid caching for better performance
2. **Batch Processing**: Process wallets in configurable batches
3. **Concurrent Requests**: Limit concurrent requests to avoid rate limits
4. **Request Delays**: Add delays between requests for stability
5. **Error Handling**: Implement retry logic with exponential backoff

### Security Best Practices

1. **API Keys**: Store API keys in environment variables
2. **Credentials**: Use service account keys for Google Sheets
3. **Rate Limiting**: Implement proper rate limiting to avoid bans
4. **Validation**: Validate all inputs, especially wallet addresses
5. **Logging**: Log errors but avoid logging sensitive data

## Support and Resources

- **GitHub Repository**: [Crypto Wealth Tracker](https://github.com/CiDsTaR/crypto-wealth-tracker)
- **Issue Tracker**: Report bugs and request features
- **Documentation**: Complete API reference and examples
- **Examples**: Ready-to-run code examples
- **Configuration**: Environment setup guides

For additional help, please check the examples in the `/examples` directory or open an issue on GitHub.
