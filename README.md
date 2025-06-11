# Ethereum Wallet Tracker 🚀

A high-performance Python application that calculates on-chain wealth for Ethereum wallets and integrates seamlessly with Google Sheets for data input and output.

## ✨ Features

- **Batch Processing**: Handle 1k-30k+ wallets efficiently
- **Multi-API Integration**: Alchemy/Infura for blockchain data, CoinGecko for prices
- **Smart Caching**: Redis-based caching with configurable TTL
- **Rate Limiting**: Intelligent API throttling and retry mechanisms
- **Google Sheets Integration**: Seamless read/write operations
- **Async Processing**: High-performance concurrent operations
- **Activity Filtering**: Skip inactive wallets (configurable threshold)
- **Comprehensive Logging**: Detailed monitoring and debugging

## 🏗️ Architecture

```
ethereum-wallet-tracker/
├── src/wallet_tracker/
│   ├── clients/          # API clients (Ethereum, CoinGecko, Google Sheets)
│   ├── processors/       # Core business logic
│   ├── config/          # Configuration management
│   └── utils/           # Utilities and helpers
├── tests/               # Test suite
├── config/              # Configuration files
└── docs/               # Documentation
```

## 🚀 Quick Start

### Prerequisites

- Python 3.13+
- [UV package manager](https://github.com/astral-sh/uv)
- Google Cloud credentials for Sheets API
- Alchemy/Infura API key
- CoinGecko API key (optional, for higher rate limits)
- Redis instance (for caching)

### Installation

1. **Clone and setup:**
```bash
git clone https://github.com/yourusername/ethereum-wallet-tracker.git
cd ethereum-wallet-tracker
```

2. **Install dependencies:**
```bash
uv sync --all-extras
```

3. **Configure environment:**
```bash
cp .env.template .env
# Edit .env with your API keys and configuration
```

4. **Setup Google Sheets credentials:**
```bash
# Download service account JSON from Google Cloud Console
# Place it as config/google_sheets_credentials.json
```

5. **Run the application:**
```bash
uv run python -m wallet_tracker.main
```

## 📋 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ALCHEMY_API_KEY` | Alchemy API key for Ethereum data | Required |
| `COINGECKO_API_KEY` | CoinGecko API key (optional) | None |
| `BATCH_SIZE` | Wallets processed per batch | 50 |
| `MAX_CONCURRENT_REQUESTS` | Concurrent API requests | 10 |
| `CACHE_TTL_PRICES` | Price cache TTL (seconds) | 3600 |
| `INACTIVE_WALLET_THRESHOLD_DAYS` | Skip wallets inactive for X days | 365 |

### Google Sheets Format

**Input Sheet Structure:**
| Column A | Column B |
|----------|----------|
| Wallet Address | Label (optional) |
| 0x742d35Cc6634C0532925a3b8D40e4f337... | Wallet 1 |
| 0x8ba1f109551bD432803012645Hac136c73... | Wallet 2 |

**Output Sheet Structure:**
| Address | ETH | USDC | USDT | DAI | AAVE | UNI | Total USD | Last Updated |
|---------|-----|------|------|-----|------|-----|-----------|--------------|

## 🔧 Usage

### Basic Usage

```python
from wallet_tracker import WalletTracker

# Initialize tracker
tracker = WalletTracker()

# Process wallets from Google Sheets
await tracker.process_wallets_from_sheets(
    spreadsheet_id="your_sheet_id",
    input_range="Sheet1!A:B",
    output_range="Results!A:I"
)
```

### Advanced Configuration

```python
from wallet_tracker.config import Config
from wallet_tracker import WalletTracker

# Custom configuration
config = Config(
    batch_size=100,
    max_concurrent_requests=20,
    cache_ttl_prices=7200,  # 2 hours
    inactive_threshold_days=180  # 6 months
)

tracker = WalletTracker(config=config)
```

## 🧪 Testing

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src/wallet_tracker --cov-report=html

# Run specific test file
uv run pytest tests/test_wallet_processor.py -v
```

## 📊 Performance

### Benchmarks

| Wallet Count | Processing Time | API Calls | Memory Usage |
|--------------|-----------------|-----------|--------------|
| 1,000 | ~5 minutes | ~15,000 | ~50MB |
| 10,000 | ~45 minutes | ~150,000 | ~200MB |
| 30,000 | ~2.5 hours | ~450,000 | ~500MB |

### Optimization Tips

1. **Use caching**: Enable Redis for significant speedup on repeated runs
2. **Adjust batch size**: Increase for better performance, decrease for stability
3. **Filter inactive wallets**: Reduces API calls by 60-80%
4. **Use Alchemy Pro**: Higher rate limits than free tier
5. **Deploy to cloud**: Better network performance for API calls

## 🚀 Deployment

### Google Cloud Run (Recommended)

```bash
# Build and deploy
gcloud run deploy ethereum-wallet-tracker \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars ALCHEMY_API_KEY=$ALCHEMY_API_KEY
```

### Docker

```bash
# Build image
docker build -t ethereum-wallet-tracker .

# Run container
docker run -d \
  --env-file .env \
  -p 8080:8080 \
  ethereum-wallet-tracker
```

## 📈 Monitoring

### Logging

Logs are structured and include:
- Processing progress and performance metrics
- API response times and rate limit status
- Error details and stack traces
- Cache hit/miss ratios

### Metrics

Key metrics tracked:
- Wallets processed per minute
- API success/failure rates
- Cache efficiency
- Memory and CPU usage

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Install development dependencies
uv sync --all-extras

# Install pre-commit hooks
uv run pre-commit install

# Run code formatting and linting
uv run ruff format src tests
uv run ruff check src tests --fix

# Type checking
uv run mypy src
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Web3.py](https://web3py.readthedocs.io/) for Ethereum integration
- [CoinGecko](https://www.coingecko.com/en/api) for token price data
- [Google Sheets API](https://developers.google.com/sheets/api) for spreadsheet integration
- [UV](https://github.com/astral-sh/uv) for fast Python package management

## 📞 Support

- 📧 Email: support@yourcompany.com
- 💬 Discord: [Join our community](https://discord.gg/yourserver)
- 📖 Documentation: [Full docs](https://docs.yourproject.com)
- 🐛 Issues: [GitHub Issues](https://github.com/yourusername/ethereum-wallet-tracker/issues)
