# Examples and Best Practices

## Overview

This guide provides practical examples, common usage patterns, and best practices for the Crypto Wealth Tracker. Each example includes complete, runnable code with explanations.

## Table of Contents

- [Quick Start Examples](#quick-start-examples)
- [Wallet Analysis Patterns](#wallet-analysis-patterns)
- [Google Sheets Integration](#google-sheets-integration)
- [Batch Processing](#batch-processing)
- [Advanced Configuration](#advanced-configuration)
- [Error Handling Patterns](#error-handling-patterns)
- [Performance Optimization](#performance-optimization)
- [Production Patterns](#production-patterns)
- [Best Practices](#best-practices)

## Quick Start Examples

### Basic Wallet Analysis

Analyze a single wallet and display results:

```python
import asyncio
from wallet_tracker.app import create_application

async def analyze_wallet():
    """Analyze a single Ethereum wallet."""
    
    # Vitalik's wallet address
    wallet_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
    
    async with create_application() as app:
        # Get complete portfolio
        portfolio = await app.ethereum_client.get_wallet_portfolio(
            wallet_address=wallet_address,
            include_metadata=True,
            include_prices=True
        )
        
        print(f"📊 Wallet Analysis for {wallet_address}")
        print(f"{'='*60}")
        print(f"ETH Balance: {portfolio.eth_balance.balance_eth:.4f} ETH")
        print(f"ETH Value: ${portfolio.eth_balance.value_usd:,.2f}")
        print(f"Total Portfolio Value: ${portfolio.total_value_usd:,.2f}")
        print(f"Number of Tokens: {len(portfolio.token_balances)}")
        print(f"Transaction Count: {portfolio.transaction_count}")
        print(f"Last Updated: {portfolio.last_updated}")
        
        if portfolio.token_balances:
            print(f"\n🪙 Top Token Holdings:")
            # Sort by value and show top 5
            sorted_tokens = sorted(
                portfolio.token_balances, 
                key=lambda x: x.value_usd or 0, 
                reverse=True
            )
            
            for token in sorted_tokens[:5]:
                print(f"  {token.symbol}: {token.balance_formatted:.4f} "
                      f"(${token.value_usd:,.2f})")

# Run the example
asyncio.run(analyze_wallet())
```

### Simple Batch Processing

Process multiple wallets from a list:

```python
import asyncio
from decimal import Decimal
from wallet_tracker.app import create_application

async def batch_analyze():
    """Analyze multiple wallets in batch."""
    
    # List of wallet addresses to analyze
    wallet_addresses = [
        {
            "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
            "label": "Vitalik Buterin",
            "row_number": 1
        },
        {
            "address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B",
            "label": "Example Whale",
            "row_number": 2
        },
        {
            "address": "0x8ba1f109551bD432803012645Hac136c73F825e01",
            "label": "DeFi User",
            "row_number": 3
        }
    ]
    
    async with create_application() as app:
        print(f"🚀 Processing {len(wallet_addresses)} wallets...")
        
        # Process wallets
        results = await app.process_wallet_list(addresses=wallet_addresses)
        
        print(f"\n📊 Batch Processing Results:")
        print(f"{'='*50}")
        print(f"Total Wallets: {results.get('input', {}).get('total_wallets', 0)}")
        print(f"Successfully Processed: {results.get('results', {}).get('processed', 0)}")
        print(f"Failed: {results.get('results', {}).get('failed', 0)}")
        print(f"Total Portfolio Value: ${results.get('portfolio_values', {}).get('total_usd', 0):,.2f}")
        print(f"Average Value: ${results.get('portfolio_values', {}).get('average_usd', 0):,.2f}")
        print(f"Processing Time: {results.get('performance', {}).get('total_time_seconds', 0):.1f}s")

asyncio.run(batch_analyze())
```

## Wallet Analysis Patterns

### Portfolio Risk Assessment

Analyze and categorize wallets by risk level:

```python
import asyncio
from decimal import Decimal
from enum import Enum
from dataclasses import dataclass
from typing import List
from wallet_tracker.app import create_application

class RiskLevel(Enum):
    LOW = "Low Risk"
    MEDIUM = "Medium Risk"
    HIGH = "High Risk"
    CRITICAL = "Critical Risk"

@dataclass
class WalletRiskProfile:
    address: str
    label: str
    total_value: Decimal
    risk_level: RiskLevel
    risk_factors: List[str]
    diversification_score: float

async def assess_wallet_risk():
    """Comprehensive wallet risk assessment."""
    
    wallets = [
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Vitalik"},
        {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "Whale"}
    ]
    
    async with create_application() as app:
        risk_profiles = []
        
        for wallet in wallets:
            portfolio = await app.ethereum_client.get_wallet_portfolio(
                wallet_address=wallet["address"],
                include_metadata=True,
                include_prices=True
            )
            
            # Calculate risk factors
            risk_factors = []
            risk_score = 0
            
            # Value concentration risk
            if portfolio.total_value_usd > Decimal("1000000"):  # > $1M
                risk_factors.append("High value concentration")
                risk_score += 2
            
            # Token diversification
            token_count = len(portfolio.token_balances)
            if token_count < 3:
                risk_factors.append("Low diversification")
                risk_score += 2
            elif token_count < 5:
                risk_factors.append("Medium diversification")
                risk_score += 1
            
            # ETH concentration
            eth_percentage = (portfolio.eth_balance.value_usd or 0) / portfolio.total_value_usd * 100
            if eth_percentage > 80:
                risk_factors.append("High ETH concentration")
                risk_score += 1
            
            # Activity risk
            if portfolio.transaction_count < 10:
                risk_factors.append("Low activity")
                risk_score += 1
            
            # Determine risk level
            if risk_score <= 1:
                risk_level = RiskLevel.LOW
            elif risk_score <= 3:
                risk_level = RiskLevel.MEDIUM
            elif risk_score <= 5:
                risk_level = RiskLevel.HIGH
            else:
                risk_level = RiskLevel.CRITICAL
            
            # Calculate diversification score
            diversification_score = min(token_count / 10, 1.0) * 100
            
            risk_profile = WalletRiskProfile(
                address=wallet["address"],
                label=wallet["label"],
                total_value=portfolio.total_value_usd,
                risk_level=risk_level,
                risk_factors=risk_factors,
                diversification_score=diversification_score
            )
            
            risk_profiles.append(risk_profile)
        
        # Display risk assessment
        print("🛡️ Wallet Risk Assessment Report")
        print("=" * 60)
        
        for profile in risk_profiles:
            print(f"\n📊 {profile.label} ({profile.address[:10]}...)")
            print(f"   Total Value: ${profile.total_value:,.2f}")
            print(f"   Risk Level: {profile.risk_level.value}")
            print(f"   Diversification: {profile.diversification_score:.1f}%")
            print(f"   Risk Factors:")
            for factor in profile.risk_factors:
                print(f"     - {factor}")

asyncio.run(assess_wallet_risk())
```

### Token Holdings Analysis

Analyze token distribution across wallets:

```python
import asyncio
from collections import defaultdict
from decimal import Decimal
from wallet_tracker.app import create_application

async def analyze_token_holdings():
    """Analyze token holdings across multiple wallets."""
    
    wallets = [
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Wallet 1"},
        {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "Wallet 2"},
        {"address": "0x8ba1f109551bD432803012645Hac136c73F825e01", "label": "Wallet 3"}
    ]
    
    async with create_application() as app:
        # Aggregate token data
        token_holdings = defaultdict(lambda: {
            'total_balance': Decimal('0'),
            'total_value': Decimal('0'),
            'holder_count': 0,
            'holders': []
        })
        
        total_portfolio_value = Decimal('0')
        
        for wallet in wallets:
            portfolio = await app.ethereum_client.get_wallet_portfolio(
                wallet_address=wallet["address"],
                include_metadata=True,
                include_prices=True
            )
            
            total_portfolio_value += portfolio.total_value_usd
            
            # ETH holdings
            if portfolio.eth_balance.balance_eth > 0:
                token_holdings['ETH']['total_balance'] += portfolio.eth_balance.balance_eth
                token_holdings['ETH']['total_value'] += portfolio.eth_balance.value_usd or Decimal('0')
                token_holdings['ETH']['holder_count'] += 1
                token_holdings['ETH']['holders'].append({
                    'wallet': wallet["label"],
                    'balance': portfolio.eth_balance.balance_eth,
                    'value': portfolio.eth_balance.value_usd or Decimal('0')
                })
            
            # Token holdings
            for token in portfolio.token_balances:
                if token.balance_formatted > 0:
                    symbol = token.symbol
                    token_holdings[symbol]['total_balance'] += token.balance_formatted
                    token_holdings[symbol]['total_value'] += token.value_usd or Decimal('0')
                    token_holdings[symbol]['holder_count'] += 1
                    token_holdings[symbol]['holders'].append({
                        'wallet': wallet["label"],
                        'balance': token.balance_formatted,
                        'value': token.value_usd or Decimal('0')
                    })
        
        # Sort tokens by total value
        sorted_tokens = sorted(
            token_holdings.items(),
            key=lambda x: x[1]['total_value'],
            reverse=True
        )
        
        print("🪙 Token Holdings Analysis")
        print("=" * 60)
        print(f"Total Portfolio Value: ${total_portfolio_value:,.2f}")
        print(f"Number of Unique Tokens: {len(token_holdings)}")
        
        print(f"\n📊 Top Holdings by Value:")
        for symbol, data in sorted_tokens[:10]:
            percentage = (data['total_value'] / total_portfolio_value) * 100
            print(f"\n{symbol}:")
            print(f"  Total Value: ${data['total_value']:,.2f} ({percentage:.1f}%)")
            print(f"  Total Balance: {data['total_balance']:,.4f}")
            print(f"  Holders: {data['holder_count']}/{len(wallets)}")
            
            # Show individual holdings
            for holder in data['holders']:
                holder_percentage = (holder['value'] / data['total_value']) * 100
                print(f"    {holder['wallet']}: {holder['balance']:,.4f} (${holder['value']:,.2f}, {holder_percentage:.1f}%)")

asyncio.run(analyze_token_holdings())
```

## Google Sheets Integration

### Complete Sheets Workflow

End-to-end Google Sheets integration with formatting:

```python
import asyncio
from datetime import datetime
from wallet_tracker.app import create_application

async def complete_sheets_workflow():
    """Complete workflow with Google Sheets integration."""
    
    # Your Google Sheets ID (replace with actual ID)
    SPREADSHEET_ID = "your_spreadsheet_id_here"
    INPUT_SHEET = "Wallet_Addresses"
    OUTPUT_SHEET = "Analysis_Results"
    SUMMARY_SHEET = "Portfolio_Summary"
    
    async with create_application() as app:
        print("📊 Starting complete Google Sheets workflow...")
        
        # Step 1: Create sample input data
        sample_data = [
            ["Address", "Label", "Category"],
            ["0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "Vitalik Buterin", "Founder"],
            ["0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "Whale Wallet", "Institutional"],
            ["0x8ba1f109551bD432803012645Hac136c73F825e01", "DeFi User", "Individual"]
        ]
        
        print("📝 Writing sample data to input sheet...")
        await app.sheets_client.write_range(
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name=INPUT_SHEET,
            range_name=f"A1:C{len(sample_data)}",
            values=sample_data
        )
        
        # Step 2: Read and process wallets
        print("📖 Reading wallet addresses from sheet...")
        addresses = await app.sheets_client.read_wallet_addresses(
            spreadsheet_id=SPREADSHEET_ID,
            range_name="A:C",
            worksheet_name=INPUT_SHEET,
            skip_header=True
        )
        
        print(f"🔄 Processing {len(addresses)} wallets...")
        results = await app.process_wallet_list([
            {
                "address": addr.address,
                "label": addr.label, 
                "row_number": addr.row_number
            } for addr in addresses
        ])
        
        # Step 3: Write results to output sheet
        print("✏️ Writing results to output sheet...")
        
        # Prepare results data
        output_headers = [
            "Address", "Label", "Category", "ETH Balance", "ETH Value (USD)",
            "Top Token", "Top Token Balance", "Total Value (USD)", 
            "Token Count", "Risk Level", "Last Updated"
        ]
        
        output_data = [output_headers]
        
        # Add results (simplified for example)
        for i, addr in enumerate(addresses):
            # This would use actual results from processing
            output_data.append([
                addr.address,
                addr.label,
                sample_data[i+1][2] if i+1 < len(sample_data) else "Unknown",
                "12.3456",  # ETH balance
                "$25,000.00",  # ETH value
                "USDC",  # Top token
                "50,000.0000",  # Top token balance
                "$75,000.00",  # Total value
                "5",  # Token count
                "Medium",  # Risk level
                datetime.now().strftime("%Y-%m-%d %H:%M")
            ])
        
        await app.sheets_client.write_range(
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name=OUTPUT_SHEET,
            range_name=f"A1:K{len(output_data)}",
            values=output_data
        )
        
        # Step 4: Create summary sheet
        print("📊 Creating portfolio summary...")
        
        summary_data = {
            "total_wallets": len(addresses),
            "total_value_usd": float(results.get('portfolio_values', {}).get('total_usd', 0)),
            "average_value_usd": float(results.get('portfolio_values', {}).get('average_usd', 0)),
            "active_wallets": results.get('activity', {}).get('active_wallets', 0),
            "eth_holders": results.get('token_holders', {}).get('eth', 0),
            "analysis_time": datetime.now(),
            "processing_time": f"{results.get('performance', {}).get('total_time_seconds', 0):.1f}s"
        }
        
        success = await app.sheets_client.create_summary_sheet(
            spreadsheet_id=SPREADSHEET_ID,
            summary_data=summary_data,
            worksheet_name=SUMMARY_SHEET
        )
        
        if success:
            print("✅ Complete workflow finished successfully!")
            print(f"📊 Results written to: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
            print(f"📈 Summary: {summary_data}")
        else:
            print("⚠️ Workflow completed with some issues")

# Run the workflow
asyncio.run(complete_sheets_workflow())
```

### Automated Reporting

Create automated reports with scheduling:

```python
import asyncio
from datetime import datetime, timedelta
from wallet_tracker.app import create_application

async def create_automated_report():
    """Create an automated portfolio report."""
    
    SPREADSHEET_ID = "your_spreadsheet_id_here"
    REPORT_SHEET = f"Report_{datetime.now().strftime('%Y%m%d')}"
    
    async with create_application() as app:
        print("🤖 Generating automated portfolio report...")
        
        # Sample wallet list (in production, read from database or config)
        wallets = [
            {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Vitalik"},
            {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "Whale"}
        ]
        
        # Process wallets
        results = await app.process_wallet_list(wallets)
        
        # Create report data
        report_timestamp = datetime.now()
        report_data = [
            ["📊 AUTOMATED PORTFOLIO REPORT", "", "", ""],
            ["", "", "", ""],
            [f"Generated: {report_timestamp.strftime('%Y-%m-%d %H:%M:%S')}", "", "", ""],
            [f"Report Period: Last 24 hours", "", "", ""],
            ["", "", "", ""],
            ["EXECUTIVE SUMMARY", "", "", ""],
            ["Metric", "Value", "Change", "Status"],
            ["Total Wallets", len(wallets), "0", "✅ Stable"],
            ["Total Portfolio Value", f"${results.get('portfolio_values', {}).get('total_usd', 0):,.2f}", "+5.2%", "📈 Growing"],
            ["Average Wallet Value", f"${results.get('portfolio_values', {}).get('average_usd', 0):,.2f}", "+3.1%", "📈 Growing"],
            ["Active Wallets", results.get('activity', {}).get('active_wallets', 0), "+1", "✅ Stable"],
            ["", "", "", ""],
            ["RISK ASSESSMENT", "", "", ""],
            ["Risk Level", "Low", "No Change", "✅ Good"],
            ["Diversification", "Medium", "+1 Token", "📈 Improving"],
            ["Liquidity", "High", "No Change", "✅ Good"],
            ["", "", "", ""],
            ["RECOMMENDATIONS", "", "", ""],
            ["• Consider rebalancing high-concentration positions", "", "", ""],
            ["• Monitor whale wallet activity", "", "", ""],
            ["• Increase diversification in small wallets", "", "", ""],
            ["", "", "", ""],
            ["NEXT REPORT", f"{(report_timestamp + timedelta(days=1)).strftime('%Y-%m-%d %H:%M')}", "", ""]
        ]
        
        # Write report to sheets
        await app.sheets_client.write_range(
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name=REPORT_SHEET,
            range_name=f"A1:D{len(report_data)}",
            values=report_data
        )
        
        print(f"✅ Automated report created: {REPORT_SHEET}")
        print(f"📊 Summary: {len(wallets)} wallets, ${results.get('portfolio_values', {}).get('total_usd', 0):,.2f} total value")

asyncio.run(create_automated_report())
```

## Batch Processing

### Large-Scale Processing with Progress Tracking

Handle thousands of wallets with progress monitoring:

```python
import asyncio
from wallet_tracker.app import create_application
from wallet_tracker.processors.batch_types import BatchConfig

async def large_scale_processing():
    """Process large number of wallets with progress tracking."""
    
    # Generate large wallet list (in production, read from database)
    wallet_addresses = []
    for i in range(100):  # Simulate 100 wallets
        wallet_addresses.append({
            "address": f"0x{''.join([f'{j:02x}' for j in range(20)])}",  # Fake address for demo
            "label": f"Wallet {i+1}",
            "row_number": i+1
        })
    
    # Add some real addresses for testing
    real_addresses = [
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Vitalik", "row_number": 101},
        {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "Whale", "row_number": 102}
    ]
    wallet_addresses.extend(real_addresses)
    
    # Custom batch configuration for large-scale processing
    batch_config = BatchConfig(
        batch_size=25,  # Smaller batches for stability
        max_concurrent_jobs_per_batch=5,  # Conservative concurrency
        request_delay_seconds=0.2,  # Delay to avoid rate limits
        skip_inactive_wallets=True,
        retry_failed_jobs=True,
        max_retries=3,
        timeout_seconds=120
    )
    
    async with create_application() as app:
        print(f"🚀 Starting large-scale processing of {len(wallet_addresses)} wallets")
        print(f"📊 Batch configuration:")
        print(f"   Batch size: {batch_config.batch_size}")
        print(f"   Max concurrent: {batch_config.max_concurrent_jobs_per_batch}")
        print(f"   Request delay: {batch_config.request_delay_seconds}s")
        
        # Progress tracking variables
        start_time = datetime.now()
        processed_count = 0
        
        # Progress callback
        def progress_callback(batch_progress):
            nonlocal processed_count
            processed_count = batch_progress.jobs_completed + batch_progress.jobs_failed + batch_progress.jobs_skipped
            
            percentage = batch_progress.get_progress_percentage()
            elapsed = datetime.now() - start_time
            
            if processed_count > 0:
                rate = processed_count / elapsed.total_seconds()
                remaining = len(wallet_addresses) - processed_count
                eta_seconds = remaining / rate if rate > 0 else 0
                eta = datetime.now() + timedelta(seconds=eta_seconds)
                eta_str = eta.strftime("%H:%M:%S")
            else:
                eta_str = "Calculating..."
            
            print(f"\r🔄 Progress: {percentage:.1f}% | "
                  f"Completed: {batch_progress.jobs_completed} | "
                  f"Failed: {batch_progress.jobs_failed} | "
                  f"Skipped: {batch_progress.jobs_skipped} | "
                  f"Value: ${batch_progress.total_value_processed:,.0f} | "
                  f"ETA: {eta_str}", end="", flush=True)
        
        # Process wallets
        results = await app.batch_processor.process_wallet_list(
            addresses=wallet_addresses,
            config_override=batch_config,
            progress_callback=progress_callback
        )
        
        # Final results
        print(f"\n\n✅ Large-scale processing completed!")
        print(f"📊 Final Results:")
        print(f"   Total Input: {len(wallet_addresses)}")
        print(f"   Successfully Processed: {results.wallets_processed}")
        print(f"   Failed: {results.wallets_failed}")
        print(f"   Skipped: {results.wallets_skipped}")
        print(f"   Total Value: ${results.total_portfolio_value:,.2f}")
        print(f"   Average Value: ${results.average_portfolio_value:,.2f}")
        print(f"   Processing Time: {results.total_processing_time:.1f}s")
        print(f"   Success Rate: {(results.wallets_processed / len(wallet_addresses)) * 100:.1f}%")

asyncio.run(large_scale_processing())
```

### Resume Failed Processing

Handle and resume failed batch operations:

```python
import asyncio
import json
from pathlib import Path
from wallet_tracker.app import create_application
from wallet_tracker.processors.wallet_types import WalletStatus

async def resume_failed_processing():
    """Resume processing of failed wallets from previous runs."""
    
    # Load previous results from file
    results_file = Path("processing_results.json")
    
    if results_file.exists():
        with open(results_file, 'r') as f:
            previous_results = json.load(f)
        
        print(f"📂 Loaded previous results: {len(previous_results)} wallets")
        
        # Find failed wallets that can be retried
        failed_wallets = []
        for result in previous_results:
            if (result.get('status') == 'failed' and 
                result.get('retry_count', 0) < 3 and
                result.get('skip_reason') not in ['invalid_address', 'blacklisted']):
                
                failed_wallets.append({
                    "address": result['address'],
                    "label": result.get('label', 'Unknown'),
                    "row_number": result.get('row_number', 0),
                    "retry_count": result.get('retry_count', 0) + 1
                })
        
        if not failed_wallets:
            print("✅ No failed wallets to retry")
            return
        
        print(f"🔄 Found {len(failed_wallets)} failed wallets to retry")
        
        async with create_application() as app:
            # Process failed wallets with more conservative settings
            retry_config = BatchConfig(
                batch_size=10,  # Smaller batches for retries
                max_concurrent_jobs_per_batch=3,  # Lower concurrency
                request_delay_seconds=0.5,  # More delay
                retry_failed_jobs=True,
                max_retries=2,  # Fewer retries
                timeout_seconds=180  # Longer timeout
            )
            
            print("🔄 Retrying failed wallets...")
            retry_results = await app.batch_processor.process_wallet_list(
                addresses=failed_wallets,
                config_override=retry_config
            )
            
            print(f"📊 Retry Results:")
            print(f"   Attempted: {len(failed_wallets)}")
            print(f"   Now Successful: {retry_results.wallets_processed}")
            print(f"   Still Failed: {retry_results.wallets_failed}")
            print(f"   Success Rate: {(retry_results.wallets_processed / len(failed_wallets)) * 100:.1f}%")
            
            # Update results file
            # (Implementation would merge results with previous data)
            
    else:
        print("❌ No previous results file found")

asyncio.run(resume_failed_processing())
```

## Advanced Configuration

### Custom Cache Configuration

Implement custom caching strategies:

```python
import asyncio
from wallet_tracker.app import create_application
from wallet_tracker.config import CacheConfig, CacheBackend

async def custom_cache_example():
    """Example of custom cache configuration and usage."""
    
    # Create custom cache configuration
    custom_cache_config = CacheConfig(
        backend=CacheBackend.HYBRID,  # Use both Redis and file cache
        redis_url="redis://localhost:6379/1",  # Different Redis database
        file_cache_dir=Path("custom_cache"),
        ttl_prices=7200,  # Cache prices for 2 hours
        ttl_balances=900,   # Cache balances for 15 minutes
        max_size_mb=1000    # 1GB cache limit
    )
    
    # Override default configuration
    from wallet_tracker.config import AppConfig
    config = AppConfig(
        cache=custom_cache_config,
        # ... other config settings
    )
    
    async with create_application(config) as app:
        print("🔧 Using custom cache configuration")
        
        # Test cache performance
        cache_manager = app.cache_manager
        
        if cache_manager:
            # Manual cache operations
            test_key = "test_price_eth"
            test_data = {"usd": 2000.0, "timestamp": datetime.now().isoformat()}
            
            # Set data
            await cache_manager.set_price("ethereum", test_data)
            print("💾 Data cached")
            
            # Get data
            cached_data = await cache_manager.get_price("ethereum")
            print(f"📖 Retrieved: {cached_data}")
            
            # Check cache health
            health = await cache_manager.health_check()
            print(f"🏥 Cache health: {health}")
            
            # Get cache statistics
            stats = await cache_manager.get_stats()
            for backend, backend_stats in stats.items():
                if isinstance(backend_stats, dict):
                    hit_rate = backend_stats.get('hit_rate_percent', 0)
                    print(f"📊 {backend}: {hit_rate:.1f}% hit rate")

asyncio.run(custom_cache_example())
```

### Rate Limiting Configuration

Configure custom rate limiting for different APIs:

```python
import asyncio
from wallet_tracker.app import create_application
from wallet_tracker.utils import ThrottleManager, ThrottleConfig, ThrottleMode

async def custom_rate_limiting():
    """Example of custom rate limiting configuration."""
    
    async with create_application() as app:
        # Create custom throttle manager
        throttle_manager = ThrottleManager()
        
        # Configure Ethereum throttling (conservative)
        ethereum_config = ThrottleConfig(
            mode=ThrottleMode.ADAPTIVE,
            requests_per_second=5.0,  # Conservative rate
            burst_size=10,
            min_delay=0.2,
            max_delay=5.0,
            adaptation_factor=1.5
        )
        
        # Configure CoinGecko throttling (aggressive)
        coingecko_config = ThrottleConfig(
            mode=ThrottleMode.BURST_THEN_THROTTLE,
            requests_per_second=0.4,  # 24 per minute free tier
            burst_size=5,
            burst_window_seconds=60.0
        )
        
        # Register throttles
        throttle_manager.register_throttle("ethereum", ethereum_config)
        throttle_manager.register_throttle("coingecko", coingecko_config)
        
        print("🚦 Custom rate limiting configured")
        
        # Simulate API calls with throttling
        for i in range(10):
            # Ethereum call
            await throttle_manager.wait("ethereum")
            print(f"🔗 Ethereum call {i+1}")
            await throttle_manager.report_success("ethereum")
            
            # CoinGecko call
            await throttle_manager.wait("coingecko")
            print(f"💰 CoinGecko call {i+1}")
            await throttle_manager.report_success("coingecko")
        
        # Show throttle statistics
        stats = throttle_manager.get_all_stats()
        for name, throttle_stats in stats.items():
            print(f"📊 {name} throttle: {throttle_stats}")

asyncio.run(custom_rate_limiting())
```

## Error Handling Patterns

### Robust Error Handling

Implement comprehensive error handling:

```python
import asyncio
import logging
from typing import List, Dict, Any
from wallet_tracker.app import create_application
from wallet_tracker.clients import (
    EthereumClientError, InvalidAddressError, APIError,
    CoinGeckoClientError, RateLimitError,
    GoogleSheetsClientError
)

async def robust_error_handling():
    """Demonstrate robust error handling patterns."""
    
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Test wallets (some invalid for error testing)
    test_wallets = [
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Valid Wallet"},
        {"address": "0xinvalid", "label": "Invalid Address"},
        {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "Another Valid"},
    ]
    
    async with create_application() as app:
        results = []
        
        for wallet in test_wallets:
            result = {
                "address": wallet["address"],
                "label": wallet["label"],
                "status": "pending",
                "error": None,
                "portfolio": None,
                "retry_count": 0
            }
            
            # Retry logic with exponential backoff
            max_retries = 3
            base_delay = 1.0
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"Processing {wallet['label']} (attempt {attempt + 1})")
                    
                    # Attempt to get portfolio
                    portfolio = await app.ethereum_client.get_wallet_portfolio(
                        wallet_address=wallet["address"],
                        include_metadata=True,
                        include_prices=True
                    )
                    
                    result["status"] = "success"
                    result["portfolio"] = {
                        "total_value": float(portfolio.total_value_usd),
                        "eth_balance": float(portfolio.eth_balance.balance_eth),
                        "token_count": len(portfolio.token_balances)
                    }
                    logger.info(f"✅ {wallet['label']}: ${portfolio.total_value_usd:,.2f}")
                    break
                    
                except InvalidAddressError as e:
                    # Don't retry invalid addresses
                    result["status"] = "invalid_address"
                    result["error"] = str(e)
                    logger.error(f"❌ {wallet['label']}: Invalid address")
                    break
                    
                except RateLimitError as e:
                    # Rate limit - wait longer and retry
                    result["status"] = "rate_limited"
                    result["error"] = str(e)
                    result["retry_count"] = attempt + 1
                    
                    if attempt < max_retries - 1:
                        wait_time = base_delay * (3 ** attempt)  # Longer wait for rate limits
                        logger.warning(f"⏱️ Rate limited, waiting {wait_time}s before retry")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"❌ {wallet['label']}: Rate limit exceeded, max retries reached")
                        
                except APIError as e:
                    # General API error - retry with exponential backoff
                    result["status"] = "api_error"
                    result["error"] = str(e)
                    result["retry_count"] = attempt + 1
                    
                    if attempt < max_retries - 1:
                        wait_time = base_delay * (2 ** attempt)
                        logger.warning(f"⚠️ API error, retrying in {wait_time}s: {e}")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"❌ {wallet['label']}: API error, max retries reached")
                        
                except EthereumClientError as e:
                    # General Ethereum client error
                    result["status"] = "client_error"
                    result["error"] = str(e)
                    logger.error(f"❌ {wallet['label']}: Client error: {e}")
                    break
                    
                except Exception as e:
                    # Unexpected error
                    result["status"] = "unexpected_error"
                    result["error"] = str(e)
                    logger.error(f"❌ {wallet['label']}: Unexpected error: {e}")
                    break
            
            results.append(result)
        
        # Summary
        print("\n📊 Error Handling Results:")
        print("=" * 50)
        
        status_counts = {}
        for result in results:
            status = result["status"]
            status_counts[status] = status_counts.get(status, 0) + 1
            
            print(f"{result['label']}: {status}")
            if result["error"]:
                print(f"  Error: {result['error']}")
            if result["retry_count"] > 0:
                print(f"  Retries: {result['retry_count']}")
        
        print(f"\nStatus Summary: {status_counts}")

asyncio.run(robust_error_handling())
```

### Graceful Degradation

Implement graceful degradation when services are unavailable:

```python
import asyncio
from wallet_tracker.app import create_application

async def graceful_degradation_example():
    """Demonstrate graceful degradation patterns."""
    
    async with create_application() as app:
        wallet_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
        
        # Check service health first
        health_status = await app.health_check()
        
        print("🏥 Service Health Check:")
        for service, healthy in health_status.items():
            status = "✅ Healthy" if healthy else "❌ Unhealthy"
            print(f"  {service}: {status}")
        
        # Adapt behavior based on service availability
        if health_status.get('ethereum_client', False):
            try:
                # Full portfolio analysis
                portfolio = await app.ethereum_client.get_wallet_portfolio(
                    wallet_address=wallet_address,
                    include_metadata=True,
                    include_prices=health_status.get('coingecko_client', False)
                )
                
                print(f"\n💰 Full Portfolio Analysis:")
                print(f"  ETH Balance: {portfolio.eth_balance.balance_eth:.4f}")
                print(f"  Token Count: {len(portfolio.token_balances)}")
                
                if health_status.get('coingecko_client', False):
                    print(f"  Total Value: ${portfolio.total_value_usd:,.2f}")
                else:
                    print(f"  Total Value: Unable to calculate (price service unavailable)")
                
            except Exception as e:
                print(f"❌ Portfolio analysis failed: {e}")
                print("🔄 Falling back to basic address validation...")
                
                # Fallback: basic address validation
                from wallet_tracker.clients.ethereum_types import is_valid_ethereum_address
                if is_valid_ethereum_address(wallet_address):
                    print("✅ Address is valid Ethereum format")
                else:
                    print("❌ Invalid Ethereum address format")
        
        else:
            print("⚠️ Ethereum client unavailable")
            print("🔄 Operating in offline mode with cached data only...")
            
            # Try to get cached data
            if app.cache_manager:
                cached_portfolio = await app.cache_manager.get_balance(wallet_address)
                if cached_portfolio:
                    print("💾 Found cached portfolio data")
                    # Display cached data
                else:
                    print("📭 No cached data available")
            else:
                print("❌ Cache also unavailable - limited functionality")

asyncio.run(graceful_degradation_example())
```

## Performance Optimization

### Parallel Processing Optimization

Optimize for maximum throughput:

```python
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from wallet_tracker.app import create_application
from wallet_tracker.processors.batch_types import BatchConfig

async def performance_optimization():
    """Demonstrate performance optimization techniques."""
    
    # Generate test wallets
    test_wallets = []
    for i in range(50):
        test_wallets.append({
            "address": f"0x{''.join([f'{j:02x}' for j in range(20)])}",  # Fake for demo
            "label": f"Wallet {i+1}",
            "row_number": i+1
        })
    
    # Add some real addresses
    real_addresses = [
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Vitalik", "row_number": 51},
        {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "Whale", "row_number": 52}
    ]
    test_wallets.extend(real_addresses)
    
    # Performance test configurations
    configs = [
        ("Conservative", BatchConfig(
            batch_size=10,
            max_concurrent_jobs_per_batch=3,
            request_delay_seconds=0.5
        )),
        ("Balanced", BatchConfig(
            batch_size=25,
            max_concurrent_jobs_per_batch=8,
            request_delay_seconds=0.2
        )),
        ("Aggressive", BatchConfig(
            batch_size=50,
            max_concurrent_jobs_per_batch=15,
            request_delay_seconds=0.1
        ))
    ]
    
    async with create_application() as app:
        print("🚀 Performance Optimization Testing")
        print("=" * 50)
        
        for config_name, batch_config in configs:
            print(f"\n🧪 Testing {config_name} Configuration:")
            print(f"   Batch size: {batch_config.batch_size}")
            print(f"   Max concurrent: {batch_config.max_concurrent_jobs_per_batch}")
            print(f"   Request delay: {batch_config.request_delay_seconds}s")
            
            start_time = time.time()
            
            # Process wallets with current configuration
            results = await app.batch_processor.process_wallet_list(
                addresses=test_wallets,
                config_override=batch_config
            )
            
            end_time = time.time()
            processing_time = end_time - start_time
            throughput = len(test_wallets) / processing_time
            
            print(f"   ⏱️ Processing time: {processing_time:.1f}s")
            print(f"   📊 Throughput: {throughput:.1f} wallets/second")
            print(f"   ✅ Success rate: {(results.wallets_processed / len(test_wallets)) * 100:.1f}%")
            
            # Wait between tests
            await asyncio.sleep(2)

asyncio.run(performance_optimization())
```

### Memory Optimization

Optimize memory usage for large datasets:

```python
import asyncio
import psutil
import gc
from wallet_tracker.app import create_application

async def memory_optimization():
    """Demonstrate memory optimization techniques."""
    
    def get_memory_usage():
        """Get current memory usage in MB."""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    
    async with create_application() as app:
        print("💾 Memory Optimization Example")
        print("=" * 40)
        
        initial_memory = get_memory_usage()
        print(f"Initial memory usage: {initial_memory:.1f} MB")
        
        # Process wallets in chunks to manage memory
        large_wallet_list = []
        for i in range(1000):  # Simulate large dataset
            large_wallet_list.append({
                "address": f"0x{''.join([f'{j:02x}' for j in range(20)])}",
                "label": f"Wallet {i+1}",
                "row_number": i+1
            })
        
        print(f"Generated {len(large_wallet_list)} test wallets")
        
        # Process in memory-efficient chunks
        chunk_size = 50
        total_processed = 0
        
        for i in range(0, len(large_wallet_list), chunk_size):
            chunk = large_wallet_list[i:i + chunk_size]
            
            print(f"\n🔄 Processing chunk {(i//chunk_size)+1}/{(len(large_wallet_list)+chunk_size-1)//chunk_size}")
            
            # Process chunk
            results = await app.batch_processor.process_wallet_list(
                addresses=chunk,
                config_override=BatchConfig(
                    batch_size=10,
                    max_concurrent_jobs_per_batch=3,
                    use_cache=True  # Enable caching to reduce memory
                )
            )
            
            total_processed += results.wallets_processed
            
            # Check memory usage
            current_memory = get_memory_usage()
            print(f"   Memory usage: {current_memory:.1f} MB (+{current_memory - initial_memory:.1f} MB)")
            
            # Force garbage collection between chunks
            gc.collect()
            
            # Clear variables to free memory
            del results
            del chunk
        
        final_memory = get_memory_usage()
        print(f"\n📊 Final Results:")
        print(f"   Total processed: {total_processed}/{len(large_wallet_list)}")
        print(f"   Final memory usage: {final_memory:.1f} MB")
        print(f"   Memory increase: {final_memory - initial_memory:.1f} MB")

asyncio.run(memory_optimization())
```

## Production Patterns

### Health Monitoring and Alerting

Implement comprehensive health monitoring:

```python
import asyncio
import json
import smtplib
from datetime import datetime
from email.mime.text import MimeText
from wallet_tracker.app import create_application

class HealthMonitor:
    """Production health monitoring system."""
    
    def __init__(self, alert_email: str = None):
        self.alert_email = alert_email
        self.health_history = []
        
    async def comprehensive_health_check(self, app):
        """Perform comprehensive health check."""
        health_report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "overall_status": "healthy",
            "services": {},
            "metrics": {},
            "alerts": []
        }
        
        try:
            # Service health checks
            service_health = await app.health_check()
            health_report["services"] = service_health
            
            # Application metrics
            metrics = await app.collect_metrics()
            health_report["metrics"] = metrics
            
            # Check for critical issues
            alerts = []
            
            # Check service availability
            critical_services = ["ethereum_client", "coingecko_client"]
            for service in critical_services:
                if not service_health.get(service, False):
                    alerts.append(f"Critical service down: {service}")
            
            # Check cache health
            if app.cache_manager:
                cache_stats = await app.cache_manager.get_stats()
                for backend, stats in cache_stats.items():
                    if isinstance(stats, dict):
                        hit_rate = stats.get('hit_rate_percent', 0)
                        if hit_rate < 20:  # Low cache hit rate
                            alerts.append(f"Low cache hit rate for {backend}: {hit_rate:.1f}%")
            
            # Check API error rates
            eth_stats = metrics.get('ethereum_client', {})
            cg_stats = metrics.get('coingecko_client', {})
            
            if eth_stats.get('api_errors', 0) > 10:
                alerts.append(f"High Ethereum API error count: {eth_stats['api_errors']}")
            
            if cg_stats.get('rate_limit_errors', 0) > 5:
                alerts.append(f"CoinGecko rate limit issues: {cg_stats['rate_limit_errors']}")
            
            # Update overall status
            if alerts:
                health_report["overall_status"] = "degraded" if len(alerts) < 3 else "critical"
                health_report["alerts"] = alerts
            
            return health_report
            
        except Exception as e:
            health_report["overall_status"] = "error"
            health_report["error"] = str(e)
            return health_report
    
    async def send_alert(self, health_report):
        """Send alert if issues detected."""
        if health_report["overall_status"] in ["degraded", "critical", "error"] and self.alert_email:
            try:
                # Create email alert
                subject = f"🚨 Wallet Tracker Alert - {health_report['overall_status'].upper()}"
                
                body = f"""
Crypto Wealth Tracker Health Alert

Status: {health_report['overall_status'].upper()}
Timestamp: {health_report['timestamp']}

Issues Detected:
"""
                for alert in health_report.get("alerts", []):
                    body += f"- {alert}\n"
                
                if "error" in health_report:
                    body += f"\nError: {health_report['error']}\n"
                
                body += f"\nFull Report:\n{json.dumps(health_report, indent=2)}"
                
                # Send email (configure your SMTP settings)
                # This is a simplified example
                print(f"📧 Alert would be sent to {self.alert_email}")
                print(f"Subject: {subject}")
                print(f"Body preview: {body[:200]}...")
                
            except Exception as e:
                print(f"❌ Failed to send alert: {e}")

async def production_health_monitoring():
    """Production health monitoring example."""
    
    monitor = HealthMonitor(alert_email="admin@yourcompany.com")
    
    async with create_application() as app:
        print("🏥 Production Health Monitoring")
        print("=" * 40)
        
        # Perform health check
        health_report = await monitor.comprehensive_health_check(app)
        
        print(f"Overall Status: {health_report['overall_status'].upper()}")
        print(f"Timestamp: {health_report['timestamp']}")
        
        # Display service status
        print(f"\n🔧 Service Status:")
        for service, status in health_report.get("services", {}).items():
            emoji = "✅" if status else "❌"
            print(f"  {emoji} {service}: {'Healthy' if status else 'Unhealthy'}")
        
        # Display alerts
        if health_report.get("alerts"):
            print(f"\n🚨 Alerts:")
            for alert in health_report["alerts"]:
                print(f"  ⚠️ {alert}")
        
        # Send alert if needed
        await monitor.send_alert(health_report)
        
        # Save health report
        with open(f"health_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
            json.dump(health_report, f, indent=2, default=str)
        
        print(f"\n💾 Health report saved")

asyncio.run(production_health_monitoring())
```

### Configuration Management

Production configuration management:

```python
import asyncio
import os
from pathlib import Path
from wallet_tracker.config import AppConfig, Environment

class ProductionConfigManager:
    """Production configuration management."""
    
    def __init__(self):
        self.config_dir = Path("/etc/wallet-tracker")
        self.secrets_dir = Path("/var/secrets/wallet-tracker")
        
    def load_production_config(self):
        """Load production configuration with secrets."""
        
        # Load secrets from secure location
        secrets = {}
        if self.secrets_dir.exists():
            for secret_file in self.secrets_dir.glob("*.txt"):
                key = secret_file.stem.upper()
                with open(secret_file, 'r') as f:
                    secrets[key] = f.read().strip()
        
        # Set environment variables from secrets
        for key, value in secrets.items():
            os.environ[key] = value
        
        # Load configuration
        from wallet_tracker.config import get_config
        config = get_config()
        
        # Validate production configuration
        self.validate_production_config(config)
        
        return config
    
    def validate_production_config(self, config: AppConfig):
        """Validate production configuration."""
        issues = []
        
        # Check environment
        if config.environment != Environment.PRODUCTION:
            issues.append("Environment should be 'production'")
        
        # Check debug mode
        if config.debug:
            issues.append("Debug mode should be disabled in production")
        
        # Check required API keys
        if not config.ethereum.alchemy_api_key:
            issues.append("Alchemy API key is required")
        
        # Check cache configuration
        if config.cache.backend == "file" and config.cache.max_size_mb < 100:
            issues.append("Cache size too small for production")
        
        # Check processing limits
        if config.processing.batch_size > 200:
            issues.append("Batch size too large - may cause memory issues")
        
        if config.processing.max_concurrent_requests > 50:
            issues.append("Too many concurrent requests - may trigger rate limits")
        
        # Check logging
        if not config.logging.file:
            issues.append("File logging should be enabled in production")
        
        if issues:
            print("⚠️ Production Configuration Issues:")
            for issue in issues:
                print(f"  - {issue}")
            raise ValueError("Production configuration validation failed")
        
        print("✅ Production configuration validated successfully")

async def production_config_example():
    """Production configuration example."""
    
    config_manager = ProductionConfigManager()
    
    try:
        # Load production configuration
        config = config_manager.load_production_config()
        
        print("🔧 Production Configuration Loaded")
        print("=" * 40)
        print(f"Environment: {config.environment.value}")
        print(f"Debug mode: {config.debug}")
        print(f"Cache backend: {config.cache.backend.value}")
        print(f"Batch size: {config.processing.batch_size}")
        print(f"Log file: {config.logging.file}")
        
        # Test configuration with actual app
        from wallet_tracker.app import create_application
        async with create_application(config) as app:
            # Perform basic health check
            health = await app.health_check()
            print(f"\n🏥 Health Check: {health}")
            
    except Exception as e:
        print(f"❌ Configuration error: {e}")

asyncio.run(production_config_example())
```

## Best Practices

### Security Best Practices

```python
import asyncio
import hashlib
import hmac
import secrets
from wallet_tracker.app import create_application

class SecurityManager:
    """Security utilities and best practices."""
    
    def __init__(self):
        self.rate_limit_cache = {}
        self.blocked_addresses = set()
    
    def validate_wallet_address(self, address: str) -> bool:
        """Secure wallet address validation."""
        try:
            # Basic format validation
            if not isinstance(address, str):
                return False
            
            address = address.strip().lower()
            
            # Check format
            if not address.startswith('0x') or len(address) != 42:
                return False
            
            # Check if hex
            int(address[2:], 16)
            
            # Check against blocked addresses
            if address in self.blocked_addresses:
                return False
            
            return True
            
        except (ValueError, TypeError):
            return False
    
    def rate_limit_check(self, client_id: str, max_requests: int = 100, window_minutes: int = 60) -> bool:
        """Simple rate limiting check."""
        import time
        
        now = time.time()
        window_start = now - (window_minutes * 60)
        
        # Clean old entries
        if client_id in self.rate_limit_cache:
            self.rate_limit_cache[client_id] = [
                req_time for req_time in self.rate_limit_cache[client_id]
                if req_time > window_start
            ]
        else:
            self.rate_limit_cache[client_id] = []
        
        # Check limit
        if len(self.rate_limit_cache[client_id]) >= max_requests:
            return False
        
        # Add current request
        self.rate_limit_cache[client_id].append(now)
        return True
    
    def sanitize_input(self, data: dict) -> dict:
        """Sanitize input data."""
        sanitized = {}
        
        for key, value in data.items():
            # Remove dangerous characters
            if isinstance(value, str):
                # Basic sanitization
                value = value.strip()[:1000]  # Limit length
                # Remove potential injection attempts
                dangerous_chars = ['<', '>', '"', "'", '&', ';']
                for char in dangerous_chars:
                    value = value.replace(char, '')
            
            sanitized[key] = value
        
        return sanitized

async def security_best_practices():
    """Demonstrate security best practices."""
    
    security_manager = SecurityManager()
    
    # Test data with potential security issues
    test_inputs = [
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Valid Address"},
        {"address": "0xinvalid", "label": "Invalid Address"},
        {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "<script>alert('xss')</script>"},
        {"address": "", "label": "Empty Address"},
    ]
    
    print("🔒 Security Best Practices")
    print("=" * 30)
    
    for i, test_input in enumerate(test_inputs):
        print(f"\n🧪 Test {i+1}: {test_input['label']}")
        
        # Rate limiting check
        client_id = f"test_client_{i}"
        if not security_manager.rate_limit_check(client_id):
            print("  ❌ Rate limit exceeded")
            continue
        
        # Input sanitization
        sanitized = security_manager.sanitize_input(test_input)
        print(f"  📝 Sanitized: {sanitized}")
        
        # Address validation
        is_valid = security_manager.validate_wallet_address(sanitized["address"])
        print(f"  ✅ Valid: {is_valid}")
        
        if is_valid:
            print("  🔄 Would process wallet...")
        else:
            print("  ⛔ Wallet rejected")

asyncio.run(security_best_practices())
```

### Data Validation Patterns

```python
import asyncio
from typing import List, Optional
from pydantic import BaseModel, validator, Field
from decimal import Decimal

class WalletInput(BaseModel):
    """Validated wallet input model."""
    
    address: str = Field(..., min_length=42, max_length=42)
    label: str = Field(..., min_length=1, max_length=100)
    category: Optional[str] = Field(None, max_length=50)
    expected_value_range: Optional[tuple[Decimal, Decimal]] = None
    
    @validator('address')
    def validate_ethereum_address(cls, v):
        """Validate Ethereum address format."""
        if not v.startswith('0x'):
            raise ValueError('Address must start with 0x')
        
        try:
            int(v[2:], 16)
        except ValueError:
            raise ValueError('Address must be valid hexadecimal')
        
        return v.lower()
    
    @validator('label')
    def validate_label(cls, v):
        """Validate and sanitize label."""
        # Remove potentially dangerous characters
        dangerous_chars = ['<', '>', '"', "'", '&', ';', '\n', '\r']
        for char in dangerous_chars:
            v = v.replace(char, '')
        
        return v.strip()

class ProcessingConfig(BaseModel):
    """Validated processing configuration."""
    
    batch_size: int = Field(default=50, ge=1, le=500)
    max_concurrent: int = Field(default=10, ge=1, le=100)
    timeout_seconds: int = Field(default=60, ge=10, le=600)
    enable_caching: bool = Field(default=True)
    
    @validator('batch_size')
    def validate_batch_size(cls, v, values):
        """Validate batch size against concurrency."""
        max_concurrent = values.get('max_concurrent', 10)
        if v < max_concurrent:
            raise ValueError('Batch size should be >= max_concurrent for efficiency')
        return v

async def data_validation_example():
    """Demonstrate data validation patterns."""
    
    print("✅ Data Validation Patterns")
    print("=" * 30)
    
    # Test valid input
    try:
        valid_wallet = WalletInput(
            address="0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
            label="Vitalik Buterin",
            category="Founder"
        )
        print(f"✅ Valid wallet: {valid_wallet.label}")
    except Exception as e:
        print(f"❌ Validation error: {e}")
    
    # Test invalid inputs
    invalid_inputs = [
        {"address": "invalid", "label": "Test"},
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": ""},
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "<script>alert('xss')</script>"}
    ]
    
    for i, invalid_input in enumerate(invalid_inputs):
        try:
            WalletInput(**invalid_input)
            print(f"⚠️ Unexpected success for input {i+1}")
        except Exception as e:
            print(f"✅ Caught invalid input {i+1}: {e}")
    
    # Test processing configuration
    try:
        config = ProcessingConfig(
            batch_size=100,
            max_concurrent=20,
            timeout_seconds=120
        )
        print(f"✅ Valid config: batch_size={config.batch_size}")
    except Exception as e:
        print(f"❌ Config validation error: {e}")

asyncio.run(data_validation_example())
```

### Logging and Monitoring Best Practices

```python
import asyncio
import logging
import json
import time
from datetime import datetime
from functools import wraps
from wallet_tracker.app import create_application

# Configure structured logging
class StructuredLogger:
    """Structured logging for production applications."""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.setup_logging()
    
    def setup_logging(self):
        """Setup structured logging format."""
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    def log_event(self, level: str, event: str, **kwargs):
        """Log structured event."""
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event,
            **kwargs
        }
        
        getattr(self.logger, level.lower())(json.dumps(log_data))
    
    def log_performance(self, operation: str, duration: float, **kwargs):
        """Log performance metrics."""
        self.log_event("info", "performance_metric", 
                      operation=operation, 
                      duration_seconds=duration,
                      **kwargs)
    
    def log_error(self, error: Exception, context: dict = None):
        """Log error with context."""
        self.log_event("error", "error_occurred",
                      error_type=type(error).__name__,
                      error_message=str(error),
                      context=context or {})

def monitor_performance(operation_name: str):
    """Decorator to monitor function performance."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            logger = StructuredLogger("performance")
            start_time = time.time()
            
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                
                logger.log_performance(
                    operation=operation_name,
                    duration=duration,
                    success=True,
                    args_count=len(args),
                    kwargs_count=len(kwargs)
                )
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                logger.log_error(e, {
                    "operation": operation_name,
                    "duration": duration,
                    "args_count": len(args),
                    "kwargs_count": len(kwargs)
                })
                raise
        
        return wrapper
    return decorator

@monitor_performance("wallet_analysis")
async def analyze_wallet_with_monitoring(app, address: str):
    """Example function with monitoring."""
    logger = StructuredLogger("wallet_analysis")
    
    logger.log_event("info", "wallet_analysis_started", address=address)
    
    portfolio = await app.ethereum_client.get_wallet_portfolio(
        wallet_address=address,
        include_metadata=True,
        include_prices=True
    )
    
    logger.log_event("info", "wallet_analysis_completed",
                     address=address,
                     total_value=float(portfolio.total_value_usd),
                     token_count=len(portfolio.token_balances))
    
    return portfolio

async def logging_best_practices():
    """Demonstrate logging and monitoring best practices."""
    
    print("📊 Logging and Monitoring Best Practices")
    print("=" * 45)
    
    logger = StructuredLogger("main")
    
    async with create_application() as app:
        # Test monitored function
        try:
            address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
            portfolio = await analyze_wallet_with_monitoring(app, address)
            
            print(f"✅ Analysis completed for {address}")
            print(f"   Total value: ${portfolio.total_value_usd:,.2f}")
            
        except Exception as e:
            logger.log_error(e, {"operation": "main_execution"})
            print(f"❌ Analysis failed: {e}")
        
        # Log application metrics
        metrics = await app.collect_metrics()
        logger.log_event("info", "application_metrics", **metrics)
        
        print("📊 Structured logs generated - check console output")

asyncio.run(logging_best_practices())
```

## Summary

This examples guide demonstrates:

1. **Quick Start**: Basic wallet analysis and batch processing
2. **Advanced Patterns**: Portfolio analysis, token holdings, risk assessment
3. **Google Sheets**: Complete workflows with formatting and automation
4. **Batch Processing**: Large-scale processing with progress tracking
5. **Configuration**: Custom cache, rate limiting, and production configs
6. **Error Handling**: Robust patterns with retry logic and graceful degradation
7. **Performance**: Optimization techniques for throughput and memory
8. **Production**: Health monitoring, security, and logging best practices

### Key Takeaways

- **Always use async context managers** for proper resource cleanup
- **Implement comprehensive error handling** with appropriate retry logic
- **Use structured logging** for better debugging and monitoring
- **Validate all inputs** before processing
- **Monitor performance** and adjust configurations based on requirements
- **Implement proper security measures** for production deployments
- **Use caching effectively** to improve performance and reduce API calls

These patterns provide a solid foundation for building robust, scalable wallet analysis applications with the Crypto Wealth Tracker.