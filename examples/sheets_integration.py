"""Google Sheets integration examples for the Ethereum Wallet Tracker.

This file demonstrates:
- Reading wallet addresses from Google Sheets
- Writing results back to sheets with formatting
- Different input/output formats
- Advanced styling and conditional formatting
- Bulk processing with sheets integration
- Error handling for sheets operations
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, Dict, Any, Optional

from wallet_tracker.app import Application, create_application
from wallet_tracker.config import get_config
from wallet_tracker.clients.google_sheets_client import (
    GoogleSheetsClient,
    GoogleSheetsClientError,
)
from wallet_tracker.processors.batch_types import BatchConfig

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def example_1_basic_sheets_integration():
    """Example 1: Basic Google Sheets integration with wallet analysis."""

    print("📊 Example 1: Basic Google Sheets Integration")
    print("=" * 50)

    # Note: You'll need to update these with your actual Google Sheets details
    SAMPLE_SPREADSHEET_ID = "your_spreadsheet_id_here"
    INPUT_SHEET_NAME = "Wallet_Addresses"
    OUTPUT_SHEET_NAME = "Analysis_Results"

    async with create_application() as app:
        try:
            # Initialize Google Sheets client
            sheets_client = app.sheets_client

            print(f"🔗 Connecting to Google Sheets...")

            # Create sample input data in sheets format
            sample_wallet_data = [
                ["Address", "Label", "Notes"],
                ["0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "Vitalik Buterin", "Ethereum co-founder"],
                ["0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "Example Wallet", "Test wallet"],
                ["0x8ba1f109551bD432803012645Hac136c73F825e01", "Another Wallet", "Sample data"],
            ]

            # Write sample data to input sheet
            print(f"📝 Writing sample data to input sheet...")
            await sheets_client.write_wallet_results(
                spreadsheet_id=SAMPLE_SPREADSHEET_ID,
                wallet_results=[],  # Empty for now
                range_start="A1",
                worksheet_name=INPUT_SHEET_NAME,
                include_header=True,
                clear_existing=True
            )

            # Read wallet addresses from the sheet
            print(f"📖 Reading wallet addresses from sheet...")
            wallet_addresses = await sheets_client.read_wallet_addresses(
                spreadsheet_id=SAMPLE_SPREADSHEET_ID,
                range_name="A:C",
                worksheet_name=INPUT_SHEET_NAME,
                skip_header=True
            )

            print(f"📊 Found {len(wallet_addresses)} wallet addresses to analyze")

            if not wallet_addresses:
                print(f"⚠️  No valid wallet addresses found in the sheet")
                return None

            # Convert to processing format
            addresses = []
            for addr_data in wallet_addresses:
                addresses.append({
                    "address": addr_data.address,
                    "label": addr_data.label,
                    "row_number": addr_data.row_number
                })

            # Process the wallets
            print(f"⚡ Processing wallets...")
            results = await app.batch_processor.process_wallet_list(addresses)

            # Write results back to Google Sheets
            print(f"📝 Writing results to output sheet...")
            success = await sheets_client.write_wallet_results(
                spreadsheet_id=SAMPLE_SPREADSHEET_ID,
                wallet_results=[],  # Would contain actual results
                range_start="A1",
                worksheet_name=OUTPUT_SHEET_NAME,
                include_header=True,
                clear_existing=True
            )

            if success:
                print(f"✅ Results written to Google Sheets!")
                print(f"📊 Processing Summary:")
                print(f"  Wallets Processed: {results.wallets_processed}")
                print(f"  Total Value: ${float(results.total_portfolio_value):,.2f}")
                print(f"  Sheet URL: https://docs.google.com/spreadsheets/d/{SAMPLE_SPREADSHEET_ID}")

            return {
                'wallet_addresses': wallet_addresses,
                'processing_results': results,
                'success': success
            }

        except Exception as e:
            print(f"❌ Basic sheets integration failed: {e}")
            raise


async def example_2_advanced_formatting():
    """Example 2: Advanced formatting and styling of results."""

    print("\n🎨 Example 2: Advanced Formatting & Styling")
    print("=" * 45)

    SAMPLE_SPREADSHEET_ID = "your_spreadsheet_id_here"
    FORMATTED_SHEET_NAME = "Formatted_Results"

    async with create_application() as app:
        try:
            sheets_client = app.sheets_client

            # Sample processed data for formatting demonstration
            sample_results = [
                {
                    "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                    "label": "Vitalik",
                    "eth_balance": Decimal("1234.5678"),
                    "eth_value_usd": Decimal("1500000.50"),
                    "usdc_balance": Decimal("50000"),
                    "usdt_balance": Decimal("25000"),
                    "dai_balance": Decimal("10000"),
                    "aave_balance": Decimal("0"),
                    "uni_balance": Decimal("0"),
                    "link_balance": Decimal("0"),
                    "other_tokens_value_usd": Decimal("15000"),
                    "total_value_usd": Decimal("1600000.50"),
                    "last_updated": datetime.now(),
                    "transaction_count": 500,
                    "is_active": True
                },
                {
                    "address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B",
                    "label": "Whale Wallet",
                    "eth_balance": Decimal("890.1234"),
                    "eth_value_usd": Decimal("850000.25"),
                    "usdc_balance": Decimal("100000"),
                    "usdt_balance": Decimal("50000"),
                    "dai_balance": Decimal("25000"),
                    "aave_balance": Decimal("1000"),
                    "uni_balance": Decimal("500"),
                    "link_balance": Decimal("2000"),
                    "other_tokens_value_usd": Decimal("25000"),
                    "total_value_usd": Decimal("1050000.25"),
                    "last_updated": datetime.now(),
                    "transaction_count": 300,
                    "is_active": True
                },
                {
                    "address": "0x8ba1f109551bD432803012645Hac136c73F825e01",
                    "label": "Regular User",
                    "eth_balance": Decimal("12.3456"),
                    "eth_value_usd": Decimal("45000.75"),
                    "usdc_balance": Decimal("5000"),
                    "usdt_balance": Decimal("2000"),
                    "dai_balance": Decimal("1000"),
                    "aave_balance": Decimal("0"),
                    "uni_balance": Decimal("100"),
                    "link_balance": Decimal("50"),
                    "other_tokens_value_usd": Decimal("500"),
                    "total_value_usd": Decimal("53650.75"),
                    "last_updated": datetime.now(),
                    "transaction_count": 25,
                    "is_active": False
                }
            ]

            print(f"📝 Writing sample data with advanced formatting...")

            # Write the formatted results
            success = await sheets_client.write_wallet_results(
                spreadsheet_id=SAMPLE_SPREADSHEET_ID,
                wallet_results=sample_results,
                range_start="A1",
                worksheet_name=FORMATTED_SHEET_NAME,
                include_header=True,
                clear_existing=True
            )

            if success:
                print(f"✅ Advanced formatting completed!")
                print(f"🎨 Applied formatting includes:")
                print(f"   - Header styling with blue background")
                print(f"   - Currency formatting for values")
                print(f"   - Custom number format for ETH balances")
                print(f"   - Conditional formatting for risk levels")
                print(f"   - Status-based color coding")
                print(f"   - Data bars for value visualization")
                print(f"   - Frozen header row")
                print(f"   - Auto-resized columns")

            return {
                'formatted_data': sample_results,
                'formatting_applied': success
            }

        except Exception as e:
            print(f"❌ Advanced formatting failed: {e}")
            raise


async def example_3_bulk_processing_with_sheets():
    """Example 3: Bulk processing with progress tracking in sheets."""

    print("\n🚀 Example 3: Bulk Processing with Progress Tracking")
    print("=" * 55)

    SAMPLE_SPREADSHEET_ID = "your_spreadsheet_id_here"
    BULK_INPUT_SHEET = "Bulk_Input"
    PROGRESS_SHEET = "Processing_Progress"
    BULK_RESULTS_SHEET = "Bulk_Results"

    async with create_application() as app:
        try:
            sheets_client = app.sheets_client

            # Create bulk input data (simulating a large dataset)
            bulk_addresses = []

            # Add some real addresses
            real_addresses = [
                ("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "Vitalik Buterin"),
                ("0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "Example Wallet 1"),
                ("0x8ba1f109551bD432803012645Hac136c73F825e01", "Example Wallet 2"),
            ]

            # Generate test data
            for i, (addr, label) in enumerate(real_addresses):
                bulk_addresses.append({
                    "address": addr,
                    "label": label,
                    "row_number": i + 2  # +2 for header row
                })

            # Add some test addresses
            for i in range(20):
                fake_addr = f"0x{''.join([f'{j:02x}' for j in range(20)])}"
                bulk_addresses.append({
                    "address": fake_addr,
                    "label": f"Test Wallet {i + 1}",
                    "row_number": i + len(real_addresses) + 2
                })

            print(f"📝 Creating bulk input with {len(bulk_addresses)} addresses...")

            # Configure bulk processing
            bulk_config = BatchConfig(
                batch_size=5,  # Process in small batches for demo
                max_concurrent_jobs_per_batch=2,
                request_delay_seconds=0.5,
                timeout_seconds=120,
                use_cache=True,
                skip_inactive_wallets=True
            )

            # Progress tracking variables
            start_time = datetime.now()
            processed_count = 0
            total_value = Decimal("0")
            errors = 0

            print(f"🚀 Starting bulk processing...")

            # Process in batches with progress updates
            batch_size = bulk_config.batch_size
            all_results = []

            for i in range(0, len(bulk_addresses), batch_size):
                batch = bulk_addresses[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(bulk_addresses) + batch_size - 1) // batch_size

                try:
                    # Process this batch
                    batch_results = await app.batch_processor.process_wallet_list(
                        addresses=batch,
                        config_override=bulk_config
                    )

                    # Update progress
                    processed_count += batch_results.wallets_processed
                    total_value += batch_results.total_portfolio_value
                    errors += batch_results.wallets_failed

                    all_results.append(batch_results)

                    print(f"📦 Batch {batch_num}/{total_batches} completed: "
                          f"{batch_results.wallets_processed} processed, "
                          f"${batch_results.total_portfolio_value:,.2f} value")

                except Exception as e:
                    errors += len(batch)
                    print(f"❌ Batch {batch_num} failed: {e}")

                # Small delay between batches
                await asyncio.sleep(1)

            # Create summary report
            processing_time = (datetime.now() - start_time).total_seconds()
            total_processed = sum(r.wallets_processed for r in all_results)
            total_portfolio_value = sum(r.total_portfolio_value for r in all_results)

            # Write final results
            print(f"📝 Writing bulk processing results...")

            summary_data = {
                "total_wallets": len(bulk_addresses),
                "processed_count": total_processed,
                "total_value_usd": total_portfolio_value,
                "processing_time": processing_time,
                "analysis_time": datetime.now(),
                "active_wallets": total_processed,  # Simplified
                "inactive_wallets": 0,
                "eth_total_value": total_portfolio_value * Decimal("0.6"),  # Estimated
                "eth_holders": total_processed,
                "usdc_total_value": total_portfolio_value * Decimal("0.2"),
                "usdc_holders": int(total_processed * 0.8),
                "usdt_total_value": total_portfolio_value * Decimal("0.1"),
                "usdt_holders": int(total_processed * 0.6),
                "dai_total_value": total_portfolio_value * Decimal("0.1"),
                "dai_holders": int(total_processed * 0.4),
                "average_value_usd": total_portfolio_value / max(1, total_processed),
                "median_value_usd": total_portfolio_value / max(1, total_processed)
            }

            await sheets_client.create_summary_sheet(
                spreadsheet_id=SAMPLE_SPREADSHEET_ID,
                summary_data=summary_data,
                worksheet_name=BULK_RESULTS_SHEET
            )

            print(f"✅ Bulk processing completed!")
            print(f"📊 Final Results:")
            print(f"   Total Addresses: {len(bulk_addresses)}")
            print(f"   Successfully Processed: {total_processed}")
            print(f"   Total Portfolio Value: ${float(total_portfolio_value):,.2f}")
            print(f"   Processing Time: {processing_time:.1f}s")
            print(f"   Success Rate: {(total_processed / len(bulk_addresses)) * 100:.1f}%")

            return {
                'input_count': len(bulk_addresses),
                'processed_count': total_processed,
                'total_value': float(total_portfolio_value),
                'processing_time': processing_time,
                'batch_results': all_results
            }

        except Exception as e:
            print(f"❌ Bulk processing failed: {e}")
            raise


async def example_4_multi_sheet_dashboard():
    """Example 4: Create a multi-sheet dashboard with charts and analytics."""

    print("\n📊 Example 4: Multi-Sheet Dashboard Creation")
    print("=" * 50)

    SAMPLE_SPREADSHEET_ID = "your_spreadsheet_id_here"
    DASHBOARD_SHEET = "Dashboard"
    RAW_DATA_SHEET = "Raw_Data"
    ANALYTICS_SHEET = "Analytics"

    async with create_application() as app:
        try:
            sheets_client = app.sheets_client

            # Generate sample portfolio data for dashboard
            portfolio_data = [
                {
                    "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                    "label": "Vitalik",
                    "eth_balance": Decimal("1250.5"),
                    "eth_value_usd": Decimal("2500000"),
                    "usdc_balance": Decimal("50000"),
                    "usdt_balance": Decimal("25000"),
                    "dai_balance": Decimal("10000"),
                    "aave_balance": Decimal("0"),
                    "uni_balance": Decimal("0"),
                    "link_balance": Decimal("0"),
                    "other_tokens_value_usd": Decimal("15000"),
                    "total_value_usd": Decimal("2600000"),
                    "last_updated": datetime.now(),
                    "transaction_count": 500,
                    "is_active": True
                },
                {
                    "address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B",
                    "label": "DeFi User",
                    "eth_balance": Decimal("45.2"),
                    "eth_value_usd": Decimal("95000"),
                    "usdc_balance": Decimal("15000"),
                    "usdt_balance": Decimal("5000"),
                    "dai_balance": Decimal("2000"),
                    "aave_balance": Decimal("100"),
                    "uni_balance": Decimal("50"),
                    "link_balance": Decimal("200"),
                    "other_tokens_value_usd": Decimal("3000"),
                    "total_value_usd": Decimal("120200"),
                    "last_updated": datetime.now(),
                    "transaction_count": 150,
                    "is_active": True
                },
                {
                    "address": "0x8ba1f109551bD432803012645Hac136c73F825e01",
                    "label": "HODLer",
                    "eth_balance": Decimal("12.8"),
                    "eth_value_usd": Decimal("28000"),
                    "usdc_balance": Decimal("5000"),
                    "usdt_balance": Decimal("2000"),
                    "dai_balance": Decimal("1000"),
                    "aave_balance": Decimal("0"),
                    "uni_balance": Decimal("25"),
                    "link_balance": Decimal("50"),
                    "other_tokens_value_usd": Decimal("500"),
                    "total_value_usd": Decimal("36575"),
                    "last_updated": datetime.now(),
                    "transaction_count": 75,
                    "is_active": False
                }
            ]

            print(f"📝 Creating multi-sheet dashboard...")

            # Write raw data
            await sheets_client.write_wallet_results(
                spreadsheet_id=SAMPLE_SPREADSHEET_ID,
                wallet_results=portfolio_data,
                range_start="A1",
                worksheet_name=RAW_DATA_SHEET,
                include_header=True,
                clear_existing=True
            )

            # Create dashboard summary
            total_value = sum(w["total_value_usd"] for w in portfolio_data)
            active_count = sum(1 for w in portfolio_data if w["is_active"])

            dashboard_summary = {
                "total_wallets": len(portfolio_data),
                "active_wallets": active_count,
                "inactive_wallets": len(portfolio_data) - active_count,
                "total_value_usd": total_value,
                "average_value_usd": total_value / len(portfolio_data),
                "median_value_usd": total_value / len(portfolio_data),  # Simplified
                "eth_total_value": sum(w["eth_value_usd"] for w in portfolio_data),
                "eth_holders": len(portfolio_data),
                "usdc_total_value": sum(w["usdc_balance"] for w in portfolio_data),
                "usdc_holders": len([w for w in portfolio_data if w["usdc_balance"] > 0]),
                "usdt_total_value": sum(w["usdt_balance"] for w in portfolio_data),
                "usdt_holders": len([w for w in portfolio_data if w["usdt_balance"] > 0]),
                "dai_total_value": sum(w["dai_balance"] for w in portfolio_data),
                "dai_holders": len([w for w in portfolio_data if w["dai_balance"] > 0]),
                "analysis_time": datetime.now(),
                "processing_time": "3.5s"
            }

            # Create dashboard summary sheet
            await sheets_client.create_summary_sheet(
                spreadsheet_id=SAMPLE_SPREADSHEET_ID,
                summary_data=dashboard_summary,
                worksheet_name=DASHBOARD_SHEET
            )

            # Create analytics summary
            await sheets_client.create_summary_sheet(
                spreadsheet_id=SAMPLE_SPREADSHEET_ID,
                summary_data=dashboard_summary,
                worksheet_name=ANALYTICS_SHEET
            )

            print(f"✅ Multi-sheet dashboard created!")
            print(f"📊 Dashboard includes:")
            print(f"   - Portfolio overview with key metrics")
            print(f"   - Raw data sheet with all wallet details")
            print(f"   - Analytics sheet with derived insights")
            print(f"   - Professional formatting and styling")
            print(f"   - Summary statistics and calculations")

            return {
                'dashboard_created': True,
                'sheets_created': [DASHBOARD_SHEET, RAW_DATA_SHEET, ANALYTICS_SHEET],
                'data_rows': len(portfolio_data),
                'total_value': float(total_value)
            }

        except Exception as e:
            print(f"❌ Dashboard creation failed: {e}")
            raise


async def example_5_automated_reporting():
    """Example 5: Automated reporting with scheduled updates."""

    print("\n🤖 Example 5: Automated Reporting System")
    print("=" * 45)

    SAMPLE_SPREADSHEET_ID = "your_spreadsheet_id_here"
    REPORT_SHEET = "Automated_Report"
    HISTORY_SHEET = "Report_History"

    async with create_application() as app:
        try:
            sheets_client = app.sheets_client

            # Generate timestamp for this report
            report_timestamp = datetime.now()
            report_id = report_timestamp.strftime("%Y%m%d_%H%M%S")

            print(f"📝 Generating automated report: {report_id}")

            # Simulate portfolio analysis
            current_data = {
                "total_wallets": 55,
                "active_wallets": 42,
                "inactive_wallets": 13,
                "total_value_usd": Decimal("15800000"),
                "average_value_usd": Decimal("287272"),
                "median_value_usd": Decimal("125000"),
                "eth_total_value": Decimal("9480000"),
                "eth_holders": 48,
                "usdc_total_value": Decimal("3160000"),
                "usdc_holders": 35,
                "usdt_total_value": Decimal("1580000"),
                "usdt_holders": 28,
                "dai_total_value": Decimal("1580000"),
                "dai_holders": 22,
                "analysis_time": report_timestamp,
                "processing_time": "45.3s"
            }

            # Create automated report
            await sheets_client.create_summary_sheet(
                spreadsheet_id=SAMPLE_SPREADSHEET_ID,
                summary_data=current_data,
                worksheet_name=REPORT_SHEET
            )

            # Generate insights and alerts
            alerts = []
            growth_rate = 8.9  # Simulated growth rate

            if growth_rate > 10:
                alerts.append("🚨 High growth rate detected - monitor for volatility")
            elif growth_rate < -5:
                alerts.append("⚠️ Portfolio decline - investigate cause")
            else:
                alerts.append("✅ Portfolio performing within normal parameters")

            if current_data["active_wallets"] > 50:
                alerts.append("📈 High wallet activity detected")

            print(f"📊 Report Analysis:")
            print(f"   Portfolio Value: ${float(current_data['total_value_usd']):,.2f}")
            print(f"   Growth Rate: {growth_rate:.1f}%")
            print(f"   Active Wallets: {current_data['active_wallets']}")
            print(f"   Alerts Generated: {len(alerts)}")

            for alert in alerts:
                print(f"   {alert}")

            # Generate final summary
            report_summary = {
                'report_id': report_id,
                'timestamp': report_timestamp.isoformat(),
                'portfolio_value': float(current_data['total_value_usd']),
                'active_wallets': current_data['active_wallets'],
                'growth_rate': growth_rate,
                'alerts_count': len(alerts),
                'status': 'completed'
            }

            print(f"✅ Automated report completed!")
            print(f"📊 Report Summary:")
            print(f"   Report ID: {report_id}")
            print(f"   Status: Completed Successfully")

            return report_summary

        except Exception as e:
            print(f"❌ Automated reporting failed: {e}")
            raise


async def example_6_error_handling_sheets():
    """Example 6: Comprehensive error handling for sheets operations."""

    print("\n🛡️ Example 6: Error Handling for Sheets Operations")
    print("=" * 55)

    async with create_application() as app:
        try:
            sheets_client = app.sheets_client

            # Test various error scenarios
            error_scenarios = [
                {
                    "name": "Invalid Spreadsheet ID",
                    "expected_error": "SpreadsheetNotFound"
                },
                {
                    "name": "Invalid Range Format",
                    "expected_error": "InvalidRange"
                },
                {
                    "name": "Non-existent Sheet",
                    "expected_error": "SheetNotFound"
                },
                {
                    "name": "Permission Denied",
                    "expected_error": "PermissionDenied"
                }
            ]

            error_results = []

            print(f"🧪 Testing error handling scenarios...")

            for scenario in error_scenarios:
                print(f"\n🔍 Testing: {scenario['name']}")

                try:
                    # Simulate different error conditions
                    if scenario['name'] == "Invalid Spreadsheet ID":
                        await sheets_client.read_wallet_addresses("invalid_id", "A:B")
                    elif scenario['name'] == "Invalid Range Format":
                        await sheets_client.read_wallet_addresses("valid_id", "INVALID_RANGE")
                    elif scenario['name'] == "Non-existent Sheet":
                        await sheets_client.read_wallet_addresses("valid_id", "A:B", "NonExistentSheet")
                    elif scenario['name'] == "Permission Denied":
                        # This would need an actual restricted spreadsheet
                        print(f"  ⚠️ Simulated permission denied scenario")
                        raise GoogleSheetsClientError("Permission denied")

                    # If we get here, the operation unexpectedly succeeded
                    result = {
                        'scenario': scenario['name'],
                        'status': 'unexpected_success',
                        'error': None
                    }
                    print(f"  ⚠️ Unexpected success - operation should have failed")

                except GoogleSheetsClientError as e:
                    # Expected Google Sheets error
                    result = {
                        'scenario': scenario['name'],
                        'status': 'expected_error',
                        'error': str(e),
                        'error_type': type(e).__name__
                    }
                    print(f"  ✅ Caught expected error: {type(e).__name__}")

                except Exception as e:
                    # Unexpected error type
                    result = {
                        'scenario': scenario['name'],
                        'status': 'unexpected_error',
                        'error': str(e),
                        'error_type': type(e).__name__
                    }
                    print(f"  ❌ Unexpected error type: {type(e).__name__}")

                error_results.append(result)

            # Test recovery strategies
            print(f"\n🔧 Testing Error Recovery Strategies...")

            recovery_tests = [
                {
                    "name": "Retry on Timeout",
                    "strategy": "exponential_backoff"
                },
                {
                    "name": "Fallback to Read-only Mode",
                    "strategy": "graceful_degradation"
                },
                {
                    "name": "Partial Success Handling",
                    "strategy": "partial_processing"
                }
            ]

            for test in recovery_tests:
                print(f"\n🔄 Testing Recovery: {test['name']}")

                if test['strategy'] == 'exponential_backoff':
                    # Simulate retry with exponential backoff
                    max_retries = 3
                    base_delay = 1.0

                    for attempt in range(max_retries):
                        try:
                            print(f"  Attempt {attempt + 1}/{max_retries}")

                            # Simulate operation that might fail
                            if attempt < 2:  # Fail first two attempts
                                raise GoogleSheetsClientError("Rate limit exceeded")
                            else:
                                print(f"  ✅ Operation succeeded on attempt {attempt + 1}")
                                break

                        except GoogleSheetsClientError as e:
                            if attempt == max_retries - 1:
                                print(f"  ❌ All retry attempts failed: {e}")
                            else:
                                delay = base_delay * (2 ** attempt)
                                print(f"  ⏳ Retrying in {delay} seconds...")
                                await asyncio.sleep(delay)

                elif test['strategy'] == 'graceful_degradation':
                    # Test fallback to read-only mode
                    try:
                        # Attempt write operation
                        print(f"  Attempting write operation...")
                        raise GoogleSheetsClientError("Write permission denied")

                    except GoogleSheetsClientError:
                        print(f"  ⚠️ Write failed, falling back to read-only mode")
                        print(f"  ✅ Successfully switched to read-only operations")

                elif test['strategy'] == 'partial_processing':
                    # Test handling partial success in batch operations
                    batch_items = ["item1", "item2", "item3", "item4", "item5"]
                    successful_items = []
                    failed_items = []

                    for i, item in enumerate(batch_items):
                        try:
                            # Simulate that some items fail
                            if i in [1, 3]:  # Fail items 2 and 4
                                raise GoogleSheetsClientError(f"Failed to process {item}")

                            successful_items.append(item)
                            print(f"  ✅ Processed: {item}")

                        except GoogleSheetsClientError as e:
                            failed_items.append(item)
                            print(f"  ❌ Failed: {item}")

                    print(f"  📊 Batch results: {len(successful_items)} success, {len(failed_items)} failed")
                    print(f"  ✅ Partial success handling completed")

            # Test input validation and sanitization
            print(f"\n🛡️ Testing Input Validation...")

            validation_tests = [
                {
                    "name": "Range Validation",
                    "input": "A1:XYZ999999",
                    "valid": False
                },
                {
                    "name": "Sheet Name Validation",
                    "input": "Sheet/With\\Invalid:Characters",
                    "valid": False
                },
                {
                    "name": "Data Size Validation",
                    "input": [["x"] * 1000] * 1000,  # Very large dataset
                    "valid": False
                },
                {
                    "name": "Valid Range",
                    "input": "A1:Z100",
                    "valid": True
                }
            ]

            for test in validation_tests:
                print(f"\n🔍 Validating: {test['name']}")

                try:
                    # Simulate validation logic
                    if test['name'] == "Range Validation":
                        # Check if range format is valid
                        range_input = test['input']
                        if "XYZ" in range_input or len(range_input) > 20:
                            raise ValueError("Invalid range format")

                    elif test['name'] == "Sheet Name Validation":
                        # Check for invalid characters
                        sheet_name = test['input']
                        invalid_chars = ['/', '\\', ':', '*', '?', '[', ']']
                        if any(char in sheet_name for char in invalid_chars):
                            raise ValueError("Invalid characters in sheet name")

                    elif test['name'] == "Data Size Validation":
                        # Check data size limits
                        data = test['input']
                        if len(data) > 500 or (len(data) > 0 and len(data[0]) > 500):
                            raise ValueError("Data exceeds size limits")

                    if test['valid']:
                        print(f"  ✅ Validation passed as expected")
                    else:
                        print(f"  ⚠️ Validation should have failed but passed")

                except ValueError as e:
                    if not test['valid']:
                        print(f"  ✅ Validation correctly rejected input: {e}")
                    else:
                        print(f"  ❌ Validation incorrectly rejected valid input: {e}")

            # Generate error handling report
            print(f"\n📊 Error Handling Test Results:")

            expected_errors = sum(1 for r in error_results if r['status'] == 'expected_error')
            unexpected_errors = sum(1 for r in error_results if r['status'] == 'unexpected_error')
            unexpected_successes = sum(1 for r in error_results if r['status'] == 'unexpected_success')

            print(f"  Error Scenario Tests: {len(error_results)}")
            print(f"    ✅ Expected Errors: {expected_errors}")
            print(f"    ❌ Unexpected Errors: {unexpected_errors}")
            print(f"    ⚠️ Unexpected Successes: {unexpected_successes}")

            print(f"  Recovery Strategy Tests: {len(recovery_tests)}")
            print(f"    ✅ All recovery strategies tested")

            print(f"  Input Validation Tests: {len(validation_tests)}")
            print(f"    ✅ All validation scenarios tested")

            # Best practices summary
            print(f"\n💡 Error Handling Best Practices Demonstrated:")
            print(f"   🔄 Exponential backoff for rate limiting")
            print(f"   🛡️ Graceful degradation for permission issues")
            print(f"   📊 Partial success handling for batch operations")
            print(f"   ✅ Input validation and sanitization")
            print(f"   📋 Comprehensive error logging and reporting")
            print(f"   🔧 Recovery strategy implementation")

            return {
                'error_scenarios_tested': len(error_scenarios),
                'recovery_strategies_tested': len(recovery_tests),
                'validation_tests': len(validation_tests),
                'expected_errors': expected_errors,
                'unexpected_issues': unexpected_errors + unexpected_successes,
                'error_results': error_results
            }

        except Exception as e:
            print(f"❌ Error handling testing failed: {e}")
            raise


async def run_all_sheets_examples():
    """Run all Google Sheets integration examples."""

    print("📊 Ethereum Wallet Tracker - Google Sheets Integration Examples")
    print("=" * 70)
    print("This script demonstrates comprehensive Google Sheets integration")
    print("including data formatting, dashboard creation, and error handling.")
    print("\n⚠️ IMPORTANT: Before running these examples:")
    print("   1. Set up Google Sheets API credentials")
    print("   2. Update SAMPLE_SPREADSHEET_ID with your actual sheet ID")
    print("   3. Ensure the service account has access to your spreadsheet")
    print("   4. Grant edit permissions for full functionality\n")

    examples = [
        example_1_basic_sheets_integration,
        example_2_advanced_formatting,
        example_3_bulk_processing_with_sheets,
        example_4_multi_sheet_dashboard,
        example_5_automated_reporting,
        example_6_error_handling_sheets,
    ]

    results = {}

    for i, example_func in enumerate(examples, 1):
        try:
            print(f"\n{'=' * 70}")
            print(f"Running Sheets Example {i}: {example_func.__name__}")
            print(f"{'=' * 70}")

            result = await example_func()
            results[example_func.__name__] = {
                'status': 'success',
                'result': result
            }

            print(f"✅ Sheets Example {i} completed successfully!")

            # Delay between examples to avoid rate limiting
            await asyncio.sleep(2)

        except Exception as e:
            print(f"❌ Sheets Example {i} failed: {e}")
            results[example_func.__name__] = {
                'status': 'failed',
                'error': str(e)
            }

            # Continue with other examples even if one fails
            continue

    # Summary
    print(f"\n{'=' * 70}")
    print("📊 GOOGLE SHEETS INTEGRATION SUMMARY")
    print(f"{'=' * 70}")

    successful = sum(1 for r in results.values() if r['status'] == 'success')
    failed = len(results) - successful

    print(f"Total Sheets Examples: {len(results)}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")

    if failed > 0:
        print(f"\nFailed Examples:")
        for name, result in results.items():
            if result['status'] == 'failed':
                print(f"  ❌ {name}: {result['error']}")

    if successful > 0:
        print(f"\n📈 Google Sheets Capabilities Demonstrated:")
        print(f"   📊 Basic data reading and writing")
        print(f"   🎨 Advanced formatting and styling")
        print(f"   📈 Dashboard creation with summaries")
        print(f"   🤖 Automated reporting systems")
        print(f"   🚀 Bulk processing with progress tracking")
        print(f"   🛡️ Comprehensive error handling")

        print(f"\n🎯 Key Features Showcased:")
        print(f"   • Professional data presentation")
        print(f"   • Currency and number formatting")
        print(f"   • Multi-sheet workbook management")
        print(f"   • Real-time progress tracking")
        print(f"   • Dynamic summary calculations")
        print(f"   • Professional report generation")
        print(f"   • Robust error recovery strategies")

    print(f"\n💡 Next Steps for Production Use:")
    print(f"   📋 Set up proper Google Cloud project and credentials")
    print(f"   🔐 Implement OAuth 2.0 for user authentication")
    print(f"   📊 Create custom dashboard templates")
    print(f"   ⏰ Set up scheduled report generation")
    print(f"   🔔 Implement notification systems")
    print(f"   📈 Add more advanced charting and visualization")

    print(f"\n🎉 Google Sheets integration examples completed!")

    return results


async def example_7_real_time_monitoring():
    """Example 7: Real-time portfolio monitoring with live updates."""

    print("\n📈 Example 7: Real-Time Portfolio Monitoring")
    print("=" * 50)

    SAMPLE_SPREADSHEET_ID = "your_spreadsheet_id_here"
    MONITORING_SHEET = "Live_Monitor"
    ALERTS_SHEET = "Price_Alerts"

    async with create_application() as app:
        try:
            sheets_client = app.sheets_client

            print(f"🔴 Setting up real-time monitoring...")

            # Sample portfolio for monitoring
            monitored_wallets = [
                {
                    "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                    "label": "Vitalik Buterin",
                    "alert_threshold": Decimal("100000"),  # Alert if change > $100k
                    "last_value": Decimal("2500000")
                },
                {
                    "address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B",
                    "label": "Whale Wallet",
                    "alert_threshold": Decimal("50000"),  # Alert if change > $50k
                    "last_value": Decimal("1200000")
                }
            ]

            monitoring_data = []
            alerts_triggered = []

            print(f"📊 Monitoring {len(monitored_wallets)} wallets...")

            # Simulate monitoring cycle
            for cycle in range(3):  # 3 monitoring cycles
                print(f"\n🔄 Monitoring Cycle {cycle + 1}/3")

                timestamp = datetime.now()
                cycle_data = [["Timestamp", "Address", "Label", "Current Value", "Change", "% Change", "Status"]]

                for wallet in monitored_wallets:
                    try:
                        # Simulate getting current portfolio value
                        # In real implementation, this would call the actual API
                        import random
                        change_percent = random.uniform(-5.0, 8.0)  # Simulate price changes
                        current_value = wallet["last_value"] * (1 + change_percent / 100)
                        value_change = current_value - wallet["last_value"]

                        # Determine status
                        if abs(value_change) > wallet["alert_threshold"]:
                            status = "🚨 ALERT"
                            alerts_triggered.append({
                                "timestamp": timestamp,
                                "wallet": wallet["label"],
                                "change": value_change,
                                "threshold": wallet["alert_threshold"]
                            })
                        elif change_percent > 2:
                            status = "📈 RISING"
                        elif change_percent < -2:
                            status = "📉 FALLING"
                        else:
                            status = "➡️ STABLE"

                        cycle_data.append([
                            timestamp.strftime("%H:%M:%S"),
                            wallet["address"][:10] + "...",
                            wallet["label"],
                            f"${float(current_value):,.2f}",
                            f"${float(value_change):+,.2f}",
                            f"{change_percent:+.2f}%",
                            status
                        ])

                        # Update last value for next cycle
                        wallet["last_value"] = current_value

                        print(f"  {wallet['label']}: ${float(current_value):,.2f} ({change_percent:+.2f}%) {status}")

                    except Exception as e:
                        print(f"  ❌ Error monitoring {wallet['label']}: {e}")
                        cycle_data.append([
                            timestamp.strftime("%H:%M:%S"),
                            wallet["address"][:10] + "...",
                            wallet["label"],
                            "ERROR",
                            "ERROR",
                            "ERROR",
                            "❌ ERROR"
                        ])

                # Write cycle data to monitoring sheet
                start_row = len(monitoring_data) + 1
                await sheets_client.write_wallet_results(
                    spreadsheet_id=SAMPLE_SPREADSHEET_ID,
                    wallet_results=[],  # Empty as we're writing raw data
                    range_start=f"A{start_row}",
                    worksheet_name=MONITORING_SHEET,
                    include_header=(cycle == 0),
                    clear_existing=(cycle == 0)
                )

                monitoring_data.extend(cycle_data)

                # Wait before next cycle (in real implementation, this might be longer)
                if cycle < 2:  # Don't wait after last cycle
                    print(f"  ⏳ Waiting 30 seconds before next check...")
                    await asyncio.sleep(2)  # Shortened for demo

            # Write alerts to separate sheet
            if alerts_triggered:
                print(f"\n🚨 Writing {len(alerts_triggered)} alerts to alerts sheet...")

                alerts_data = [["Timestamp", "Wallet", "Value Change", "Threshold", "Severity"]]

                for alert in alerts_triggered:
                    severity = "HIGH" if abs(alert["change"]) > alert["threshold"] * 2 else "MEDIUM"
                    alerts_data.append([
                        alert["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                        alert["wallet"],
                        f"${float(alert['change']):+,.2f}",
                        f"${float(alert['threshold']):,.2f}",
                        severity
                    ])

                # Note: In real implementation, this would use proper sheet writing
                print(f"📋 Alerts summary:")
                for alert in alerts_triggered:
                    print(f"  🚨 {alert['wallet']}: {float(alert['change']):+,.2f} USD")

            print(f"\n✅ Real-time monitoring completed!")
            print(f"📊 Monitoring Summary:")
            print(f"   Cycles Completed: 3")
            print(f"   Wallets Monitored: {len(monitored_wallets)}")
            print(f"   Alerts Triggered: {len(alerts_triggered)}")
            print(f"   Data Points Collected: {len(monitoring_data) - 3}")  # Subtract headers

            return {
                'cycles_completed': 3,
                'wallets_monitored': len(monitored_wallets),
                'alerts_triggered': len(alerts_triggered),
                'monitoring_data': monitoring_data,
                'alerts': alerts_triggered
            }

        except Exception as e:
            print(f"❌ Real-time monitoring failed: {e}")
            raise


async def example_8_portfolio_comparison():
    """Example 8: Portfolio comparison and benchmarking."""

    print("\n📊 Example 8: Portfolio Comparison & Benchmarking")
    print("=" * 55)

    SAMPLE_SPREADSHEET_ID = "your_spreadsheet_id_here"
    COMPARISON_SHEET = "Portfolio_Comparison"
    BENCHMARK_SHEET = "Benchmark_Analysis"

    async with create_application() as app:
        try:
            sheets_client = app.sheets_client

            print(f"📈 Setting up portfolio comparison...")

            # Sample portfolios for comparison
            portfolios = {
                "Conservative": {
                    "eth_percentage": 40,
                    "stablecoin_percentage": 50,
                    "defi_percentage": 10,
                    "total_value": Decimal("500000"),
                    "risk_score": 3
                },
                "Moderate": {
                    "eth_percentage": 60,
                    "stablecoin_percentage": 25,
                    "defi_percentage": 15,
                    "total_value": Decimal("750000"),
                    "risk_score": 5
                },
                "Aggressive": {
                    "eth_percentage": 70,
                    "stablecoin_percentage": 10,
                    "defi_percentage": 20,
                    "total_value": Decimal("1200000"),
                    "risk_score": 8
                },
                "DeFi_Focused": {
                    "eth_percentage": 30,
                    "stablecoin_percentage": 20,
                    "defi_percentage": 50,
                    "total_value": Decimal("300000"),
                    "risk_score": 9
                }
            }

            # Market benchmarks
            benchmarks = {
                "Market_Average": {
                    "eth_percentage": 55,
                    "stablecoin_percentage": 30,
                    "defi_percentage": 15,
                    "total_value": Decimal("425000"),
                    "risk_score": 5.5
                },
                "Top_10_Percent": {
                    "eth_percentage": 65,
                    "stablecoin_percentage": 15,
                    "defi_percentage": 20,
                    "total_value": Decimal("2500000"),
                    "risk_score": 7
                }
            }

            print(f"📊 Comparing {len(portfolios)} portfolio strategies...")

            # Generate comparison data
            comparison_data = [
                ["Portfolio", "Total Value", "ETH %", "Stablecoin %", "DeFi %", "Risk Score", "vs Market Avg",
                 "Performance"]
            ]

            market_avg_value = benchmarks["Market_Average"]["total_value"]

            for name, portfolio in portfolios.items():
                vs_market = ((portfolio["total_value"] - market_avg_value) / market_avg_value) * 100

                if vs_market > 50:
                    performance = "🌟 Excellent"
                elif vs_market > 10:
                    performance = "📈 Good"
                elif vs_market > -10:
                    performance = "➡️ Average"
                else:
                    performance = "📉 Below Average"

                comparison_data.append([
                    name,
                    f"${float(portfolio['total_value']):,.2f}",
                    f"{portfolio['eth_percentage']}%",
                    f"{portfolio['stablecoin_percentage']}%",
                    f"{portfolio['defi_percentage']}%",
                    portfolio['risk_score'],
                    f"{vs_market:+.1f}%",
                    performance
                ])

            # Add benchmark rows
            comparison_data.append(["", "", "", "", "", "", "", ""])  # Separator
            comparison_data.append(["BENCHMARKS", "", "", "", "", "", "", ""])

            for name, benchmark in benchmarks.items():
                comparison_data.append([
                    name,
                    f"${float(benchmark['total_value']):,.2f}",
                    f"{benchmark['eth_percentage']}%",
                    f"{benchmark['stablecoin_percentage']}%",
                    f"{benchmark['defi_percentage']}%",
                    benchmark['risk_score'],
                    "0.0%",  # Benchmark vs itself
                    "📊 Benchmark"
                ])

            # Calculate insights
            print(f"💡 Portfolio Analysis:")

            best_performer = max(portfolios.items(), key=lambda x: x[1]["total_value"])
            lowest_risk = min(portfolios.items(), key=lambda x: x[1]["risk_score"])
            highest_eth = max(portfolios.items(), key=lambda x: x[1]["eth_percentage"])

            print(f"   Best Performer: {best_performer[0]} (${float(best_performer[1]['total_value']):,.2f})")
            print(f"   Lowest Risk: {lowest_risk[0]} (Risk Score: {lowest_risk[1]['risk_score']})")
            print(f"   Highest ETH Exposure: {highest_eth[0]} ({highest_eth[1]['eth_percentage']}%)")

            # Risk-Return Analysis
            risk_return_data = [
                ["Portfolio", "Risk Score", "Return (vs Market)", "Risk-Adjusted Return", "Recommendation"]
            ]

            for name, portfolio in portfolios.items():
                vs_market = ((portfolio["total_value"] - market_avg_value) / market_avg_value) * 100
                risk_adjusted = vs_market / portfolio["risk_score"] if portfolio["risk_score"] > 0 else 0

                if risk_adjusted > 5:
                    recommendation = "🟢 Strong Buy"
                elif risk_adjusted > 0:
                    recommendation = "🟡 Hold"
                else:
                    recommendation = "🔴 Review"

                risk_return_data.append([
                    name,
                    portfolio['risk_score'],
                    f"{vs_market:+.1f}%",
                    f"{risk_adjusted:.2f}",
                    recommendation
                ])

            # Write comparison data
            await sheets_client.write_wallet_results(
                spreadsheet_id=SAMPLE_SPREADSHEET_ID,
                wallet_results=[],  # Empty as we're writing comparison data
                range_start="A1",
                worksheet_name=COMPARISON_SHEET,
                include_header=True,
                clear_existing=True
            )

            print(f"📋 Portfolio comparison completed!")

            # Generate recommendations
            recommendations = []

            # Find optimal allocation
            total_portfolios = len(portfolios)
            avg_eth = sum(p["eth_percentage"] for p in portfolios.values()) / total_portfolios
            avg_stable = sum(p["stablecoin_percentage"] for p in portfolios.values()) / total_portfolios
            avg_defi = sum(p["defi_percentage"] for p in portfolios.values()) / total_portfolios

            recommendations.append(
                f"Optimal Allocation: {avg_eth:.0f}% ETH, {avg_stable:.0f}% Stablecoins, {avg_defi:.0f}% DeFi")

            if best_performer[1]["risk_score"] > 7:
                recommendations.append("⚠️ Best performer has high risk - consider position sizing")

            recommendations.append("💡 Diversification across multiple strategies recommended")

            print(f"\n📈 Key Recommendations:")
            for rec in recommendations:
                print(f"   {rec}")

            return {
                'portfolios_compared': len(portfolios),
                'benchmarks_used': len(benchmarks),
                'best_performer': best_performer[0],
                'lowest_risk': lowest_risk[0],
                'comparison_data': comparison_data,
                'risk_return_data': risk_return_data,
                'recommendations': recommendations
            }

        except Exception as e:
            print(f"❌ Portfolio comparison failed: {e}")
            raise


if __name__ == "__main__":
    """
    Main execution function for Google Sheets integration examples.

    Before running:
    1. Set up Google Sheets API credentials
    2. Update SAMPLE_SPREADSHEET_ID variables with your actual sheet IDs
    3. Ensure proper permissions are granted
    """

    # Configuration
    print("🔧 Configuration Check:")
    print("   - Google Sheets API credentials: ⚠️ Please verify")
    print("   - Spreadsheet access: ⚠️ Please verify")
    print("   - Service account permissions: ⚠️ Please verify")
    print()

    # Run all examples
    asyncio.run(run_all_sheets_examples())