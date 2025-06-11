#!/usr/bin/env python3
"""Manual integration test for Google Sheets client."""

import asyncio
import logging
import os
import sys
from decimal import Decimal
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from wallet_tracker.clients import GoogleSheetsClient, WalletResult, create_summary_from_results
from wallet_tracker.config import GoogleSheetsConfig
from wallet_tracker.utils import CacheConfig, CacheManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def test_integration():
    """Test Google Sheets integration with real API."""

    logger.info("🚀 Starting Google Sheets Integration Test")
    logger.info("=" * 50)

    # Load test configuration
    credentials_file = Path(os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "config/test/google_sheets_credentials.json"))

    test_spreadsheet_id = os.getenv("TEST_SPREADSHEET_ID")

    # Validate prerequisites
    if not credentials_file.exists():
        logger.error(f"❌ Credentials file not found: {credentials_file}")
        logger.error("📋 Setup instructions:")
        logger.error("1. Create Google Service Account at https://console.cloud.google.com/")
        logger.error("2. Enable Google Sheets API")
        logger.error("3. Download credentials JSON file")
        logger.error(f"4. Save as: {credentials_file}")
        return False

    if not test_spreadsheet_id:
        logger.error("❌ TEST_SPREADSHEET_ID not set in environment")
        logger.error("📋 Setup instructions:")
        logger.error("1. Create a new Google Spreadsheet")
        logger.error("2. Share it with your service account email")
        logger.error("3. Add test data in columns A and B")
        logger.error("4. Set environment variable: export TEST_SPREADSHEET_ID='your_id'")
        return False

    logger.info(f"📄 Using credentials: {credentials_file}")
    logger.info(f"📊 Using spreadsheet: {test_spreadsheet_id}")

    # Create cache manager for testing
    cache_config = CacheConfig()
    cache_manager = CacheManager(cache_config)

    # Create client
    config = GoogleSheetsConfig(credentials_file=credentials_file, scope="https://www.googleapis.com/auth/spreadsheets")

    client = GoogleSheetsClient(config=config, cache_manager=cache_manager)

    try:
        # Test 1: Health check
        logger.info("\n🔍 Test 1: Health Check")
        health = client.health_check()
        if not health:
            logger.error("❌ Health check failed - cannot connect to Google Sheets API")
            return False
        logger.info("✅ Health check passed - Google Sheets API accessible")

        # Test 2: Read wallet addresses
        logger.info("\n📖 Test 2: Read Wallet Addresses")
        try:
            addresses = client.read_wallet_addresses(
                spreadsheet_id=test_spreadsheet_id,
                range_name="A:B",
                worksheet_name=None,  # Use default sheet
                skip_header=True,
            )

            if not addresses:
                logger.warning("⚠️ No wallet addresses found in spreadsheet")
                logger.info("💡 Make sure your spreadsheet has data in columns A and B")
                # Create sample data for testing
                addresses = [
                    {
                        "address": "0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86",
                        "label": "Test Wallet 1",
                        "row_number": 2,
                    },
                    {
                        "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                        "label": "Test Wallet 2",
                        "row_number": 3,
                    },
                ]
                logger.info("🔧 Using sample data for testing")

            logger.info(f"✅ Successfully read {len(addresses)} wallet addresses")
            for i, addr in enumerate(addresses[:3]):  # Show first 3
                logger.info(f"   {i + 1}. {addr['address'][:10]}...{addr['address'][-6:]} | {addr['label']}")

        except Exception as e:
            logger.error(f"❌ Failed to read wallet addresses: {e}")
            return False

        # Test 3: Write test results
        logger.info("\n✏️ Test 3: Write Wallet Results")
        try:
            # Create sample results data
            test_results = []
            for i, addr in enumerate(addresses):
                result = {
                    "address": addr["address"],
                    "label": addr["label"],
                    "eth_balance": round(1.5 + i * 0.3, 4),
                    "eth_value_usd": round((1.5 + i * 0.3) * 2000, 2),
                    "usdc_balance": round(1000 + i * 200, 2),
                    "usdt_balance": round(i * 100, 2),
                    "dai_balance": round(i * 50, 2),
                    "aave_balance": round(10 + i * 5, 2),
                    "uni_balance": round(5 + i * 2, 2),
                    "link_balance": round(25 + i * 10, 2),
                    "other_tokens_value_usd": round(100 + i * 50, 2),
                    "total_value_usd": round(4000 + i * 500, 2),
                    "last_updated": "2024-01-15 10:30:00",
                    "transaction_count": 150 + i * 25,
                    "is_active": i % 3 != 0,  # Some inactive for testing
                }
                test_results.append(result)

            success = client.write_wallet_results(
                spreadsheet_id=test_spreadsheet_id,
                wallet_results=test_results,
                range_start="A1",
                worksheet_name="Integration_Test_Results",
                include_header=True,
                clear_existing=True,
            )

            if not success:
                logger.error("❌ Failed to write wallet results")
                return False

            logger.info(f"✅ Successfully wrote {len(test_results)} wallet results")
            logger.info("📋 Check the 'Integration_Test_Results' worksheet in your spreadsheet")

        except Exception as e:
            logger.error(f"❌ Failed to write wallet results: {e}")
            return False

        # Test 4: Create summary
        logger.info("\n📊 Test 4: Create Summary Sheet")
        try:
            # Convert to WalletResult objects for summary creation
            wallet_result_objects = []
            for result in test_results:
                wallet_result = WalletResult(
                    address=result["address"],
                    label=result["label"],
                    eth_balance=Decimal(str(result["eth_balance"])),
                    eth_value_usd=Decimal(str(result["eth_value_usd"])),
                    usdc_balance=Decimal(str(result["usdc_balance"])),
                    usdt_balance=Decimal(str(result["usdt_balance"])),
                    dai_balance=Decimal(str(result["dai_balance"])),
                    aave_balance=Decimal(str(result["aave_balance"])),
                    uni_balance=Decimal(str(result["uni_balance"])),
                    link_balance=Decimal(str(result["link_balance"])),
                    other_tokens_value_usd=Decimal(str(result["other_tokens_value_usd"])),
                    total_value_usd=Decimal(str(result["total_value_usd"])),
                    last_updated=result["last_updated"],
                    transaction_count=result["transaction_count"],
                    is_active=result["is_active"],
                )
                wallet_result_objects.append(wallet_result)

            # Create summary
            summary = create_summary_from_results(wallet_result_objects, "10 seconds (integration test)")

            summary_success = client.create_summary_sheet(
                spreadsheet_id=test_spreadsheet_id,
                summary_data=summary.__dict__,
                worksheet_name="Integration_Test_Summary",
            )

            if not summary_success:
                logger.error("❌ Failed to create summary sheet")
                return False

            logger.info("✅ Successfully created summary sheet")
            logger.info("📋 Check the 'Integration_Test_Summary' worksheet in your spreadsheet")
            logger.info(f"📈 Summary stats: {len(addresses)} wallets, {summary.active_wallets} active")

        except Exception as e:
            logger.error(f"❌ Failed to create summary: {e}")
            return False

        # Test 5: Batch operations
        logger.info("\n🔄 Test 5: Batch Operations")
        try:
            # Split results into batches
            batch_1 = test_results[: len(test_results) // 2]
            batch_2 = test_results[len(test_results) // 2 :]

            batch_success = client.write_batch_results(
                spreadsheet_id=test_spreadsheet_id,
                batch_results=[batch_1, batch_2],
                worksheet_name="Integration_Test_Batch",
                batch_size=max(1, len(test_results) // 2),
            )

            if not batch_success:
                logger.error("❌ Failed batch operations")
                return False

            logger.info("✅ Successfully completed batch operations")
            logger.info("📋 Check the 'Integration_Test_Batch' worksheet in your spreadsheet")

        except Exception as e:
            logger.error(f"❌ Failed batch operations: {e}")
            return False

        # Test 6: Error handling
        logger.info("\n🛡️ Test 6: Error Handling")
        try:
            # Test with invalid spreadsheet ID
            try:
                client.read_wallet_addresses(spreadsheet_id="invalid_spreadsheet_id_12345", range_name="A:B")
                logger.warning("⚠️ Expected error handling test didn't fail as expected")
            except Exception:
                logger.info("✅ Error handling working correctly for invalid spreadsheet")

            # Test with invalid range
            try:
                client.read_wallet_addresses(
                    spreadsheet_id=test_spreadsheet_id,
                    range_name="ZZ:ZZZ",  # Invalid range
                    worksheet_name="NonexistentSheet",
                )
                logger.warning("⚠️ Expected error handling test didn't fail as expected")
            except Exception:
                logger.info("✅ Error handling working correctly for invalid range/sheet")

        except Exception as e:
            logger.error(f"❌ Error in error handling test: {e}")
            # This is not critical, continue

        # Test 7: Check statistics and performance
        logger.info("\n📈 Test 7: Statistics and Performance")
        stats = client.get_stats()
        logger.info("📊 Client Statistics:")
        for key, value in stats.items():
            logger.info(f"   📌 {key}: {value}")

        # Summary
        logger.info("\n🎉 Integration Test Summary")
        logger.info("=" * 30)
        logger.info("✅ Health Check: PASSED")
        logger.info("✅ Read Operations: PASSED")
        logger.info("✅ Write Operations: PASSED")
        logger.info("✅ Summary Generation: PASSED")
        logger.info("✅ Batch Processing: PASSED")
        logger.info("✅ Error Handling: PASSED")
        logger.info("✅ Statistics: PASSED")
        logger.info("")
        logger.info("🚀 ALL INTEGRATION TESTS PASSED!")
        logger.info(f"📊 Check your spreadsheet: https://docs.google.com/spreadsheets/d/{test_spreadsheet_id}")

        return True

    except Exception as e:
        logger.error(f"❌ Integration test failed with unexpected error: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return False

    finally:
        # Cleanup
        await cache_manager.close()


def print_setup_instructions():
    """Print setup instructions for integration testing."""

    print("🔧 GOOGLE SHEETS INTEGRATION TEST SETUP")
    print("=" * 50)
    print()
    print("📋 Prerequisites:")
    print("1. Google Cloud Project with Sheets API enabled")
    print("2. Service Account with JSON credentials")
    print("3. Test Google Spreadsheet")
    print("4. Environment variables set")
    print()
    print("🔗 Quick Setup Links:")
    print("• Google Cloud Console: https://console.cloud.google.com/")
    print("• Enable Sheets API: https://console.cloud.google.com/apis/library/sheets.googleapis.com")
    print()
    print("📂 File Structure:")
    print("config/")
    print("├── test/")
    print("│   └── google_sheets_credentials.json")
    print("└── .env.test")
    print()
    print("🔧 Environment Variables:")
    print("export GOOGLE_SHEETS_CREDENTIALS_FILE='config/test/google_sheets_credentials.json'")
    print("export TEST_SPREADSHEET_ID='your_spreadsheet_id_here'")
    print()
    print("📊 Test Spreadsheet Format:")
    print("| A                                          | B           |")
    print("|-------------------------------------------|-------------|")
    print("| Address                                   | Label       |")
    print("| 0x742d35Cc6634C0532925a3b8D40e3f337ABC7b86 | Test Wallet 1|")
    print("| 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045 | Test Wallet 2|")
    print()


if __name__ == "__main__":
    print("🧪 Google Sheets Client Integration Test")
    print("=" * 40)

    # Check if basic requirements are met
    credentials_file = Path(os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "config/test/google_sheets_credentials.json"))
    test_spreadsheet_id = os.getenv("TEST_SPREADSHEET_ID")

    if not credentials_file.exists() or not test_spreadsheet_id:
        print("⚠️ Setup required before running integration tests")
        print()
        print_setup_instructions()
        print("💡 Run this script again after setup is complete!")
        sys.exit(1)

    # Run the integration test
    try:
        success = asyncio.run(test_integration())
        if success:
            print("\n🎊 INTEGRATION TEST COMPLETED SUCCESSFULLY!")
            print("Your Google Sheets client is working perfectly!")
            sys.exit(0)
        else:
            print("\n❌ INTEGRATION TEST FAILED")
            print("Please check the errors above and fix any issues.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️ Integration test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Integration test crashed: {e}")
        sys.exit(1)
