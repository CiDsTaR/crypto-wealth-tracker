"""Advanced batch processing examples for the Crypto Wealth Tracker.

This file demonstrates:
- Large-scale batch processing patterns
- Custom configuration and optimization
- Advanced error handling and recovery
- Performance monitoring and tuning
- Priority-based processing
- Resource management
"""

import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from wallet_tracker.app import create_application
from wallet_tracker.processors.batch_types import BatchConfig, BatchProgress, QueuePriority, estimate_batch_resources
from wallet_tracker.processors.wallet_types import (
    ProcessingPriority,
    create_jobs_from_addresses,
    group_jobs_by_priority,
)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def example_1_large_scale_batch():
    """Example 1: Large-scale batch processing with optimization."""

    print("🚀 Example 1: Large-Scale Batch Processing")
    print("=" * 50)

    # Generate a large list of wallet addresses for testing
    # In practice, these would come from your data source
    test_addresses = []

    # Add some real addresses and generate test addresses
    real_addresses = [
        "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",  # vitalik.eth
        "0x742d35Cc6634C0532925a3b8D40e4f337F42090B",  # Example wallet
        "0x8ba1f109551bD432803012645Hac136c73F825e01",  # Another example
    ]

    # Add real addresses
    for i, addr in enumerate(real_addresses):
        test_addresses.append({"address": addr, "label": f"Real Wallet {i + 1}", "row_number": i + 1})

    # Generate some test addresses (these may not exist, but demonstrate the pattern)
    for i in range(47):  # Total 50 addresses
        fake_addr = f"0x{''.join([f'{j:02x}' for j in range(20)])}"  # Generate fake address
        test_addresses.append({"address": fake_addr, "label": f"Test Wallet {i + 4}", "row_number": i + 4})

    print(f"📊 Processing {len(test_addresses)} wallets in optimized batches")

    async with create_application() as app:
        try:
            # Estimate resource requirements
            estimated_resources = estimate_batch_resources(
                wallet_count=len(test_addresses), include_prices=True, enable_cache=True
            )

            print("📋 Estimated Resources:")
            print(f"  Memory: {estimated_resources.max_memory_mb}MB")
            print(f"  Processing Time: {estimated_resources.max_processing_time_minutes}min")
            print(f"  API Calls/min: {estimated_resources.max_api_calls_per_minute}")

            # Custom batch configuration for large-scale processing
            custom_config = BatchConfig(
                batch_size=20,  # Smaller batches for better error isolation
                max_concurrent_jobs_per_batch=5,  # Conservative concurrency
                request_delay_seconds=0.2,  # Longer delays to respect rate limits
                timeout_seconds=120,  # Longer timeout for complex wallets
                retry_failed_jobs=True,
                max_retries=2,
                use_cache=True,
                skip_inactive_wallets=True,
                inactive_threshold_days=180,  # 6 months
                min_value_threshold_usd=Decimal("10.0"),  # Skip very low value wallets
            )

            print("⚙️ Custom Batch Configuration:")
            print(f"  Batch Size: {custom_config.batch_size}")
            print(f"  Max Concurrent: {custom_config.max_concurrent_jobs_per_batch}")
            print(f"  Request Delay: {custom_config.request_delay_seconds}s")
            print(f"  Timeout: {custom_config.timeout_seconds}s")

            # Progress tracking
            progress_updates = []

            def progress_callback(progress: BatchProgress):
                """Track progress updates."""
                progress_updates.append(
                    {
                        "timestamp": datetime.now(UTC),
                        "completed": progress.jobs_completed,
                        "failed": progress.jobs_failed,
                        "skipped": progress.jobs_skipped,
                        "total_value": float(progress.total_value_processed),
                        "percentage": progress.get_progress_percentage(),
                    }
                )

                if len(progress_updates) % 10 == 0 or progress.get_progress_percentage() >= 100:
                    print(
                        f"📈 Progress: {progress.get_progress_percentage():.1f}% "
                        f"({progress.jobs_completed}✅ {progress.jobs_failed}❌ {progress.jobs_skipped}⏭️) "
                        f"${progress.total_value_processed:,.0f}"
                    )

            # Execute batch processing
            start_time = time.time()

            results = await app.batch_processor.process_wallet_list(
                addresses=test_addresses, config_override=custom_config, progress_callback=progress_callback
            )

            end_time = time.time()
            total_duration = end_time - start_time

            # Analysis of results
            print("\n📊 Large-Scale Processing Results:")
            print(f"  Total Duration: {total_duration:.1f}s ({total_duration / 60:.1f}min)")
            print(f"  Wallets Processed: {results.wallets_processed}")
            print(f"  Processing Rate: {results.wallets_processed / total_duration:.1f} wallets/second")
            print(f"  Total Value: ${results.total_portfolio_value:,.2f}")
            print(f"  Cache Hit Rate: {results.cache_hit_rate:.1f}%")
            print(f"  API Calls: {results.api_calls_total}")

            # Save detailed progress data
            progress_file = Path("large_batch_progress.json")
            with open(progress_file, "w") as f:
                json.dump(
                    [{**update, "timestamp": update["timestamp"].isoformat()} for update in progress_updates],
                    f,
                    indent=2,
                )

            print(f"💾 Progress data saved to: {progress_file}")

            return results

        except Exception as e:
            print(f"❌ Large-scale batch processing failed: {e}")
            raise


async def example_2_priority_based_processing():
    """Example 2: Priority-based processing with different wallet classes."""

    print("\n🎯 Example 2: Priority-Based Processing")
    print("=" * 45)

    # Create wallets with different priorities
    wallet_data = [
        # High priority - known high-value wallets
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Vitalik", "priority": "HIGH"},
        # Normal priority - regular wallets
        {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "Regular 1", "priority": "NORMAL"},
        {"address": "0x8ba1f109551bD432803012645Hac136c73F825e01", "label": "Regular 2", "priority": "NORMAL"},
        # Low priority - test or less important wallets
        {"address": "0x123456789abcdef123456789abcdef1234567890", "label": "Test 1", "priority": "LOW"},
        {"address": "0xabcdef123456789abcdef123456789abcdef12345", "label": "Test 2", "priority": "LOW"},
    ]

    async with create_application() as app:
        try:
            # Convert to processing format with priorities
            addresses = []
            for i, wallet in enumerate(wallet_data):
                addresses.append({"address": wallet["address"], "label": wallet["label"], "row_number": i + 1})

            # Create jobs with priorities
            jobs = create_jobs_from_addresses(addresses)

            # Assign priorities based on wallet data
            priority_map = {
                "HIGH": ProcessingPriority.HIGH,
                "NORMAL": ProcessingPriority.NORMAL,
                "LOW": ProcessingPriority.LOW,
            }

            for job, wallet in zip(jobs, wallet_data, strict=False):
                job.priority = priority_map.get(wallet.get("priority", "NORMAL"), ProcessingPriority.NORMAL)

            # Group jobs by priority
            priority_groups = group_jobs_by_priority(jobs)

            print("📊 Priority Distribution:")
            for priority, job_list in priority_groups.items():
                print(f"  {priority.value}: {len(job_list)} wallets")

            # Process each priority group separately
            all_results = []
            total_start_time = time.time()

            for priority in [ProcessingPriority.HIGH, ProcessingPriority.NORMAL, ProcessingPriority.LOW]:
                if priority not in priority_groups:
                    continue

                priority_jobs = priority_groups[priority]
                print(f"\n🎯 Processing {priority.value} priority wallets ({len(priority_jobs)} wallets)")

                # Convert jobs back to address format for processing
                priority_addresses = []
                for job in priority_jobs:
                    priority_addresses.append(
                        {"address": job.address, "label": job.label, "row_number": job.row_number}
                    )

                # Custom configuration based on priority
                if priority == ProcessingPriority.HIGH:
                    config = BatchConfig(
                        batch_size=5,  # Smaller batches for high priority
                        max_concurrent_jobs_per_batch=2,  # Lower concurrency for better reliability
                        request_delay_seconds=0.1,  # Faster processing
                        timeout_seconds=180,  # Longer timeout
                        max_retries=3,  # More retries
                        use_cache=True,
                        skip_inactive_wallets=False,  # Don't skip any high priority wallets
                    )
                elif priority == ProcessingPriority.NORMAL:
                    config = BatchConfig(
                        batch_size=10,
                        max_concurrent_jobs_per_batch=3,
                        request_delay_seconds=0.2,
                        timeout_seconds=120,
                        max_retries=2,
                        use_cache=True,
                        skip_inactive_wallets=True,
                        inactive_threshold_days=90,
                    )
                else:  # LOW priority
                    config = BatchConfig(
                        batch_size=20,  # Larger batches for efficiency
                        max_concurrent_jobs_per_batch=5,  # Higher concurrency
                        request_delay_seconds=0.5,  # Slower processing
                        timeout_seconds=60,  # Shorter timeout
                        max_retries=1,  # Fewer retries
                        use_cache=True,
                        skip_inactive_wallets=True,
                        inactive_threshold_days=365,  # Skip very old wallets
                        min_value_threshold_usd=Decimal("1.0"),  # Skip low value wallets
                    )

                # Process this priority group
                start_time = time.time()

                results = await app.batch_processor.process_wallet_list(
                    addresses=priority_addresses, config_override=config
                )

                end_time = time.time()
                duration = end_time - start_time

                print(f"  ⏱️  {priority.value} completed in {duration:.1f}s")
                print(f"  📊 Processed: {results.wallets_processed}")
                print(f"  💰 Total Value: ${results.total_portfolio_value:,.2f}")

                all_results.append({"priority": priority.value, "results": results, "duration": duration})

            total_duration = time.time() - total_start_time

            # Summary of priority-based processing
            print("\n📈 Priority Processing Summary:")
            print(f"  Total Duration: {total_duration:.1f}s")

            total_processed = sum(r["results"].wallets_processed for r in all_results)
            total_value = sum(float(r["results"].total_portfolio_value) for r in all_results)

            print(f"  Total Wallets Processed: {total_processed}")
            print(f"  Total Portfolio Value: ${total_value:,.2f}")

            for result in all_results:
                priority = result["priority"]
                processed = result["results"].wallets_processed
                value = float(result["results"].total_portfolio_value)
                duration = result["duration"]

                print(f"  {priority}: {processed} wallets, ${value:,.2f}, {duration:.1f}s")

            return all_results

        except Exception as e:
            print(f"❌ Priority-based processing failed: {e}")
            raise


async def example_3_custom_configuration_tuning():
    """Example 3: Custom configuration and performance tuning."""

    print("\n⚙️ Example 3: Custom Configuration & Performance Tuning")
    print("=" * 60)

    # Test different configurations to find optimal settings
    test_configurations = [
        {
            "name": "Conservative",
            "config": BatchConfig(
                batch_size=5,
                max_concurrent_jobs_per_batch=2,
                request_delay_seconds=0.5,
                timeout_seconds=180,
                max_retries=3,
            ),
        },
        {
            "name": "Balanced",
            "config": BatchConfig(
                batch_size=10,
                max_concurrent_jobs_per_batch=3,
                request_delay_seconds=0.2,
                timeout_seconds=120,
                max_retries=2,
            ),
        },
        {
            "name": "Aggressive",
            "config": BatchConfig(
                batch_size=20,
                max_concurrent_jobs_per_batch=5,
                request_delay_seconds=0.1,
                timeout_seconds=60,
                max_retries=1,
            ),
        },
    ]

    # Test addresses for configuration testing
    test_addresses = [
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Test 1", "row_number": 1},
        {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "Test 2", "row_number": 2},
        {"address": "0x8ba1f109551bD432803012645Hac136c73F825e01", "label": "Test 3", "row_number": 3},
    ]

    configuration_results = []

    async with create_application() as app:
        try:
            for config_test in test_configurations:
                print(f"\n🧪 Testing {config_test['name']} Configuration:")

                config = config_test["config"]
                print(f"  Batch Size: {config.batch_size}")
                print(f"  Max Concurrent: {config.max_concurrent_jobs_per_batch}")
                print(f"  Request Delay: {config.request_delay_seconds}s")
                print(f"  Timeout: {config.timeout_seconds}s")

                # Test this configuration
                start_time = time.time()

                try:
                    results = await app.batch_processor.process_wallet_list(
                        addresses=test_addresses, config_override=config
                    )

                    end_time = time.time()
                    duration = end_time - start_time

                    # Calculate performance metrics
                    throughput = results.wallets_processed / duration if duration > 0 else 0
                    success_rate = (results.wallets_processed / len(test_addresses)) * 100

                    config_result = {
                        "name": config_test["name"],
                        "duration": duration,
                        "throughput": throughput,
                        "success_rate": success_rate,
                        "wallets_processed": results.wallets_processed,
                        "total_value": float(results.total_portfolio_value),
                        "api_calls": results.api_calls_total,
                        "cache_hit_rate": results.cache_hit_rate,
                        "status": "success",
                    }

                    print(f"  ✅ Duration: {duration:.1f}s")
                    print(f"  📊 Throughput: {throughput:.1f} wallets/second")
                    print(f"  🎯 Success Rate: {success_rate:.1f}%")
                    print(f"  💾 Cache Hit Rate: {results.cache_hit_rate:.1f}%")

                except Exception as e:
                    print(f"  ❌ Configuration failed: {e}")
                    config_result = {"name": config_test["name"], "status": "failed", "error": str(e)}

                configuration_results.append(config_result)

                # Small delay between tests
                await asyncio.sleep(2)

            # Analysis of configuration results
            print("\n📊 Configuration Comparison:")
            print(f"{'Config':<12} {'Duration':<10} {'Throughput':<12} {'Success%':<10} {'Cache%':<8}")
            print(f"{'-' * 12} {'-' * 10} {'-' * 12} {'-' * 10} {'-' * 8}")

            successful_configs = [r for r in configuration_results if r.get("status") == "success"]

            for result in successful_configs:
                print(
                    f"{result['name']:<12} "
                    f"{result['duration']:<10.1f} "
                    f"{result['throughput']:<12.1f} "
                    f"{result['success_rate']:<10.1f} "
                    f"{result['cache_hit_rate']:<8.1f}"
                )

            # Recommend best configuration
            if successful_configs:
                # Score based on throughput and success rate
                for result in successful_configs:
                    result["score"] = (result["throughput"] * result["success_rate"]) / 100

                best_config = max(successful_configs, key=lambda x: x["score"])
                print(f"\n🏆 Recommended Configuration: {best_config['name']}")
                print(f"  Score: {best_config['score']:.1f}")
                print("  Best balance of speed and reliability")

            return configuration_results

        except Exception as e:
            print(f"❌ Configuration tuning failed: {e}")
            raise


async def example_4_error_recovery_patterns():
    """Example 4: Advanced error handling and recovery patterns."""

    print("\n🛡️ Example 4: Error Recovery Patterns")
    print("=" * 40)

    # Mix of valid and problematic addresses for testing error handling
    test_addresses = [
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Valid 1", "row_number": 1},
        {"address": "invalid_address", "label": "Invalid", "row_number": 2},
        {"address": "", "label": "Empty", "row_number": 3},
        {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "Valid 2", "row_number": 4},
        {"address": "0x0000000000000000000000000000000000000000", "label": "Zero Address", "row_number": 5},
    ]

    async with create_application() as app:
        try:
            # Configuration with robust error handling
            resilient_config = BatchConfig(
                batch_size=3,  # Small batches to isolate errors
                max_concurrent_jobs_per_batch=2,
                request_delay_seconds=0.3,
                timeout_seconds=90,
                retry_failed_jobs=True,
                max_retries=3,
                use_cache=True,
            )

            print("🛡️ Processing with resilient configuration...")
            print(f"  Max Retries: {resilient_config.max_retries}")

            # Track errors during processing
            error_details = []

            # TODO: Override by progress callback
            def error_callback(error_info: dict[str, Any]):
                """Track error details."""
                error_details.append(
                    {
                        "timestamp": datetime.now(),
                        "address": error_info.get("address"),
                        "error_type": error_info.get("error_type"),
                        "error_message": error_info.get("error_message"),
                        "retry_count": error_info.get("retry_count", 0),
                    }
                )

                print(
                    f"⚠️  Error: {error_info.get('address', 'Unknown')[:10]}... "
                    f"({error_info.get('error_type', 'Unknown')})"
                )

            # Process with error tracking
            start_time = time.time()

            results = await app.batch_processor.process_wallet_list(
                addresses=test_addresses, config_override=resilient_config
            )

            end_time = time.time()
            duration = end_time - start_time

            # Analyze error patterns
            print("\n📊 Error Recovery Results:")
            print(f"  Total Duration: {duration:.1f}s")
            print(f"  Wallets Processed: {results.wallets_processed}")
            print(f"  Wallets Failed: {len(error_details)}")
            print(f"  Success Rate: {(results.wallets_processed / len(test_addresses)) * 100:.1f}%")

            # Error analysis
            if error_details:
                print("\n🔍 Error Analysis:")

                error_types = {}
                for error in error_details:
                    error_type = error.get("error_type", "Unknown")
                    error_types[error_type] = error_types.get(error_type, 0) + 1

                for error_type, count in error_types.items():
                    print(f"  {error_type}: {count} occurrences")

                # Show retry patterns
                retried_errors = [e for e in error_details if e.get("retry_count", 0) > 0]
                if retried_errors:
                    print(f"  Retried Errors: {len(retried_errors)}")
                    avg_retries = sum(e.get("retry_count", 0) for e in retried_errors) / len(retried_errors)
                    print(f"  Average Retries: {avg_retries:.1f}")

            # Recovery recommendations
            print("\n💡 Recovery Recommendations:")
            if len(error_details) == 0:
                print("  ✅ No errors encountered - configuration is working well")
            elif len(error_details) < len(test_addresses) * 0.1:  # Less than 10% errors
                print("  ✅ Low error rate - current configuration is acceptable")
            elif len(error_details) < len(test_addresses) * 0.3:  # Less than 30% errors
                print("  ⚠️  Moderate error rate - consider:")
                print("     - Increasing timeout values")
                print("     - Reducing batch size")
                print("     - Adding more retry attempts")
            else:
                print("  ❌ High error rate - consider:")
                print("     - Checking API key configuration")
                print("     - Reducing concurrency")
                print("     - Implementing exponential backoff")
                print("     - Validating input data quality")

            return {"results": results, "error_details": error_details, "duration": duration}

        except Exception as e:
            print(f"❌ Error recovery testing failed: {e}")
            raise


async def example_5_performance_monitoring():
    """Example 5: Real-time performance monitoring and optimization."""

    print("\n📊 Example 5: Performance Monitoring & Optimization")
    print("=" * 55)

    # Create monitoring data structure
    performance_metrics = {
        "start_time": time.time(),
        "processing_phases": [],
        "resource_usage": [],
        "api_call_patterns": [],
        "error_rates": [],
    }

    test_addresses = [
        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Monitor 1", "row_number": 1},
        {"address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B", "label": "Monitor 2", "row_number": 2},
        {"address": "0x8ba1f109551bD432803012645Hac136c73F825e01", "label": "Monitor 3", "row_number": 3},
    ]

    async with create_application() as app:
        try:
            print("📊 Starting performance monitoring...")

            # Custom monitoring configuration
            monitoring_config = BatchConfig(
                batch_size=2,
                max_concurrent_jobs_per_batch=2,
                request_delay_seconds=0.2,
                timeout_seconds=120,
                use_cache=True,
            )

            # Monitoring callbacks
            def phase_callback(phase: str, metrics: dict[str, Any]):
                """Track processing phases."""
                performance_metrics["processing_phases"].append(
                    {"timestamp": time.time(), "phase": phase, "metrics": metrics.copy()}
                )

                print(f"📈 Phase: {phase} - {metrics.get('description', '')}")

            def resource_callback(resource_info: dict[str, Any]):
                """Track resource usage."""
                performance_metrics["resource_usage"].append(
                    {
                        "timestamp": time.time(),
                        "memory_mb": resource_info.get("memory_mb", 0),
                        "cpu_percent": resource_info.get("cpu_percent", 0),
                        "active_connections": resource_info.get("active_connections", 0),
                    }
                )

                if len(performance_metrics["resource_usage"]) % 5 == 0:
                    print(
                        f"💾 Resources: {resource_info.get('memory_mb', 0):.0f}MB RAM, "
                        f"{resource_info.get('cpu_percent', 0):.1f}% CPU"
                    )

            def api_callback(api_info: dict[str, Any]):
                """Track API call patterns."""
                performance_metrics["api_call_patterns"].append(
                    {
                        "timestamp": time.time(),
                        "service": api_info.get("service"),
                        "endpoint": api_info.get("endpoint"),
                        "response_time_ms": api_info.get("response_time_ms"),
                        "status_code": api_info.get("status_code"),
                        "cache_hit": api_info.get("cache_hit", False),
                    }
                )

                print(
                    f"🔗 API: {api_info.get('service')} - "
                    f"{api_info.get('response_time_ms', 0):.0f}ms "
                    f"{'(cached)' if api_info.get('cache_hit') else ''}"
                )

            # Process with detailed monitoring
            results = await app.batch_processor.process_wallet_list(
                addresses=test_addresses,
                config_override=monitoring_config,
                #phase_callback=phase_callback,
                #resource_callback=resource_callback,
                #api_callback=api_callback,
            )

            performance_metrics["end_time"] = time.time()
            total_duration = performance_metrics["end_time"] - performance_metrics["start_time"]

            # Analyze performance data
            print("\n📊 Performance Analysis:")
            print(f"  Total Duration: {total_duration:.1f}s")
            print(f"  Wallets Processed: {results.wallets_processed}")
            print(f"  Overall Throughput: {results.wallets_processed / total_duration:.1f} wallets/second")

            # Phase analysis
            if performance_metrics["processing_phases"]:
                print("\n⏱️  Processing Phases:")
                phase_times = {}

                for i, phase in enumerate(performance_metrics["processing_phases"]):
                    if i > 0:
                        prev_time = performance_metrics["processing_phases"][i - 1]["timestamp"]
                        phase_duration = phase["timestamp"] - prev_time
                        phase_times[phase["phase"]] = phase_times.get(phase["phase"], 0) + phase_duration

                for phase, duration in phase_times.items():
                    percentage = (duration / total_duration) * 100
                    print(f"  {phase}: {duration:.1f}s ({percentage:.1f}%)")

            # API call analysis
            if performance_metrics["api_call_patterns"]:
                api_calls = performance_metrics["api_call_patterns"]

                print("\n🔗 API Performance:")
                print(f"  Total API Calls: {len(api_calls)}")

                # Group by service
                service_stats = {}
                for call in api_calls:
                    service = call.get("service", "Unknown")
                    if service not in service_stats:
                        service_stats[service] = {"count": 0, "total_time": 0, "cache_hits": 0, "errors": 0}

                    service_stats[service]["count"] += 1
                    service_stats[service]["total_time"] += call.get("response_time_ms", 0)
                    if call.get("cache_hit"):
                        service_stats[service]["cache_hits"] += 1
                    if call.get("status_code", 200) >= 400:
                        service_stats[service]["errors"] += 1

                for service, stats in service_stats.items():
                    avg_time = stats["total_time"] / stats["count"] if stats["count"] > 0 else 0
                    cache_rate = (stats["cache_hits"] / stats["count"]) * 100 if stats["count"] > 0 else 0
                    error_rate = (stats["errors"] / stats["count"]) * 100 if stats["count"] > 0 else 0

                    print(f"  {service}:")
                    print(f"    Calls: {stats['count']}")
                    print(f"    Avg Response: {avg_time:.0f}ms")
                    print(f"    Cache Rate: {cache_rate:.1f}%")
                    print(f"    Error Rate: {error_rate:.1f}%")

            # Performance recommendations
            print("\n💡 Performance Recommendations:")

            # Based on cache hit rate
            if results.cache_hit_rate < 50:
                print(f"  📦 Low cache hit rate ({results.cache_hit_rate:.1f}%) - consider:")
                print("     - Increasing cache TTL")
                print("     - Pre-warming cache for common addresses")
                print("     - Using Redis for distributed caching")

            # Based on API response times
            if performance_metrics["api_call_patterns"]:
                avg_api_time = sum(c.get("response_time_ms", 0) for c in performance_metrics["api_call_patterns"])
                avg_api_time /= len(performance_metrics["api_call_patterns"])

                if avg_api_time > 1000:  # More than 1 second
                    print(f"  🐌 Slow API responses ({avg_api_time:.0f}ms avg) - consider:")
                    print("     - Reducing request delay")
                    print("     - Using faster API endpoints")
                    print("     - Implementing request batching")
                elif avg_api_time < 200:  # Less than 200ms
                    print(f"  🚀 Fast API responses ({avg_api_time:.0f}ms avg) - can optimize:")
                    print("     - Increase concurrency")
                    print("     - Reduce request delays")
                    print("     - Process larger batches")

            # Save performance data
            perf_file = Path("performance_analysis.json")
            with open(perf_file, "w") as f:
                # Convert timestamps to ISO format for JSON serialization
                perf_data = performance_metrics.copy()
                for phase in perf_data["processing_phases"]:
                    phase["timestamp"] = datetime.fromtimestamp(phase["timestamp"]).isoformat()
                for resource in perf_data["resource_usage"]:
                    resource["timestamp"] = datetime.fromtimestamp(resource["timestamp"]).isoformat()
                for api_call in perf_data["api_call_patterns"]:
                    api_call["timestamp"] = datetime.fromtimestamp(api_call["timestamp"]).isoformat()

                json.dump(perf_data, f, indent=2)

            print(f"💾 Performance data saved to: {perf_file}")

            return {"results": results, "performance_metrics": performance_metrics, "duration": total_duration}

        except Exception as e:
            print(f"❌ Performance monitoring failed: {e}")
            raise


async def example_6_resource_optimization():
    """Example 6: Resource optimization and memory management."""

    print("\n🧠 Example 6: Resource Optimization & Memory Management")
    print("=" * 60)

    async with create_application() as app:
        try:
            # Test different resource configurations
            resource_configs = [
                {
                    "name": "Memory Efficient",
                    "config": BatchConfig(
                        batch_size=5,  # Small batches to minimize memory
                        max_concurrent_jobs_per_batch=2,
                        request_delay_seconds=0.3,
                        use_cache=False,  # Disable cache to save memory
                    ),
                },
                {
                    "name": "Speed Optimized",
                    "config": BatchConfig(
                        batch_size=25,  # Large batches for speed
                        max_concurrent_jobs_per_batch=8,
                        request_delay_seconds=0.05,
                        use_cache=True,  # Use cache for speed
                    ),
                },
                {
                    "name": "Balanced",
                    "config": BatchConfig(
                        batch_size=10,
                        max_concurrent_jobs_per_batch=4,
                        request_delay_seconds=0.15,
                        use_cache=True,
                    ),
                },
            ]

            # Test addresses for resource testing
            test_addresses = []
            real_addresses = [
                "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                "0x742d35Cc6634C0532925a3b8D40e4f337F42090B",
                "0x8ba1f109551bD432803012645Hac136c73F825e01",
            ]

            for i, addr in enumerate(real_addresses):
                test_addresses.append({"address": addr, "label": f"Resource Test {i + 1}", "row_number": i + 1})

            resource_results = []

            for config_test in resource_configs:
                print(f"\n🧪 Testing {config_test['name']} Configuration:")

                config = config_test["config"]

                # Monitor resource usage during processing
                resource_usage = []

                def resource_monitor():
                    """Simple resource monitoring."""
                    import psutil

                    process = psutil.Process()

                    resource_usage.append(
                        {
                            "timestamp": time.time(),
                            "memory_mb": process.memory_info().rss / 1024 / 1024,
                            "cpu_percent": process.cpu_percent(),
                        }
                    )

                # Start resource monitoring
                monitoring_task = None
                try:
                    # Begin monitoring
                    async def monitor_resources():
                        while True:
                            resource_monitor()
                            await asyncio.sleep(0.5)

                    monitoring_task = asyncio.create_task(monitor_resources())

                    # Process wallets
                    start_time = time.time()

                    results = await app.batch_processor.process_wallet_list(
                        addresses=test_addresses, config_override=config
                    )

                    end_time = time.time()
                    duration = end_time - start_time

                    # Stop monitoring
                    monitoring_task.cancel()

                    # Analyze resource usage
                    if resource_usage:
                        peak_memory = max(r["memory_mb"] for r in resource_usage)
                        avg_memory = sum(r["memory_mb"] for r in resource_usage) / len(resource_usage)
                        avg_cpu = sum(r["cpu_percent"] for r in resource_usage) / len(resource_usage)
                    else:
                        peak_memory = avg_memory = avg_cpu = 0

                    resource_result = {
                        "name": config_test["name"],
                        "duration": duration,
                        "peak_memory_mb": peak_memory,
                        "avg_memory_mb": avg_memory,
                        "avg_cpu_percent": avg_cpu,
                        "wallets_processed": results.wallets_processed,
                        "cache_hit_rate": results.cache_hit_rate,
                        "api_calls": results.api_calls_total,
                        "memory_efficiency": results.wallets_processed / peak_memory if peak_memory > 0 else 0,
                        "speed_efficiency": results.wallets_processed / duration if duration > 0 else 0,
                    }

                    print(f"  ⏱️  Duration: {duration:.1f}s")
                    print(f"  💾 Peak Memory: {peak_memory:.1f}MB")
                    print(f"  🧠 Avg Memory: {avg_memory:.1f}MB")
                    print(f"  ⚡ Avg CPU: {avg_cpu:.1f}%")
                    print(f"  📊 Memory Efficiency: {resource_result['memory_efficiency']:.2f} wallets/MB")
                    print(f"  🚀 Speed Efficiency: {resource_result['speed_efficiency']:.2f} wallets/second")

                except Exception as e:
                    if monitoring_task:
                        monitoring_task.cancel()

                    resource_result = {"name": config_test["name"], "status": "failed", "error": str(e)}
                    print(f"  ❌ Test failed: {e}")

                resource_results.append(resource_result)

                # Clean up between tests
                await asyncio.sleep(2)

            # Compare resource efficiency
            print("\n📊 Resource Efficiency Comparison:")
            print(f"{'Config':<15} {'Duration':<10} {'Peak RAM':<10} {'Efficiency':<12} {'Speed':<10}")
            print(f"{'-' * 15} {'-' * 10} {'-' * 10} {'-' * 12} {'-' * 10}")

            successful_tests = [r for r in resource_results if "duration" in r]

            for result in successful_tests:
                print(
                    f"{result['name']:<15} "
                    f"{result['duration']:<10.1f} "
                    f"{result['peak_memory_mb']:<10.1f} "
                    f"{result['memory_efficiency']:<12.2f} "
                    f"{result['speed_efficiency']:<10.2f}"
                )

            # Resource optimization recommendations
            print("\n💡 Resource Optimization Recommendations:")

            if successful_tests:
                # Find most memory efficient
                most_memory_efficient = max(successful_tests, key=lambda x: x.get("memory_efficiency", 0))
                fastest = max(successful_tests, key=lambda x: x.get("speed_efficiency", 0))

                print(f"  🧠 Most Memory Efficient: {most_memory_efficient['name']}")
                print(f"     {most_memory_efficient['memory_efficiency']:.2f} wallets/MB")

                print(f"  🚀 Fastest Processing: {fastest['name']}")
                print(f"     {fastest['speed_efficiency']:.2f} wallets/second")

                # General recommendations
                avg_memory = sum(r.get("peak_memory_mb", 0) for r in successful_tests) / len(successful_tests)
                if avg_memory > 200:
                    print("  📦 High memory usage detected - consider:")
                    print("     - Reducing batch sizes")
                    print("     - Enabling garbage collection")
                    print("     - Disabling cache for large datasets")
                elif avg_memory < 50:
                    print("  💚 Low memory usage - can optimize for speed:")
                    print("     - Increase batch sizes")
                    print("     - Enable caching")
                    print("     - Increase concurrency")

            return resource_results

        except Exception as e:
            print(f"❌ Resource optimization testing failed: {e}")
            raise


async def example_7_advanced_queue_management():
    """Example 7: Advanced queue management and job scheduling."""

    print("\n⚡ Example 7: Advanced Queue Management & Job Scheduling")
    print("=" * 65)

    async with create_application() as app:
        try:
            # Create jobs with different characteristics
            job_sets = [
                {
                    "name": "High Value Wallets",
                    "addresses": [
                        {"address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "label": "Vitalik", "row_number": 1}
                    ],
                    "priority": QueuePriority.HIGH,
                    "estimated_complexity": "high",
                },
                {
                    "name": "Regular Wallets",
                    "addresses": [
                        {
                            "address": "0x742d35Cc6634C0532925a3b8D40e4f337F42090B",
                            "label": "Regular 1",
                            "row_number": 2,
                        },
                        {
                            "address": "0x8ba1f109551bD432803012645Hac136c73F825e01",
                            "label": "Regular 2",
                            "row_number": 3,
                        },
                    ],
                    "priority": QueuePriority.NORMAL,
                    "estimated_complexity": "medium",
                },
                {
                    "name": "Background Processing",
                    "addresses": [
                        {
                            "address": "0x123456789abcdef123456789abcdef1234567890",
                            "label": "Background 1",
                            "row_number": 4,
                        },
                        {
                            "address": "0xabcdef123456789abcdef123456789abcdef12345",
                            "label": "Background 2",
                            "row_number": 5,
                        },
                    ],
                    "priority": QueuePriority.LOW,
                    "estimated_complexity": "low",
                },
            ]

            print("📋 Setting up advanced queue management...")

            # Queue statistics
            queue_stats = {
                "jobs_submitted": 0,
                "jobs_completed": 0,
                "jobs_failed": 0,
                "processing_times": [],
                "queue_wait_times": [],
            }

            # Submit jobs to different priority queues
            all_job_results = []

            for job_set in job_sets:
                print(f"\n🎯 Processing {job_set['name']} (Priority: {job_set['priority'].value})")

                # Configure processing based on priority and complexity
                if job_set["priority"] == QueuePriority.HIGH:
                    config = BatchConfig(
                        batch_size=1,  # Process individually for high priority
                        max_concurrent_jobs_per_batch=1,
                        request_delay_seconds=0.05,
                        timeout_seconds=300,  # Longer timeout
                        max_retries=3,
                        use_cache=True,
                    )
                elif job_set["priority"] == QueuePriority.NORMAL:
                    config = BatchConfig(
                        batch_size=5,
                        max_concurrent_jobs_per_batch=3,
                        request_delay_seconds=0.15,
                        timeout_seconds=120,
                        max_retries=2,
                        use_cache=True,
                    )
                else:  # LOW priority
                    config = BatchConfig(
                        batch_size=10,
                        max_concurrent_jobs_per_batch=5,
                        request_delay_seconds=0.5,  # Slower processing
                        timeout_seconds=60,
                        max_retries=1,
                        use_cache=True,
                        skip_inactive_wallets=True,
                        min_value_threshold_usd=Decimal("5.0"),
                    )

                # Track queue timing
                queue_start_time = time.time()

                try:
                    # Process this job set
                    processing_start_time = time.time()

                    results = await app.batch_processor.process_wallet_list(
                        addresses=job_set["addresses"], config_override=config
                    )

                    processing_end_time = time.time()

                    # Calculate timing metrics
                    queue_wait_time = processing_start_time - queue_start_time
                    processing_time = processing_end_time - processing_start_time

                    queue_stats["jobs_submitted"] += len(job_set["addresses"])
                    queue_stats["jobs_completed"] += results.wallets_processed
                    queue_stats["processing_times"].append(processing_time)
                    queue_stats["queue_wait_times"].append(queue_wait_time)

                    job_result = {
                        "name": job_set["name"],
                        "priority": job_set["priority"].value,
                        "addresses_submitted": len(job_set["addresses"]),
                        "wallets_processed": results.wallets_processed,
                        "queue_wait_time": queue_wait_time,
                        "processing_time": processing_time,
                        "total_value": float(results.total_portfolio_value),
                        "cache_hit_rate": results.cache_hit_rate,
                        "status": "completed",
                    }

                    print(f"  ✅ Completed: {results.wallets_processed} wallets")
                    print(f"  ⏳ Queue Wait: {queue_wait_time:.2f}s")
                    print(f"  ⚡ Processing: {processing_time:.2f}s")
                    print(f"  💰 Total Value: ${results.total_portfolio_value:,.2f}")

                except Exception as e:
                    queue_stats["jobs_failed"] += len(job_set["addresses"])

                    job_result = {
                        "name": job_set["name"],
                        "priority": job_set["priority"].value,
                        "status": "failed",
                        "error": str(e),
                    }

                    print(f"  ❌ Failed: {e}")

                all_job_results.append(job_result)

                # Small delay between job sets to simulate realistic queue behavior
                await asyncio.sleep(1)

            # Queue performance analysis
            print("\n📊 Queue Performance Analysis:")
            print(f"  Jobs Submitted: {queue_stats['jobs_submitted']}")
            print(f"  Jobs Completed: {queue_stats['jobs_completed']}")
            print(f"  Jobs Failed: {queue_stats['jobs_failed']}")
            print(f"  Success Rate: {(queue_stats['jobs_completed'] / queue_stats['jobs_submitted']) * 100:.1f}%")

            if queue_stats["processing_times"]:
                avg_processing_time = sum(queue_stats["processing_times"]) / len(queue_stats["processing_times"])
                avg_queue_wait = sum(queue_stats["queue_wait_times"]) / len(queue_stats["queue_wait_times"])

                print(f"  Avg Processing Time: {avg_processing_time:.2f}s")
                print(f"  Avg Queue Wait Time: {avg_queue_wait:.2f}s")
                print(
                    f"  Queue Efficiency: {(avg_processing_time / (avg_processing_time + avg_queue_wait)) * 100:.1f}%"
                )

            # Priority-based analysis
            print("\n🎯 Priority-Based Performance:")
            priority_stats = {}

            for result in all_job_results:
                if result.get("status") == "completed":
                    priority = result["priority"]
                    if priority not in priority_stats:
                        priority_stats[priority] = {
                            "count": 0,
                            "total_processing_time": 0,
                            "total_queue_wait": 0,
                            "total_value": 0,
                        }

                    priority_stats[priority]["count"] += 1
                    priority_stats[priority]["total_processing_time"] += result.get("processing_time", 0)
                    priority_stats[priority]["total_queue_wait"] += result.get("queue_wait_time", 0)
                    priority_stats[priority]["total_value"] += result.get("total_value", 0)

            for priority, stats in priority_stats.items():
                if stats["count"] > 0:
                    avg_proc = stats["total_processing_time"] / stats["count"]
                    avg_wait = stats["total_queue_wait"] / stats["count"]

                    print(f"  {priority}:")
                    print(f"    Avg Processing: {avg_proc:.2f}s")
                    print(f"    Avg Queue Wait: {avg_wait:.2f}s")
                    print(f"    Total Value: ${stats['total_value']:,.2f}")

            # Queue optimization recommendations
            print("\n💡 Queue Optimization Recommendations:")

            if queue_stats["jobs_completed"] > 0:
                success_rate = (queue_stats["jobs_completed"] / queue_stats["jobs_submitted"]) * 100

                if success_rate >= 95:
                    print(f"  ✅ Excellent queue performance ({success_rate:.1f}% success)")
                    print("     - Current configuration is working well")
                    print("     - Consider increasing throughput with higher concurrency")
                elif success_rate >= 80:
                    print(f"  ⚠️  Good queue performance ({success_rate:.1f}% success)")
                    print("     - Minor optimizations recommended")
                    print("     - Review failed job patterns")
                else:
                    print(f"  ❌ Poor queue performance ({success_rate:.1f}% success)")
                    print("     - Reduce batch sizes for better error isolation")
                    print("     - Implement better error handling")
                    print("     - Review timeout configurations")

                if avg_queue_wait > avg_processing_time:
                    print("  ⏳ High queue wait times detected:")
                    print("     - Increase parallel processing capacity")
                    print("     - Implement queue preemption for high priority jobs")
                    print("     - Consider dedicated queues per priority level")

            return {"queue_stats": queue_stats, "job_results": all_job_results, "priority_stats": priority_stats}

        except Exception as e:
            print(f"❌ Advanced queue management failed: {e}")
            raise


async def run_all_advanced_examples():
    """Run all advanced batch processing examples."""

    print("🚀 Crypto Wealth Tracker - Advanced Batch Processing Examples")
    print("=" * 70)
    print("This script demonstrates advanced batch processing patterns, optimization,")
    print("and monitoring techniques for large-scale wallet analysis.\n")

    examples = [
        example_1_large_scale_batch,
        example_2_priority_based_processing,
        example_3_custom_configuration_tuning,
        example_4_error_recovery_patterns,
        example_5_performance_monitoring,
        example_6_resource_optimization,
        example_7_advanced_queue_management,
    ]

    results = {}

    for i, example_func in enumerate(examples, 1):
        try:
            print(f"\n{'=' * 70}")
            print(f"Running Advanced Example {i}: {example_func.__name__}")
            print(f"{'=' * 70}")

            result = await example_func()
            results[example_func.__name__] = {"status": "success", "result": result}

            print(f"✅ Advanced Example {i} completed successfully!")

            # Longer delay between advanced examples
            await asyncio.sleep(3)

        except Exception as e:
            print(f"❌ Advanced Example {i} failed: {e}")
            results[example_func.__name__] = {"status": "failed", "error": str(e)}

            # Continue with other examples even if one fails
            continue

    # Summary
    print(f"\n{'=' * 70}")
    print("📊 ADVANCED EXAMPLES SUMMARY")
    print(f"{'=' * 70}")

    successful = sum(1 for r in results.values() if r["status"] == "success")
    failed = len(results) - successful

    print(f"Total Advanced Examples: {len(results)}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")

    if failed > 0:
        print("\nFailed Examples:")
        for name, result in results.items():
            if result["status"] == "failed":
                print(f"  ❌ {name}: {result['error']}")

    # Performance insights
    if successful > 0:
        print("\n📈 Key Performance Insights:")

        # Extract some insights from successful examples
        successful_results = [r["result"] for r in results.values() if r["status"] == "success"]

        print("  🎯 Successfully demonstrated advanced batch processing patterns")
        print("  ⚡ Tested multiple optimization strategies")
        print("  🛡️  Validated error recovery mechanisms")
        print("  📊 Collected comprehensive performance metrics")
        print("  🧠 Analyzed resource usage patterns")
        print("  ⚙️  Explored configuration tuning strategies")

    print("\n🎉 Advanced batch processing examples completed!")
    print("📚 These examples demonstrate enterprise-level features:")
    print("   - Large-scale processing optimization")
    print("   - Priority-based job scheduling")
    print("   - Performance monitoring and tuning")
    print("   - Resource management strategies")
    print("   - Advanced error handling patterns")
    print("   - Queue management techniques")

    print("\n💡 Next Steps:")
    print("   - Adapt these patterns to your specific use case")
    print("   - Integrate performance monitoring in production")
    print("   - Implement custom priority schemes")
    print("   - Set up automated optimization")

    return results


if __name__ == "__main__":
    # Run the advanced examples
    asyncio.run(run_all_advanced_examples())
