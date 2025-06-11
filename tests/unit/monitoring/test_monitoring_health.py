"""Tests for health checking system."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wallet_tracker.monitoring.health import (
    HealthChecker,
    HealthCheckScheduler,
    HealthStatus,
    ServiceHealth,
    create_health_alert_handler,
    quick_health_check,
)


class TestServiceHealth:
    """Test ServiceHealth class."""

    def test_service_health_creation(self):
        """Test ServiceHealth object creation."""
        health = ServiceHealth(
            service_name="test_service", status=HealthStatus.HEALTHY, response_time=0.5, metadata={"version": "1.0"}
        )

        assert health.service_name == "test_service"
        assert health.status == HealthStatus.HEALTHY
        assert health.response_time == 0.5
        assert health.metadata["version"] == "1.0"
        assert health.is_healthy is True
        assert health.is_degraded is False
        assert health.is_unhealthy is False
        assert health.check_count == 0
        assert health.consecutive_failures == 0

    def test_service_health_status_properties(self):
        """Test status property methods."""
        health = ServiceHealth("test", HealthStatus.DEGRADED)
        assert health.is_degraded is True
        assert health.is_healthy is False
        assert health.is_unhealthy is False

        health.status = HealthStatus.UNHEALTHY
        assert health.is_unhealthy is True
        assert health.is_healthy is False
        assert health.is_degraded is False

    def test_service_health_update_status(self):
        """Test status updates."""
        health = ServiceHealth("test", HealthStatus.UNKNOWN)

        # First update - success
        health.update_status(status=HealthStatus.HEALTHY, response_time=0.3, metadata={"test": "value"})

        assert health.status == HealthStatus.HEALTHY
        assert health.response_time == 0.3
        assert health.check_count == 1
        assert health.consecutive_failures == 0
        assert health.last_healthy_time is not None

        # Second update - failure
        health.update_status(status=HealthStatus.UNHEALTHY, error_message="Service down")

        assert health.status == HealthStatus.UNHEALTHY
        assert health.error_message == "Service down"
        assert health.check_count == 2
        assert health.consecutive_failures == 1

        # Third update - another failure
        health.update_status(status=HealthStatus.UNHEALTHY)
        assert health.consecutive_failures == 2

    def test_service_health_to_dict(self):
        """Test serialization to dictionary."""
        health = ServiceHealth(
            service_name="test_service", status=HealthStatus.HEALTHY, response_time=0.5, metadata={"key": "value"}
        )

        health_dict = health.to_dict()

        assert health_dict["service_name"] == "test_service"
        assert health_dict["status"] == "healthy"
        assert health_dict["is_healthy"] is True
        assert health_dict["response_time_ms"] == 500.0
        assert health_dict["metadata"]["key"] == "value"
        assert "checked_at" in health_dict
        assert health_dict["check_count"] == 0


class TestHealthChecker:
    """Test HealthChecker class."""

    @pytest.fixture
    def mock_ethereum_client(self):
        """Create mock Ethereum client."""
        client = AsyncMock()
        client.get_stats.return_value = {"portfolio_requests": 10, "api_errors": 0, "cache_hits": 5, "rate_limit": 100}
        return client

    @pytest.fixture
    def mock_coingecko_client(self):
        """Create mock CoinGecko client."""
        client = AsyncMock()
        client.health_check = AsyncMock(return_value=True)
        client.get_stats.return_value = {
            "price_requests": 20,
            "has_api_key": True,
            "rate_limit": 30,
            "rate_limit_errors": 0,
        }
        return client

    @pytest.fixture
    def mock_sheets_client(self):
        """Create mock Google Sheets client."""
        client = MagicMock()
        client.health_check.return_value = True
        client.get_stats.return_value = {
            "authenticated": True,
            "read_operations": 5,
            "write_operations": 2,
            "api_errors": 0,
        }
        return client

    @pytest.fixture
    def mock_cache_manager(self):
        """Create mock cache manager."""
        manager = AsyncMock()
        manager.health_check = AsyncMock(return_value={"price_cache": True, "balance_cache": True})
        manager.get_stats = AsyncMock(return_value={"hits": 100, "misses": 20, "backend": "file"})
        return manager

    @pytest.fixture
    def health_checker(self, mock_ethereum_client, mock_coingecko_client, mock_sheets_client, mock_cache_manager):
        """Create HealthChecker instance with mocked dependencies."""
        return HealthChecker(
            ethereum_client=mock_ethereum_client,
            coingecko_client=mock_coingecko_client,
            sheets_client=mock_sheets_client,
            cache_manager=mock_cache_manager,
            timeout_seconds=10.0,
            degraded_threshold_ms=2000.0,
            unhealthy_threshold_ms=5000.0,
        )

    @pytest.mark.asyncio
    async def test_check_all_services_success(self, health_checker):
        """Test successful health check of all services."""
        health_status = await health_checker.check_all_services()

        assert len(health_status) == 4
        assert health_status["ethereum_client"] is True
        assert health_status["coingecko_client"] is True
        assert health_status["sheets_client"] is True
        assert health_status["cache_manager"] is True

    @pytest.mark.asyncio
    async def test_check_all_services_with_failures(self, health_checker, mock_coingecko_client):
        """Test health check with some service failures."""
        # Make CoinGecko client fail
        mock_coingecko_client.health_check.return_value = False

        health_status = await health_checker.check_all_services()

        assert health_status["ethereum_client"] is True
        assert health_status["coingecko_client"] is False
        assert health_status["sheets_client"] is True
        assert health_status["cache_manager"] is True

    @pytest.mark.asyncio
    async def test_check_ethereum_client_healthy(self, health_checker, mock_ethereum_client):
        """Test Ethereum client health check - healthy case."""
        health = await health_checker._check_ethereum_client(detailed=False)

        assert health.service_name == "ethereum_client"
        assert health.status == HealthStatus.HEALTHY
        assert health.response_time is not None
        assert health.error_message is None
        assert "stats" in health.metadata

    @pytest.mark.asyncio
    async def test_check_ethereum_client_high_errors(self, health_checker, mock_ethereum_client):
        """Test Ethereum client health check - high error count."""
        mock_ethereum_client.get_stats.return_value = {
            "portfolio_requests": 10,
            "api_errors": 15,  # High error count
            "cache_hits": 5,
            "rate_limit": 100,
        }

        health = await health_checker._check_ethereum_client()

        assert health.status == HealthStatus.DEGRADED
        assert "High API error count" in health.error_message

    @pytest.mark.asyncio
    async def test_check_ethereum_client_exception(self, health_checker, mock_ethereum_client):
        """Test Ethereum client health check with exception."""
        mock_ethereum_client.get_stats.side_effect = Exception("Connection failed")

        health = await health_checker._check_ethereum_client()

        assert health.status == HealthStatus.UNHEALTHY
        assert "Connection failed" in health.error_message

    @pytest.mark.asyncio
    async def test_check_coingecko_client_healthy(self, health_checker):
        """Test CoinGecko client health check - healthy case."""
        health = await health_checker._check_coingecko_client()

        assert health.service_name == "coingecko_client"
        assert health.status == HealthStatus.HEALTHY
        assert health.response_time is not None

    @pytest.mark.asyncio
    async def test_check_coingecko_client_rate_limited(self, health_checker, mock_coingecko_client):
        """Test CoinGecko client with rate limiting issues."""
        mock_coingecko_client.get_stats.return_value = {
            "price_requests": 20,
            "has_api_key": True,
            "rate_limit": 30,
            "rate_limit_errors": 10,  # High rate limit errors
        }

        health = await health_checker._check_coingecko_client()

        assert health.status == HealthStatus.DEGRADED
        assert "Rate limit issues" in health.error_message

    @pytest.mark.asyncio
    async def test_check_coingecko_client_detailed_timeout(self, health_checker, mock_coingecko_client):
        """Test CoinGecko client detailed check with timeout."""

        # Make the detailed check (get_eth_price) timeout
        async def timeout_func():
            await asyncio.sleep(15)  # Longer than timeout
            return 2000.0

        mock_coingecko_client.get_eth_price = timeout_func

        health = await health_checker._check_coingecko_client(detailed=True)

        assert health.status == HealthStatus.UNHEALTHY
        assert health.error_message == "CoinGecko API health check failed"

    @pytest.mark.asyncio
    async def test_check_sheets_client_healthy(self, health_checker):
        """Test Google Sheets client health check - healthy case."""
        health = await health_checker._check_sheets_client()

        assert health.service_name == "sheets_client"
        assert health.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_check_sheets_client_not_authenticated(self, health_checker, mock_sheets_client):
        """Test Google Sheets client not authenticated."""
        mock_sheets_client.get_stats.return_value = {
            "authenticated": False,
            "read_operations": 0,
            "write_operations": 0,
        }

        health = await health_checker._check_sheets_client()

        assert health.status == HealthStatus.UNHEALTHY
        assert "not authenticated" in health.error_message

    @pytest.mark.asyncio
    async def test_check_cache_manager_healthy(self, health_checker):
        """Test cache manager health check - healthy case."""
        health = await health_checker._check_cache_manager()

        assert health.service_name == "cache_manager"
        assert health.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_check_cache_manager_partial_failure(self, health_checker, mock_cache_manager):
        """Test cache manager with some backends failing."""
        mock_cache_manager.health_check.return_value = {
            "price_cache": True,
            "balance_cache": False,  # One backend failing
        }

        health = await health_checker._check_cache_manager()

        assert health.status == HealthStatus.DEGRADED
        assert "Some cache backends unhealthy" in health.error_message

    @pytest.mark.asyncio
    async def test_check_cache_manager_all_failed(self, health_checker, mock_cache_manager):
        """Test cache manager with all backends failing."""
        mock_cache_manager.health_check.return_value = {"price_cache": False, "balance_cache": False}

        health = await health_checker._check_cache_manager()

        assert health.status == HealthStatus.UNHEALTHY
        assert "All cache backends unhealthy" in health.error_message

    def test_determine_status_from_response_time(self, health_checker):
        """Test status determination based on response time."""
        # Fast response - healthy
        status = health_checker._determine_status_from_response_time(500, "test_service")
        assert status == HealthStatus.HEALTHY

        # Slow response - degraded
        status = health_checker._determine_status_from_response_time(3000, "test_service")
        assert status == HealthStatus.DEGRADED

        # Very slow response - unhealthy
        status = health_checker._determine_status_from_response_time(10000, "test_service")
        assert status == HealthStatus.UNHEALTHY

    def test_get_service_health(self, health_checker):
        """Test getting specific service health."""
        # Create a mock service health
        test_health = ServiceHealth("test_service", HealthStatus.HEALTHY)
        health_checker._service_health["test_service"] = test_health

        retrieved = health_checker.get_service_health("test_service")
        assert retrieved == test_health

        # Test non-existent service
        assert health_checker.get_service_health("nonexistent") is None

    def test_get_all_service_health(self, health_checker):
        """Test getting all service health."""
        # Add some test health objects
        health1 = ServiceHealth("service1", HealthStatus.HEALTHY)
        health2 = ServiceHealth("service2", HealthStatus.DEGRADED)

        health_checker._service_health["service1"] = health1
        health_checker._service_health["service2"] = health2

        all_health = health_checker.get_all_service_health()

        assert len(all_health) == 2
        assert all_health["service1"] == health1
        assert all_health["service2"] == health2

    def test_get_health_summary_empty(self, health_checker):
        """Test health summary with no services."""
        summary = health_checker.get_health_summary()

        assert summary["overall_status"] == "unknown"
        assert summary["healthy_services"] == 0
        assert summary["total_services"] == 0
        assert summary["degraded_services"] == 0
        assert summary["unhealthy_services"] == 0

    def test_get_health_summary_with_services(self, health_checker):
        """Test health summary with various service states."""
        # Add services with different health states
        health_checker._service_health["healthy"] = ServiceHealth("healthy", HealthStatus.HEALTHY, response_time=0.5)
        health_checker._service_health["degraded"] = ServiceHealth("degraded", HealthStatus.DEGRADED, response_time=3.0)
        health_checker._service_health["unhealthy"] = ServiceHealth("unhealthy", HealthStatus.UNHEALTHY)

        summary = health_checker.get_health_summary()

        assert summary["overall_status"] == "unhealthy"  # Worst case
        assert summary["healthy_services"] == 1
        assert summary["degraded_services"] == 1
        assert summary["unhealthy_services"] == 1
        assert summary["total_services"] == 3
        assert summary["average_response_time_ms"] == 1750.0  # (500 + 3000 + 0) / 2 services with response time

    def test_get_health_history(self, health_checker):
        """Test health history retrieval."""
        # Add some history
        test_entry = {"timestamp": datetime.now(UTC).isoformat(), "summary": {"test_service": True}}
        health_checker._add_to_history(test_entry)

        history = health_checker.get_health_history()
        assert len(history) == 1
        assert history[0] == test_entry

        # Test limit
        for i in range(10):
            health_checker._add_to_history({"test": i})

        limited_history = health_checker.get_health_history(limit=5)
        assert len(limited_history) == 5

    def test_get_health_trends(self, health_checker):
        """Test health trends analysis."""
        # Add some historical data
        now = datetime.now(UTC)

        for i in range(5):
            timestamp = (now - timedelta(hours=i)).isoformat()
            entry = {
                "timestamp": timestamp,
                "summary": {
                    "service1": i % 2 == 0,  # Alternating healthy/unhealthy
                    "service2": True,  # Always healthy
                },
            }
            health_checker._add_to_history(entry)

        trends = health_checker.get_health_trends(hours=24)

        assert trends["period_hours"] == 24
        assert trends["total_checks"] == 5
        assert "service1" in trends["trends"]
        assert "service2" in trends["trends"]

        # Service1 should have 60% availability (3 out of 5 healthy)
        assert trends["trends"]["service1"]["availability_percent"] == 60.0
        # Service2 should have 100% availability
        assert trends["trends"]["service2"]["availability_percent"] == 100.0

    def test_clear_health_history(self, health_checker):
        """Test clearing health history."""
        # Add some history
        health_checker._add_to_history({"test": "data"})
        assert len(health_checker._health_history) == 1

        health_checker.clear_health_history()
        assert len(health_checker._health_history) == 0

    def test_reset_service_health(self, health_checker):
        """Test resetting service health."""
        # Add some services
        health_checker._service_health["service1"] = ServiceHealth("service1", HealthStatus.HEALTHY)
        health_checker._service_health["service2"] = ServiceHealth("service2", HealthStatus.DEGRADED)

        # Reset specific service
        health_checker.reset_service_health("service1")
        assert "service1" not in health_checker._service_health
        assert "service2" in health_checker._service_health

        # Reset all services
        health_checker.reset_service_health()
        assert len(health_checker._service_health) == 0

    def test_export_health_report_json(self, health_checker):
        """Test exporting health report as JSON."""
        # Add a service
        health_checker._service_health["test"] = ServiceHealth("test", HealthStatus.HEALTHY)

        report = health_checker.export_health_report(format="json")

        assert isinstance(report, str)
        import json

        report_data = json.loads(report)

        assert "generated_at" in report_data
        assert "summary" in report_data
        assert "services" in report_data
        assert "trends" in report_data

    def test_export_health_report_text(self, health_checker):
        """Test exporting health report as text."""
        # Add a service
        health_checker._service_health["test"] = ServiceHealth("test", HealthStatus.HEALTHY)

        report = health_checker.export_health_report(format="text")

        assert isinstance(report, str)
        assert "WALLET TRACKER HEALTH REPORT" in report
        assert "OVERALL HEALTH SUMMARY" in report
        assert "SERVICE DETAILS" in report


class TestHealthCheckScheduler:
    """Test HealthCheckScheduler class."""

    @pytest.fixture
    def mock_health_checker(self):
        """Create mock health checker."""
        checker = AsyncMock()
        checker.check_all_services = AsyncMock(return_value={"test_service": True})
        return checker

    @pytest.fixture
    def scheduler(self, mock_health_checker):
        """Create HealthCheckScheduler instance."""
        return HealthCheckScheduler(mock_health_checker)

    def test_scheduler_creation(self, scheduler, mock_health_checker):
        """Test scheduler creation."""
        assert scheduler.health_checker == mock_health_checker
        assert scheduler._running is False
        assert len(scheduler._scheduled_tasks) == 0

    def test_get_scheduler_status_not_running(self, scheduler):
        """Test scheduler status when not running."""
        status = scheduler.get_scheduler_status()

        assert status["running"] is False
        assert status["active_tasks"] == 0
        assert status["total_tasks"] == 0

    @pytest.mark.asyncio
    async def test_schedule_periodic_checks(self, scheduler):
        """Test scheduling periodic checks."""
        # Mock alert callback
        alert_callback = AsyncMock()

        scheduler.schedule_periodic_checks(
            interval_minutes=1, detailed_check_interval_minutes=2, alert_callback=alert_callback
        )

        assert scheduler._running is True
        assert len(scheduler._scheduled_tasks) == 2

        # Stop immediately to avoid running the tasks
        scheduler.stop_scheduled_checks()

    def test_stop_scheduled_checks(self, scheduler):
        """Test stopping scheduled checks."""
        # Create mock tasks
        task1 = AsyncMock()
        task2 = AsyncMock()
        scheduler._scheduled_tasks = [task1, task2]
        scheduler._running = True

        scheduler.stop_scheduled_checks()

        assert scheduler._running is False
        task1.cancel.assert_called_once()
        task2.cancel.assert_called_once()
        assert len(scheduler._scheduled_tasks) == 0


class TestUtilityFunctions:
    """Test utility functions."""

    @pytest.mark.asyncio
    async def test_quick_health_check(self):
        """Test quick health check utility function."""
        # Create mock clients
        mock_ethereum = AsyncMock()
        mock_ethereum.get_stats.return_value = {"api_errors": 0}

        mock_coingecko = AsyncMock()
        mock_coingecko.health_check.return_value = True
        mock_coingecko.get_stats.return_value = {"rate_limit_errors": 0}

        results = await quick_health_check(ethereum_client=mock_ethereum, coingecko_client=mock_coingecko)

        assert "ethereum_client" in results
        assert "coingecko_client" in results
        assert isinstance(results["ethereum_client"], bool)
        assert isinstance(results["coingecko_client"], bool)

    @pytest.mark.asyncio
    async def test_create_health_alert_handler(self):
        """Test creating health alert handler."""
        handler = create_health_alert_handler(log_level="ERROR")

        # Test that it's callable
        assert callable(handler)

        # Test calling it (should not raise exception)
        alert_data = {"type": "test_alert", "services": ["test_service"], "timestamp": datetime.now(UTC).isoformat()}

        await handler(alert_data)

    @pytest.mark.asyncio
    async def test_create_health_alert_handler_with_webhook(self):
        """Test health alert handler with webhook."""
        webhook_url = "https://hooks.example.com/webhook"
        handler = create_health_alert_handler(webhook_url=webhook_url)

        alert_data = {"type": "test"}

        # Mock aiohttp to test webhook call
        with patch("aiohttp.ClientSession") as mock_session:
            mock_response = AsyncMock()
            mock_session.return_value.__aenter__.return_value.post.return_value = mock_response

            await handler(alert_data)

            # Verify webhook was attempted (though it may fail due to mocking)


@pytest.mark.asyncio
async def test_continuous_monitoring_integration():
    """Integration test for continuous monitoring."""
    # Create a real health checker with mock dependencies
    mock_ethereum = AsyncMock()
    mock_ethereum.get_stats.return_value = {"api_errors": 0}

    health_checker = HealthChecker(ethereum_client=mock_ethereum, timeout_seconds=1.0)

    # Test that continuous monitoring can be started and stopped
    monitoring_task = asyncio.create_task(
        health_checker.run_continuous_monitoring(interval_seconds=1, alert_callback=None)
    )

    # Let it run briefly
    await asyncio.sleep(0.1)

    # Cancel the monitoring
    monitoring_task.cancel()

    try:
        await monitoring_task
    except asyncio.CancelledError:
        pass  # Expected
