"""Tests for monitoring package initialization and utilities."""

from unittest.mock import MagicMock, patch

from wallet_tracker.monitoring import (
    # Core classes
    HealthChecker,
    HealthCheckScheduler,
    HealthStatus,
    Metric,
    MetricsCollector,
    MetricType,
    ServiceHealth,
    create_health_alert_handler,
    create_monitoring_context,
    get_global_health_checker,
    # Global instance functions
    get_global_metrics_collector,
    health_check_monitoring_system,
    quick_health_check,
    # Utility functions
    record_operation_metrics,
    setup_monitoring_system,
)


class TestPackageImports:
    """Test that all expected components can be imported."""

    def test_core_health_imports(self):
        """Test health checking imports."""
        assert HealthChecker is not None
        assert HealthStatus is not None
        assert ServiceHealth is not None
        assert HealthCheckScheduler is not None
        assert quick_health_check is not None
        assert create_health_alert_handler is not None

    def test_core_metrics_imports(self):
        """Test metrics imports."""
        assert MetricsCollector is not None
        assert Metric is not None
        assert MetricType is not None

    def test_global_instance_imports(self):
        """Test global instance function imports."""
        assert get_global_metrics_collector is not None
        assert get_global_health_checker is not None

    def test_utility_imports(self):
        """Test utility function imports."""
        assert setup_monitoring_system is not None
        assert health_check_monitoring_system is not None
        assert record_operation_metrics is not None
        assert create_monitoring_context is not None


class TestGlobalInstances:
    """Test global instance management."""

    def teardown_method(self):
        """Clean up global instances after each test."""
        import wallet_tracker.monitoring

        wallet_tracker.monitoring._global_metrics_collector = None
        wallet_tracker.monitoring._global_health_checker = None

    def test_get_global_metrics_collector_creates_instance(self):
        """Test that get_global_metrics_collector creates instance."""
        mock_config = MagicMock()

        collector = get_global_metrics_collector(config=mock_config)

        assert collector is not None
        assert isinstance(collector, MetricsCollector)
        assert collector.config == mock_config

    def test_get_global_metrics_collector_returns_same_instance(self):
        """Test that get_global_metrics_collector returns same instance on subsequent calls."""
        mock_config = MagicMock()

        collector1 = get_global_metrics_collector(config=mock_config)
        collector2 = get_global_metrics_collector()

        assert collector1 is collector2

    def test_get_global_metrics_collector_with_default_config(self):
        """Test get_global_metrics_collector with default config."""
        with patch("wallet_tracker.monitoring.get_config") as mock_get_config:
            mock_config = MagicMock()
            mock_get_config.return_value = mock_config

            collector = get_global_metrics_collector()

            assert collector is not None
            mock_get_config.assert_called_once()

    def test_get_global_health_checker_creates_instance(self):
        """Test that get_global_health_checker creates instance."""
        mock_ethereum = MagicMock()
        mock_coingecko = MagicMock()

        checker = get_global_health_checker(ethereum_client=mock_ethereum, coingecko_client=mock_coingecko)

        assert checker is not None
        assert isinstance(checker, HealthChecker)
        assert checker.ethereum_client == mock_ethereum
        assert checker.coingecko_client == mock_coingecko

    def test_get_global_health_checker_returns_same_instance(self):
        """Test that get_global_health_checker returns same instance."""
        mock_ethereum = MagicMock()

        checker1 = get_global_health_checker(ethereum_client=mock_ethereum)
        checker2 = get_global_health_checker()

        assert checker1 is checker2


class TestSetupMonitoringSystem:
    """Test monitoring system setup."""

    def teardown_method(self):
        """Clean up global instances after each test."""
        import wallet_tracker.monitoring

        wallet_tracker.monitoring._global_metrics_collector = None
        wallet_tracker.monitoring._global_health_checker = None

    def test_setup_monitoring_system_basic(self):
        """Test basic monitoring system setup."""
        mock_config = MagicMock()
        mock_ethereum = MagicMock()
        mock_coingecko = MagicMock()

        components = setup_monitoring_system(
            config=mock_config, ethereum_client=mock_ethereum, coingecko_client=mock_coingecko
        )

        assert "metrics_collector" in components
        assert "health_checker" in components
        assert isinstance(components["metrics_collector"], MetricsCollector)
        assert isinstance(components["health_checker"], HealthChecker)

    def test_setup_monitoring_system_with_continuous_monitoring(self):
        """Test monitoring system setup with continuous monitoring enabled."""
        mock_config = MagicMock()
        mock_ethereum = MagicMock()

        components = setup_monitoring_system(
            config=mock_config,
            ethereum_client=mock_ethereum,
            enable_continuous_monitoring=True,
            monitoring_interval_minutes=1,
        )

        assert "metrics_collector" in components
        assert "health_checker" in components
        assert "scheduler" in components
        assert "alert_handler" in components

        assert isinstance(components["scheduler"], HealthCheckScheduler)
        assert callable(components["alert_handler"])

        # Clean up scheduler
        components["scheduler"].stop_scheduled_checks()

    def test_setup_monitoring_system_all_clients(self):
        """Test setup with all client types."""
        mock_config = MagicMock()
        mock_ethereum = MagicMock()
        mock_coingecko = MagicMock()
        mock_sheets = MagicMock()
        mock_cache = MagicMock()
