"""Monitoring package for health checking and metrics collection.

This package provides comprehensive monitoring capabilities including:
- Health checking for all application services
- Performance metrics collection and analysis
- System resource monitoring
- Service availability tracking
- Alert generation and reporting

Key Components:
- health: Health checking system with continuous monitoring
- metrics: Metrics collection with various metric types (counters, gauges, histograms, timers, rates)

Usage Examples:

Health Checking:
    from wallet_tracker.monitoring import HealthChecker, quick_health_check

    # Quick health check
    health_results = await quick_health_check(
        ethereum_client=eth_client,
        coingecko_client=cg_client
    )

    # Comprehensive health monitoring
    health_checker = HealthChecker(
        ethereum_client=eth_client,
        coingecko_client=cg_client,
        sheets_client=sheets_client,
        cache_manager=cache_manager
    )

    health_status = await health_checker.check_all_services(include_detailed_checks=True)
    summary = health_checker.get_health_summary()

Metrics Collection:
    from wallet_tracker.monitoring import MetricsCollector, MetricType

    metrics = MetricsCollector(config)

    # Record different types of metrics
    metrics.increment_counter("wallets_processed", 1)
    metrics.set_gauge("active_connections", 5)
    metrics.observe_histogram("processing_time", 1.25)

    # Timer context manager
    with metrics.timer_context("api_request"):
        result = await api_call()

    # Get current metrics
    current_metrics = await metrics.get_current_metrics()

    # Export metrics
    prometheus_format = metrics.export_metrics('prometheus')

Continuous Monitoring:
    from wallet_tracker.monitoring import HealthCheckScheduler, create_health_alert_handler

    # Setup alert handler
    alert_handler = create_health_alert_handler(
        webhook_url="https://hooks.slack.com/...",
        log_level="WARNING"
    )

    # Schedule periodic checks
    scheduler = HealthCheckScheduler(health_checker)
    scheduler.schedule_periodic_checks(
        interval_minutes=5,
        detailed_check_interval_minutes=30,
        alert_callback=alert_handler
    )
"""

from .health import (
    # Core health checking classes
    HealthChecker,
    HealthStatus,
    ServiceHealth,
    HealthCheckScheduler,

    # Utility functions
    quick_health_check,
    create_health_alert_handler,
)

from .metrics import (
    # Core metrics classes
    MetricsCollector,
    Metric,
    MetricType,
)


# Package version and metadata
__version__ = "1.0.0"
__author__ = "Wallet Tracker Team"
__description__ = "Comprehensive monitoring and health checking system"


# Global instances for convenience
_global_metrics_collector = None
_global_health_checker = None


def get_global_metrics_collector(config=None) -> MetricsCollector:
    """Get global metrics collector instance.

    Args:
        config: Optional configuration override

    Returns:
        Global MetricsCollector instance
    """
    global _global_metrics_collector
    if _global_metrics_collector is None:
        if config is None:
            from ..config import get_config
            config = get_config()
        _global_metrics_collector = MetricsCollector(config)
    return _global_metrics_collector


def get_global_health_checker(
    ethereum_client=None,
    coingecko_client=None,
    sheets_client=None,
    cache_manager=None
) -> HealthChecker:
    """Get global health checker instance.

    Args:
        ethereum_client: Ethereum client instance
        coingecko_client: CoinGecko client instance
        sheets_client: Google Sheets client instance
        cache_manager: Cache manager instance

    Returns:
        Global HealthChecker instance
    """
    global _global_health_checker
    if _global_health_checker is None:
        _global_health_checker = HealthChecker(
            ethereum_client=ethereum_client,
            coingecko_client=coingecko_client,
            sheets_client=sheets_client,
            cache_manager=cache_manager
        )
    return _global_health_checker


def setup_monitoring_system(
    config=None,
    ethereum_client=None,
    coingecko_client=None,
    sheets_client=None,
    cache_manager=None,
    enable_continuous_monitoring=False,
    monitoring_interval_minutes=5
) -> dict:
    """Setup the complete monitoring system.

    Args:
        config: Application configuration
        ethereum_client: Ethereum client instance
        coingecko_client: CoinGecko client instance
        sheets_client: Google Sheets client instance
        cache_manager: Cache manager instance
        enable_continuous_monitoring: Whether to start continuous monitoring
        monitoring_interval_minutes: Interval for continuous monitoring

    Returns:
        Dictionary with initialized monitoring components
    """
    # Initialize metrics collector
    metrics_collector = get_global_metrics_collector(config)

    # Initialize health checker
    health_checker = get_global_health_checker(
        ethereum_client=ethereum_client,
        coingecko_client=coingecko_client,
        sheets_client=sheets_client,
        cache_manager=cache_manager
    )

    components = {
        'metrics_collector': metrics_collector,
        'health_checker': health_checker
    }

    # Setup continuous monitoring if requested
    if enable_continuous_monitoring:
        scheduler = HealthCheckScheduler(health_checker)

        # Create default alert handler
        alert_handler = create_health_alert_handler(log_level="WARNING")

        scheduler.schedule_periodic_checks(
            interval_minutes=monitoring_interval_minutes,
            detailed_check_interval_minutes=monitoring_interval_minutes * 6,  # 6x for detailed
            alert_callback=alert_handler
        )

        components['scheduler'] = scheduler
        components['alert_handler'] = alert_handler

    return components


async def health_check_monitoring_system() -> dict:
    """Perform health check on the monitoring system itself.

    Returns:
        Health check results for monitoring components
    """
    results = {
        'healthy': True,
        'components': {},
        'issues': []
    }

    try:
        # Check metrics collector
        global _global_metrics_collector
        if _global_metrics_collector:
            try:
                summary = _global_metrics_collector.get_metrics_summary()
                results['components']['metrics_collector'] = {
                    'healthy': True,
                    'total_metrics': summary['total_metrics'],
                    'active_timers': summary['active_timers']
                }
            except Exception as e:
                results['healthy'] = False
                results['issues'].append(f"Metrics collector issue: {e}")
                results['components']['metrics_collector'] = {'healthy': False, 'error': str(e)}
        else:
            results['components']['metrics_collector'] = {'healthy': False, 'error': 'Not initialized'}

        # Check health checker
        global _global_health_checker
        if _global_health_checker:
            try:
                summary = _global_health_checker.get_health_summary()
                results['components']['health_checker'] = {
                    'healthy': True,
                    'total_services': summary['total_services'],
                    'healthy_services': summary['healthy_services']
                }
            except Exception as e:
                results['healthy'] = False
                results['issues'].append(f"Health checker issue: {e}")
                results['components']['health_checker'] = {'healthy': False, 'error': str(e)}
        else:
            results['components']['health_checker'] = {'healthy': False, 'error': 'Not initialized'}

    except Exception as e:
        results['healthy'] = False
        results['issues'].append(f"Monitoring system check failed: {e}")

    return results


# Convenience functions for common monitoring tasks

async def record_operation_metrics(
    operation_name: str,
    duration: float,
    success: bool = True,
    metrics_collector: MetricsCollector = None,
    **additional_metrics
) -> None:
    """Record metrics for a completed operation.

    Args:
        operation_name: Name of the operation
        duration: Operation duration in seconds
        success: Whether operation was successful
        metrics_collector: Metrics collector instance (uses global if None)
        **additional_metrics: Additional metrics to record
    """
    if metrics_collector is None:
        metrics_collector = get_global_metrics_collector()

    # Record basic operation metrics
    metrics_collector.increment_counter(f"{operation_name}_total")
    metrics_collector.record_timer(f"{operation_name}_duration", duration)

    if not success:
        metrics_collector.increment_counter(f"{operation_name}_errors_total")

    # Record any additional metrics
    for metric_name, value in additional_metrics.items():
        if isinstance(value, (int, float)):
            metrics_collector.set_gauge(f"{operation_name}_{metric_name}", value)


def create_monitoring_context(
    operation_name: str,
    metrics_collector: MetricsCollector = None
):
    """Create monitoring context manager for operations.

    Args:
        operation_name: Name of the operation
        metrics_collector: Metrics collector instance

    Usage:
        async with create_monitoring_context("wallet_processing") as ctx:
            # Do work
            ctx.record_success()
    """
    class MonitoringContext:
        def __init__(self, op_name: str, collector: MetricsCollector):
            self.operation_name = op_name
            self.metrics_collector = collector
            self.start_time = None
            self.success = True
            self.additional_metrics = {}

        async def __aenter__(self):
            import time
            self.start_time = time.time()
            self.metrics_collector.increment_counter(f"{self.operation_name}_started_total")
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            import time
            duration = time.time() - self.start_time if self.start_time else 0
            success = exc_type is None and self.success

            await record_operation_metrics(
                operation_name=self.operation_name,
                duration=duration,
                success=success,
                metrics_collector=self.metrics_collector,
                **self.additional_metrics
            )

        def record_success(self):
            """Mark operation as successful."""
            self.success = True

        def record_failure(self):
            """Mark operation as failed."""
            self.success = False

        def add_metric(self, name: str, value: float):
            """Add additional metric to be recorded."""
            self.additional_metrics[name] = value

    if metrics_collector is None:
        metrics_collector = get_global_metrics_collector()

    return MonitoringContext(operation_name, metrics_collector)


# Export all public components
__all__ = [
    # Health checking
    'HealthChecker',
    'HealthStatus',
    'ServiceHealth',
    'HealthCheckScheduler',
    'quick_health_check',
    'create_health_alert_handler',

    # Metrics collection
    'MetricsCollector',
    'Metric',
    'MetricType',

    # Global instances
    'get_global_metrics_collector',
    'get_global_health_checker',

    # System setup and utilities
    'setup_monitoring_system',
    'health_check_monitoring_system',
    'record_operation_metrics',
    'create_monitoring_context',
]