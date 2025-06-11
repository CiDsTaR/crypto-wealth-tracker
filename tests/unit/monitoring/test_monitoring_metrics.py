"""Tests for metrics collection system."""

import time
from collections import deque
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from wallet_tracker.monitoring.metrics import (
    Metric,
    MetricsCollector,
    MetricType,
)


class TestMetricType:
    """Test MetricType enum."""

    def test_metric_type_values(self):
        """Test MetricType enum values."""
        assert MetricType.COUNTER == "counter"
        assert MetricType.GAUGE == "gauge"
        assert MetricType.HISTOGRAM == "histogram"
        assert MetricType.TIMER == "timer"
        assert MetricType.RATE == "rate"


class TestMetric:
    """Test Metric class."""

    def test_metric_creation(self):
        """Test metric creation with different types."""
        # Counter metric
        counter = Metric("test_counter", MetricType.COUNTER, "Test counter", "requests")
        assert counter.name == "test_counter"
        assert counter.type == MetricType.COUNTER
        assert counter.description == "Test counter"
        assert counter.unit == "requests"
        assert counter.value == 0
        assert counter.tags == {}

        # Gauge metric
        gauge = Metric("test_gauge", MetricType.GAUGE, tags={"host": "server1"})
        assert gauge.value == 0
        assert gauge.tags["host"] == "server1"

        # Histogram metric
        histogram = Metric("test_histogram", MetricType.HISTOGRAM)
        assert isinstance(histogram.values, deque)
        assert histogram.sum == 0.0
        assert histogram.count == 0
        assert histogram.min_value == float("inf")
        assert histogram.max_value == float("-inf")

        # Timer metric
        timer = Metric("test_timer", MetricType.TIMER)
        assert isinstance(timer.values, deque)

        # Rate metric
        rate = Metric("test_rate", MetricType.RATE)
        assert isinstance(rate.events, deque)

    def test_counter_operations(self):
        """Test counter metric operations."""
        counter = Metric("test", MetricType.COUNTER)

        # Test increment
        counter.increment()
        assert counter.value == 1

        counter.increment(5)
        assert counter.value == 6

        # Test get_value
        assert counter.get_value() == 6

        # Test invalid operations
        with pytest.raises(ValueError):
            counter.set(10)  # Can't set counter

        with pytest.raises(ValueError):
            counter.observe(5)  # Can't observe counter

    def test_gauge_operations(self):
        """Test gauge metric operations."""
        gauge = Metric("test", MetricType.GAUGE)

        # Test set
        gauge.set(42.5)
        assert gauge.value == 42.5
        assert gauge.get_value() == 42.5

        gauge.set(-10)
        assert gauge.value == -10

        # Test invalid operations
        with pytest.raises(ValueError):
            gauge.increment()  # Can't increment gauge

        with pytest.raises(ValueError):
            gauge.observe(5)  # Can't observe gauge

    def test_histogram_operations(self):
        """Test histogram metric operations."""
        histogram = Metric("test", MetricType.HISTOGRAM)

        # Test observe
        histogram.observe(10.0)
        histogram.observe(20.0)
        histogram.observe(30.0)

        assert histogram.count == 3
        assert histogram.sum == 60.0
        assert histogram.min_value == 10.0
        assert histogram.max_value == 30.0

        # Test get_value
        value = histogram.get_value()
        assert value["count"] == 3
        assert value["sum"] == 60.0
        assert value["avg"] == 20.0
        assert value["min"] == 10.0
        assert value["max"] == 30.0
        assert value["p50"] == 20.0

        # Test invalid operations
        with pytest.raises(ValueError):
            histogram.increment()

        with pytest.raises(ValueError):
            histogram.set(10)

    def test_histogram_percentiles(self):
        """Test histogram percentile calculations."""
        histogram = Metric("test", MetricType.HISTOGRAM)

        # Add values for percentile testing
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        for val in values:
            histogram.observe(val)

        result = histogram.get_value()

        # Test percentiles
        assert result["p50"] == 5.5  # Median
        assert result["p95"] == 9.55
        assert result["p99"] == 9.91

    def test_histogram_empty(self):
        """Test histogram with no observations."""
        histogram = Metric("test", MetricType.HISTOGRAM)

        value = histogram.get_value()
        assert value["count"] == 0
        assert value["sum"] == 0.0
        assert value["avg"] == 0.0
        assert value["min"] == 0.0
        assert value["max"] == 0.0
        assert value["p50"] == 0.0

    def test_timer_operations(self):
        """Test timer metric operations."""
        timer = Metric("test", MetricType.TIMER)

        # Timer should behave like histogram
        timer.observe(0.5)
        timer.observe(1.0)
        timer.observe(1.5)

        value = timer.get_value()
        assert value["count"] == 3
        assert value["avg"] == 1.0

        # Test invalid operations
        with pytest.raises(ValueError):
            timer.increment()

    def test_rate_operations(self):
        """Test rate metric operations."""
        rate = Metric("test", MetricType.RATE)

        # Test record_event
        rate.record_event()
        rate.record_event()
        rate.record_event()

        assert len(rate.events) == 3

        # Test get_value (rates)
        value = rate.get_value()
        assert isinstance(value, dict)
        assert "1m" in value
        assert "5m" in value
        assert "15m" in value
        assert "1h" in value

        # All events are recent, so rates should be positive
        assert value["1m"] > 0

        # Test invalid operations
        with pytest.raises(ValueError):
            rate.increment()

        with pytest.raises(ValueError):
            rate.set(10)

        with pytest.raises(ValueError):
            rate.observe(5)

    def test_rate_time_windows(self):
        """Test rate metric time window calculations."""
        rate = Metric("test", MetricType.RATE)

        # Manually add events with specific timestamps
        now = datetime.now(UTC)

        # Add events at different times
        rate.events.append(now - timedelta(seconds=30))  # 30s ago
        rate.events.append(now - timedelta(seconds=90))  # 1.5m ago
        rate.events.append(now - timedelta(seconds=600))  # 10m ago
        rate.events.append(now - timedelta(seconds=3600))  # 1h ago

        # Mock datetime.now to return our 'now'
        with patch("wallet_tracker.monitoring.metrics.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.UTC = UTC

            value = rate.get_value()

            # Only first event should be in 1m window
            assert value["1m"] > 0
            # First two events should be in 5m window
            assert value["5m"] > value["1m"]

    def test_metric_reset(self):
        """Test metric reset functionality."""
        # Test counter reset
        counter = Metric("test", MetricType.COUNTER)
        counter.increment(5)
        counter.reset()
        assert counter.value == 0

        # Test gauge reset
        gauge = Metric("test", MetricType.GAUGE)
        gauge.set(42)
        gauge.reset()
        assert gauge.value == 0

        # Test histogram reset
        histogram = Metric("test", MetricType.HISTOGRAM)
        histogram.observe(10)
        histogram.observe(20)
        histogram.reset()
        assert histogram.count == 0
        assert histogram.sum == 0.0
        assert len(histogram.values) == 0

        # Test rate reset
        rate = Metric("test", MetricType.RATE)
        rate.record_event()
        rate.record_event()
        rate.reset()
        assert len(rate.events) == 0

    def test_metric_to_dict(self):
        """Test metric serialization to dictionary."""
        metric = Metric(
            name="test_metric",
            metric_type=MetricType.COUNTER,
            description="Test metric",
            unit="requests",
            tags={"service": "api"},
        )
        metric.increment(5)

        metric_dict = metric.to_dict()

        assert metric_dict["name"] == "test_metric"
        assert metric_dict["type"] == "counter"
        assert metric_dict["description"] == "Test metric"
        assert metric_dict["unit"] == "requests"
        assert metric_dict["tags"]["service"] == "api"
        assert metric_dict["value"] == 5
        assert "created_at" in metric_dict
        assert "last_updated" in metric_dict

    def test_percentile_calculation_edge_cases(self):
        """Test percentile calculation edge cases."""
        histogram = Metric("test", MetricType.HISTOGRAM)

        # Single value
        histogram.observe(42)
        value = histogram.get_value()
        assert value["p50"] == 42
        assert value["p95"] == 42
        assert value["p99"] == 42

        # Two values
        histogram.reset()
        histogram.observe(10)
        histogram.observe(20)
        value = histogram.get_value()
        assert value["p50"] == 15  # Interpolated median


class TestMetricsCollector:
    """Test MetricsCollector class."""

    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        config = MagicMock()
        return config

    @pytest.fixture
    def metrics_collector(self, mock_config):
        """Create MetricsCollector instance."""
        return MetricsCollector(mock_config)

    def test_metrics_collector_initialization(self, metrics_collector):
        """Test MetricsCollector initialization."""
        assert metrics_collector.config is not None
        assert isinstance(metrics_collector._metrics, dict)
        assert isinstance(metrics_collector._metric_history, list)
        assert isinstance(metrics_collector._active_timers, dict)

        # Check that core metrics are initialized
        assert "wallets_processed_total" in metrics_collector._metrics
        assert "api_requests_total" in metrics_collector._metrics
        assert "cache_hits_total" in metrics_collector._metrics

    def test_register_metric(self, metrics_collector):
        """Test metric registration."""
        metric = metrics_collector.register_metric(
            name="test_metric",
            metric_type=MetricType.COUNTER,
            description="Test metric",
            unit="requests",
            tags={"component": "test"},
        )

        assert metric.name == "test_metric"
        assert metric.type == MetricType.COUNTER
        assert "test_metric" in metrics_collector._metrics

    def test_register_metric_overwrite_warning(self, metrics_collector):
        """Test metric registration overwrite warning."""
        # Register first metric
        metrics_collector.register_metric("duplicate", MetricType.COUNTER)

        # Register again with same name - should log warning
        with patch("wallet_tracker.monitoring.metrics.logger") as mock_logger:
            metrics_collector.register_metric("duplicate", MetricType.GAUGE)
            mock_logger.warning.assert_called_once()

    def test_get_metric(self, metrics_collector):
        """Test getting metric by name."""
        # Get existing metric
        metric = metrics_collector.get_metric("wallets_processed_total")
        assert metric is not None
        assert metric.type == MetricType.COUNTER

        # Get non-existent metric
        assert metrics_collector.get_metric("nonexistent") is None

    def test_increment_counter(self, metrics_collector):
        """Test incrementing counter metrics."""
        metrics_collector.increment_counter("test_counter", 5)

        metric = metrics_collector.get_metric("test_counter")
        assert metric is not None
        assert metric.type == MetricType.COUNTER
        assert metric.get_value() == 5

        # Increment again
        metrics_collector.increment_counter("test_counter", 3)
        assert metric.get_value() == 8

    def test_increment_counter_with_tags(self, metrics_collector):
        """Test incrementing counter with tags."""
        tags = {"service": "api", "method": "GET"}
        metrics_collector.increment_counter("requests", 1, tags=tags)

        # Should create metric with tags in key
        metric_key = "requests[method=GET,service=api]"
        assert metric_key in metrics_collector._metrics

    def test_set_gauge(self, metrics_collector):
        """Test setting gauge metrics."""
        metrics_collector.set_gauge("test_gauge", 42.5)

        metric = metrics_collector.get_metric("test_gauge")
        assert metric is not None
        assert metric.type == MetricType.GAUGE
        assert metric.get_value() == 42.5

        # Set new value
        metrics_collector.set_gauge("test_gauge", 100)
        assert metric.get_value() == 100

    def test_observe_histogram(self, metrics_collector):
        """Test observing histogram metrics."""
        metrics_collector.observe_histogram("test_histogram", 10.0)
        metrics_collector.observe_histogram("test_histogram", 20.0)
        metrics_collector.observe_histogram("test_histogram", 30.0)

        metric = metrics_collector.get_metric("test_histogram")
        assert metric is not None
        assert metric.type == MetricType.HISTOGRAM

        value = metric.get_value()
        assert value["count"] == 3
        assert value["avg"] == 20.0

    def test_record_timer(self, metrics_collector):
        """Test recording timer metrics."""
        metrics_collector.record_timer("test_timer", 1.5)
        metrics_collector.record_timer("test_timer", 2.0)

        metric = metrics_collector.get_metric("test_timer")
        assert metric is not None
        assert metric.type == MetricType.TIMER

        value = metric.get_value()
        assert value["count"] == 2
        assert value["avg"] == 1.75

    def test_record_rate_event(self, metrics_collector):
        """Test recording rate events."""
        metrics_collector.record_rate_event("test_rate")
        metrics_collector.record_rate_event("test_rate")

        metric = metrics_collector.get_metric("test_rate")
        assert metric is not None
        assert metric.type == MetricType.RATE

        value = metric.get_value()
        assert isinstance(value, dict)
        assert "1m" in value

    def test_timer_context_manager(self, metrics_collector):
        """Test timer context manager."""
        with metrics_collector.timer_context("test_operation"):
            time.sleep(0.01)  # Small delay

        metric = metrics_collector.get_metric("test_operation")
        assert metric is not None

        value = metric.get_value()
        assert value["count"] == 1
        assert value["avg"] > 0  # Should have measured some time

    def test_start_stop_timer(self, metrics_collector):
        """Test manual timer start/stop."""
        timer_id = metrics_collector.start_timer("manual_timer")
        assert timer_id in metrics_collector._active_timers

        time.sleep(0.01)  # Small delay

        duration = metrics_collector.stop_timer(timer_id)
        assert duration > 0
        assert timer_id not in metrics_collector._active_timers

        # Check metric was recorded
        metric = metrics_collector.get_metric("manual_timer")
        assert metric is not None
        assert metric.get_value()["count"] == 1

    def test_stop_nonexistent_timer(self, metrics_collector):
        """Test stopping non-existent timer."""
        with patch("wallet_tracker.monitoring.metrics.logger") as mock_logger:
            duration = metrics_collector.stop_timer("nonexistent")
            assert duration == 0.0
            mock_logger.warning.assert_called_once()

    def test_metric_key_creation(self, metrics_collector):
        """Test metric key creation with tags."""
        # No tags
        key = metrics_collector._create_metric_key("metric_name", None)
        assert key == "metric_name"

        # With tags
        tags = {"host": "server1", "env": "prod"}
        key = metrics_collector._create_metric_key("metric_name", tags)
        assert key == "metric_name[env=prod,host=server1]"  # Sorted tags

    @pytest.mark.asyncio
    async def test_record_processing_run(self, metrics_collector):
        """Test recording processing run metrics."""
        processing_results = {
            "results": {"processed": 10, "failed": 2, "skipped": 1},
            "portfolio_values": {"total_usd": 50000.0},
            "activity": {"active_wallets": 8, "inactive_wallets": 2},
            "performance": {
                "total_time_seconds": 30.0,
                "average_time_per_wallet": 3.0,
                "api_calls_total": 50,
                "cache_hit_rate": 80.0,
            },
        }

        await metrics_collector.record_processing_run(processing_results)

        # Check that metrics were recorded
        assert metrics_collector.get_metric("wallets_processed_total").get_value() == 10
        assert metrics_collector.get_metric("wallets_failed_total").get_value() == 2
        assert metrics_collector.get_metric("total_portfolio_value").get_value() == 50000.0
        assert metrics_collector.get_metric("active_wallets_count").get_value() == 8

    @pytest.mark.asyncio
    async def test_record_processing_run_with_object(self, metrics_collector):
        """Test recording processing run with object that has get_summary_dict."""

        class MockResults:
            def get_summary_dict(self):
                return {"results": {"processed": 5}, "portfolio_values": {"total_usd": 10000.0}}

        mock_results = MockResults()
        await metrics_collector.record_processing_run(mock_results)

        assert metrics_collector.get_metric("wallets_processed_total").get_value() == 5
        assert metrics_collector.get_metric("total_portfolio_value").get_value() == 10000.0

    def test_record_api_call(self, metrics_collector):
        """Test recording API call metrics."""
        metrics_collector.record_api_call(service="ethereum", duration=1.5, success=True, status_code=200)

        # Should create metrics with service tags
        api_requests = None
        api_duration = None

        for name, metric in metrics_collector._metrics.items():
            if "api_requests_total" in name and "service=ethereum" in name:
                api_requests = metric
            elif "api_request_duration" in name and "service=ethereum" in name:
                api_duration = metric

        assert api_requests is not None
        assert api_requests.get_value() == 1

        assert api_duration is not None
        assert api_duration.get_value()["count"] == 1

    def test_record_api_call_failure(self, metrics_collector):
        """Test recording failed API call."""
        metrics_collector.record_api_call(service="coingecko", duration=5.0, success=False, status_code=500)

        # Should record error metric
        error_metric = None
        for name, metric in metrics_collector._metrics.items():
            if "api_errors_total" in name and "service=coingecko" in name:
                error_metric = metric
                break

        assert error_metric is not None
        assert error_metric.get_value() == 1

    def test_record_api_call_rate_limit(self, metrics_collector):
        """Test recording rate limited API call."""
        metrics_collector.record_api_call(service="coingecko", duration=1.0, success=False, status_code=429)

        # Should record both error and rate limit metrics
        rate_limit_metric = None
        for name, metric in metrics_collector._metrics.items():
            if "api_rate_limits_total" in name and "service=coingecko" in name:
                rate_limit_metric = metric
                break

        assert rate_limit_metric is not None
        assert rate_limit_metric.get_value() == 1

    def test_record_cache_operation(self, metrics_collector):
        """Test recording cache operation metrics."""
        # Cache hit
        metrics_collector.record_cache_operation(operation="get", duration=0.01, hit=True, backend="redis")

        # Cache miss
        metrics_collector.record_cache_operation(operation="get", duration=0.05, hit=False, backend="redis")

        # Find the hit metric
        hit_metric = None
        miss_metric = None
        for name, metric in metrics_collector._metrics.items():
            if "cache_hits_total" in name and "backend=redis" in name:
                hit_metric = metric
            elif "cache_misses_total" in name and "backend=redis" in name:
                miss_metric = metric

        assert hit_metric is not None
        assert hit_metric.get_value() == 1

        assert miss_metric is not None
        assert miss_metric.get_value() == 1

    @pytest.mark.asyncio
    async def test_collect_system_metrics(self, metrics_collector):
        """Test collecting system metrics."""
        # Mock psutil
        with patch("wallet_tracker.monitoring.metrics.psutil") as mock_psutil:
            # Mock memory info
            mock_memory = MagicMock()
            mock_memory.used = 1000000000  # 1GB
            mock_memory.percent = 75.0
            mock_psutil.virtual_memory.return_value = mock_memory

            # Mock CPU
            mock_psutil.cpu_percent.return_value = 50.0

            # Mock process
            mock_process = MagicMock()
            mock_memory_info = MagicMock()
            mock_memory_info.rss = 100000000  # 100MB
            mock_memory_info.vms = 200000000  # 200MB
            mock_process.memory_info.return_value = mock_memory_info
            mock_psutil.Process.return_value = mock_process

            await metrics_collector.collect_system_metrics()

            # Check that system metrics were recorded
            assert metrics_collector.get_metric("memory_usage").get_value() == 1000000000
            assert metrics_collector.get_metric("memory_usage_percent").get_value() == 75.0
            assert metrics_collector.get_metric("cpu_usage").get_value() == 50.0

    @pytest.mark.asyncio
    async def test_collect_system_metrics_no_psutil(self, metrics_collector):
        """Test collecting system metrics when psutil is not available."""
        with patch("wallet_tracker.monitoring.metrics.psutil", side_effect=ImportError):
            with patch("wallet_tracker.monitoring.metrics.logger") as mock_logger:
                await metrics_collector.collect_system_metrics()
                mock_logger.debug.assert_called_with("psutil not available, skipping system metrics")

    @pytest.mark.asyncio
    async def test_get_current_metrics(self, metrics_collector):
        """Test getting current metrics."""
        # Add some test metrics
        metrics_collector.increment_counter("test_counter", 5)
        metrics_collector.set_gauge("test_gauge", 42)

        current_metrics = await metrics_collector.get_current_metrics()

        assert "timestamp" in current_metrics
        assert "metrics" in current_metrics
        assert "summary" in current_metrics

        # Check specific metrics
        metrics_data = current_metrics["metrics"]
        assert "test_counter" in metrics_data
        assert "test_gauge" in metrics_data

        # Check summary
        summary = current_metrics["summary"]
        assert summary["total_metrics"] > 0
        assert "metric_types" in summary

    def test_export_metrics_prometheus(self, metrics_collector):
        """Test exporting metrics in Prometheus format."""
        # Add test metrics
        metrics_collector.increment_counter("http_requests_total", 100)
        metrics_collector.set_gauge("memory_usage_bytes", 1000000)

        prometheus_output = metrics_collector.export_metrics("prometheus")

        assert "# HELP http_requests_total" in prometheus_output
        assert "# TYPE http_requests_total counter" in prometheus_output
        assert "http_requests_total 100" in prometheus_output

        assert "# TYPE memory_usage_bytes gauge" in prometheus_output
        assert "memory_usage_bytes 1000000" in prometheus_output

    def test_export_metrics_json(self, metrics_collector):
        """Test exporting metrics in JSON format."""
        import json

        metrics_collector.increment_counter("test_metric", 42)

        json_output = metrics_collector.export_metrics("json")
        parsed_json = json.loads(json_output)

        assert "timestamp" in parsed_json
        assert "metrics" in parsed_json

    def test_export_metrics_influxdb(self, metrics_collector):
        """Test exporting metrics in InfluxDB format."""
        metrics_collector.increment_counter("requests", 100)
        metrics_collector.set_gauge("memory", 50000, tags={"host": "server1"})

        influxdb_output = metrics_collector.export_metrics("influxdb")

        lines = influxdb_output.strip().split("\n")
        assert len(lines) >= 2

        # Check format
        for line in lines:
            parts = line.split(" ")
            assert len(parts) >= 2  # metric_name field=value timestamp

    def test_export_metrics_invalid_format(self, metrics_collector):
        """Test exporting metrics with invalid format."""
        with pytest.raises(ValueError, match="Unsupported export format"):
            metrics_collector.export_metrics("invalid_format")

    def test_reset_all_metrics(self, metrics_collector):
        """Test resetting all metrics."""
        # Add some values
        metrics_collector.increment_counter("test_counter", 5)
        metrics_collector.set_gauge("test_gauge", 42)

        # Reset all
        metrics_collector.reset_all_metrics()

        # Check values are reset
        assert metrics_collector.get_metric("test_counter").get_value() == 0
        assert metrics_collector.get_metric("test_gauge").get_value() == 0

    def test_reset_specific_metric(self, metrics_collector):
        """Test resetting specific metric."""
        # Add some values
        metrics_collector.increment_counter("counter1", 5)
        metrics_collector.increment_counter("counter2", 10)

        # Reset only counter1
        result = metrics_collector.reset_metric("counter1")
        assert result is True
        assert metrics_collector.get_metric("counter1").get_value() == 0
        assert metrics_collector.get_metric("counter2").get_value() == 10

        # Try to reset non-existent metric
        result = metrics_collector.reset_metric("nonexistent")
        assert result is False

    def test_get_metrics_summary(self, metrics_collector):
        """Test getting metrics summary."""
        # Add some metrics and timers
        metrics_collector.increment_counter("test_counter")
        timer_id = metrics_collector.start_timer("test_timer")

        summary = metrics_collector.get_metrics_summary()

        assert summary["total_metrics"] > 0
        assert summary["active_timers"] == 1
        assert summary["history_size"] == 0
        assert "collection_started" in summary

        # Clean up timer
        metrics_collector.stop_timer(timer_id)


class TestMetricsIntegration:
    """Integration tests for metrics system."""

    @pytest.mark.asyncio
    async def test_full_metrics_workflow(self):
        """Test complete metrics collection workflow."""
        mock_config = MagicMock()
        collector = MetricsCollector(mock_config)

        # Simulate a processing run
        with collector.timer_context("processing_operation"):
            # Simulate some work
            collector.increment_counter("items_processed", 10)
            collector.record_rate_event("processing_rate")

            # Simulate API calls
            collector.record_api_call("ethereum", 1.2, True, 200)
            collector.record_api_call("coingecko", 0.8, True, 200)

            # Simulate cache operations
            collector.record_cache_operation("get", 0.01, True, "redis")
            collector.record_cache_operation("set", 0.02, None, "redis")

            time.sleep(0.01)  # Small delay for timer

        # Record final metrics
        collector.set_gauge("active_connections", 5)

        # Get current state
        current_metrics = await collector.get_current_metrics()
        assert current_metrics["metrics"]["items_processed"]["value"] == 10
        assert current_metrics["metrics"]["active_connections"]["value"] == 5

        # Export in different formats
        prometheus_output = collector.export_metrics("prometheus")
        assert "items_processed 10" in prometheus_output

        json_output = collector.export_metrics("json")
        import json

        json_data = json.loads(json_output)
        assert json_data["metrics"]["active_connections"]["value"] == 5
