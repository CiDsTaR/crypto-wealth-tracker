"""Metrics collection and monitoring system for performance tracking."""

import logging
import time
from collections import defaultdict, deque
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from ..config import AppConfig

logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """Types of metrics that can be collected."""

    COUNTER = "counter"  # Monotonic increasing counter
    GAUGE = "gauge"  # Current value that can go up/down
    HISTOGRAM = "histogram"  # Distribution of values
    TIMER = "timer"  # Duration measurements
    RATE = "rate"  # Rate of events over time


class Metric:
    """Represents a single metric with its metadata and values."""

    def __init__(
        self,
        name: str,
        metric_type: MetricType,
        description: str = "",
        unit: str = "",
        tags: dict[str, str] | None = None,
    ):
        self.name = name
        self.type = metric_type
        self.description = description
        self.unit = unit
        self.tags = tags or {}
        self.created_at = datetime.now(UTC)

        # Value storage based on type
        if metric_type == MetricType.COUNTER or metric_type == MetricType.GAUGE:
            self.value = 0
        elif metric_type in [MetricType.HISTOGRAM, MetricType.TIMER]:
            self.values = deque(maxlen=1000)  # Keep last 1000 values
            self.sum = 0.0
            self.count = 0
            self.min_value = float("inf")
            self.max_value = float("-inf")
        elif metric_type == MetricType.RATE:
            self.events = deque(maxlen=1000)  # Keep last 1000 timestamps

        self.last_updated = self.created_at

    def increment(self, value: float = 1.0) -> None:
        """Increment counter metric."""
        if self.type != MetricType.COUNTER:
            raise ValueError(f"Cannot increment {self.type} metric")

        self.value += value
        self.last_updated = datetime.now(UTC)

    def set(self, value: float) -> None:
        """Set gauge metric value."""
        if self.type != MetricType.GAUGE:
            raise ValueError(f"Cannot set {self.type} metric")

        self.value = value
        self.last_updated = datetime.now(UTC)

    def observe(self, value: float) -> None:
        """Observe value for histogram/timer metric."""
        if self.type not in [MetricType.HISTOGRAM, MetricType.TIMER]:
            raise ValueError(f"Cannot observe {self.type} metric")

        self.values.append(value)
        self.sum += value
        self.count += 1
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.max_value, value)
        self.last_updated = datetime.now(UTC)

    def record_event(self) -> None:
        """Record event for rate metric."""
        if self.type != MetricType.RATE:
            raise ValueError(f"Cannot record event for {self.type} metric")

        self.events.append(datetime.now(UTC))
        self.last_updated = datetime.now(UTC)

    def get_value(self) -> float | dict[str, float]:
        """Get current metric value(s)."""
        if self.type in [MetricType.COUNTER, MetricType.GAUGE]:
            return self.value

        elif self.type in [MetricType.HISTOGRAM, MetricType.TIMER]:
            if self.count == 0:
                return {"count": 0, "sum": 0.0, "avg": 0.0, "min": 0.0, "max": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}

            sorted_values = sorted(self.values)
            return {
                "count": self.count,
                "sum": self.sum,
                "avg": self.sum / self.count,
                "min": self.min_value,
                "max": self.max_value,
                "p50": self._percentile(sorted_values, 50),
                "p95": self._percentile(sorted_values, 95),
                "p99": self._percentile(sorted_values, 99),
            }

        elif self.type == MetricType.RATE:
            now = datetime.now(UTC)

            # Calculate rates for different time windows
            rates = {}

            for window_name, window_seconds in [("1m", 60), ("5m", 300), ("15m", 900), ("1h", 3600)]:
                cutoff = now - timedelta(seconds=window_seconds)
                events_in_window = [e for e in self.events if e > cutoff]
                rate_per_second = len(events_in_window) / window_seconds
                rates[window_name] = rate_per_second

            return rates

    def _percentile(self, sorted_values: list[float], percentile: int) -> float:
        """Calculate percentile value."""
        if not sorted_values:
            return 0.0

        index = (percentile / 100.0) * (len(sorted_values) - 1)
        lower_index = int(index)
        upper_index = min(lower_index + 1, len(sorted_values) - 1)

        if lower_index == upper_index:
            return sorted_values[lower_index]

        # Linear interpolation
        weight = index - lower_index
        return sorted_values[lower_index] * (1 - weight) + sorted_values[upper_index] * weight

    def reset(self) -> None:
        """Reset metric to initial state."""
        if self.type == MetricType.COUNTER or self.type == MetricType.GAUGE:
            self.value = 0
        elif self.type in [MetricType.HISTOGRAM, MetricType.TIMER]:
            self.values.clear()
            self.sum = 0.0
            self.count = 0
            self.min_value = float("inf")
            self.max_value = float("-inf")
        elif self.type == MetricType.RATE:
            self.events.clear()

        self.last_updated = datetime.now(UTC)

    def to_dict(self) -> dict[str, Any]:
        """Convert metric to dictionary representation."""
        return {
            "name": self.name,
            "type": self.type.value,
            "description": self.description,
            "unit": self.unit,
            "tags": self.tags,
            "value": self.get_value(),
            "created_at": self.created_at.isoformat(),
            "last_updated": self.last_updated.isoformat(),
        }


class MetricsCollector:
    """
    Comprehensive metrics collection system for monitoring application performance.

    Collects and tracks:
    - Processing performance metrics
    - API response times and success rates
    - Cache hit rates and performance
    - System resource usage
    - Business metrics (wallets processed, total value, etc.)
    """

    def __init__(self, config: AppConfig):
        """Initialize metrics collector.

        Args:
            config: Application configuration
        """
        self.config = config
        self._metrics: dict[str, Metric] = {}
        self._metric_history: list[dict[str, Any]] = []
        self._max_history_size = 1000

        # Performance tracking
        self._active_timers: dict[str, float] = {}

        # Initialize core metrics
        self._initialize_core_metrics()

    def _initialize_core_metrics(self) -> None:
        """Initialize core application metrics."""

        # Processing metrics
        self.register_metric(
            "wallets_processed_total", MetricType.COUNTER, "Total number of wallets processed", "wallets"
        )

        self.register_metric(
            "wallets_failed_total", MetricType.COUNTER, "Total number of wallets that failed processing", "wallets"
        )

        self.register_metric("wallets_skipped_total", MetricType.COUNTER, "Total number of wallets skipped", "wallets")

        self.register_metric(
            "processing_duration", MetricType.HISTOGRAM, "Time taken to process individual wallets", "seconds"
        )

        self.register_metric(
            "batch_processing_duration", MetricType.HISTOGRAM, "Time taken to process batches", "seconds"
        )

        self.register_metric("total_portfolio_value", MetricType.GAUGE, "Total portfolio value processed", "USD")

        # API metrics
        self.register_metric("api_requests_total", MetricType.COUNTER, "Total API requests made", "requests")

        self.register_metric("api_request_duration", MetricType.HISTOGRAM, "API request response times", "seconds")

        self.register_metric("api_errors_total", MetricType.COUNTER, "Total API errors", "errors")

        self.register_metric("api_rate_limits_total", MetricType.COUNTER, "Total API rate limit hits", "rate_limits")

        # Cache metrics
        self.register_metric("cache_hits_total", MetricType.COUNTER, "Total cache hits", "hits")

        self.register_metric("cache_misses_total", MetricType.COUNTER, "Total cache misses", "misses")

        self.register_metric(
            "cache_operations_duration", MetricType.HISTOGRAM, "Cache operation response times", "seconds"
        )

        # Business metrics
        self.register_metric("active_wallets_count", MetricType.GAUGE, "Number of active wallets found", "wallets")

        self.register_metric("inactive_wallets_count", MetricType.GAUGE, "Number of inactive wallets found", "wallets")

        self.register_metric("processing_rate", MetricType.RATE, "Rate of wallet processing", "wallets/second")

        # System metrics
        self.register_metric("memory_usage", MetricType.GAUGE, "Current memory usage", "bytes")

        self.register_metric("cpu_usage", MetricType.GAUGE, "Current CPU usage", "percent")

    def register_metric(
        self,
        name: str,
        metric_type: MetricType,
        description: str = "",
        unit: str = "",
        tags: dict[str, str] | None = None,
    ) -> Metric:
        """Register a new metric.

        Args:
            name: Metric name
            metric_type: Type of metric
            description: Metric description
            unit: Unit of measurement
            tags: Additional tags

        Returns:
            Created metric object
        """
        if name in self._metrics:
            logger.warning(f"Metric {name} already exists, overwriting")

        metric = Metric(name, metric_type, description, unit, tags)
        self._metrics[name] = metric

        logger.debug(f"Registered metric: {name} ({metric_type.value})")
        return metric

    def get_metric(self, name: str) -> Metric | None:
        """Get metric by name.

        Args:
            name: Metric name

        Returns:
            Metric object or None if not found
        """
        return self._metrics.get(name)

    def increment_counter(self, name: str, value: float = 1.0, tags: dict[str, str] | None = None) -> None:
        """Increment a counter metric.

        Args:
            name: Metric name
            value: Value to increment by
            tags: Additional tags for this measurement
        """
        metric = self._get_or_create_metric(name, MetricType.COUNTER, tags)
        metric.increment(value)

    def set_gauge(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Set a gauge metric value.

        Args:
            name: Metric name
            value: Value to set
            tags: Additional tags for this measurement
        """
        metric = self._get_or_create_metric(name, MetricType.GAUGE, tags)
        metric.set(value)

    def observe_histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Observe a value for histogram metric.

        Args:
            name: Metric name
            value: Value to observe
            tags: Additional tags for this measurement
        """
        metric = self._get_or_create_metric(name, MetricType.HISTOGRAM, tags)
        metric.observe(value)

    def record_timer(self, name: str, duration: float, tags: dict[str, str] | None = None) -> None:
        """Record a timer measurement.

        Args:
            name: Metric name
            duration: Duration in seconds
            tags: Additional tags for this measurement
        """
        metric = self._get_or_create_metric(name, MetricType.TIMER, tags)
        metric.observe(duration)

    def record_rate_event(self, name: str, tags: dict[str, str] | None = None) -> None:
        """Record an event for rate metric.

        Args:
            name: Metric name
            tags: Additional tags for this measurement
        """
        metric = self._get_or_create_metric(name, MetricType.RATE, tags)
        metric.record_event()

    def _get_or_create_metric(self, name: str, metric_type: MetricType, tags: dict[str, str] | None = None) -> Metric:
        """Get existing metric or create new one."""
        metric_key = self._create_metric_key(name, tags)

        if metric_key not in self._metrics:
            self._metrics[metric_key] = Metric(name, metric_type, tags=tags)

        return self._metrics[metric_key]

    def _create_metric_key(self, name: str, tags: dict[str, str] | None = None) -> str:
        """Create unique key for metric with tags."""
        if not tags:
            return name

        tag_string = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}[{tag_string}]"

    def start_timer(self, name: str) -> str:
        """Start a timer for measuring duration.

        Args:
            name: Timer name

        Returns:
            Timer ID for stopping the timer
        """
        timer_id = f"{name}_{id(self)}_{time.time()}"
        self._active_timers[timer_id] = time.time()
        return timer_id

    def stop_timer(self, timer_id: str, metric_name: str | None = None) -> float:
        """Stop a timer and record the duration.

        Args:
            timer_id: Timer ID returned by start_timer
            metric_name: Optional metric name (defaults to timer name)

        Returns:
            Duration in seconds
        """
        if timer_id not in self._active_timers:
            logger.warning(f"Timer {timer_id} not found")
            return 0.0

        start_time = self._active_timers.pop(timer_id)
        duration = time.time() - start_time

        # Extract name from timer_id if metric_name not provided
        if metric_name is None:
            metric_name = timer_id.split("_")[0]

        self.record_timer(metric_name, duration)
        return duration

    def timer_context(self, name: str):
        """Context manager for timing operations.

        Args:
            name: Timer metric name

        Usage:
            with metrics.timer_context("api_request"):
                await make_api_call()
        """

        class TimerContext:
            def __init__(self, collector: "MetricsCollector", metric_name: str):
                self.collector = collector
                self.metric_name = metric_name
                self.timer_id = None

            def __enter__(self):
                self.timer_id = self.collector.start_timer(self.metric_name)
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.collector.stop_timer(self.timer_id, self.metric_name)

        return TimerContext(self, name)

    async def record_processing_run(self, processing_results: Any) -> None:
        """Record metrics from a processing run.

        Args:
            processing_results: Results from wallet processing
        """
        if hasattr(processing_results, "get_summary_dict"):
            results_dict = processing_results.get_summary_dict()
        elif isinstance(processing_results, dict):
            results_dict = processing_results
        else:
            logger.warning("Cannot extract metrics from processing results")
            return

        # Record processing metrics
        if "results" in results_dict:
            results = results_dict["results"]
            self.increment_counter("wallets_processed_total", results.get("processed", 0))
            self.increment_counter("wallets_failed_total", results.get("failed", 0))
            self.increment_counter("wallets_skipped_total", results.get("skipped", 0))

        # Record portfolio values
        if "portfolio_values" in results_dict:
            portfolio = results_dict["portfolio_values"]
            self.set_gauge("total_portfolio_value", portfolio.get("total_usd", 0))

        # Record activity metrics
        if "activity" in results_dict:
            activity = results_dict["activity"]
            self.set_gauge("active_wallets_count", activity.get("active_wallets", 0))
            self.set_gauge("inactive_wallets_count", activity.get("inactive_wallets", 0))

        # Record performance metrics
        if "performance" in results_dict:
            performance = results_dict["performance"]

            total_time = performance.get("total_time_seconds", 0)
            if total_time > 0:
                self.observe_histogram("batch_processing_duration", total_time)

            avg_time = performance.get("average_time_per_wallet", 0)
            if avg_time > 0:
                self.observe_histogram("processing_duration", avg_time)

            # Record API metrics
            api_calls = performance.get("api_calls_total", 0)
            if api_calls > 0:
                self.increment_counter("api_requests_total", api_calls)

            # Record cache metrics
            cache_hit_rate = performance.get("cache_hit_rate", 0)
            if cache_hit_rate > 0:
                # Estimate cache hits/misses from rate
                estimated_hits = api_calls * (cache_hit_rate / 100)
                estimated_misses = api_calls - estimated_hits
                self.increment_counter("cache_hits_total", estimated_hits)
                self.increment_counter("cache_misses_total", estimated_misses)

    def record_api_call(
        self, service: str, duration: float, success: bool = True, status_code: int | None = None
    ) -> None:
        """Record metrics for an API call.

        Args:
            service: Service name (e.g., 'ethereum', 'coingecko')
            duration: Request duration in seconds
            success: Whether the request was successful
            status_code: HTTP status code
        """
        tags = {"service": service}

        if status_code:
            tags["status_code"] = str(status_code)

        # Record API metrics
        self.increment_counter("api_requests_total", tags=tags)
        self.record_timer("api_request_duration", duration, tags=tags)

        if not success:
            self.increment_counter("api_errors_total", tags=tags)

        if status_code == 429:  # Rate limit
            self.increment_counter("api_rate_limits_total", tags=tags)

    def record_cache_operation(
        self, operation: str, duration: float, hit: bool | None = None, backend: str | None = None
    ) -> None:
        """Record metrics for a cache operation.

        Args:
            operation: Operation type ('get', 'set', 'delete')
            duration: Operation duration in seconds
            hit: Whether it was a cache hit (for get operations)
            backend: Cache backend name
        """
        tags = {"operation": operation}

        if backend:
            tags["backend"] = backend

        self.record_timer("cache_operations_duration", duration, tags=tags)

        if hit is not None:
            if hit:
                self.increment_counter("cache_hits_total", tags=tags)
            else:
                self.increment_counter("cache_misses_total", tags=tags)

    async def collect_system_metrics(self) -> None:
        """Collect system resource metrics."""
        try:
            import psutil

            # Memory usage
            memory = psutil.virtual_memory()
            self.set_gauge("memory_usage", memory.used)
            self.set_gauge("memory_usage_percent", memory.percent)

            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            self.set_gauge("cpu_usage", cpu_percent)

            # Process-specific metrics
            process = psutil.Process()
            process_memory = process.memory_info()
            self.set_gauge("process_memory_rss", process_memory.rss)
            self.set_gauge("process_memory_vms", process_memory.vms)

        except ImportError:
            logger.debug("psutil not available, skipping system metrics")
        except Exception as e:
            logger.warning(f"Failed to collect system metrics: {e}")

    async def get_current_metrics(self) -> dict[str, Any]:
        """Get current values of all metrics.

        Returns:
            Dictionary with current metric values
        """
        # Collect fresh system metrics
        await self.collect_system_metrics()

        metrics_data = {}

        for name, metric in self._metrics.items():
            metrics_data[name] = metric.to_dict()

        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "metrics": metrics_data,
            "summary": self._generate_summary(),
        }

    def _generate_summary(self) -> dict[str, Any]:
        """Generate metrics summary."""
        summary = {"total_metrics": len(self._metrics), "metric_types": defaultdict(int), "last_updated": None}

        latest_update = None

        for metric in self._metrics.values():
            summary["metric_types"][metric.type.value] += 1

            if latest_update is None or metric.last_updated > latest_update:
                latest_update = metric.last_updated

        if latest_update:
            summary["last_updated"] = latest_update.isoformat()

        return dict(summary)

    def export_metrics(self, format: str = "prometheus") -> str:
        """Export metrics in specified format.

        Args:
            format: Export format ('prometheus', 'json', 'influxdb')

        Returns:
            Formatted metrics string
        """
        if format.lower() == "prometheus":
            return self._export_prometheus()
        elif format.lower() == "json":
            import json

            return json.dumps(self.get_current_metrics(), indent=2)
        elif format.lower() == "influxdb":
            return self._export_influxdb()
        else:
            raise ValueError(f"Unsupported export format: {format}")

    def _export_prometheus(self) -> str:
        """Export metrics in Prometheus format."""
        lines = []

        for name, metric in self._metrics.items():
            # Add help comment
            if metric.description:
                lines.append(f"# HELP {name} {metric.description}")

            # Add type comment
            prom_type = {
                MetricType.COUNTER: "counter",
                MetricType.GAUGE: "gauge",
                MetricType.HISTOGRAM: "histogram",
                MetricType.TIMER: "histogram",
                MetricType.RATE: "gauge",
            }.get(metric.type, "gauge")

            lines.append(f"# TYPE {name} {prom_type}")

            # Add metric values
            value = metric.get_value()

            if isinstance(value, dict):
                # Histogram/timer metrics
                for suffix, val in value.items():
                    metric_name = f"{name}_{suffix}"
                    lines.append(f"{metric_name} {val}")
            else:
                # Simple metrics
                lines.append(f"{name} {value}")

            lines.append("")  # Empty line between metrics

        return "\n".join(lines)

    def _export_influxdb(self) -> str:
        """Export metrics in InfluxDB line protocol format."""
        lines = []
        timestamp = int(time.time() * 1000000000)  # Nanoseconds

        for name, metric in self._metrics.items():
            tags_str = ""
            if metric.tags:
                tag_pairs = [f"{k}={v}" for k, v in metric.tags.items()]
                tags_str = "," + ",".join(tag_pairs)

            value = metric.get_value()

            if isinstance(value, dict):
                # Multiple fields for histogram/timer
                field_pairs = [f"{k}={v}" for k, v in value.items()]
                fields_str = ",".join(field_pairs)
            else:
                fields_str = f"value={value}"

            line = f"{name}{tags_str} {fields_str} {timestamp}"
            lines.append(line)

        return "\n".join(lines)

    def reset_all_metrics(self) -> None:
        """Reset all metrics to initial state."""
        for metric in self._metrics.values():
            metric.reset()

        logger.info("All metrics reset")

    def reset_metric(self, name: str) -> bool:
        """Reset specific metric.

        Args:
            name: Metric name

        Returns:
            True if metric was reset, False if not found
        """
        if name in self._metrics:
            self._metrics[name].reset()
            logger.info(f"Metric {name} reset")
            return True
        return False

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get summary statistics of metrics collection.

        Returns:
            Summary statistics
        """
        return {
            "total_metrics": len(self._metrics),
            "active_timers": len(self._active_timers),
            "history_size": len(self._metric_history),
            "collection_started": min(m.created_at for m in self._metrics.values()).isoformat()
            if self._metrics
            else None,
            "last_updated": max(m.last_updated for m in self._metrics.values()).isoformat() if self._metrics else None,
        }
