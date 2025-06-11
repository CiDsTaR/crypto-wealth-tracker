"""Performance monitoring and optimization utilities."""

import asyncio
import functools
import gc
import json
import logging
import statistics
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from typing import Any

import psutil

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Container for performance metrics."""

    durations: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    counters: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    gauges: dict[str, float] = field(default_factory=dict)
    timestamps: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))


class Timer:
    """High-precision timer for performance measurement."""

    def __init__(self, collect_stats: bool = False):
        """Initialize timer.

        Args:
            collect_stats: Whether to collect statistics across measurements
        """
        self.collect_stats = collect_stats
        self._start_time = None
        self._end_time = None
        self._measurements = [] if collect_stats else None

    def start(self):
        """Start timing."""
        self._start_time = time.perf_counter()
        self._end_time = None

    def stop(self):
        """Stop timing."""
        if self._start_time is None:
            raise RuntimeError("Timer not started")

        self._end_time = time.perf_counter()

        if self.collect_stats and self._measurements is not None:
            self._measurements.append(self.elapsed)

    def restart(self):
        """Restart timing from zero."""
        self.start()

    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self._start_time is None:
            return 0.0

        end_time = self._end_time or time.perf_counter()
        return end_time - self._start_time

    def get_stats(self) -> dict[str, Any]:
        """Get timing statistics."""
        if not self.collect_stats or not self._measurements:
            return {}

        return {
            "count": len(self._measurements),
            "total": sum(self._measurements),
            "average": statistics.mean(self._measurements),
            "min": min(self._measurements),
            "max": max(self._measurements),
            "median": statistics.median(self._measurements),
        }

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()

    async def __aenter__(self):
        """Async context manager entry."""
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.stop()


class Profiler:
    """Code profiler for measuring execution time of code blocks."""

    def __init__(self):
        """Initialize profiler."""
        self._stats = defaultdict(
            lambda: {"count": 0, "total_time": 0.0, "min_time": float("inf"), "max_time": 0.0, "times": []}
        )
        self._current_stack = []

    @contextmanager
    def profile(self, name: str):
        """Profile a code block.

        Args:
            name: Profile name/identifier
        """
        start_time = time.perf_counter()
        self._current_stack.append(name)

        try:
            yield
        finally:
            end_time = time.perf_counter()
            duration = end_time - start_time

            # Update statistics
            stats = self._stats[name]
            stats["count"] += 1
            stats["total_time"] += duration
            stats["min_time"] = min(stats["min_time"], duration)
            stats["max_time"] = max(stats["max_time"], duration)
            stats["times"].append(duration)

            self._current_stack.pop()

    def get_stats(self) -> dict[str, dict[str, Any]]:
        """Get profiling statistics."""
        result = {}

        for name, stats in self._stats.items():
            if stats["count"] > 0:
                result[name] = {
                    "count": stats["count"],
                    "total_time": stats["total_time"],
                    "average_time": stats["total_time"] / stats["count"],
                    "min_time": stats["min_time"],
                    "max_time": stats["max_time"],
                    "median_time": statistics.median(stats["times"]) if stats["times"] else 0,
                }

        return result

    def get_hierarchy(self) -> dict[str, Any]:
        """Get hierarchical profiling structure."""
        # Simple implementation - could be enhanced for true hierarchy
        hierarchy = {}
        for name in self._stats:
            parts = name.split(".")
            current = hierarchy
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {"children": {}}
                current = current[part]["children"]
            current[parts[-1]] = self._stats[name]

        return hierarchy

    def reset(self):
        """Reset all profiling data."""
        self._stats.clear()


class AsyncProfiler:
    """Async profiler for measuring async operations."""

    def __init__(self):
        """Initialize async profiler."""
        self._stats = defaultdict(lambda: {"count": 0, "total_time": 0.0, "times": []})

    @asynccontextmanager
    async def profile(self, name: str):
        """Profile an async code block.

        Args:
            name: Profile name/identifier
        """
        start_time = time.perf_counter()

        try:
            yield
        finally:
            end_time = time.perf_counter()
            duration = end_time - start_time

            # Update statistics
            stats = self._stats[name]
            stats["count"] += 1
            stats["total_time"] += duration
            stats["times"].append(duration)

    def get_stats(self) -> dict[str, dict[str, Any]]:
        """Get async profiling statistics."""
        result = {}

        for name, stats in self._stats.items():
            if stats["count"] > 0:
                result[name] = {
                    "count": stats["count"],
                    "total_time": stats["total_time"],
                    "average_time": stats["total_time"] / stats["count"],
                    "times": stats["times"],
                }

        return result


class MemoryTracker:
    """Memory usage tracker."""

    def __init__(self):
        """Initialize memory tracker."""
        self._process = psutil.Process()
        self._stats = defaultdict(lambda: {"start_memory": 0, "end_memory": 0, "peak_memory": 0, "measurements": []})

    def get_current_usage(self) -> float:
        """Get current memory usage in MB."""
        return self._process.memory_info().rss / 1024 / 1024

    @contextmanager
    def track(self, name: str):
        """Track memory usage for a code block.

        Args:
            name: Tracking identifier
        """
        gc.collect()  # Force garbage collection
        start_memory = self.get_current_usage()

        stats = self._stats[name]
        stats["start_memory"] = start_memory
        peak_memory = start_memory

        try:
            yield
        finally:
            end_memory = self.get_current_usage()
            stats["end_memory"] = end_memory
            stats["peak_memory"] = max(peak_memory, end_memory)
            stats["measurements"].append(
                {"start": start_memory, "end": end_memory, "delta": end_memory - start_memory, "timestamp": time.time()}
            )

    def get_stats(self) -> dict[str, dict[str, Any]]:
        """Get memory tracking statistics."""
        return dict(self._stats)


class MemoryLeakDetector:
    """Memory leak detection utility."""

    def __init__(self, sample_interval: float = 1.0, max_samples: int = 100):
        """Initialize memory leak detector.

        Args:
            sample_interval: Time between memory samples in seconds
            max_samples: Maximum number of samples to keep
        """
        self.sample_interval = sample_interval
        self.max_samples = max_samples
        self._samples = deque(maxlen=max_samples)
        self._monitoring = False
        self._monitor_task = None

    async def start_monitoring(self):
        """Start memory monitoring."""
        if self._monitoring:
            return

        self._monitoring = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def stop_monitoring(self):
        """Stop memory monitoring."""
        self._monitoring = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

    async def _monitor_loop(self):
        """Memory monitoring loop."""
        process = psutil.Process()

        while self._monitoring:
            try:
                memory_mb = process.memory_info().rss / 1024 / 1024
                self._samples.append({"memory_mb": memory_mb, "timestamp": time.time()})
                await asyncio.sleep(self.sample_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Error in memory monitoring: {e}")

    def check_for_leaks(self) -> dict[str, Any]:
        """Check for memory leaks based on collected samples."""
        if len(self._samples) < 10:
            return {"leak_detected": False, "reason": "Insufficient samples"}

        # Simple leak detection: check if memory is consistently growing
        recent_samples = list(self._samples)[-10:]
        memory_values = [s["memory_mb"] for s in recent_samples]

        # Calculate trend
        x = list(range(len(memory_values)))
        slope = statistics.correlation(x, memory_values) if len(set(memory_values)) > 1 else 0

        leak_detected = slope > 0.7  # Strong positive correlation
        memory_growth = memory_values[-1] - memory_values[0]

        return {
            "leak_detected": leak_detected,
            "memory_growth": memory_growth,
            "growth_rate": memory_growth / len(recent_samples),
            "correlation": slope,
            "current_memory": memory_values[-1],
            "sample_count": len(self._samples),
        }


class ObjectTracker:
    """Object creation and deletion tracker."""

    def __init__(self):
        """Initialize object tracker."""
        self._stats = defaultdict(lambda: {"objects_created": 0, "creation_times": []})

    @contextmanager
    def track_objects(self, name: str):
        """Track object creation in a code block.

        Args:
            name: Tracking identifier
        """
        # Simple implementation - in practice you'd use gc.get_objects()
        # or more sophisticated tracking
        start_time = time.time()

        try:
            yield
        finally:
            end_time = time.time()
            duration = end_time - start_time

            # Estimate objects created (simplified)
            # In real implementation, you'd track actual object creation
            estimated_objects = max(1, int(duration * 1000))  # Rough estimate

            stats = self._stats[name]
            stats["objects_created"] += estimated_objects
            stats["creation_times"].append(duration)

    def get_stats(self) -> dict[str, dict[str, Any]]:
        """Get object tracking statistics."""
        return dict(self._stats)


class MetricsCollector:
    """Performance metrics collector."""

    def __init__(self):
        """Initialize metrics collector."""
        self._metrics = PerformanceMetrics()

    def record_duration(self, name: str, duration: float):
        """Record a duration metric.

        Args:
            name: Metric name
            duration: Duration in seconds
        """
        self._metrics.durations[name].append(duration)
        self._metrics.timestamps[name].append(time.time())

    def record_counter(self, name: str, value: int = 1):
        """Record a counter metric.

        Args:
            name: Counter name
            value: Value to add
        """
        self._metrics.counters[name] += value

    def record_gauge(self, name: str, value: float):
        """Record a gauge metric.

        Args:
            name: Gauge name
            value: Current value
        """
        self._metrics.gauges[name] = value

    def get_metrics(self) -> dict[str, Any]:
        """Get all collected metrics."""
        result = {"durations": {}, "counters": dict(self._metrics.counters), "gauges": dict(self._metrics.gauges)}

        # Calculate duration statistics
        for name, durations in self._metrics.durations.items():
            if durations:
                result["durations"][name] = {
                    "count": len(durations),
                    "total": sum(durations),
                    "average": statistics.mean(durations),
                    "min": min(durations),
                    "max": max(durations),
                    "median": statistics.median(durations),
                }

        return result

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus format."""
        lines = []

        # Duration metrics
        for name, durations in self._metrics.durations.items():
            if durations:
                avg_duration = statistics.mean(durations)
                lines.append(f"# HELP {name}_duration_seconds Duration of {name}")
                lines.append(f"# TYPE {name}_duration_seconds gauge")
                lines.append(f"{name}_duration_seconds {avg_duration}")

        # Counter metrics
        for name, value in self._metrics.counters.items():
            lines.append(f"# HELP {name}_total Total count of {name}")
            lines.append(f"# TYPE {name}_total counter")
            lines.append(f"{name}_total {value}")

        # Gauge metrics
        for name, value in self._metrics.gauges.items():
            lines.append(f"# HELP {name} Current value of {name}")
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")

        return "\n".join(lines)

    def export_json(self) -> str:
        """Export metrics in JSON format."""
        return json.dumps(self.get_metrics(), indent=2)

    def reset(self):
        """Reset all metrics."""
        self._metrics = PerformanceMetrics()


class RealTimeMetrics:
    """Real-time metrics streaming."""

    def __init__(self):
        """Initialize real-time metrics."""
        self._callbacks = []
        self._metrics_queue = asyncio.Queue()

    def add_callback(self, callback: Callable):
        """Add metrics callback.

        Args:
            callback: Function to call with metric data
        """
        self._callbacks.append(callback)

    async def record_async(self, name: str, duration: float):
        """Record async metric and notify callbacks.

        Args:
            name: Metric name
            duration: Duration value
        """
        metric_data = {"name": name, "value": duration, "timestamp": time.time(), "type": "duration"}

        # Notify callbacks
        for callback in self._callbacks:
            try:
                callback(metric_data)
            except Exception as e:
                logger.warning(f"Error in metrics callback: {e}")


def memoize(maxsize: int = 128):
    """Memoization decorator for performance optimization.

    Args:
        maxsize: Maximum cache size

    Returns:
        Decorated function
    """

    def decorator(func):
        cache = {}
        cache_order = deque()

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key
            key = str(args) + str(sorted(kwargs.items()))

            if key in cache:
                return cache[key]

            # Execute function
            result = func(*args, **kwargs)

            # Add to cache
            if len(cache) >= maxsize:
                # Remove oldest entry
                oldest_key = cache_order.popleft()
                del cache[oldest_key]

            cache[key] = result
            cache_order.append(key)

            return result

        return wrapper

    return decorator


def async_memoize(maxsize: int = 64):
    """Async memoization decorator.

    Args:
        maxsize: Maximum cache size

    Returns:
        Decorated async function
    """

    def decorator(func):
        cache = {}
        cache_order = deque()

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Create cache key
            key = str(args) + str(sorted(kwargs.items()))

            if key in cache:
                return cache[key]

            # Execute function
            result = await func(*args, **kwargs)

            # Add to cache
            if len(cache) >= maxsize:
                # Remove oldest entry
                oldest_key = cache_order.popleft()
                del cache[oldest_key]

            cache[key] = result
            cache_order.append(key)

            return result

        return wrapper

    return decorator


class BatchProcessor:
    """Batch processor for performance optimization."""

    def __init__(self, batch_size: int, max_wait_time: float, handler: Callable):
        """Initialize batch processor.

        Args:
            batch_size: Maximum items per batch
            max_wait_time: Maximum wait time before processing
            handler: Function to process batches
        """
        self.batch_size = batch_size
        self.max_wait_time = max_wait_time
        self.handler = handler
        self._batch = []
        self._last_process_time = time.time()
        self._lock = threading.Lock()

    def add_item(self, item: Any) -> Any:
        """Add item to batch for processing.

        Args:
            item: Item to process

        Returns:
            Processing result
        """
        with self._lock:
            self._batch.append(item)

            # Check if we should process the batch
            should_process = (
                len(self._batch) >= self.batch_size or time.time() - self._last_process_time >= self.max_wait_time
            )

            if should_process:
                return self._process_batch()

        return None

    def _process_batch(self):
        """Process current batch."""
        if not self._batch:
            return None

        batch_to_process = self._batch.copy()
        self._batch.clear()
        self._last_process_time = time.time()

        try:
            return self.handler(batch_to_process)
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return None


class AsyncBatchProcessor:
    """Async batch processor."""

    def __init__(self, batch_size: int, max_wait_time: float, handler: Callable):
        """Initialize async batch processor.

        Args:
            batch_size: Maximum items per batch
            max_wait_time: Maximum wait time before processing
            handler: Async function to process batches
        """
        self.batch_size = batch_size
        self.max_wait_time = max_wait_time
        self.handler = handler
        self._batch = []
        self._last_process_time = time.time()
        self._lock = asyncio.Lock()

    async def add_item(self, item: Any) -> Any:
        """Add item to batch for processing.

        Args:
            item: Item to process

        Returns:
            Processing result
        """
        async with self._lock:
            self._batch.append(item)

            # Check if we should process the batch
            should_process = (
                len(self._batch) >= self.batch_size or time.time() - self._last_process_time >= self.max_wait_time
            )

            if should_process:
                return await self._process_batch()

        return None

    async def _process_batch(self):
        """Process current batch."""
        if not self._batch:
            return None

        batch_to_process = self._batch.copy()
        self._batch.clear()
        self._last_process_time = time.time()

        try:
            return await self.handler(batch_to_process)
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return None


class ConnectionPool:
    """Connection pool for performance optimization."""

    def __init__(self, factory: Callable, max_size: int = 10, timeout: float = 5.0):
        """Initialize connection pool.

        Args:
            factory: Function to create new connections
            max_size: Maximum pool size
            timeout: Connection timeout
        """
        self.factory = factory
        self.max_size = max_size
        self.timeout = timeout
        self._pool = []
        self._in_use = set()
        self._lock = threading.Lock()

    def get_connection(self):
        """Get connection from pool."""
        with self._lock:
            # Try to get existing connection
            if self._pool:
                conn = self._pool.pop()
                self._in_use.add(conn)
                return conn

            # Create new connection if under limit
            if len(self._in_use) < self.max_size:
                conn = self.factory()
                self._in_use.add(conn)
                return conn

            raise RuntimeError("Connection pool exhausted")

    def return_connection(self, conn):
        """Return connection to pool."""
        with self._lock:
            if conn in self._in_use:
                self._in_use.remove(conn)
                self._pool.append(conn)

    def close_all(self):
        """Close all connections."""
        with self._lock:
            for conn in list(self._pool) + list(self._in_use):
                if hasattr(conn, "close"):
                    try:
                        conn.close()
                    except Exception:
                        pass

            self._pool.clear()
            self._in_use.clear()


class PerformanceReporter:
    """Performance reporting utility."""

    def __init__(self):
        """Initialize performance reporter."""
        self._measurements = defaultdict(list)

    def add_measurement(self, operation: str, duration: float, success: bool, **metadata):
        """Add performance measurement.

        Args:
            operation: Operation name
            duration: Duration in seconds
            success: Whether operation succeeded
            **metadata: Additional metadata
        """
        self._measurements[operation].append(
            {"duration": duration, "success": success, "timestamp": time.time(), **metadata}
        )

    def generate_report(self) -> dict[str, Any]:
        """Generate performance report."""
        report = {}

        for operation, measurements in self._measurements.items():
            if not measurements:
                continue

            durations = [m["duration"] for m in measurements]
            successes = [m for m in measurements if m["success"]]

            report[operation] = {
                "total_calls": len(measurements),
                "success_count": len(successes),
                "success_rate": len(successes) / len(measurements),
                "average_duration": statistics.mean(durations),
                "min_duration": min(durations),
                "max_duration": max(durations),
                "median_duration": statistics.median(durations),
                "total_duration": sum(durations),
            }

        return report


class PerformanceAlerts:
    """Performance alerting system."""

    def __init__(self):
        """Initialize performance alerts."""
        self._thresholds = {}
        self._handlers = []

    def add_handler(self, handler: Callable):
        """Add alert handler.

        Args:
            handler: Function to handle alerts
        """
        self._handlers.append(handler)

    def set_threshold(self, metric: str, max_value: float | None = None, min_value: float | None = None):
        """Set performance threshold.

        Args:
            metric: Metric name
            max_value: Maximum allowed value
            min_value: Minimum allowed value
        """
        self._thresholds[metric] = {"max_value": max_value, "min_value": min_value}

    def check_metric(self, metric: str, value: float):
        """Check metric against thresholds.

        Args:
            metric: Metric name
            value: Metric value
        """
        if metric not in self._thresholds:
            return

        threshold = self._thresholds[metric]
        alert_triggered = False
        reason = ""

        if threshold["max_value"] is not None and value > threshold["max_value"]:
            alert_triggered = True
            reason = f"Value {value} exceeds maximum {threshold['max_value']}"

        if threshold["min_value"] is not None and value < threshold["min_value"]:
            alert_triggered = True
            reason = f"Value {value} below minimum {threshold['min_value']}"

        if alert_triggered:
            alert_data = {
                "metric": metric,
                "value": value,
                "threshold": threshold,
                "reason": reason,
                "timestamp": time.time(),
            }

            for handler in self._handlers:
                try:
                    handler(alert_data)
                except Exception as e:
                    logger.error(f"Error in alert handler: {e}")


class TrendAnalyzer:
    """Performance trend analysis."""

    def __init__(self):
        """Initialize trend analyzer."""
        self._data_points = defaultdict(list)

    def add_data_point(self, metric: str, timestamp: float, value: float):
        """Add data point for trend analysis.

        Args:
            metric: Metric name
            timestamp: Data timestamp
            value: Metric value
        """
        self._data_points[metric].append({"timestamp": timestamp, "value": value})

    def analyze_trends(self) -> dict[str, Any]:
        """Analyze trends for all metrics."""
        trends = {}

        for metric, points in self._data_points.items():
            if len(points) < 2:
                continue

            # Sort by timestamp
            points.sort(key=lambda x: x["timestamp"])

            # Calculate trend
            values = [p["value"] for p in points]
            x = list(range(len(values)))

            if len(set(values)) > 1:
                correlation = statistics.correlation(x, values)

                if correlation > 0.7:
                    direction = "increasing"
                elif correlation < -0.7:
                    direction = "decreasing"
                else:
                    direction = "stable"
            else:
                correlation = 0
                direction = "stable"

            trends[metric] = {
                "direction": direction,
                "correlation": correlation,
                "slope": correlation,  # Simplified
                "start_value": values[0],
                "end_value": values[-1],
                "change_percent": ((values[-1] - values[0]) / values[0] * 100) if values[0] != 0 else 0,
            }

        return trends


class BottleneckDetector:
    """Performance bottleneck detection."""

    def __init__(self):
        """Initialize bottleneck detector."""
        self._measurements = []

    def add_measurement(
        self, component: str, duration: float, cpu_usage: float = 0.0, memory_usage: float = 0.0, **metrics
    ):
        """Add performance measurement.

        Args:
            component: Component name
            duration: Duration in seconds
            cpu_usage: CPU usage (0-1)
            memory_usage: Memory usage (0-1)
            **metrics: Additional metrics
        """
        self._measurements.append(
            {
                "component": component,
                "duration": duration,
                "cpu_usage": cpu_usage,
                "memory_usage": memory_usage,
                "timestamp": time.time(),
                **metrics,
            }
        )

    def detect_bottlenecks(self) -> list[dict[str, Any]]:
        """Detect performance bottlenecks."""
        if not self._measurements:
            return []

        # Group by component
        by_component = defaultdict(list)
        for measurement in self._measurements:
            by_component[measurement["component"]].append(measurement)

        bottlenecks = []

        for component, measurements in by_component.items():
            avg_duration = statistics.mean([m["duration"] for m in measurements])
            avg_cpu = statistics.mean([m["cpu_usage"] for m in measurements])
            avg_memory = statistics.mean([m["memory_usage"] for m in measurements])

            # Simple bottleneck detection criteria
            is_bottleneck = (
                avg_duration > 0.1  # Slow operations
                or avg_cpu > 0.8  # High CPU usage
                or avg_memory > 0.8  # High memory usage
            )

            if is_bottleneck:
                bottlenecks.append(
                    {
                        "component": component,
                        "average_duration": avg_duration,
                        "average_cpu_usage": avg_cpu,
                        "average_memory_usage": avg_memory,
                        "severity": "high" if avg_duration > 1.0 else "medium",
                    }
                )

        # Sort by severity
        bottlenecks.sort(key=lambda x: x["average_duration"], reverse=True)

        return bottlenecks


class BenchmarkRunner:
    """Benchmark execution utility."""

    def __init__(self):
        """Initialize benchmark runner."""
        pass

    def benchmark(self, functions: dict[str, Callable], iterations: int = 10) -> dict[str, dict[str, Any]]:
        """Run benchmarks on functions.

        Args:
            functions: Dictionary of name -> function
            iterations: Number of iterations per function

        Returns:
            Benchmark results
        """
        results = {}

        for name, func in functions.items():
            times = []

            for _ in range(iterations):
                start_time = time.perf_counter()
                try:
                    result = func()
                    success = True
                except Exception:
                    result = None
                    success = False
                end_time = time.perf_counter()

                times.append(end_time - start_time)

            results[name] = {
                "iterations": iterations,
                "average": statistics.mean(times),
                "min": min(times),
                "max": max(times),
                "median": statistics.median(times),
                "total": sum(times),
            }

        return results


class AsyncBenchmarkRunner:
    """Async benchmark execution utility."""

    def __init__(self):
        """Initialize async benchmark runner."""
        pass

    async def benchmark(self, functions: dict[str, Callable], iterations: int = 10) -> dict[str, dict[str, Any]]:
        """Run async benchmarks on functions.

        Args:
            functions: Dictionary of name -> async function
            iterations: Number of iterations per function

        Returns:
            Benchmark results
        """
        results = {}

        for name, func in functions.items():
            times = []

            for _ in range(iterations):
                start_time = time.perf_counter()
                try:
                    result = await func()
                    success = True
                except Exception:
                    result = None
                    success = False
                end_time = time.perf_counter()

                times.append(end_time - start_time)

            results[name] = {
                "iterations": iterations,
                "average": statistics.mean(times),
                "min": min(times),
                "max": max(times),
                "median": statistics.median(times),
                "total": sum(times),
            }

        return results


class LoadTester:
    """Load testing utility."""

    def __init__(self, target_function: Callable):
        """Initialize load tester.

        Args:
            target_function: Function to load test
        """
        self.target_function = target_function

    def run_load_test(
        self, concurrent_users: int, duration_seconds: float, ramp_up_time: float = 0.0
    ) -> dict[str, Any]:
        """Run load test.

        Args:
            concurrent_users: Number of concurrent users
            duration_seconds: Test duration
            ramp_up_time: Time to ramp up users

        Returns:
            Load test results
        """
        results = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "response_times": [],
            "start_time": time.time(),
        }

        def worker():
            end_time = time.time() + duration_seconds

            while time.time() < end_time:
                start = time.perf_counter()
                try:
                    self.target_function()
                    success = True
                except Exception:
                    success = False
                end = time.perf_counter()

                response_time = end - start

                results["total_requests"] += 1
                if success:
                    results["successful_requests"] += 1
                else:
                    results["failed_requests"] += 1
                results["response_times"].append(response_time)

        # Start workers
        threads = []
        for i in range(concurrent_users):
            if ramp_up_time > 0:
                time.sleep(ramp_up_time / concurrent_users)

            thread = threading.Thread(target=worker)
            thread.start()
            threads.append(thread)

        # Wait for completion
        for thread in threads:
            thread.join()

        # Calculate final statistics
        if results["response_times"]:
            results["average_response_time"] = statistics.mean(results["response_times"])
            results["min_response_time"] = min(results["response_times"])
            results["max_response_time"] = max(results["response_times"])

        total_time = time.time() - results["start_time"]
        results["requests_per_second"] = results["total_requests"] / total_time if total_time > 0 else 0
        results["error_rate"] = (
            results["failed_requests"] / results["total_requests"] if results["total_requests"] > 0 else 0
        )

        return results


class AsyncLoadTester:
    """Async load testing utility."""

    def __init__(self, target_function: Callable):
        """Initialize async load tester.

        Args:
            target_function: Async function to load test
        """
        self.target_function = target_function

    async def run_load_test(self, concurrent_users: int, duration_seconds: float) -> dict[str, Any]:
        """Run async load test.

        Args:
            concurrent_users: Number of concurrent users
            duration_seconds: Test duration

        Returns:
            Load test results
        """
        results = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "response_times": [],
            "start_time": time.time(),
        }

        async def worker():
            end_time = time.time() + duration_seconds

            while time.time() < end_time:
                start = time.perf_counter()
                try:
                    await self.target_function()
                    success = True
                except Exception:
                    success = False
                end = time.perf_counter()

                response_time = end - start

                results["total_requests"] += 1
                if success:
                    results["successful_requests"] += 1
                else:
                    results["failed_requests"] += 1
                results["response_times"].append(response_time)

        # Start workers
        tasks = [asyncio.create_task(worker()) for _ in range(concurrent_users)]

        # Wait for completion
        await asyncio.gather(*tasks, return_exceptions=True)

        # Calculate final statistics
        if results["response_times"]:
            results["average_response_time"] = statistics.mean(results["response_times"])
            results["min_response_time"] = min(results["response_times"])
            results["max_response_time"] = max(results["response_times"])

        total_time = time.time() - results["start_time"]
        results["requests_per_second"] = results["total_requests"] / total_time if total_time > 0 else 0
        results["error_rate"] = (
            results["failed_requests"] / results["total_requests"] if results["total_requests"] > 0 else 0
        )

        return results


class PerformanceMonitor:
    """Comprehensive performance monitoring."""

    def __init__(self):
        """Initialize performance monitor."""
        self._operations = {}
        self._profiler = Profiler()

    @asynccontextmanager
    async def track_operation(self, name: str):
        """Track an operation's performance.

        Args:
            name: Operation name
        """
        start_time = time.perf_counter()

        try:
            yield
        finally:
            duration = time.perf_counter() - start_time

            if name not in self._operations:
                self._operations[name] = []

            self._operations[name].append({"duration": duration, "timestamp": time.time()})

    def generate_report(self) -> dict[str, Any]:
        """Generate comprehensive performance report."""
        report = {}

        for name, operations in self._operations.items():
            if not operations:
                continue

            durations = [op["duration"] for op in operations]

            report[name] = {
                "count": len(operations),
                "total_duration": sum(durations),
                "average_duration": statistics.mean(durations),
                "min_duration": min(durations),
                "max_duration": max(durations),
                "median_duration": statistics.median(durations),
            }

        return report


class RegressionDetector:
    """Performance regression detection."""

    def __init__(self):
        """Initialize regression detector."""
        self._baseline_data = defaultdict(list)
        self._current_data = defaultdict(list)

    def add_baseline(self, operation: str, duration: float):
        """Add baseline performance data.

        Args:
            operation: Operation name
            duration: Duration in seconds
        """
        self._baseline_data[operation].append(duration)

    def add_current(self, operation: str, duration: float):
        """Add current performance data.

        Args:
            operation: Operation name
            duration: Duration in seconds
        """
        self._current_data[operation].append(duration)

    def detect_regressions(self, threshold_percent: float = 20.0) -> dict[str, Any]:
        """Detect performance regressions.

        Args:
            threshold_percent: Regression threshold percentage

        Returns:
            Regression detection results
        """
        results = {}

        for operation in self._baseline_data:
            if operation not in self._current_data:
                continue

            baseline_avg = statistics.mean(self._baseline_data[operation])
            current_avg = statistics.mean(self._current_data[operation])

            if baseline_avg == 0:
                continue

            percentage_change = ((current_avg - baseline_avg) / baseline_avg) * 100
            regression_detected = percentage_change > threshold_percent

            results[operation] = {
                "baseline_average": baseline_avg,
                "current_average": current_avg,
                "percentage_change": percentage_change,
                "regression_detected": regression_detected,
                "threshold_percent": threshold_percent,
            }

        return results


def profile(name: str):
    """Decorator for profiling functions.

    Args:
        name: Profile name

    Returns:
        Decorated function
    """

    def decorator(func):
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.perf_counter()
                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    duration = time.perf_counter() - start_time
                    logger.debug(f"Profile {name}: {duration:.4f}s")

            return async_wrapper
        else:

            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.perf_counter()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    duration = time.perf_counter() - start_time
                    logger.debug(f"Profile {name}: {duration:.4f}s")

            return sync_wrapper

    return decorator
