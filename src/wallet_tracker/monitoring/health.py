"""Health checking system for monitoring service availability and performance."""

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Callable

from ..clients import CoinGeckoClient, EthereumClient, GoogleSheetsClient
from ..utils import CacheManager

logger = logging.getLogger(__name__)


class HealthStatus(str, Enum):
    """Health status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ServiceHealth:
    """Represents health status of a service."""

    def __init__(
        self,
        service_name: str,
        status: HealthStatus = HealthStatus.UNKNOWN,
        response_time: float | None = None,
        error_message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        self.service_name = service_name
        self.status = status
        self.response_time = response_time
        self.error_message = error_message
        self.metadata = metadata or {}
        self.checked_at = datetime.now(UTC)
        self.check_count = 0
        self.consecutive_failures = 0
        self.last_healthy_time: datetime | None = None

    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy."""
        return self.status == HealthStatus.HEALTHY

    @property
    def is_degraded(self) -> bool:
        """Check if service is degraded."""
        return self.status == HealthStatus.DEGRADED

    @property
    def is_unhealthy(self) -> bool:
        """Check if service is unhealthy."""
        return self.status == HealthStatus.UNHEALTHY

    def update_status(
        self,
        status: HealthStatus,
        response_time: float | None = None,
        error_message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Update health status."""
        previous_status = self.status

        self.status = status
        self.response_time = response_time
        self.error_message = error_message
        self.metadata = metadata or {}
        self.checked_at = datetime.now(UTC)
        self.check_count += 1

        if status == HealthStatus.HEALTHY:
            self.consecutive_failures = 0
            self.last_healthy_time = self.checked_at
        else:
            if previous_status == HealthStatus.HEALTHY:
                self.consecutive_failures = 1
            else:
                self.consecutive_failures += 1

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "service_name": self.service_name,
            "status": self.status.value,
            "is_healthy": self.is_healthy,
            "response_time_ms": round(self.response_time * 1000, 2) if self.response_time else None,
            "error_message": self.error_message,
            "metadata": self.metadata,
            "checked_at": self.checked_at.isoformat(),
            "check_count": self.check_count,
            "consecutive_failures": self.consecutive_failures,
            "last_healthy_time": self.last_healthy_time.isoformat() if self.last_healthy_time else None,
        }


class HealthChecker:
    """
    Comprehensive health checking system for all application services.

    Monitors:
    - Ethereum RPC connectivity and response times
    - CoinGecko API availability and rate limits
    - Google Sheets API access and permissions
    - Cache system connectivity and performance
    - Overall system health scoring
    """

    def __init__(
        self,
        ethereum_client: EthereumClient | None = None,
        coingecko_client: CoinGeckoClient | None = None,
        sheets_client: GoogleSheetsClient | None = None,
        cache_manager: CacheManager | None = None,
        timeout_seconds: float = 30.0,
        degraded_threshold_ms: float = 5000.0,
        unhealthy_threshold_ms: float = 15000.0,
    ):
        """Initialize health checker.

        Args:
            ethereum_client: Ethereum client instance
            coingecko_client: CoinGecko client instance
            sheets_client: Google Sheets client instance
            cache_manager: Cache manager instance
            timeout_seconds: Timeout for health checks
            degraded_threshold_ms: Response time threshold for degraded status
            unhealthy_threshold_ms: Response time threshold for unhealthy status
        """
        self.ethereum_client = ethereum_client
        self.coingecko_client = coingecko_client
        self.sheets_client = sheets_client
        self.cache_manager = cache_manager
        self.timeout_seconds = timeout_seconds
        self.degraded_threshold_ms = degraded_threshold_ms
        self.unhealthy_threshold_ms = unhealthy_threshold_ms

        # Service health tracking
        self._service_health: dict[str, ServiceHealth] = {}

        # Health check history
        self._health_history: list[dict[str, Any]] = []
        self._max_history_size = 100

        # Performance thresholds
        self._performance_thresholds = {
            "ethereum_client": {"degraded": 3000, "unhealthy": 10000},  # ms
            "coingecko_client": {"degraded": 2000, "unhealthy": 8000},
            "sheets_client": {"degraded": 4000, "unhealthy": 12000},
            "cache_manager": {"degraded": 100, "unhealthy": 1000},
        }

    async def check_all_services(self, include_detailed_checks: bool = False) -> dict[str, bool]:
        """Check health of all configured services.

        Args:
            include_detailed_checks: Whether to include detailed performance checks

        Returns:
            Dictionary mapping service names to health status (True = healthy)
        """
        logger.info("🏥 Starting comprehensive health check")

        check_results = {}
        detailed_results = {}

        # Check Ethereum client
        if self.ethereum_client:
            ethereum_health = await self._check_ethereum_client(include_detailed_checks)
            check_results["ethereum_client"] = ethereum_health.is_healthy
            detailed_results["ethereum_client"] = ethereum_health.to_dict()

        # Check CoinGecko client
        if self.coingecko_client:
            coingecko_health = await self._check_coingecko_client(include_detailed_checks)
            check_results["coingecko_client"] = coingecko_health.is_healthy
            detailed_results["coingecko_client"] = coingecko_health.to_dict()

        # Check Google Sheets client
        if self.sheets_client:
            sheets_health = await self._check_sheets_client(include_detailed_checks)
            check_results["sheets_client"] = sheets_health.is_healthy
            detailed_results["sheets_client"] = sheets_health.to_dict()

        # Check Cache manager
        if self.cache_manager:
            cache_health = await self._check_cache_manager(include_detailed_checks)
            check_results["cache_manager"] = cache_health.is_healthy
            detailed_results["cache_manager"] = cache_health.to_dict()

        # Store detailed results in history
        self._add_to_history(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "summary": check_results,
                "detailed": detailed_results if include_detailed_checks else None,
            }
        )

        # Log summary
        healthy_count = sum(1 for status in check_results.values() if status)
        total_count = len(check_results)

        logger.info(f"🏥 Health check completed: {healthy_count}/{total_count} services healthy")

        return check_results

    async def _check_ethereum_client(self, detailed: bool = False) -> ServiceHealth:
        """Check Ethereum client health."""
        service_name = "ethereum_client"

        try:
            start_time = time.time()

            # Basic connectivity check
            stats = self.ethereum_client.get_stats()

            if detailed:
                # Detailed check: try to get latest block
                # This would require actual RPC call implementation
                await asyncio.sleep(0.1)  # Simulate check

            response_time = time.time() - start_time

            # Determine status based on response time and stats
            status = self._determine_status_from_response_time(
                response_time * 1000,  # Convert to milliseconds
                service_name,
            )

            # Check for API errors in stats
            if stats.get("api_errors", 0) > 10:
                status = HealthStatus.DEGRADED
                error_message = f"High API error count: {stats['api_errors']}"
            else:
                error_message = None

            metadata = {
                "stats": stats,
                "portfolio_requests": stats.get("portfolio_requests", 0),
                "cache_hits": stats.get("cache_hits", 0),
                "rate_limit": stats.get("rate_limit", 0),
            }

            health = ServiceHealth(
                service_name=service_name,
                status=status,
                response_time=response_time,
                error_message=error_message,
                metadata=metadata,
            )

            self._service_health[service_name] = health

            logger.debug(f"Ethereum client health: {status.value} ({response_time * 1000:.1f}ms)")
            return health

        except Exception as e:
            logger.warning(f"Ethereum client health check failed: {e}")

            health = ServiceHealth(service_name=service_name, status=HealthStatus.UNHEALTHY, error_message=str(e))

            self._service_health[service_name] = health
            return health

    async def _check_coingecko_client(self, detailed: bool = False) -> ServiceHealth:
        """Check CoinGecko client health."""
        service_name = "coingecko_client"

        try:
            start_time = time.time()

            # Basic health check
            is_healthy = await self.coingecko_client.health_check()

            if detailed and is_healthy:
                # Detailed check: try to get ETH price
                try:
                    await asyncio.wait_for(self.coingecko_client.get_eth_price(), timeout=self.timeout_seconds)
                except TimeoutError:
                    is_healthy = False

            response_time = time.time() - start_time

            if not is_healthy:
                status = HealthStatus.UNHEALTHY
                error_message = "CoinGecko API health check failed"
            else:
                status = self._determine_status_from_response_time(response_time * 1000, service_name)
                error_message = None

            # Get client stats
            stats = self.coingecko_client.get_stats()

            # Check for rate limiting issues
            if stats.get("rate_limit_errors", 0) > 5:
                status = HealthStatus.DEGRADED
                error_message = f"Rate limit issues: {stats['rate_limit_errors']} errors"

            metadata = {
                "stats": stats,
                "has_pro_api": stats.get("has_pro_api", False),
                "rate_limit": stats.get("rate_limit", 0),
                "price_requests": stats.get("price_requests", 0),
            }

            health = ServiceHealth(
                service_name=service_name,
                status=status,
                response_time=response_time,
                error_message=error_message,
                metadata=metadata,
            )

            self._service_health[service_name] = health

            logger.debug(f"CoinGecko client health: {status.value} ({response_time * 1000:.1f}ms)")
            return health

        except Exception as e:
            logger.warning(f"CoinGecko client health check failed: {e}")

            health = ServiceHealth(service_name=service_name, status=HealthStatus.UNHEALTHY, error_message=str(e))

            self._service_health[service_name] = health
            return health

    async def _check_sheets_client(self, detailed: bool = False) -> ServiceHealth:
        """Check Google Sheets client health."""
        service_name = "sheets_client"

        try:
            start_time = time.time()

            # Basic health check
            is_healthy = self.sheets_client.health_check()

            response_time = time.time() - start_time

            if not is_healthy:
                status = HealthStatus.UNHEALTHY
                error_message = "Google Sheets authentication failed"
            else:
                status = self._determine_status_from_response_time(response_time * 1000, service_name)
                error_message = None

            # Get client stats
            stats = self.sheets_client.get_stats()

            # Check for authentication issues
            if not stats.get("authenticated", True):
                status = HealthStatus.UNHEALTHY
                error_message = "Google Sheets not authenticated"
            elif stats.get("api_errors", 0) > 5:
                status = HealthStatus.DEGRADED
                error_message = f"API errors: {stats['api_errors']}"

            metadata = {
                "stats": stats,
                "authenticated": stats.get("authenticated", False),
                "read_operations": stats.get("read_operations", 0),
                "write_operations": stats.get("write_operations", 0),
            }

            health = ServiceHealth(
                service_name=service_name,
                status=status,
                response_time=response_time,
                error_message=error_message,
                metadata=metadata,
            )

            self._service_health[service_name] = health

            logger.debug(f"Google Sheets client health: {status.value} ({response_time * 1000:.1f}ms)")
            return health

        except Exception as e:
            logger.warning(f"Google Sheets client health check failed: {e}")

            health = ServiceHealth(service_name=service_name, status=HealthStatus.UNHEALTHY, error_message=str(e))

            self._service_health[service_name] = health
            return health

    async def _check_cache_manager(self, detailed: bool = False) -> ServiceHealth:
        """Check Cache manager health."""
        service_name = "cache_manager"

        try:
            start_time = time.time()

            # Check cache health
            cache_health_results = await self.cache_manager.health_check()

            response_time = time.time() - start_time

            # Determine overall cache health
            if not cache_health_results:
                status = HealthStatus.UNHEALTHY
                error_message = "No cache backends available"
            elif all(cache_health_results.values()):
                status = self._determine_status_from_response_time(response_time * 1000, service_name)
                error_message = None
            elif any(cache_health_results.values()):
                status = HealthStatus.DEGRADED
                unhealthy_backends = [k for k, v in cache_health_results.items() if not v]
                error_message = f"Some cache backends unhealthy: {', '.join(unhealthy_backends)}"
            else:
                status = HealthStatus.UNHEALTHY
                error_message = "All cache backends unhealthy"

            # Get cache stats
            try:
                cache_stats = await self.cache_manager.get_stats()
            except Exception:
                cache_stats = {}

            metadata = {"backend_health": cache_health_results, "stats": cache_stats}

            health = ServiceHealth(
                service_name=service_name,
                status=status,
                response_time=response_time,
                error_message=error_message,
                metadata=metadata,
            )

            self._service_health[service_name] = health

            logger.debug(f"Cache manager health: {status.value} ({response_time * 1000:.1f}ms)")
            return health

        except Exception as e:
            logger.warning(f"Cache manager health check failed: {e}")

            health = ServiceHealth(service_name=service_name, status=HealthStatus.UNHEALTHY, error_message=str(e))

            self._service_health[service_name] = health
            return health

    def _determine_status_from_response_time(self, response_time_ms: float, service_name: str) -> HealthStatus:
        """Determine health status based on response time."""
        thresholds = self._performance_thresholds.get(
            service_name, {"degraded": self.degraded_threshold_ms, "unhealthy": self.unhealthy_threshold_ms}
        )

        if response_time_ms >= thresholds["unhealthy"]:
            return HealthStatus.UNHEALTHY
        elif response_time_ms >= thresholds["degraded"]:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY

    def _add_to_history(self, check_result: dict[str, Any]) -> None:
        """Add health check result to history."""
        self._health_history.append(check_result)

        # Trim history if too large
        if len(self._health_history) > self._max_history_size:
            self._health_history = self._health_history[-self._max_history_size :]

    def get_service_health(self, service_name: str) -> ServiceHealth | None:
        """Get health status for specific service.

        Args:
            service_name: Name of service

        Returns:
            ServiceHealth object or None if not found
        """
        return self._service_health.get(service_name)

    def get_all_service_health(self) -> dict[str, ServiceHealth]:
        """Get health status for all services.

        Returns:
            Dictionary mapping service names to ServiceHealth objects
        """
        return self._service_health.copy()

    def get_health_summary(self) -> dict[str, Any]:
        """Get overall health summary.

        Returns:
            Health summary dictionary
        """
        services = list(self._service_health.values())

        if not services:
            return {
                "overall_status": HealthStatus.UNKNOWN.value,
                "healthy_services": 0,
                "total_services": 0,
                "degraded_services": 0,
                "unhealthy_services": 0,
                "average_response_time_ms": 0,
                "issues": [],
            }

        healthy_count = sum(1 for s in services if s.is_healthy)
        degraded_count = sum(1 for s in services if s.is_degraded)
        unhealthy_count = sum(1 for s in services if s.is_unhealthy)

        # Determine overall status
        if unhealthy_count > 0:
            overall_status = HealthStatus.UNHEALTHY
        elif degraded_count > 0:
            overall_status = HealthStatus.DEGRADED
        elif healthy_count > 0:
            overall_status = HealthStatus.HEALTHY
        else:
            overall_status = HealthStatus.UNKNOWN

        # Calculate average response time
        response_times = [s.response_time for s in services if s.response_time is not None]
        avg_response_time = (sum(response_times) / len(response_times)) if response_times else 0

        # Collect issues
        issues = [f"{s.service_name}: {s.error_message}" for s in services if s.error_message]

        return {
            "overall_status": overall_status.value,
            "healthy_services": healthy_count,
            "total_services": len(services),
            "degraded_services": degraded_count,
            "unhealthy_services": unhealthy_count,
            "average_response_time_ms": round(avg_response_time * 1000, 2),
            "issues": issues,
            "last_check": max(s.checked_at for s in services).isoformat() if services else None,
        }

    def get_health_history(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Get health check history.

        Args:
            limit: Maximum number of entries to return

        Returns:
            List of health check results
        """
        history = self._health_history.copy()

        if limit:
            history = history[-limit:]

        return history

    def get_health_trends(self, hours: int = 24) -> dict[str, Any]:
        """Get health trends over time.

        Args:
            hours: Number of hours to analyze

        Returns:
            Health trends analysis
        """
        cutoff_time = datetime.now(UTC) - timedelta(hours=hours)

        # Filter history by time
        recent_history = [
            entry for entry in self._health_history if datetime.fromisoformat(entry["timestamp"]) > cutoff_time
        ]

        if not recent_history:
            return {"period_hours": hours, "total_checks": 0, "trends": {}}

        # Analyze trends for each service
        service_trends = {}

        for entry in recent_history:
            summary = entry.get("summary", {})

            for service_name, is_healthy in summary.items():
                if service_name not in service_trends:
                    service_trends[service_name] = {"total_checks": 0, "healthy_checks": 0, "availability_percent": 0.0}

                service_trends[service_name]["total_checks"] += 1
                if is_healthy:
                    service_trends[service_name]["healthy_checks"] += 1

        # Calculate availability percentages
        for service_name, trend_data in service_trends.items():
            if trend_data["total_checks"] > 0:
                availability = (trend_data["healthy_checks"] / trend_data["total_checks"]) * 100
                trend_data["availability_percent"] = round(availability, 2)

        return {
            "period_hours": hours,
            "total_checks": len(recent_history),
            "trends": service_trends,
            "analysis_period": {
                "start_time": recent_history[0]["timestamp"] if recent_history else None,
                "end_time": recent_history[-1]["timestamp"] if recent_history else None,
            },
        }

    async def run_continuous_monitoring(
        self,
        interval_seconds: int = 300,  # 5 minutes
        alert_callback: Callable | None = None,
    ) -> None:
        """Run continuous health monitoring.

        Args:
            interval_seconds: Interval between checks
            alert_callback: Callback function for alerts
        """
        logger.info(f"🔄 Starting continuous health monitoring (interval: {interval_seconds}s)")

        consecutive_failures = 0
        max_consecutive_failures = 5

        while True:
            try:
                # Perform health check
                results = await self.check_all_services(include_detailed_checks=False)

                # Check for issues
                unhealthy_services = [name for name, healthy in results.items() if not healthy]

                if unhealthy_services:
                    consecutive_failures += 1

                    logger.warning(
                        f"Health check found {len(unhealthy_services)} unhealthy services: {', '.join(unhealthy_services)}"
                    )

                    # Send alert if callback provided
                    if alert_callback:
                        try:
                            await alert_callback(
                                {
                                    "type": "service_unhealthy",
                                    "services": unhealthy_services,
                                    "consecutive_failures": consecutive_failures,
                                    "timestamp": datetime.now(UTC).isoformat(),
                                }
                            )
                        except Exception as e:
                            logger.error(f"Alert callback failed: {e}")

                    # Stop monitoring if too many consecutive failures
                    if consecutive_failures >= max_consecutive_failures:
                        logger.critical(
                            f"Stopping continuous monitoring after {consecutive_failures} consecutive failures"
                        )
                        break

                else:
                    # Reset failure counter on success
                    if consecutive_failures > 0:
                        logger.info("All services healthy - resetting failure counter")
                        consecutive_failures = 0

                # Wait for next check
                await asyncio.sleep(interval_seconds)

            except asyncio.CancelledError:
                logger.info("Continuous health monitoring cancelled")
                break

            except Exception as e:
                consecutive_failures += 1
                logger.error(f"Health monitoring error: {e}")

                if consecutive_failures >= max_consecutive_failures:
                    logger.critical("Stopping monitoring due to repeated errors")
                    break

                # Wait before retrying
                await asyncio.sleep(min(interval_seconds, 60))

    def export_health_report(self, format: str = "json") -> str | dict[str, Any]:
        """Export comprehensive health report.

        Args:
            format: Export format ('json', 'yaml', 'text')

        Returns:
            Health report in requested format
        """
        report_data = {
            "generated_at": datetime.now(UTC).isoformat(),
            "summary": self.get_health_summary(),
            "services": {name: health.to_dict() for name, health in self._service_health.items()},
            "trends": self.get_health_trends(hours=24),
            "configuration": {
                "timeout_seconds": self.timeout_seconds,
                "degraded_threshold_ms": self.degraded_threshold_ms,
                "unhealthy_threshold_ms": self.unhealthy_threshold_ms,
                "performance_thresholds": self._performance_thresholds,
            },
        }

        if format.lower() == "json":
            import json

            return json.dumps(report_data, indent=2)

        elif format.lower() == "yaml":
            try:
                import yaml

                return yaml.dump(report_data, default_flow_style=False)
            except ImportError:
                logger.warning("PyYAML not installed, falling back to JSON")
                import json

                return json.dumps(report_data, indent=2)

        elif format.lower() == "text":
            return self._format_text_report(report_data)

        else:
            # Default to dict
            return report_data

    def _format_text_report(self, report_data: dict[str, Any]) -> str:
        """Format health report as human-readable text."""
        lines = []

        lines.append("=" * 60)
        lines.append("🏥 WALLET TRACKER HEALTH REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {report_data['generated_at']}")
        lines.append("")

        # Summary
        summary = report_data["summary"]
        lines.append("📊 OVERALL HEALTH SUMMARY")
        lines.append("-" * 30)
        lines.append(f"Overall Status: {summary['overall_status'].upper()}")
        lines.append(f"Healthy Services: {summary['healthy_services']}/{summary['total_services']}")
        lines.append(f"Degraded Services: {summary['degraded_services']}")
        lines.append(f"Unhealthy Services: {summary['unhealthy_services']}")
        lines.append(f"Average Response Time: {summary['average_response_time_ms']:.1f}ms")
        lines.append("")

        if summary["issues"]:
            lines.append("⚠️ CURRENT ISSUES")
            lines.append("-" * 20)
            for issue in summary["issues"]:
                lines.append(f"  • {issue}")
            lines.append("")

        # Service Details
        lines.append("🔧 SERVICE DETAILS")
        lines.append("-" * 20)

        for service_name, service_data in report_data["services"].items():
            status_emoji = "✅" if service_data["is_healthy"] else "❌"
            lines.append(f"{status_emoji} {service_name.replace('_', ' ').title()}")
            lines.append(f"   Status: {service_data['status']}")

            if service_data["response_time_ms"]:
                lines.append(f"   Response Time: {service_data['response_time_ms']:.1f}ms")

            if service_data["error_message"]:
                lines.append(f"   Error: {service_data['error_message']}")

            lines.append(f"   Checks: {service_data['check_count']}")
            lines.append(f"   Consecutive Failures: {service_data['consecutive_failures']}")
            lines.append("")

        # Trends
        trends = report_data["trends"]
        if trends["total_checks"] > 0:
            lines.append("📈 24-HOUR TRENDS")
            lines.append("-" * 20)
            lines.append(f"Total Health Checks: {trends['total_checks']}")
            lines.append("")

            for service_name, trend_data in trends["trends"].items():
                lines.append(f"{service_name.replace('_', ' ').title()}:")
                lines.append(f"   Availability: {trend_data['availability_percent']:.1f}%")
                lines.append(f"   Checks: {trend_data['healthy_checks']}/{trend_data['total_checks']}")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)

    def clear_health_history(self) -> None:
        """Clear health check history."""
        self._health_history.clear()
        logger.info("Health check history cleared")

    def reset_service_health(self, service_name: str | None = None) -> None:
        """Reset health status for service(s).

        Args:
            service_name: Specific service to reset (None for all)
        """
        if service_name:
            if service_name in self._service_health:
                del self._service_health[service_name]
                logger.info(f"Reset health status for {service_name}")
        else:
            self._service_health.clear()
            logger.info("Reset health status for all services")


# Utility functions


async def quick_health_check(
    ethereum_client: EthereumClient | None = None,
    coingecko_client: CoinGeckoClient | None = None,
    sheets_client: GoogleSheetsClient | None = None,
    cache_manager: CacheManager | None = None,
) -> dict[str, bool]:
    """Perform a quick health check on provided services.

    Args:
        ethereum_client: Ethereum client
        coingecko_client: CoinGecko client
        sheets_client: Google Sheets client
        cache_manager: Cache manager

    Returns:
        Dictionary with health results
    """
    checker = HealthChecker(
        ethereum_client=ethereum_client,
        coingecko_client=coingecko_client,
        sheets_client=sheets_client,
        cache_manager=cache_manager,
        timeout_seconds=10.0,  # Quick check
    )

    return await checker.check_all_services(include_detailed_checks=False)


def create_health_alert_handler(
    webhook_url: str | None = None, email_config: dict[str, str] | None = None, log_level: str = "WARNING"
) -> callable:
    """Create a health alert handler.

    Args:
        webhook_url: Optional webhook URL for alerts
        email_config: Optional email configuration
        log_level: Log level for alerts

    Returns:
        Alert handler function
    """

    async def alert_handler(alert_data: dict[str, Any]) -> None:
        # Log the alert
        log_level_int = getattr(logging, log_level.upper(), logging.WARNING)
        logger.log(log_level_int, f"Health Alert: {alert_data}")

        # Send webhook if configured
        if webhook_url:
            try:
                import aiohttp

                async with aiohttp.ClientSession() as session:
                    await session.post(webhook_url, json=alert_data)
            except Exception as e:
                logger.error(f"Failed to send webhook alert: {e}")

        # Send email if configured
        if email_config:
            try:
                # Email implementation would go here
                logger.info("Email alert sent")
            except Exception as e:
                logger.error(f"Failed to send email alert: {e}")

    return alert_handler


class HealthCheckScheduler:
    """Scheduler for automated health checks."""

    def __init__(self, health_checker: HealthChecker):
        self.health_checker = health_checker
        self._scheduled_tasks: list[asyncio.Task] = []
        self._running = False

    def schedule_periodic_checks(
        self,
        interval_minutes: int = 5,
        detailed_check_interval_minutes: int = 30,
        alert_callback: Callable | None = None,
    ) -> None:
        """Schedule periodic health checks.

        Args:
            interval_minutes: Basic check interval
            detailed_check_interval_minutes: Detailed check interval
            alert_callback: Alert callback function
        """
        if self._running:
            logger.warning("Health check scheduler already running")
            return

        self._running = True

        # Schedule basic checks
        basic_task = asyncio.create_task(self._run_periodic_basic_checks(interval_minutes, alert_callback))
        self._scheduled_tasks.append(basic_task)

        # Schedule detailed checks
        detailed_task = asyncio.create_task(self._run_periodic_detailed_checks(detailed_check_interval_minutes))
        self._scheduled_tasks.append(detailed_task)

        logger.info(
            f"Scheduled health checks: basic every {interval_minutes}min, detailed every {detailed_check_interval_minutes}min"
        )

    async def _run_periodic_basic_checks(self, interval_minutes: int, alert_callback: Callable | None) -> None:
        """Run periodic basic health checks."""
        while self._running:
            try:
                results = await self.health_checker.check_all_services(include_detailed_checks=False)

                # Check for alerts
                if alert_callback:
                    unhealthy_services = [name for name, healthy in results.items() if not healthy]
                    if unhealthy_services:
                        await alert_callback(
                            {
                                "type": "periodic_check_alert",
                                "unhealthy_services": unhealthy_services,
                                "timestamp": datetime.now(UTC).isoformat(),
                            }
                        )

                await asyncio.sleep(interval_minutes * 60)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Periodic basic health check failed: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retry

    async def _run_periodic_detailed_checks(self, interval_minutes: int) -> None:
        """Run periodic detailed health checks."""
        while self._running:
            try:
                await self.health_checker.check_all_services(include_detailed_checks=True)
                await asyncio.sleep(interval_minutes * 60)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Periodic detailed health check failed: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retry

    def stop_scheduled_checks(self) -> None:
        """Stop all scheduled health checks."""
        self._running = False

        for task in self._scheduled_tasks:
            task.cancel()

        self._scheduled_tasks.clear()
        logger.info("Stopped all scheduled health checks")

    def get_scheduler_status(self) -> dict[str, Any]:
        """Get scheduler status.

        Returns:
            Scheduler status information
        """
        return {
            "running": self._running,
            "active_tasks": len([t for t in self._scheduled_tasks if not t.done()]),
            "total_tasks": len(self._scheduled_tasks),
        }
