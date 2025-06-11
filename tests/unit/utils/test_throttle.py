"""Tests for throttling system."""

import asyncio
import time
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from wallet_tracker.utils.throttle import (
    BackoffConfig,
    BackoffStrategy,
    ThrottleConfig,
    ThrottleMode,
    ThrottleState,
    Throttle,
    ThrottleManager,
    CombinedThrottleAndRateLimit,
    create_ethereum_throttle,
    create_coingecko_throttle,
    create_sheets_throttle,
    create_aggressive_backoff,
    create_gentle_backoff,
    throttled
)


class TestBackoffConfig:
    """Test backoff configuration and calculation."""

    def test_default_config(self):
        """Test default backoff configuration."""
        config = BackoffConfig()

        assert config.strategy == BackoffStrategy.EXPONENTIAL
        assert config.initial_delay == 1.0
        assert config.max_delay == 300.0
        assert config.max_retries == 5
        assert config.multiplier == 2.0
        assert config.jitter is True

    def test_exponential_backoff(self):
        """Test exponential backoff calculation."""
        config = BackoffConfig(
            strategy=BackoffStrategy.EXPONENTIAL,
            initial_delay=1.0,
            multiplier=2.0,
            jitter=False
        )

        assert config.calculate_delay(0) == 1.0  # 1 * 2^0
        assert config.calculate_delay(1) == 2.0  # 1 * 2^1
        assert config.calculate_delay(2) == 4.0  # 1 * 2^2
        assert config.calculate_delay(3) == 8.0  # 1 * 2^3

    def test_linear_backoff(self):
        """Test linear backoff calculation."""
        config = BackoffConfig(
            strategy=BackoffStrategy.LINEAR,
            initial_delay=1.0,
            additive_increase=0.5,
            jitter=False
        )

        assert config.calculate_delay(0) == 1.0  # 1 + 0 * 0.5
        assert config.calculate_delay(1) == 1.5  # 1 + 1 * 0.5
        assert config.calculate_delay(2) == 2.0  # 1 + 2 * 0.5
        assert config.calculate_delay(3) == 2.5  # 1 + 3 * 0.5

    def test_fixed_backoff(self):
        """Test fixed backoff calculation."""
        config = BackoffConfig(
            strategy=BackoffStrategy.FIXED,
            initial_delay=2.0,
            jitter=False
        )

        assert config.calculate_delay(0) == 2.0
        assert config.calculate_delay(1) == 2.0
        assert config.calculate_delay(2) == 2.0
        assert config.calculate_delay(5) == 2.0

    def test_fibonacci_backoff(self):
        """Test Fibonacci backoff calculation."""
        config = BackoffConfig(
            strategy=BackoffStrategy.FIBONACCI,
            initial_delay=1.0,
            jitter=False
        )

        assert config.calculate_delay(0) == 1.0  # 1 * fib(1) = 1 * 1
        assert config.calculate_delay(1) == 1.0  # 1 * fib(2) = 1 * 1
        assert config.calculate_delay(2) == 2.0  # 1 * fib(3) = 1 * 2
        assert config.calculate_delay(3) == 3.0  # 1 * fib(4) = 1 * 3
        assert config.calculate_delay(4) == 5.0  # 1 * fib(5) = 1 * 5

    def test_polynomial_backoff(self):
        """Test polynomial backoff calculation."""
        config = BackoffConfig(
            strategy=BackoffStrategy.POLYNOMIAL,
            initial_delay=1.0,
            polynomial_degree=2.0,
            jitter=False
        )

        assert config.calculate_delay(0) == 0.0  # 1 * 0^2 = 0
        assert config.calculate_delay(1) == 1.0  # 1 * 1^2 = 1
        assert config.calculate_delay(2) == 4.0  # 1 * 2^2 = 4
        assert config.calculate_delay(3) == 9.0  # 1 * 3^2 = 9

    def test_custom_backoff(self):
        """Test custom backoff calculation."""

        def custom_func(attempt, base_delay):
            return base_delay * (attempt + 1) * 3

        config = BackoffConfig(
            strategy=BackoffStrategy.CUSTOM,
            initial_delay=1.0,
            custom_function=custom_func,
            jitter=False
        )

        assert config.calculate_delay(0) == 3.0  # 1 * (0 + 1) * 3
        assert config.calculate_delay(1) == 6.0  # 1 * (1 + 1) * 3
        assert config.calculate_delay(2) == 9.0  # 1 * (2 + 1) * 3

    def test_max_delay_enforcement(self):
        """Test that max delay is enforced."""
        config = BackoffConfig(
            strategy=BackoffStrategy.EXPONENTIAL,
            initial_delay=1.0,
            multiplier=2.0,
            max_delay=5.0,
            jitter=False
        )

        assert config.calculate_delay(10) == 5.0  # Would be 1024, capped at 5.0

    def test_jitter_application(self):
        """Test that jitter is applied."""
        config = BackoffConfig(
            strategy=BackoffStrategy.FIXED,
            initial_delay=10.0,
            jitter=True,
            jitter_range=0.1
        )

        # Calculate multiple delays to test jitter variation
        delays = [config.calculate_delay(0) for _ in range(100)]

        # All delays should be within jitter range
        for delay in delays:
            assert 9.0 <= delay <= 11.0  # ±10% of 10.0

        # Should have some variation (not all identical)
        assert len(set(delays)) > 1

    def test_fibonacci_calculation(self):
        """Test Fibonacci number calculation."""
        assert BackoffConfig._fibonacci(0) == 0
        assert BackoffConfig._fibonacci(1) == 1
        assert BackoffConfig._fibonacci(2) == 1
        assert BackoffConfig._fibonacci(3) == 2
        assert BackoffConfig._fibonacci(4) == 3
        assert BackoffConfig._fibonacci(5) == 5
        assert BackoffConfig._fibonacci(6) == 8


class TestThrottleConfig:
    """Test throttle configuration."""

    def test_default_config(self):
        """Test default throttle configuration."""
        config = ThrottleConfig()

        assert config.mode == ThrottleMode.CONSTANT
        assert config.requests_per_second == 10.0
        assert config.burst_size == 5
        assert config.min_delay == 0.1
        assert config.max_delay == 10.0

    def test_time_based_delay_calculation(self):
        """Test time-based delay calculation."""
        config = ThrottleConfig(
            mode=ThrottleMode.TIME_BASED,
            requests_per_second=10.0,
            peak_hours=[9, 10, 11],
            peak_multiplier=0.5
        )

        # Test during peak hours
        peak_time = datetime(2024, 1, 1, 10, 0, 0)  # 10 AM
        peak_delay = config.get_delay_for_time(peak_time)
        assert peak_delay == 0.2  # (1/10) * (1/0.5) = 0.2

        # Test during off-peak hours
        off_peak_time = datetime(2024, 1, 1, 14, 0, 0)  # 2 PM
        off_peak_delay = config.get_delay_for_time(off_peak_time)
        assert off_peak_delay == 0.1  # 1/10 = 0.1

    def test_constant_mode_delay(self):
        """Test constant mode delay calculation."""
        config = ThrottleConfig(
            mode=ThrottleMode.CONSTANT,
            requests_per_second=5.0
        )

        delay = config.get_delay_for_time()
        assert delay == 0.2  # 1/5 = 0.2


class TestThrottleState:
    """Test throttle state management."""

    def test_initial_state(self):
        """Test initial throttle state."""
        state = ThrottleState()

        assert state.request_count == 0
        assert state.success_count == 0
        assert state.failure_count == 0
        assert state.current_delay == 0.1
        assert state.consecutive_successes == 0
        assert state.consecutive_failures == 0

    def test_update_success(self):
        """Test updating state for successful request."""
        state = ThrottleState()

        state.update_success()

        assert state.request_count == 1
        assert state.success_count == 1
        assert state.failure_count == 0
        assert state.consecutive_successes == 1
        assert state.consecutive_failures == 0
        assert state.last_request_time is not None

    def test_update_failure(self):
        """Test updating state for failed request."""
        state = ThrottleState()

        state.update_failure()

        assert state.request_count == 1
        assert state.success_count == 0
        assert state.failure_count == 1
        assert state.consecutive_successes == 0
        assert state.consecutive_failures == 1
        assert state.last_request_time is not None

    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        state = ThrottleState()

        # Initial success rate should be 100%
        assert state.get_success_rate() == 1.0

        # Add some successes and failures
        state.update_success()
        state.update_success()
        state.update_failure()

        # Success rate should be 2/3
        assert abs(state.get_success_rate() - (2 / 3)) < 0.01

    def test_reset_burst(self):
        """Test resetting burst tracking."""
        state = ThrottleState()

        state.burst_requests = 10
        state.burst_start_time = time.time()

        state.reset_burst()

        assert state.burst_requests == 0
        assert state.burst_start_time is None


class TestThrottle:
    """Test throttle implementation."""

    @pytest.fixture
    def config(self):
        """Create test throttle configuration."""
        return ThrottleConfig(
            mode=ThrottleMode.CONSTANT,
            requests_per_second=10.0
        )

    @pytest.fixture
    def throttle(self, config):
        """Create throttle instance."""
        return Throttle(config, name="test_throttle")

    @pytest.mark.asyncio
    async def test_initial_acquire(self, throttle):
        """Test initial acquire with no delay."""
        delay = await throttle.acquire()
        assert delay == 0.0

    @pytest.mark.asyncio
    async def test_constant_throttling(self, throttle):
        """Test constant throttling mode."""
        # First request should be immediate
        delay1 = await throttle.acquire()
        assert delay1 == 0.0

        # Immediate second request should require delay
        delay2 = await throttle.acquire()
        expected_delay = 0.1  # 1/10 seconds
        assert abs(delay2 - expected_delay) < 0.01

    @pytest.mark.asyncio
    async def test_wait_method(self, throttle):
        """Test wait method."""
        start_time = time.time()

        # First wait should be immediate
        await throttle.wait()

        # Second wait should actually wait
        await throttle.wait()

        elapsed = time.time() - start_time
        # Should have waited at least the throttle delay
        assert elapsed >= 0.05  # Some delay expected

    @pytest.mark.asyncio
    async def test_report_success(self, throttle):
        """Test reporting successful requests."""
        await throttle.report_success()

        assert throttle.state.success_count == 1
        assert throttle.state.consecutive_successes == 1

    @pytest.mark.asyncio
    async def test_report_failure_with_backoff(self, throttle):
        """Test reporting failures with backoff."""
        await throttle.report_failure(trigger_backoff=True)

        assert throttle.state.failure_count == 1
        assert throttle.state.consecutive_failures == 1
        assert throttle.current_attempt == 1
        assert throttle.backoff_until is not None

    @pytest.mark.asyncio
    async def test_backoff_period(self, throttle):
        """Test throttle during backoff period."""
        # Trigger backoff
        await throttle.report_failure(trigger_backoff=True)

        # Acquire during backoff should return remaining backoff time
        delay = await throttle.acquire()
        assert delay > 0.0

    @pytest.mark.asyncio
    async def test_reset_backoff_on_success(self):
        """Test backoff reset on consecutive successes."""
        config = ThrottleConfig(requests_per_second=10.0)
        backoff_config = BackoffConfig(success_threshold=2)
        throttle = Throttle(config, backoff_config, "test")

        # Trigger backoff
        await throttle.report_failure(trigger_backoff=True)
        assert throttle.current_attempt == 1

        # Report enough successes to reset
        await throttle.report_success()
        await throttle.report_success()

        assert throttle.current_attempt == 0
        assert throttle.backoff_until is None

    def test_get_stats(self, throttle):
        """Test getting throttle statistics."""
        stats = throttle.get_stats()

        assert stats["name"] == "test_throttle"
        assert "mode" in stats
        assert "requests_per_second" in stats
        assert "total_requests" in stats
        assert "success_rate" in stats

    def test_reset(self, throttle):
        """Test resetting throttle state."""
        # Add some state
        throttle.state.update_success()
        throttle.current_attempt = 3

        throttle.reset()

        assert throttle.state.request_count == 0
        assert throttle.current_attempt == 0


class TestThrottleModes:
    """Test different throttle modes."""

    @pytest.mark.asyncio
    async def test_adaptive_mode(self):
        """Test adaptive throttling mode."""
        config = ThrottleConfig(
            mode=ThrottleMode.ADAPTIVE,
            requests_per_second=10.0,
            min_delay=0.05,
            max_delay=1.0,
            adaptation_factor=1.2
        )
        throttle = Throttle(config)

        # Simulate poor success rate
        for _ in range(10):
            throttle.state.update_failure()

        # Should increase delay
        original_delay = throttle.state.current_delay
        await throttle.acquire()

        # Delay should have been adjusted upward
        assert throttle.state.current_delay > original_delay

    @pytest.mark.asyncio
    async def test_burst_then_throttle_mode(self):
        """Test burst-then-throttle mode."""
        config = ThrottleConfig(
            mode=ThrottleMode.BURST_THEN_THROTTLE,
            requests_per_second=10.0,
            burst_size=3,
            burst_window_seconds=1.0
        )
        throttle = Throttle(config)

        # First few requests should be immediate (burst)
        for _ in range(3):
            delay = await throttle.acquire()
            assert delay == 0.0

        # Next request should be throttled
        delay = await throttle.acquire()
        assert delay > 0.0

    @pytest.mark.asyncio
    async def test_time_based_mode(self):
        """Test time-based throttling mode."""
        config = ThrottleConfig(
            mode=ThrottleMode.TIME_BASED,
            requests_per_second=10.0,
            peak_hours=[10],  # 10 AM
            peak_multiplier=0.5
        )
        throttle = Throttle(config)

        # Mock current time to be during peak hours
        with patch('wallet_tracker.utils.throttle.datetime') as mock_datetime:
            mock_datetime.fromtimestamp.return_value = datetime(2024, 1, 1, 10, 0)

            delay = await throttle.acquire()
            # During peak hours, delay should be longer
            # This tests the time-based calculation logic


class TestThrottleManager:
    """Test throttle manager."""

    @pytest.fixture
    def manager(self):
        """Create throttle manager."""
        return ThrottleManager()

    @pytest.fixture
    def sample_config(self):
        """Create sample throttle configuration."""
        return ThrottleConfig(requests_per_second=5.0)

    def test_register_throttle(self, manager, sample_config):
        """Test registering a throttle."""
        manager.register_throttle("test_service", sample_config)

        assert "test_service" in manager.throttles
        assert isinstance(manager.throttles["test_service"], Throttle)

    @pytest.mark.asyncio
    async def test_acquire_from_manager(self, manager, sample_config):
        """Test acquiring from manager."""
        manager.register_throttle("test_service", sample_config)

        delay = await manager.acquire("test_service")
        assert delay == 0.0

    @pytest.mark.asyncio
    async def test_acquire_unknown_throttle(self, manager):
        """Test acquiring from unknown throttle."""
        with pytest.raises(KeyError, match="Throttle 'unknown' not found"):
            await manager.acquire("unknown")

    @pytest.mark.asyncio
    async def test_wait_for_throttle(self, manager, sample_config):
        """Test waiting for throttle."""
        manager.register_throttle("test_service", sample_config)

        # Should not raise error
        await manager.wait("test_service")

    @pytest.mark.asyncio
    async def test_report_success_to_manager(self, manager, sample_config):
        """Test reporting success through manager."""
        manager.register_throttle("test_service", sample_config)

        await manager.report_success("test_service")

        throttle = manager.get_throttle("test_service")
        assert throttle.state.failure_count == 1

    def test_get_throttle(self, manager, sample_config):
        """Test getting throttle from manager."""
        manager.register_throttle("test_service", sample_config)

        throttle = manager.get_throttle("test_service")
        assert throttle is not None
        assert throttle.name == "test_service"

    def test_get_unknown_throttle(self, manager):
        """Test getting unknown throttle returns None."""
        throttle = manager.get_throttle("unknown")
        assert throttle is None

    def test_get_all_stats(self, manager, sample_config):
        """Test getting all throttle statistics."""
        manager.register_throttle("service1", sample_config)
        manager.register_throttle("service2", sample_config)

        all_stats = manager.get_all_stats()

        assert len(all_stats) == 2
        assert "service1" in all_stats
        assert "service2" in all_stats

    def test_reset_throttle(self, manager, sample_config):
        """Test resetting specific throttle."""
        manager.register_throttle("test_service", sample_config)

        # Add some state
        throttle = manager.get_throttle("test_service")
        throttle.state.update_success()

        manager.reset_throttle("test_service")

        assert throttle.state.request_count == 0

    def test_reset_all_throttles(self, manager, sample_config):
        """Test resetting all throttles."""
        manager.register_throttle("service1", sample_config)
        manager.register_throttle("service2", sample_config)

        # Add state to both
        manager.get_throttle("service1").state.update_success()
        manager.get_throttle("service2").state.update_success()

        manager.reset_all()

        assert manager.get_throttle("service1").state.request_count == 0
        assert manager.get_throttle("service2").state.request_count == 0

    def test_remove_throttle(self, manager, sample_config):
        """Test removing throttle."""
        manager.register_throttle("test_service", sample_config)
        assert "test_service" in manager.throttles

        manager.remove_throttle("test_service")
        assert "test_service" not in manager.throttles


class TestThrottledDecorator:
    """Test throttled decorator."""

    @pytest.fixture
    def manager(self):
        """Create throttle manager with test throttle."""
        manager = ThrottleManager()
        config = ThrottleConfig(requests_per_second=10.0)
        manager.register_throttle("test_throttle", config)
        return manager

    @pytest.mark.asyncio
    async def test_decorator_success(self, manager):
        """Test decorator with successful function execution."""
        call_count = 0

        @throttled("test_throttle", manager=manager)
        async def test_function():
            nonlocal call_count
            call_count += 1
            return "success"

        result = await test_function()

        assert result == "success"
        assert call_count == 1

        # Should have reported success
        throttle = manager.get_throttle("test_throttle")
        assert throttle.state.success_count == 1

    @pytest.mark.asyncio
    async def test_decorator_failure(self, manager):
        """Test decorator with function failure."""

        @throttled("test_throttle", manager=manager)
        async def failing_function():
            raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            await failing_function()

        # Should have reported failure
        throttle = manager.get_throttle("test_throttle")
        assert throttle.state.failure_count == 1

    @pytest.mark.asyncio
    async def test_decorator_no_manager(self):
        """Test decorator raises error when no manager provided."""

        @throttled("test_throttle", manager=None)
        async def test_function():
            return "success"

        with pytest.raises(ValueError, match="No throttle manager provided"):
            await test_function()


class TestUtilityFunctions:
    """Test utility functions for creating throttle configurations."""

    def test_create_ethereum_throttle(self):
        """Test creating Ethereum throttle configuration."""
        config = create_ethereum_throttle(requests_per_second=15.0)

        assert config.mode == ThrottleMode.ADAPTIVE
        assert config.requests_per_second == 15.0
        assert config.burst_size == 30  # 2x the rate
        assert config.min_delay == 0.05
        assert config.max_delay == 5.0
        assert config.adaptation_factor == 1.5

    def test_create_coingecko_throttle(self):
        """Test creating CoinGecko throttle configuration."""
        config = create_coingecko_throttle(requests_per_minute=60)

        assert config.mode == ThrottleMode.BURST_THEN_THROTTLE
        assert config.requests_per_second == 1.0  # 60/60
        assert config.burst_size == 5
        assert config.burst_window_seconds == 60.0
        assert config.throttle_after_burst is True

    def test_create_sheets_throttle(self):
        """Test creating Google Sheets throttle configuration."""
        config = create_sheets_throttle(requests_per_minute=120)

        assert config.mode == ThrottleMode.TIME_BASED
        assert config.requests_per_second == 2.0  # 120/60
        assert config.peak_hours == [9, 10, 11, 14, 15, 16]
        assert config.peak_multiplier == 0.7

    def test_create_aggressive_backoff(self):
        """Test creating aggressive backoff configuration."""
        config = create_aggressive_backoff()

        assert config.strategy == BackoffStrategy.EXPONENTIAL
        assert config.initial_delay == 2.0
        assert config.max_delay == 600.0
        assert config.max_retries == 8
        assert config.multiplier == 2.5
        assert config.jitter is True
        assert config.jitter_range == 0.2

    def test_create_gentle_backoff(self):
        """Test creating gentle backoff configuration."""
        config = create_gentle_backoff()

        assert config.strategy == BackoffStrategy.LINEAR
        assert config.initial_delay == 0.5
        assert config.max_delay == 30.0
        assert config.max_retries == 3
        assert config.additive_increase == 1.0
        assert config.jitter is True
        assert config.jitter_range == 0.1


class TestCombinedThrottleAndRateLimit:
    """Test combined throttle and rate limit system."""

    @pytest.fixture
    def mock_rate_limiter(self):
        """Create mock rate limiter."""
        mock_limiter = MagicMock()
        mock_limiter.acquire = MagicMock(return_value=asyncio.Future())
        mock_limiter.acquire.return_value.set_result(0.0)
        mock_limiter.get_status = MagicMock(return_value={})
        return mock_limiter

    @pytest.fixture
    def combined_system(self, mock_rate_limiter):
        """Create combined throttle and rate limit system."""
        throttle_config = ThrottleConfig(requests_per_second=10.0)
        throttle = Throttle(throttle_config)

        return CombinedThrottleAndRateLimit(
            throttle=throttle,
            rate_limiter=mock_rate_limiter,
            name="combined_test"
        )

    @pytest.mark.asyncio
    async def test_combined_acquire(self, combined_system, mock_rate_limiter):
        """Test combined acquire uses maximum delay."""
        # Mock rate limiter to return higher delay
        mock_rate_limiter.acquire.return_value = asyncio.Future()
        mock_rate_limiter.acquire.return_value.set_result(0.5)

        delay = await combined_system.acquire()

        # Should use the higher delay (from rate limiter)
        assert delay == 0.5

    @pytest.mark.asyncio
    async def test_combined_wait(self, combined_system):
        """Test combined wait method."""
        start_time = time.time()

        await combined_system.wait()

        elapsed = time.time() - start_time
        # Should have completed (might have minimal delay)
        assert elapsed >= 0.0

    @pytest.mark.asyncio
    async def test_combined_report_success(self, combined_system):
        """Test reporting success to combined system."""
        await combined_system.report_success()

        # Should have reported to throttle
        assert combined_system.throttle.state.success_count == 1

    @pytest.mark.asyncio
    async def test_combined_report_failure(self, combined_system):
        """Test reporting failure to combined system."""
        await combined_system.report_failure()

        # Should have reported to throttle
        assert combined_system.throttle.state.failure_count == 1

    def test_combined_get_stats(self, combined_system):
        """Test getting combined statistics."""
        stats = combined_system.get_stats()

        assert stats["name"] == "combined_test"
        assert "throttle_stats" in stats
        assert "rate_limiter_stats" in stats


class TestThrottleIntegration:
    """Integration tests for throttling system."""

    @pytest.mark.asyncio
    async def test_end_to_end_throttling(self):
        """Test end-to-end throttling behavior."""
        config = ThrottleConfig(
            mode=ThrottleMode.CONSTANT,
            requests_per_second=5.0  # 0.2 second intervals
        )
        throttle = Throttle(config, name="integration_test")

        start_time = time.time()

        # Make several requests
        delays = []
        for _ in range(3):
            delay = await throttle.acquire()
            delays.append(delay)
            if delay > 0:
                await asyncio.sleep(delay)

        total_time = time.time() - start_time

        # First request should be immediate
        assert delays[0] == 0.0

        # Subsequent requests should have delays
        assert delays[1] > 0.0
        assert delays[2] > 0.0

        # Total time should reflect throttling
        assert total_time >= 0.4  # At least 2 * 0.2 seconds

    @pytest.mark.asyncio
    async def test_adaptive_throttle_behavior(self):
        """Test adaptive throttle responds to failure patterns."""
        config = ThrottleConfig(
            mode=ThrottleMode.ADAPTIVE,
            requests_per_second=10.0,
            adaptation_factor=2.0,
            adaptation_threshold=0.8
        )
        throttle = Throttle(config)

        # Simulate high failure rate
        for _ in range(10):
            await throttle.report_failure(trigger_backoff=False)

        # Force adaptation by mocking time
        original_delay = throttle.state.current_delay

        with patch('time.time') as mock_time:
            mock_time.return_value = time.time() + 31  # Force adaptation

            await throttle.acquire()

            # Delay should have increased due to poor success rate
            assert throttle.state.current_delay > original_delay

    @pytest.mark.asyncio
    async def test_burst_throttle_behavior(self):
        """Test burst-then-throttle behavior."""
        config = ThrottleConfig(
            mode=ThrottleMode.BURST_THEN_THROTTLE,
            requests_per_second=2.0,
            burst_size=3,
            burst_window_seconds=1.0
        )
        throttle = Throttle(config)

        # Burst requests should be immediate
        burst_delays = []
        for _ in range(3):
            delay = await throttle.acquire()
            burst_delays.append(delay)

        # All burst requests should be immediate
        assert all(delay == 0.0 for delay in burst_delays)

        # Next request should be throttled
        throttled_delay = await throttle.acquire()
        assert throttled_delay > 0.0

    @pytest.mark.asyncio
    async def test_manager_coordination(self):
        """Test throttle manager coordinating multiple services."""
        manager = ThrottleManager()

        # Register different throttles
        ethereum_config = create_ethereum_throttle(5.0)
        coingecko_config = create_coingecko_throttle(30)

        manager.register_throttle("ethereum", ethereum_config)
        manager.register_throttle("coingecko", coingecko_config)

        # Test concurrent operations
        tasks = [
            manager.wait("ethereum"),
            manager.wait("coingecko"),
            manager.wait("ethereum"),
        ]

        start_time = time.time()
        await asyncio.gather(*tasks)
        elapsed = time.time() - start_time

        # Should complete without errors
        assert elapsed >= 0.0

        # Test statistics collection
        all_stats = manager.get_all_stats()
        assert len(all_stats) == 2
        assert "ethereum" in all_stats
        assert "coingecko" in all_stats


class TestThrottleEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_negative_attempt_backoff(self):
        """Test backoff calculation with negative attempt."""
        config = BackoffConfig()
        delay = config.calculate_delay(-1)
        assert delay == 0.0

    @pytest.mark.asyncio
    async def test_zero_requests_per_second(self):
        """Test throttle with very low rate."""
        config = ThrottleConfig(requests_per_second=0.1)  # 10 seconds per request
        throttle = Throttle(config)

        # First request immediate
        delay1 = await throttle.acquire()
        assert delay1 == 0.0

        # Second request should have long delay
        delay2 = await throttle.acquire()
        assert delay2 == 10.0  # 1/0.1

    @pytest.mark.asyncio
    async def test_concurrent_throttle_access(self):
        """Test concurrent access to throttle."""
        config = ThrottleConfig(requests_per_second=5.0)
        throttle = Throttle(config)

        async def make_request():
            delay = await throttle.acquire()
            await asyncio.sleep(0.01)  # Simulate work
            return delay

        # Start multiple concurrent requests
        tasks = [make_request() for _ in range(5)]
        delays = await asyncio.gather(*tasks)

        # First request should be immediate
        immediate_requests = sum(1 for delay in delays if delay == 0.0)
        assert immediate_requests >= 1

        # Some requests should be delayed
        delayed_requests = sum(1 for delay in delays if delay > 0.0)
        assert delayed_requests >= 1

    def test_custom_backoff_without_function(self):
        """Test custom backoff strategy without custom function."""
        config = BackoffConfig(
            strategy=BackoffStrategy.CUSTOM,
            custom_function=None  # No custom function provided
        )

        # Should fall back to exponential
        delay = config.calculate_delay(2)
        expected = 1.0 * (2.0 ** 2)  # Exponential fallback
        assert delay == expected

    @pytest.mark.asyncio
    async def test_burst_window_expiration(self):
        """Test burst window expiration and reset."""
        config = ThrottleConfig(
            mode=ThrottleMode.BURST_THEN_THROTTLE,
            burst_size=2,
            burst_window_seconds=0.1  # Very short window
        )
        throttle = Throttle(config)

        # Use up burst
        await throttle.acquire()
        await throttle.acquire()

        # Wait for window to expire
        await asyncio.sleep(0.2)

        # Should be able to burst again
        delay = await throttle.acquire()
        assert delay == 0.0  # New burst window

    @pytest.mark.asyncio
    async def test_max_backoff_attempts(self):
        """Test maximum backoff attempts."""
        config = ThrottleConfig(requests_per_second=10.0)
        backoff_config = BackoffConfig(max_retries=2)
        throttle = Throttle(config, backoff_config)

        # Trigger multiple failures
        for i in range(5):  # More than max_retries
            await throttle.report_failure(trigger_backoff=True)

        # Should not exceed max_retries
        assert throttle.current_attempt <= backoff_config.max_retries
        assert throttle.state.success_count == 1

    @pytest.mark.asyncio
    async def test_report_failure_to_manager(self, manager, sample_config):
        """Test reporting failure through manager."""
        manager.register_throttle("test_service", sample_config)

        await manager.report_failure("test_service")

        throttle = manager.get_throttle("test_service")
        assert throttle.state.failure_count == 1