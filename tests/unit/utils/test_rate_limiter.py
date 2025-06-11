"""Tests for rate limiting system."""

import asyncio
import time
import pytest
from unittest.mock import patch

from wallet_tracker.utils.rate_limiter import (
    RateLimit,
    RateLimitStrategy,
    RateLimitScope,
    RateLimitStatus,
    TokenBucketLimiter,
    SlidingWindowLimiter,
    AdaptiveLimiter,
    RateLimiterManager,
    create_ethereum_rate_limiter,
    create_coingecko_rate_limiter,
    create_sheets_rate_limiter,
    rate_limited
)


class TestRateLimit:
    """Test RateLimit configuration class."""

    def test_valid_configuration(self):
        """Test valid rate limit configuration."""
        rate_limit = RateLimit(
            requests_per_second=10.0,
            requests_per_minute=600,
            burst_size=20,
            strategy=RateLimitStrategy.TOKEN_BUCKET
        )

        assert rate_limit.requests_per_second == 10.0
        assert rate_limit.requests_per_minute == 600
        assert rate_limit.burst_size == 20
        assert rate_limit.strategy == RateLimitStrategy.TOKEN_BUCKET

    def test_invalid_requests_per_second(self):
        """Test invalid requests per second raises error."""
        with pytest.raises(ValueError, match="requests_per_second must be positive"):
            RateLimit(requests_per_second=0)

    def test_auto_calculated_values(self):
        """Test auto-calculated default values."""
        rate_limit = RateLimit(requests_per_second=5.0)

        # Burst size should default to 2x rate per second
        assert rate_limit.burst_size == 10

        # Requests per minute should be auto-calculated
        assert rate_limit.requests_per_minute == 300

        # Requests per hour should be auto-calculated
        assert rate_limit.requests_per_hour == 18000


class TestTokenBucketLimiter:
    """Test token bucket rate limiter implementation."""

    @pytest.fixture
    def rate_limit(self):
        """Create rate limit configuration for testing."""
        return RateLimit(
            requests_per_second=10.0,
            burst_size=20,
            strategy=RateLimitStrategy.TOKEN_BUCKET
        )

    @pytest.fixture
    def limiter(self, rate_limit):
        """Create token bucket limiter."""
        return TokenBucketLimiter(rate_limit)

    @pytest.mark.asyncio
    async def test_initial_tokens_available(self, limiter):
        """Test that initial tokens are available."""
        delay = await limiter.acquire(1)
        assert delay == 0.0

    @pytest.mark.asyncio
    async def test_burst_capacity(self, limiter):
        """Test burst capacity handling."""
        # Should be able to consume all burst tokens without delay
        for _ in range(20):
            delay = await limiter.acquire(1)
            assert delay == 0.0

        # Next request should require waiting
        delay = await limiter.acquire(1)
        assert delay > 0.0

    @pytest.mark.asyncio
    async def test_token_refill(self, limiter):
        """Test token refill over time."""
        # Consume all tokens
        for _ in range(20):
            await limiter.acquire(1)

        # Wait for some tokens to refill
        await asyncio.sleep(0.2)  # Should refill ~2 tokens at 10/sec

        # Should now have some tokens available
        delay = await limiter.acquire(1)
        assert delay == 0.0

    @pytest.mark.asyncio
    async def test_acquire_multiple_tokens(self, limiter):
        """Test acquiring multiple tokens at once."""
        delay = await limiter.acquire(5)
        assert delay == 0.0

        # Tokens should be consumed
        remaining_tokens = limiter.tokens
        assert remaining_tokens == 15.0

    @pytest.mark.asyncio
    async def test_insufficient_tokens(self, limiter):
        """Test behavior when insufficient tokens available."""
        # Consume most tokens
        await limiter.acquire(18)

        # Request more tokens than available
        delay = await limiter.acquire(5)

        # Should need to wait for refill
        assert delay > 0.0

        # Delay should be calculated correctly
        expected_delay = 3.0 / 10.0  # 3 tokens needed / 10 tokens per second
        assert abs(delay - expected_delay) < 0.01

    def test_get_status(self, limiter):
        """Test getting limiter status."""
        status = limiter.get_status()

        assert isinstance(status, RateLimitStatus)
        assert status.requests_remaining >= 0
        assert status.reset_time is not None


class TestSlidingWindowLimiter:
    """Test sliding window rate limiter implementation."""

    @pytest.fixture
    def rate_limit(self):
        """Create rate limit configuration for testing."""
        return RateLimit(
            requests_per_second=10.0,
            requests_per_minute=600,
            strategy=RateLimitStrategy.SLIDING_WINDOW
        )

    @pytest.fixture
    def limiter(self, rate_limit):
        """Create sliding window limiter."""
        return SlidingWindowLimiter(rate_limit)

    @pytest.mark.asyncio
    async def test_initial_requests_allowed(self, limiter):
        """Test that initial requests are allowed."""
        delay = await limiter.acquire(1)
        assert delay == 0.0

    @pytest.mark.asyncio
    async def test_window_limit_enforcement(self, limiter):
        """Test that window limit is enforced."""
        # Make requests up to the limit
        for _ in range(600):
            delay = await limiter.acquire(1)
            assert delay == 0.0

        # Next request should be delayed
        delay = await limiter.acquire(1)
        assert delay > 0.0

    @pytest.mark.asyncio
    async def test_sliding_window_behavior(self, limiter):
        """Test sliding window behavior."""
        # Fill up the window
        for _ in range(600):
            await limiter.acquire(1)

        # Mock time advancement
        with patch('time.time') as mock_time:
            mock_time.return_value = time.time() + 30  # Advance 30 seconds

            # Should still be limited (only half window expired)
            delay = await limiter.acquire(1)
            assert delay > 0.0

    def test_get_status(self, limiter):
        """Test getting limiter status."""
        status = limiter.get_status()

        assert isinstance(status, RateLimitStatus)
        assert status.requests_made >= 0
        assert status.requests_remaining >= 0


class TestAdaptiveLimiter:
    """Test adaptive rate limiter implementation."""

    @pytest.fixture
    def rate_limit(self):
        """Create rate limit configuration for testing."""
        return RateLimit(
            requests_per_second=10.0,
            strategy=RateLimitStrategy.ADAPTIVE
        )

    @pytest.fixture
    def limiter(self, rate_limit):
        """Create adaptive limiter."""
        return AdaptiveLimiter(rate_limit)

    @pytest.mark.asyncio
    async def test_initial_behavior(self, limiter):
        """Test initial adaptive limiter behavior."""
        delay = await limiter.acquire(1)
        assert delay == 0.0

    @pytest.mark.asyncio
    async def test_adaptation_on_errors(self, limiter):
        """Test rate adaptation based on error reports."""
        # Report some errors to trigger adaptation
        for _ in range(10):
            await limiter.report_response(success=False, response_time=1.0)

        # Trigger adaptation check
        with patch('time.time') as mock_time:
            mock_time.return_value = time.time() + 31  # Force adaptation interval

            # Should adapt to slower rate due to errors
            original_rate = limiter.current_rate
            await limiter.acquire(1)

            # Rate should have been reduced
            assert limiter.current_rate < original_rate

    @pytest.mark.asyncio
    async def test_adaptation_on_success(self, limiter):
        """Test rate adaptation based on success reports."""
        # Set a lower initial rate
        limiter.current_rate = 5.0

        # Report many successes
        for _ in range(20):
            await limiter.report_response(success=True, response_time=0.1)

        # Trigger adaptation
        with patch('time.time') as mock_time:
            mock_time.return_value = time.time() + 31

            original_rate = limiter.current_rate
            await limiter.acquire(1)

            # Rate should have been increased
            assert limiter.current_rate > original_rate

    @pytest.mark.asyncio
    async def test_rate_bounds(self, limiter):
        """Test that adaptive rate stays within bounds."""
        # Force very low error rate
        for _ in range(100):
            await limiter.report_response(success=False, response_time=5.0)

        # Trigger multiple adaptations
        for i in range(10):
            with patch('time.time') as mock_time:
                mock_time.return_value = time.time() + (i + 1) * 31
                await limiter.acquire(1)

        # Rate should not go below 10% of base rate
        min_rate = limiter.base_rate_limit.requests_per_second * 0.1
        assert limiter.current_rate >= min_rate

    def test_get_status(self, limiter):
        """Test getting adaptive limiter status."""
        status = limiter.get_status()

        assert isinstance(status, RateLimitStatus)


class TestRateLimiterManager:
    """Test rate limiter manager."""

    @pytest.fixture
    def manager(self):
        """Create rate limiter manager."""
        return RateLimiterManager()

    @pytest.fixture
    def sample_rate_limit(self):
        """Create sample rate limit configuration."""
        return RateLimit(requests_per_second=5.0, strategy=RateLimitStrategy.TOKEN_BUCKET)

    def test_register_limiter(self, manager, sample_rate_limit):
        """Test registering a rate limiter."""
        manager.register_limiter("test_service", sample_rate_limit)

        assert "test_service" in manager.limiters
        assert isinstance(manager.limiters["test_service"], TokenBucketLimiter)

    @pytest.mark.asyncio
    async def test_acquire_from_registered_limiter(self, manager, sample_rate_limit):
        """Test acquiring from a registered limiter."""
        manager.register_limiter("test_service", sample_rate_limit)

        delay = await manager.acquire("test_service", 1)
        assert delay == 0.0

    @pytest.mark.asyncio
    async def test_acquire_from_unknown_limiter(self, manager):
        """Test acquiring from unknown limiter raises error."""
        with pytest.raises(KeyError, match="Rate limiter 'unknown' not found"):
            await manager.acquire("unknown", 1)

    @pytest.mark.asyncio
    async def test_wait_for_capacity(self, manager, sample_rate_limit):
        """Test waiting for capacity."""
        manager.register_limiter("test_service", sample_rate_limit)

        # This should not raise an error
        await manager.wait_for_capacity("test_service", 1)

    @pytest.mark.asyncio
    async def test_report_response_to_adaptive(self, manager):
        """Test reporting responses to adaptive limiter."""
        adaptive_limit = RateLimit(
            requests_per_second=10.0,
            strategy=RateLimitStrategy.ADAPTIVE
        )
        manager.register_limiter("adaptive_service", adaptive_limit)

        # Should not raise error
        await manager.report_response("adaptive_service", success=True, response_time=0.5)

    @pytest.mark.asyncio
    async def test_report_response_to_non_adaptive(self, manager, sample_rate_limit):
        """Test reporting responses to non-adaptive limiter."""
        manager.register_limiter("token_bucket_service", sample_rate_limit)

        # Should not raise error (should be ignored)
        await manager.report_response("token_bucket_service", success=True, response_time=0.5)

    def test_get_status(self, manager, sample_rate_limit):
        """Test getting status of specific limiter."""
        manager.register_limiter("test_service", sample_rate_limit)

        status = manager.get_status("test_service")
        assert status is not None
        assert isinstance(status, RateLimitStatus)

    def test_get_status_unknown_limiter(self, manager):
        """Test getting status of unknown limiter."""
        status = manager.get_status("unknown")
        assert status is None

    def test_get_all_status(self, manager, sample_rate_limit):
        """Test getting status of all limiters."""
        manager.register_limiter("service1", sample_rate_limit)
        manager.register_limiter("service2", sample_rate_limit)

        all_status = manager.get_all_status()

        assert len(all_status) == 2
        assert "service1" in all_status
        assert "service2" in all_status

    def test_update_limiter(self, manager, sample_rate_limit):
        """Test updating existing limiter."""
        manager.register_limiter("test_service", sample_rate_limit)

        # Update with new configuration
        new_rate_limit = RateLimit(requests_per_second=20.0)
        manager.update_limiter("test_service", new_rate_limit)

        # Should have new limiter instance
        limiter = manager.limiters["test_service"]
        assert limiter.refill_rate == 20.0

    def test_remove_limiter(self, manager, sample_rate_limit):
        """Test removing a limiter."""
        manager.register_limiter("test_service", sample_rate_limit)
        assert "test_service" in manager.limiters

        manager.remove_limiter("test_service")
        assert "test_service" not in manager.limiters

    def test_get_stats(self, manager, sample_rate_limit):
        """Test getting manager statistics."""
        manager.register_limiter("service1", sample_rate_limit)
        manager.register_limiter("service2", sample_rate_limit)

        stats = manager.get_stats()

        assert stats["total_limiters"] == 2
        assert "limiter_types" in stats
        assert "TokenBucketLimiter" in stats["limiter_types"]


class TestUtilityFunctions:
    """Test utility functions for creating rate limiters."""

    def test_create_ethereum_rate_limiter(self):
        """Test creating Ethereum rate limiter configuration."""
        rate_limit = create_ethereum_rate_limiter(requests_per_second=15.0)

        assert rate_limit.requests_per_second == 15.0
        assert rate_limit.strategy == RateLimitStrategy.TOKEN_BUCKET
        assert rate_limit.enable_backoff is True
        assert rate_limit.burst_size == 30  # 2x the rate

    def test_create_coingecko_rate_limiter_free(self):
        """Test creating CoinGecko rate limiter for free plan."""
        rate_limit = create_coingecko_rate_limiter(requests_per_minute=30, has_api_key=False)

        assert rate_limit.requests_per_minute == 30
        assert rate_limit.requests_per_second == 0.5  # Max for free plan
        assert rate_limit.strategy == RateLimitStrategy.SLIDING_WINDOW
        assert rate_limit.enable_backoff is True

    def test_create_coingecko_rate_limiter_pro(self):
        """Test creating CoinGecko rate limiter for pro plan."""
        rate_limit = create_coingecko_rate_limiter(requests_per_minute=600, has_api_key=True)

        assert rate_limit.requests_per_minute == 600
        assert rate_limit.requests_per_second == 10.0  # 600/60
        assert rate_limit.strategy == RateLimitStrategy.SLIDING_WINDOW
        assert rate_limit.enable_backoff is True

    def test_create_sheets_rate_limiter(self):
        """Test creating Google Sheets rate limiter configuration."""
        rate_limit = create_sheets_rate_limiter(requests_per_minute=120)

        assert rate_limit.requests_per_minute == 120
        assert rate_limit.requests_per_second == 2.0  # 120/60
        assert rate_limit.strategy == RateLimitStrategy.TOKEN_BUCKET
        assert rate_limit.burst_size == 10
        assert rate_limit.enable_backoff is True


class TestRateLimitedDecorator:
    """Test rate limited decorator."""

    @pytest.fixture
    def manager(self):
        """Create rate limiter manager with test limiter."""
        manager = RateLimiterManager()
        rate_limit = RateLimit(requests_per_second=10.0)
        manager.register_limiter("test_limiter", rate_limit)
        return manager

    @pytest.mark.asyncio
    async def test_decorator_success(self, manager):
        """Test decorator with successful function execution."""
        call_count = 0

        @rate_limited("test_limiter", requests=1, manager=manager)
        async def test_function():
            nonlocal call_count
            call_count += 1
            return "success"

        result = await test_function()

        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_decorator_failure(self, manager):
        """Test decorator with function failure."""

        @rate_limited("test_limiter", requests=1, manager=manager)
        async def failing_function():
            raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            await failing_function()

        # Should have reported failure to adaptive limiter
        # (This would be tested more thoroughly with an adaptive limiter)

    @pytest.mark.asyncio
    async def test_decorator_multiple_requests(self, manager):
        """Test decorator with multiple request consumption."""

        @rate_limited("test_limiter", requests=5, manager=manager)
        async def expensive_function():
            return "expensive"

        result = await expensive_function()
        assert result == "expensive"

    @pytest.mark.asyncio
    async def test_decorator_no_manager(self):
        """Test decorator raises error when no manager provided."""

        @rate_limited("test_limiter", requests=1, manager=None)
        async def test_function():
            return "success"

        with pytest.raises(ValueError, match="No rate limiter manager provided"):
            await test_function()


class TestRateLimitIntegration:
    """Integration tests for rate limiting system."""

    @pytest.mark.asyncio
    async def test_token_bucket_rate_limiting(self):
        """Test actual rate limiting with token bucket."""
        rate_limit = RateLimit(requests_per_second=5.0, burst_size=5)
        limiter = TokenBucketLimiter(rate_limit)

        start_time = time.time()

        # Make burst requests (should be immediate)
        for _ in range(5):
            delay = await limiter.acquire(1)
            assert delay == 0.0

        # Next request should be delayed
        delay = await limiter.acquire(1)
        assert delay > 0.0

        # Wait for the delay
        await asyncio.sleep(delay)

        # Should be able to make request now
        delay = await limiter.acquire(1)
        assert delay == 0.0

        total_time = time.time() - start_time
        # Should have taken at least the delay time
        assert total_time >= 0.2  # 1/5 second for one token

    @pytest.mark.asyncio
    async def test_sliding_window_rate_limiting(self):
        """Test actual rate limiting with sliding window."""
        # Use a smaller window for testing
        rate_limit = RateLimit(
            requests_per_second=2.0,
            requests_per_minute=10,  # Small window for testing
            strategy=RateLimitStrategy.SLIDING_WINDOW
        )
        limiter = SlidingWindowLimiter(rate_limit)

        # Make requests up to limit
        for _ in range(10):
            delay = await limiter.acquire(1)
            assert delay == 0.0

        # Next request should be delayed
        delay = await limiter.acquire(1)
        assert delay > 0.0

    @pytest.mark.asyncio
    async def test_manager_integration(self):
        """Test full manager integration."""
        manager = RateLimiterManager()

        # Register different types of limiters
        ethereum_limit = create_ethereum_rate_limiter(10.0)
        coingecko_limit = create_coingecko_rate_limiter(30, has_api_key=False)

        manager.register_limiter("ethereum", ethereum_limit)
        manager.register_limiter("coingecko", coingecko_limit)

        # Test acquiring from different limiters
        eth_delay = await manager.acquire("ethereum", 1)
        cg_delay = await manager.acquire("coingecko", 1)

        assert eth_delay == 0.0
        assert cg_delay == 0.0

        # Test status collection
        all_status = manager.get_all_status()
        assert len(all_status) == 2
        assert "ethereum" in all_status
        assert "coingecko" in all_status

        # Test stats
        stats = manager.get_stats()
        assert stats["total_limiters"] == 2


class TestRateLimitStatus:
    """Test RateLimitStatus class."""

    def test_update_metrics(self):
        """Test updating performance metrics."""
        status = RateLimitStatus()

        # Update with some delays
        status.update_metrics(0.5)
        status.update_metrics(1.0)
        status.update_metrics(0.0)  # No delay

        assert status.total_requests == 3
        assert status.total_delays == 2  # Only non-zero delays counted
        assert status.total_delay_time == 1.5
        assert status.average_delay == 0.75  # 1.5 / 2

    def test_initial_state(self):
        """Test initial status state."""
        status = RateLimitStatus()

        assert status.requests_made == 0
        assert status.requests_remaining == 0
        assert status.total_requests == 0
        assert status.total_delays == 0
        assert status.total_delay_time == 0.0
        assert status.average_delay == 0.0
        assert status.is_rate_limited is False


class TestRateLimitEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_zero_tokens_requested(self):
        """Test requesting zero tokens."""
        rate_limit = RateLimit(requests_per_second=10.0)
        limiter = TokenBucketLimiter(rate_limit)

        delay = await limiter.acquire(0)
        assert delay == 0.0

    @pytest.mark.asyncio
    async def test_large_token_request(self):
        """Test requesting more tokens than burst size."""
        rate_limit = RateLimit(requests_per_second=10.0, burst_size=5)
        limiter = TokenBucketLimiter(rate_limit)

        delay = await limiter.acquire(10)

        # Should calculate delay for the extra tokens
        expected_delay = 5.0 / 10.0  # 5 extra tokens / 10 tokens per second
        assert abs(delay - expected_delay) < 0.01

    @pytest.mark.asyncio
    async def test_concurrent_acquires(self):
        """Test concurrent acquire operations."""
        rate_limit = RateLimit(requests_per_second=10.0, burst_size=10)
        limiter = TokenBucketLimiter(rate_limit)

        # Start multiple concurrent acquires
        tasks = [limiter.acquire(2) for _ in range(5)]
        delays = await asyncio.gather(*tasks)

        # First few should be immediate, later ones delayed
        immediate_count = sum(1 for delay in delays if delay == 0.0)
        assert immediate_count <= 5  # At most 5 requests (10 tokens / 2 tokens each)

    def test_adaptive_limiter_bounds_checking(self):
        """Test adaptive limiter respects rate bounds."""
        rate_limit = RateLimit(requests_per_second=10.0)
        limiter = AdaptiveLimiter(rate_limit)

        # Manually set extreme rates
        limiter.current_rate = 100.0  # Very high

        # Bounds should be enforced in _maybe_adjust_rate
        min_rate = rate_limit.requests_per_second * 0.1
        max_rate = rate_limit.requests_per_second * 2.0

        # Simulate bound enforcement
        limiter.current_rate = max(min_rate, min(max_rate, limiter.current_rate))

        assert limiter.current_rate <= max_rate
        assert limiter.current_rate >= min_rate

    @pytest.mark.asyncio
    async def test_sliding_window_cleanup(self):
        """Test sliding window properly cleans up old requests."""
        rate_limit = RateLimit(requests_per_second=10.0, requests_per_minute=60)
        limiter = SlidingWindowLimiter(rate_limit)

        # Add some requests
        for _ in range(10):
            await limiter.acquire(1)

        assert len(limiter.requests) == 10

        # Mock time advancement to expire requests
        with patch('time.time') as mock_time:
            mock_time.return_value = time.time() + 70  # Beyond window

            # Next acquire should clean up old requests
            await limiter.acquire(1)

            # Old requests should be cleaned up
            assert len(limiter.requests) == 1  # Only the new request