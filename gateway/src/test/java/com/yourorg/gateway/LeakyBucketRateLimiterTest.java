package com.yourorg.gateway;

import com.yourorg.gateway.ratelimit.LeakyBucketRateLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.filter.ratelimit.RateLimiter;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Exercises the limiter against a real Redis on localhost:6379.
 *
 * <p>Testcontainers/embedded Redis would isolate this further, but a live Redis is the
 * thing the Lua actually has to work against, so we use it directly. Tests skip rather
 * than fail when Redis is unreachable.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LeakyBucketRateLimiterTest {

    @Autowired
    private LeakyBucketRateLimiter rateLimiter;

    @Autowired
    private ReactiveStringRedisTemplate redisTemplate;

    @BeforeEach
    void redisIsReachable() {
        Boolean ok = redisTemplate.getConnectionFactory()
                .getReactiveConnection()
                .ping()
                .map("PONG"::equalsIgnoreCase)
                .onErrorReturn(false)
                .block();
        assumeTrue(Boolean.TRUE.equals(ok), "Redis not reachable on localhost:6379");
    }

    /** Registers a route config and returns the route id to use. */
    private String routeWith(int capacity, int leakRatePerSec) {
        String routeId = "test-route-" + UUID.randomUUID();
        LeakyBucketRateLimiter.Config config = new LeakyBucketRateLimiter.Config();
        config.setCapacity(capacity);
        config.setLeakRatePerSec(leakRatePerSec);
        rateLimiter.getConfig().put(routeId, config);
        return routeId;
    }

    @Test
    @DisplayName("admits exactly capacity requests in a burst, then rejects")
    void rejectsOnceCapacityIsReached() {
        // Leak of 1/sec means a fast in-process burst drains a negligible amount.
        String routeId = routeWith(5, 1);
        String user = "user-" + UUID.randomUUID();

        for (int i = 1; i <= 5; i++) {
            RateLimiter.Response res = rateLimiter.isAllowed(routeId, user).block();
            assertThat(res).isNotNull();
            assertThat(res.isAllowed())
                    .as("request %d of 5 should be admitted", i)
                    .isTrue();
        }

        RateLimiter.Response overflow = rateLimiter.isAllowed(routeId, user).block();
        assertThat(overflow).isNotNull();
        assertThat(overflow.isAllowed()).as("6th request exceeds capacity").isFalse();
    }

    @Test
    @DisplayName("reports remaining headroom in the response headers")
    void exposesRateLimitHeaders() {
        String routeId = routeWith(5, 1);
        String user = "user-" + UUID.randomUUID();

        RateLimiter.Response res = rateLimiter.isAllowed(routeId, user).block();

        assertThat(res).isNotNull();
        assertThat(res.getHeaders()).containsEntry("X-RateLimit-Capacity", "5");
        assertThat(res.getHeaders()).containsEntry("X-RateLimit-Remaining", "4");
    }

    @Test
    @DisplayName("bucket drains over time and admits again")
    void drainsOverTime() throws InterruptedException {
        // Capacity 2, leaking 10/sec: fully drained well inside the sleep below.
        String routeId = routeWith(2, 10);
        String user = "user-" + UUID.randomUUID();

        assertThat(rateLimiter.isAllowed(routeId, user).block().isAllowed()).isTrue();
        assertThat(rateLimiter.isAllowed(routeId, user).block().isAllowed()).isTrue();
        assertThat(rateLimiter.isAllowed(routeId, user).block().isAllowed())
                .as("bucket is full")
                .isFalse();

        Thread.sleep(500); // 500ms at 10/sec leaks 5 units, more than the 2 held

        assertThat(rateLimiter.isAllowed(routeId, user).block().isAllowed())
                .as("bucket should have drained")
                .isTrue();
    }

    @Test
    @DisplayName("one key exhausting its bucket does not affect another key")
    void keysAreIsolated() {
        String routeId = routeWith(3, 1);
        String noisy = "noisy-" + UUID.randomUUID();
        String quiet = "quiet-" + UUID.randomUUID();

        for (int i = 0; i < 3; i++) {
            rateLimiter.isAllowed(routeId, noisy).block();
        }
        assertThat(rateLimiter.isAllowed(routeId, noisy).block().isAllowed())
                .as("noisy key is exhausted")
                .isFalse();

        assertThat(rateLimiter.isAllowed(routeId, quiet).block().isAllowed())
                .as("quiet key is unaffected")
                .isTrue();
    }

    @Test
    @DisplayName("the same key on different routes gets independent buckets")
    void routesAreIsolated() {
        String routeA = routeWith(2, 1);
        String routeB = routeWith(2, 1);
        String user = "user-" + UUID.randomUUID();

        rateLimiter.isAllowed(routeA, user).block();
        rateLimiter.isAllowed(routeA, user).block();
        assertThat(rateLimiter.isAllowed(routeA, user).block().isAllowed())
                .as("route A is exhausted")
                .isFalse();

        assertThat(rateLimiter.isAllowed(routeB, user).block().isAllowed())
                .as("route B has its own bucket")
                .isTrue();
    }

    @Test
    @DisplayName("concurrent requests never admit more than capacity plus what leaked")
    void concurrentRequestsRespectCapacity() {
        String routeId = routeWith(20, 1);
        String user = "user-" + UUID.randomUUID();

        long start = System.nanoTime();
        List<Boolean> decisions = Flux.range(0, 200)
                .flatMap(i -> rateLimiter.isAllowed(routeId, user), 200)
                .map(RateLimiter.Response::isAllowed)
                .collectList()
                .block();
        double elapsedSec = (System.nanoTime() - start) / 1e9;

        long admitted = decisions.stream().filter(Boolean::booleanValue).count();
        assertThat(admitted)
                .as("200 concurrent requests against capacity 20, leaking 1/sec over %.2fs", elapsedSec)
                .isBetween(20L, 20L + (long) Math.ceil(elapsedSec));
    }

    @Test
    @DisplayName("a denied request reports zero remaining, even with a fractional level")
    void deniedRequestReportsNoHeadroom() {
        String routeId = routeWith(20, 1);
        String user = "user-" + UUID.randomUUID();
        seedBucket(routeId, user, "19.5", redisNowMillis());

        RateLimiter.Response res = rateLimiter.isAllowed(routeId, user).block();

        assertThat(res.isAllowed()).as("19.5 + 1 exceeds capacity 20").isFalse();
        assertThat(res.getHeaders()).containsEntry("X-RateLimit-Remaining", "0");
    }

    @Test
    @DisplayName("a last_leak timestamp in the future does not refill the bucket")
    void futureLastLeakDoesNotInflateLevel() {
        String routeId = routeWith(20, 5);
        String user = "user-" + UUID.randomUUID();
        // As if written by a clock 60s ahead: negative elapsed must not add 300 units.
        seedBucket(routeId, user, "0", redisNowMillis() + 60_000);

        RateLimiter.Response res = rateLimiter.isAllowed(routeId, user).block();

        assertThat(res.isAllowed()).isTrue();
        assertThat(res.getHeaders()).containsEntry("X-RateLimit-Remaining", "19");
    }

    private void seedBucket(String routeId, String user, String level, long lastLeakMillis) {
        String key = "leaky_bucket:{" + routeId + "}:" + user;
        redisTemplate.opsForHash()
                .putAll(key, Map.of("level", level, "last_leak", String.valueOf(lastLeakMillis)))
                .block();
    }

    private long redisNowMillis() {
        return redisTemplate.getConnectionFactory().getReactiveConnection().serverCommands().time().block();
    }
}
