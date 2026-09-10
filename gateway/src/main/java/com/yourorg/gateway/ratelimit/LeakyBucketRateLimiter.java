package com.yourorg.gateway.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.ratelimit.AbstractRateLimiter;
import org.springframework.cloud.gateway.support.ConfigurationService;
import org.springframework.context.annotation.Primary;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;
import reactor.core.publisher.Mono;

import jakarta.validation.constraints.Min;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Leaky-bucket rate limiter backed by Redis.
 *
 * <p>All bucket state lives in Redis (not gateway memory), so the limit stays correct
 * across multiple gateway instances. The leak/admit decision runs inside a Lua script
 * so the read-modify-write is atomic.
 */
@Component
@Primary
public class LeakyBucketRateLimiter extends AbstractRateLimiter<LeakyBucketRateLimiter.Config> {

    private static final Logger log = LoggerFactory.getLogger(LeakyBucketRateLimiter.class);

    public static final String CONFIGURATION_PROPERTY_NAME = "leaky-bucket-rate-limiter";

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final RedisScript<List<Long>> script;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public LeakyBucketRateLimiter(ReactiveRedisTemplate<String, String> redisTemplate,
                                  ConfigurationService configurationService) {
        super(Config.class, CONFIGURATION_PROPERTY_NAME, configurationService);
        this.redisTemplate = redisTemplate;
        this.script = (RedisScript) RedisScript.of(new ClassPathResource("leaky_bucket.lua"), List.class);
    }

    @Override
    public Mono<Response> isAllowed(String routeId, String id) {
        Config routeConfig = getConfig().getOrDefault(routeId, new Config());

        int capacity = routeConfig.getCapacity();
        int leakRatePerSec = routeConfig.getLeakRatePerSec();

        // The hash tag {routeId} keeps a route's keys on one slot if Redis Cluster is used later.
        String bucketKey = "leaky_bucket:{" + routeId + "}:" + id;

        List<String> keys = List.of(bucketKey);
        List<String> args = List.of(
                String.valueOf(capacity),
                String.valueOf(leakRatePerSec)
        );

        // Lettuce emits a MULTI reply element-by-element, so accumulate rather than take
        // the first emission. This mirrors Spring Cloud Gateway's own RedisRateLimiter.
        return redisTemplate.execute(script, keys, args)
                .reduce(new ArrayList<Long>(), (acc, item) -> {
                    acc.addAll(item);
                    return acc;
                })
                .map(res -> {
                    boolean allowed = res.get(0) == 1L;
                    long currentLevel = res.get(1);
                    return new Response(allowed, headers(capacity, currentLevel));
                })
                .onErrorResume(e -> {
                    // Fail open: a Redis outage should not take the gateway down with it.
                    // No stack trace: during an outage this fires on every request, and a
                    // trace per request floods the log (100+ lines each). The most specific
                    // cause keeps the actual Redis error (e.g. OOM) that the wrapper hides.
                    log.warn("Leaky bucket check failed for route={} id={} - allowing request: {}",
                            routeId, id, NestedExceptionUtils.getMostSpecificCause(e).toString());
                    return Mono.just(new Response(true, Map.of()));
                });
    }

    private Map<String, String> headers(int capacity, long currentLevel) {
        return Map.of(
                "X-RateLimit-Remaining", String.valueOf(Math.max(0, capacity - currentLevel)),
                "X-RateLimit-Capacity", String.valueOf(capacity)
        );
    }

    @Validated
    public static class Config {
        @Min(1)
        private int capacity = 10;
        @Min(1)
        private int leakRatePerSec = 1;

        public int getCapacity() { return capacity; }
        public void setCapacity(int capacity) { this.capacity = capacity; }
        public int getLeakRatePerSec() { return leakRatePerSec; }
        public void setLeakRatePerSec(int leakRatePerSec) { this.leakRatePerSec = leakRatePerSec; }
    }
}
