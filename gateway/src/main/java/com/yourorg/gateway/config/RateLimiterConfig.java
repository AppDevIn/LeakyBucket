package com.yourorg.gateway.config;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

@Configuration
public class RateLimiterConfig {

    @Bean
    public KeyResolver userKeyResolver() {
        return exchange -> {
            String userId = exchange.getRequest().getHeaders().getFirst("X-User-Id");
            // A blank header counts as anonymous; otherwise "" gets a bucket of its own.
            return Mono.just(userId != null && !userId.isBlank() ? userId : "anonymous");
        };
    }
}
