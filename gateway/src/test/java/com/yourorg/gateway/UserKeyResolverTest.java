package com.yourorg.gateway;

import com.yourorg.gateway.config.RateLimiterConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import static org.assertj.core.api.Assertions.assertThat;

class UserKeyResolverTest {

    private final KeyResolver resolver = new RateLimiterConfig().userKeyResolver();

    private String resolve(MockServerHttpRequest.BaseBuilder<?> request) {
        return resolver.resolve(MockServerWebExchange.from(request)).block();
    }

    @Test
    @DisplayName("uses the X-User-Id header as the key")
    void usesHeader() {
        assertThat(resolve(MockServerHttpRequest.get("/users/1").header("X-User-Id", "alice"))).isEqualTo("alice");
    }

    @Test
    @DisplayName("a missing header shares the anonymous bucket")
    void missingHeaderIsAnonymous() {
        assertThat(resolve(MockServerHttpRequest.get("/users/1"))).isEqualTo("anonymous");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t"})
    @DisplayName("a blank header shares the anonymous bucket instead of getting its own")
    void blankHeaderIsAnonymous(String blank) {
        assertThat(resolve(MockServerHttpRequest.get("/users/1").header("X-User-Id", blank))).isEqualTo("anonymous");
    }
}
