package com.yourorg.gateway.config;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.resource.Delay;
import org.springframework.boot.autoconfigure.data.redis.ClientResourcesBuilderCustomizer;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Makes a Redis outage surface as a fast error so the rate limiter can fail open.
 *
 * <p>Lettuce's defaults queue commands while disconnected and back off reconnects for
 * up to 30s, so a Redis outage hung every rate-limited request instead of failing open.
 * The command timeout itself is {@code spring.data.redis.timeout}.
 */
@Configuration
public class RedisClientConfig {

    @Bean
    public LettuceClientConfigurationBuilderCustomizer rejectCommandsWhileDisconnected() {
        return builder -> {
            // Keep the options Spring Boot already built (timeouts, socket options).
            ClientOptions current = builder.build().getClientOptions().orElseGet(ClientOptions::create);
            builder.clientOptions(current.mutate()
                    .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                    .build());
        };
    }

    @Bean
    public ClientResourcesBuilderCustomizer fastReconnect() {
        // Limiting resumes within ~1s of Redis coming back rather than up to 30s later.
        return builder -> builder.reconnectDelay(
                Delay.exponential(Duration.ofMillis(50), Duration.ofSeconds(1), 2, TimeUnit.MILLISECONDS));
    }
}
