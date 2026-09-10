package com.yourorg.gateway;

import com.yourorg.gateway.ratelimit.LeakyBucketRateLimiter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.filter.ratelimit.RateLimiter;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Redis failing while the gateway is running: the limiter must fail open quickly
 * rather than hold requests until Redis returns.
 *
 * <p>The gateway talks to the real Redis on localhost:6379 through {@link FaultProxy},
 * which can stall traffic or cut it off without touching Redis itself.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RedisOutageTest {

    private static final FaultProxy proxy = FaultProxy.start(6379);

    @DynamicPropertySource
    static void redisThroughProxy(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.port", proxy::port);
    }

    @Autowired
    private LeakyBucketRateLimiter rateLimiter;

    private String routeId;

    @BeforeEach
    void setUp() {
        assumeTrue(FaultProxy.redisReachable(), "Redis not reachable on localhost:6379");
        routeId = "outage-route-" + UUID.randomUUID();
        LeakyBucketRateLimiter.Config config = new LeakyBucketRateLimiter.Config();
        config.setCapacity(5);
        config.setLeakRatePerSec(1);
        rateLimiter.getConfig().put(routeId, config);
        assertThat(isLimited(check())).as("limiter is using Redis before the fault").isTrue();
    }

    @AfterEach
    void heal() {
        proxy.heal();
    }

    @AfterAll
    static void stopProxy() {
        proxy.close();
    }

    @Test
    @DisplayName("a stalled Redis fails open within the command timeout")
    void stalledRedisFailsOpen() {
        proxy.stall();

        long start = System.nanoTime();
        RateLimiter.Response res = check();
        Duration took = Duration.ofNanos(System.nanoTime() - start);

        assertThat(res.isAllowed()).isTrue();
        assertThat(isLimited(res)).as("answer came from the fail-open path").isFalse();
        assertThat(took).as("bounded by spring.data.redis.timeout, not Lettuce's 60s default")
                .isLessThan(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("an unreachable Redis fails open immediately and limiting resumes once it is back")
    void downRedisFailsOpenAndRecovers() throws InterruptedException {
        proxy.cut();
        Thread.sleep(200); // let Lettuce notice the connection is gone

        long start = System.nanoTime();
        RateLimiter.Response res = check();
        Duration took = Duration.ofNanos(System.nanoTime() - start);

        assertThat(res.isAllowed()).isTrue();
        assertThat(isLimited(res)).isFalse();
        assertThat(took).as("commands are rejected, not queued, while disconnected")
                .isLessThan(Duration.ofMillis(300));

        proxy.heal();
        long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        boolean resumed = false;
        while (!resumed && System.nanoTime() < deadline) {
            resumed = isLimited(check());
            Thread.sleep(50);
        }
        assertThat(resumed).as("limiting resumes within 3s of Redis returning").isTrue();
    }

    private RateLimiter.Response check() {
        return rateLimiter.isAllowed(routeId, "user-" + UUID.randomUUID()).block(Duration.ofSeconds(10));
    }

    /** Only responses computed by the Lua script carry rate-limit headers. */
    private static boolean isLimited(RateLimiter.Response res) {
        return res.getHeaders().containsKey("X-RateLimit-Capacity");
    }

    /** TCP proxy to Redis that can hold traffic (stall) or drop and refuse it (cut). */
    static final class FaultProxy implements AutoCloseable {
        private final ServerSocket server;
        private final int targetPort;
        private final List<Socket> open = new CopyOnWriteArrayList<>();
        private volatile boolean stalled;
        private volatile boolean cut;

        private FaultProxy(int targetPort) throws IOException {
            this.targetPort = targetPort;
            this.server = new ServerSocket(0);
            Thread acceptor = new Thread(this::acceptLoop, "fault-proxy-accept");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        static FaultProxy start(int targetPort) {
            try {
                return new FaultProxy(targetPort);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        static boolean redisReachable() {
            try (Socket s = new Socket("localhost", 6379)) {
                return true;
            } catch (IOException e) {
                return false;
            }
        }

        int port() {
            return server.getLocalPort();
        }

        void stall() {
            stalled = true;
        }

        void cut() {
            cut = true;
            open.forEach(FaultProxy::closeQuietly);
            open.clear();
        }

        void heal() {
            stalled = false;
            cut = false;
        }

        @Override
        public void close() {
            cut();
            closeQuietly(server);
        }

        private void acceptLoop() {
            while (!server.isClosed()) {
                try {
                    Socket client = server.accept();
                    if (cut) {
                        client.close();
                        continue;
                    }
                    Socket upstream = new Socket("localhost", targetPort);
                    open.add(client);
                    open.add(upstream);
                    pump(client, upstream);
                    pump(upstream, client);
                } catch (IOException e) {
                    // server closed or upstream refused; keep accepting until closed
                }
            }
        }

        private void pump(Socket from, Socket to) {
            Thread t = new Thread(() -> {
                byte[] buf = new byte[16384];
                try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                    int n;
                    while ((n = in.read(buf)) != -1) {
                        while (stalled && !cut) {
                            Thread.sleep(10);
                        }
                        if (cut) {
                            return;
                        }
                        out.write(buf, 0, n);
                        out.flush();
                    }
                } catch (IOException | InterruptedException e) {
                    // connection closed
                } finally {
                    closeQuietly(from);
                    closeQuietly(to);
                }
            }, "fault-proxy-pump");
            t.setDaemon(true);
            t.start();
        }

        private static void closeQuietly(AutoCloseable c) {
            try {
                c.close();
            } catch (Exception ignored) {
                // already closed
            }
        }
    }
}
