package com.example.fs.io;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Scylla connection settings shipped to the sinks on the TaskManagers. */
public final class CqlConfig implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CqlConfig.class);

    public final ArrayList<String> contactPoints;
    public final String localDc;
    /** More than one keyspace = dual write (e.g. "fs,fs_v2" during a blue/green rebuild). */
    public final ArrayList<String> keyspaces;
    public final int maxInFlight;
    public final long requestTimeoutMs;

    public CqlConfig(List<String> contactPoints, String localDc, List<String> keyspaces,
                     int maxInFlight, long requestTimeoutMs) {
        this.contactPoints = new ArrayList<>(contactPoints);
        this.localDc = localDc;
        this.keyspaces = new ArrayList<>(keyspaces);
        this.maxInFlight = maxInFlight;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    CqlSession connect(ClassLoader classLoader) throws InterruptedException {
        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(requestTimeoutMs))
                .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM")
                .withBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, true)
                .build();
        DriverException last = null;
        for (int attempt = 1; attempt <= 30; attempt++) {
            CqlSessionBuilder b = CqlSession.builder()
                    .withClassLoader(classLoader)
                    .withConfigLoader(loader)
                    .withLocalDatacenter(localDc);
            for (String hp : contactPoints) {
                String[] p = hp.split(":");
                b.addContactPoint(new InetSocketAddress(p[0], p.length > 1 ? Integer.parseInt(p[1]) : 9042));
            }
            try {
                return b.build();
            } catch (DriverException e) {
                last = e;
                LOG.warn("Scylla not reachable (attempt {}): {}", attempt, e.getMessage());
                Thread.sleep(2000);
            }
        }
        throw last;
    }
}
