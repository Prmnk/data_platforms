package com.example.fs;

import com.example.fs.io.CqlConfig;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Job settings, read from environment variables on the JobManager. */
final class JobConfig {
    final String brokers = env("FS_BROKERS", "redpanda:9092");
    final String txnTopic = env("FS_TXN_TOPIC", "transactions");
    final String ordersTopic = env("FS_ORDERS_TOPIC", "orders");
    final String usersTopic = env("FS_USERS_TOPIC", "users");
    final String changelogTopic = env("FS_CHANGELOG_TOPIC", "features.changelog");
    final String groupId = env("FS_GROUP_ID", "fs-aggregator");
    final String timezone = env("FS_TZ", "Asia/Kolkata");
    final long flushMs = Long.parseLong(env("FS_FLUSH_MS", "100"));
    final long dedupTtlMs = Long.parseLong(env("FS_DEDUP_TTL_HOURS", "168")) * 3_600_000L;
    final CqlConfig cql = new CqlConfig(
            split(env("FS_SCYLLA_CONTACT_POINTS", "scylla:9042")),
            env("FS_SCYLLA_DC", "datacenter1"),
            split(env("FS_KEYSPACES", "fs")),
            Integer.parseInt(env("FS_SCYLLA_MAX_IN_FLIGHT", "256")),
            Long.parseLong(env("FS_SCYLLA_TIMEOUT_MS", "5000")));

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? def : v;
    }

    private static List<String> split(String csv) {
        return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }
}
