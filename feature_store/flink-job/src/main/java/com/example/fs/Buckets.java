package com.example.fs;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * Maps an event time to calendar buckets in the business timezone.
 * A bucket is identified by its label (e.g. "2026-10-19"), so "today" rolling over
 * is just a new key: there is no reset job.
 */
public final class Buckets implements Serializable {

    public enum Grain {
        H(Duration.ofHours(1), Duration.ofHours(48)),
        D(Duration.ofHours(25), Duration.ofDays(7)),
        M(Duration.ofDays(31), Duration.ofDays(400)),   // covers current month + last 12
        Y(Duration.ofDays(366), Duration.ofDays(400));  // keeps last year around for YoY

        /** Upper bound of the period length (DST-safe). */
        public final Duration maxPeriod;
        /** How long a bucket is kept after its period ends. */
        public final Duration retention;

        Grain(Duration maxPeriod, Duration retention) {
            this.maxPeriod = maxPeriod;
            this.retention = retention;
        }
    }

    public static final class Key {
        public final Grain grain;
        public final String label;
        public final long startMs;
        public final long expiresAtMs;

        Key(Grain grain, String label, long startMs, long expiresAtMs) {
            this.grain = grain;
            this.label = label;
            this.startMs = startMs;
            this.expiresAtMs = expiresAtMs;
        }
    }

    private static final DateTimeFormatter HOUR = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH");

    private final String zoneId;
    private transient ZoneId zone;

    public Buckets(String zoneId) {
        this.zoneId = zoneId;
        ZoneId.of(zoneId); // fail fast on a bad zone
    }

    private ZoneId zone() {
        if (zone == null) {
            zone = ZoneId.of(zoneId);
        }
        return zone;
    }

    public Key keyFor(Grain grain, long eventTimeMs) {
        ZonedDateTime t = Instant.ofEpochMilli(eventTimeMs).atZone(zone());
        LocalDate date = t.toLocalDate();
        ZonedDateTime start;
        ZonedDateTime end;
        String label;
        switch (grain) {
            case H:
                start = t.truncatedTo(ChronoUnit.HOURS);
                end = start.plusHours(1);
                label = start.format(HOUR);
                break;
            case D:
                start = date.atStartOfDay(zone());
                end = date.plusDays(1).atStartOfDay(zone());
                label = date.toString();
                break;
            case M:
                LocalDate firstOfMonth = date.withDayOfMonth(1);
                start = firstOfMonth.atStartOfDay(zone());
                end = firstOfMonth.plusMonths(1).atStartOfDay(zone());
                label = YearMonth.from(firstOfMonth).toString();
                break;
            case Y:
                LocalDate firstOfYear = date.withDayOfYear(1);
                start = firstOfYear.atStartOfDay(zone());
                end = firstOfYear.plusYears(1).atStartOfDay(zone());
                label = String.valueOf(firstOfYear.getYear());
                break;
            default:
                throw new IllegalArgumentException("Unknown grain " + grain);
        }
        long expiresAt = end.toInstant().plus(grain.retention).toEpochMilli();
        return new Key(grain, label, start.toInstant().toEpochMilli(), expiresAt);
    }
}
