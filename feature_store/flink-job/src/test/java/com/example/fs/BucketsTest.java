package com.example.fs;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BucketsTest {

    private final Buckets ist = new Buckets("Asia/Kolkata");

    private static long ms(String isoZoned) {
        return ZonedDateTime.parse(isoZoned).toInstant().toEpochMilli();
    }

    @Test
    void labelsUseLocalCalendarNotUtc() {
        // 2026-10-18T20:00Z is already 2026-10-19 01:30 in IST
        long t = ms("2026-10-18T20:00:00Z");
        assertEquals("2026-10-19", ist.keyFor(Buckets.Grain.D, t).label);
        assertEquals("2026-10", ist.keyFor(Buckets.Grain.M, t).label);
        assertEquals("2026", ist.keyFor(Buckets.Grain.Y, t).label);
        assertEquals("2026-10-19T01", ist.keyFor(Buckets.Grain.H, t).label);
    }

    @Test
    void yearBoundary() {
        long t = ms("2026-12-31T19:00:00Z"); // 2027-01-01 00:30 IST
        assertEquals("2027", ist.keyFor(Buckets.Grain.Y, t).label);
        assertEquals("2027-01", ist.keyFor(Buckets.Grain.M, t).label);
    }

    @Test
    void expiryIsPeriodEndPlusRetention() {
        long t = ms("2026-10-19T12:00:00+05:30[Asia/Kolkata]");
        Buckets.Key day = ist.keyFor(Buckets.Grain.D, t);
        long dayEnd = ms("2026-10-20T00:00:00+05:30[Asia/Kolkata]");
        assertEquals(dayEnd + Buckets.Grain.D.retention.toMillis(), day.expiresAtMs);
        assertEquals(ms("2026-10-19T00:00:00+05:30[Asia/Kolkata]"), day.startMs);
    }

    @Test
    void dstDayIsTwentyFiveHours() {
        Buckets ny = new Buckets("America/New_York");
        long t = ms("2026-11-01T12:00:00-05:00[America/New_York]"); // DST ends that day
        Buckets.Key k = ny.keyFor(Buckets.Grain.D, t);
        long end = ZonedDateTime.of(2026, 11, 2, 0, 0, 0, 0, ZoneId.of("America/New_York")).toInstant().toEpochMilli();
        assertEquals(25 * 3_600_000L, end - k.startMs);
    }
}
