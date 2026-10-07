package com.github.lechandonga.durabletaskqueuejava.tests.support;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

/** 可手动推进时间的时钟，用于确定性地测试退避与租约过期。 */
public class ManualClock extends Clock {

    private final AtomicLong nowMillis;

    public ManualClock() {
        this(System.currentTimeMillis());
    }

    public ManualClock(long initialMillis) {
        this.nowMillis = new AtomicLong(initialMillis);
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(nowMillis.get());
    }

    @Override
    public long millis() {
        return nowMillis.get();
    }

    public void advanceMillis(long deltaMillis) {
        nowMillis.addAndGet(deltaMillis);
    }
}
