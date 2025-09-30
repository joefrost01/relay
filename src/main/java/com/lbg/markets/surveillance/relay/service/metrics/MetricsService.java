package com.lbg.markets.surveillance.relay.service.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class MetricsService {
    @Inject
    MeterRegistry registry;

    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();

    public void recordTransferComplete(String source, long durationMs) {
        getCounter("transfers.completed", "source", source).increment();
        getTimer("transfer.duration", "source", source)
                .record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public void recordTransferFailed(String source, String reason) {
        getCounter("transfers.failed", "source", source, "reason", reason).increment();
    }

    public void recordEvent(String eventType, Map<String, Object> data) {
        getCounter("events", "type", eventType).increment();
        // Log if needed, but don't overcomplicate
    }

    private Counter getCounter(String name, String... tags) {
        String key = name + String.join(",", tags);
        return counters.computeIfAbsent(key, k ->
                Counter.builder(name).tags(tags).register(registry));
    }

    private Timer getTimer(String name, String... tags) {
        String key = name + String.join(",", tags);
        return timers.computeIfAbsent(key, k ->
                Timer.builder(name).tags(tags).register(registry));
    }
}