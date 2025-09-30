package com.lbg.markets.surveillance.relay.service.metrics;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
@IfBuildProfile("dev")
public class SimpleMetricsService implements MetricsService {

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();

    @Override
    public void recordEvent(String eventType, Map<String, Object> data) {
        Log.infof("Event: %s - %s", eventType, data);
        incrementCounter("events." + eventType);
    }

    @Override
    public void recordError(String errorType, String source, String message) {
        Log.errorf("Error: %s from %s - %s", errorType, source, message);
        incrementCounter("errors." + errorType);
    }

    @Override
    public void incrementCounter(String name, String... tags) {
        String key = buildKey(name, tags);
        counters.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
    }

    @Override
    public void recordDuration(String name, long durationMs, String... tags) {
        Log.debugf("Duration: %s = %dms", name, durationMs);
    }

    private String buildKey(String name, String... tags) {
        if (tags.length == 0) return name;
        return name + "." + String.join(".", tags);
    }

    public Map<String, Long> getCounters() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        counters.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }
}