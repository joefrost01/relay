package com.lbg.markets.surveillance.relay.service.metrics;

import java.util.Map;

public interface MetricsService {
    void recordEvent(String eventType, Map<String, Object> data);

    void recordError(String errorType, String source, String message);

    void incrementCounter(String name, String... tags);

    void recordDuration(String name, long durationMs, String... tags);
}