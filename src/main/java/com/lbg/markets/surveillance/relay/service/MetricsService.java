package com.lbg.markets.surveillance.relay.service;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class MetricsService {
    @Inject
    MeterRegistry registry;

    public void recordTransferComplete(String source, long duration) {
        registry.counter("transfers.completed", "source", source).increment();
        registry.timer("transfer.duration", "source", source).record(duration, TimeUnit.MILLISECONDS);
    }

    public void recordTransferFailed(String source, String reason) {
        registry.counter("transfers.failed", "source", source, "reason", reason).increment();
    }
}