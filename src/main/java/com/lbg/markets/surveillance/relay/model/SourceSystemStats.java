package com.lbg.markets.surveillance.relay.model;

import java.time.Duration;
import java.time.Instant;

/**
 * DTO for source system statistics.
 */
public class SourceSystemStats {
    public String systemId;
    public String displayName;
    public SourceSystem.SourceStatus status;
    public SourceSystem.HealthStatus health;
    public Long totalFiles;
    public Long totalBytes;
    public Double successRate;
    public Integer filestoday;
    public Instant lastFileReceived;
    public String lastFilename;
    public Integer consecutiveErrors;
    public Long avgProcessingTimeMs;
    public Boolean isOverdue;

    public static SourceSystemStats from(SourceSystem source) {
        SourceSystemStats stats = new SourceSystemStats();
        stats.systemId = source.systemId;
        stats.displayName = source.displayName;
        stats.status = source.status;
        stats.health = source.healthStatus;
        stats.totalFiles = source.totalFiles;
        stats.totalBytes = source.totalBytes;
        stats.successRate = source.getSuccessRate();
        stats.filestoday = source.filesToday;
        stats.lastFileReceived = source.lastFileReceived;
        stats.lastFilename = source.lastFilename;
        stats.consecutiveErrors = source.consecutiveErrors;
        stats.avgProcessingTimeMs = source.avgProcessingTimeMs;
        stats.isOverdue = source.isOverdue(Duration.ofHours(1));
        return stats;
    }
}
