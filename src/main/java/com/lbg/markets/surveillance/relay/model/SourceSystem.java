package com.lbg.markets.surveillance.relay.model;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.transaction.Transactional;

import jakarta.persistence.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Entity representing a configured source system.
 * Stores runtime state and statistics for each source.
 */
@Entity
@Table(name = "source_systems")
@NamedQueries({
        @NamedQuery(
                name = "SourceSystem.findEnabled",
                query = "SELECT s FROM SourceSystem s WHERE s.enabled = true ORDER BY s.priority"
        ),
        @NamedQuery(
                name = "SourceSystem.findByPriority",
                query = "SELECT s FROM SourceSystem s WHERE s.enabled = true ORDER BY s.priority, s.systemId"
        ),
        @NamedQuery(
                name = "SourceSystem.findOverdue",
                query = "SELECT s FROM SourceSystem s WHERE s.enabled = true " +
                        "AND s.expectedNextFile < :now AND s.lastFileReceived < :cutoff"
        )
})
public class SourceSystem extends PanacheEntity {

    /**
     * Unique identifier for the source system.
     * Matches the configuration ID.
     */
    @Column(name = "system_id", nullable = false, unique = true, length = 100)
    public String systemId;

    /**
     * Display name for UI and reports.
     */
    @Column(name = "display_name", length = 200)
    public String displayName;

    /**
     * Current path being monitored.
     * Can change if primary path fails and fallback is used.
     */
    @Column(name = "current_path", nullable = false, length = 500)
    public String currentPath;

    /**
     * Whether this source is currently enabled.
     */
    @Column(name = "enabled", nullable = false)
    public Boolean enabled = true;

    /**
     * Processing priority (1 = highest).
     */
    @Column(name = "priority", nullable = false)
    public Integer priority = 5;

    /**
     * Current status of the source system.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 50)
    public SourceStatus status = SourceStatus.ACTIVE;

    /**
     * Health status based on SLA and error rates.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "health_status", nullable = false, length = 50)
    public HealthStatus healthStatus = HealthStatus.HEALTHY;

    /**
     * Last time this source was scanned.
     */
    @Column(name = "last_scan_time")
    public Instant lastScanTime;

    /**
     * Last time a file was received from this source.
     */
    @Column(name = "last_file_received")
    public Instant lastFileReceived;

    /**
     * Name of the last file processed.
     */
    @Column(name = "last_filename", length = 255)
    public String lastFilename;

    /**
     * Expected time for next file (for SLA monitoring).
     */
    @Column(name = "expected_next_file")
    public Instant expectedNextFile;

    /**
     * Number of files processed today.
     */
    @Column(name = "files_today")
    public Integer filesToday = 0;

    /**
     * Number of files processed in total.
     */
    @Column(name = "total_files")
    public Long totalFiles = 0L;

    /**
     * Total bytes processed.
     */
    @Column(name = "total_bytes")
    public Long totalBytes = 0L;

    /**
     * Number of successful transfers.
     */
    @Column(name = "successful_transfers")
    public Long successfulTransfers = 0L;

    /**
     * Number of failed transfers.
     */
    @Column(name = "failed_transfers")
    public Long failedTransfers = 0L;

    /**
     * Current error count (resets on successful transfer).
     */
    @Column(name = "consecutive_errors")
    public Integer consecutiveErrors = 0;

    /**
     * Average processing time in milliseconds.
     */
    @Column(name = "avg_processing_time_ms")
    public Long avgProcessingTimeMs;

    /**
     * Last error message if any.
     */
    @Column(name = "last_error", length = 2000)
    public String lastError;

    /**
     * Last error timestamp.
     */
    @Column(name = "last_error_time")
    public Instant lastErrorTime;

    /**
     * Configuration version/hash to detect changes.
     */
    @Column(name = "config_version", length = 64)
    public String configVersion;

    /**
     * JSON metadata for additional properties.
     */
    @Column(name = "metadata", columnDefinition = "TEXT")
    public String metadata;

    /**
     * Created timestamp.
     */
    @Column(name = "created_at", nullable = false)
    public Instant createdAt;

    /**
     * Last updated timestamp.
     */
    @Column(name = "updated_at")
    public Instant updatedAt;

    /**
     * Version for optimistic locking.
     */
    @Version
    @Column(name = "version")
    public Long version;

    // ============ Lifecycle Hooks ============

    @PrePersist
    public void onCreate() {
        createdAt = Instant.now();
        updatedAt = createdAt;
    }

    @PreUpdate
    public void onUpdate() {
        updatedAt = Instant.now();
    }

    // ============ Business Methods ============

    /**
     * Record a successful file scan.
     */
    public void recordScan() {
        this.lastScanTime = Instant.now();
        this.updatedAt = this.lastScanTime;
    }

    /**
     * Record a file being detected.
     */
    public void recordFileDetected(String filename, Long fileSize) {
        this.lastFileReceived = Instant.now();
        this.lastFilename = filename;
        this.filesToday++;
        this.totalFiles++;
        if (fileSize != null) {
            this.totalBytes += fileSize;
        }
        this.updatedAt = Instant.now();

        // Update expected next file based on pattern
        updateExpectedNextFile();
    }

    /**
     * Record a successful transfer.
     */
    public void recordSuccess(long processingTimeMs) {
        this.successfulTransfers++;
        this.consecutiveErrors = 0;
        this.healthStatus = HealthStatus.HEALTHY;

        // Update average processing time
        if (this.avgProcessingTimeMs == null) {
            this.avgProcessingTimeMs = processingTimeMs;
        } else {
            // Weighted average favoring recent times
            this.avgProcessingTimeMs = (this.avgProcessingTimeMs * 9 + processingTimeMs) / 10;
        }

        this.updatedAt = Instant.now();
    }

    /**
     * Record a failed transfer.
     */
    public void recordFailure(String error) {
        this.failedTransfers++;
        this.consecutiveErrors++;
        this.lastError = error != null && error.length() > 2000
                ? error.substring(0, 2000)
                : error;
        this.lastErrorTime = Instant.now();

        // Update health status based on consecutive errors
        if (this.consecutiveErrors >= 10) {
            this.healthStatus = HealthStatus.CRITICAL;
            this.status = SourceStatus.ERROR;
        } else if (this.consecutiveErrors >= 5) {
            this.healthStatus = HealthStatus.DEGRADED;
        } else if (this.consecutiveErrors >= 3) {
            this.healthStatus = HealthStatus.WARNING;
        }

        this.updatedAt = Instant.now();
    }

    /**
     * Mark source as unavailable (e.g., path not accessible).
     */
    public void markUnavailable(String reason) {
        this.status = SourceStatus.UNAVAILABLE;
        this.healthStatus = HealthStatus.CRITICAL;
        this.lastError = reason;
        this.lastErrorTime = Instant.now();
        this.updatedAt = Instant.now();
    }

    /**
     * Mark source as available again.
     */
    public void markAvailable() {
        if (this.status == SourceStatus.UNAVAILABLE) {
            this.status = SourceStatus.ACTIVE;
            if (this.consecutiveErrors == 0) {
                this.healthStatus = HealthStatus.HEALTHY;
            }
        }
        this.updatedAt = Instant.now();
    }

    /**
     * Reset daily counters.
     */
    public void resetDailyCounters() {
        this.filesToday = 0;
        this.updatedAt = Instant.now();
    }

    /**
     * Update expected next file time based on historical pattern.
     */
    private void updateExpectedNextFile() {
        // Simple implementation - could be enhanced with pattern learning
        // For now, just add 24 hours to last received
        this.expectedNextFile = this.lastFileReceived.plus(Duration.ofDays(1));
    }

    /**
     * Check if source is overdue based on SLA.
     */
    public boolean isOverdue(Duration gracePeriod) {
        if (expectedNextFile == null) {
            return false;
        }
        return Instant.now().isAfter(expectedNextFile.plus(gracePeriod));
    }

    /**
     * Get success rate percentage.
     */
    public double getSuccessRate() {
        long total = successfulTransfers + failedTransfers;
        if (total == 0) {
            return 100.0;
        }
        return (double) successfulTransfers / total * 100;
    }

    /**
     * Check if source should be processed based on priority and health.
     */
    public boolean shouldProcess() {
        return enabled
                && status == SourceStatus.ACTIVE
                && healthStatus != HealthStatus.CRITICAL;
    }

    // ============ Static Query Methods ============

    public static Optional<SourceSystem> findBySystemId(String systemId) {
        return find("systemId", systemId).firstResultOptional();
    }

    public static List<SourceSystem> findEnabled() {
        return list("enabled = ?1 ORDER BY priority, systemId", true);
    }

    public static List<SourceSystem> findByStatus(SourceStatus status) {
        return list("status", status);
    }

    public static List<SourceSystem> findUnhealthy() {
        return list("healthStatus != ?1", HealthStatus.HEALTHY);
    }

    public static List<SourceSystem> findOverdue(Duration gracePeriod) {
        Instant cutoff = Instant.now().minus(gracePeriod);
        return list(
                "enabled = true AND expectedNextFile < ?1 AND lastFileReceived < ?2",
                Instant.now(), cutoff
        );
    }

    public static long countByHealth(HealthStatus health) {
        return count("healthStatus", health);
    }

    /**
     * Update or create source system from configuration.
     */
    @Transactional
    public static SourceSystem updateFromConfig(String systemId, String displayName,
                                                String path, int priority, boolean enabled) {
        SourceSystem source = findBySystemId(systemId).orElse(new SourceSystem());

        boolean isNew = source.id == null;

        source.systemId = systemId;
        source.displayName = displayName != null ? displayName : systemId;
        source.currentPath = path;
        source.priority = priority;
        source.enabled = enabled;

        if (isNew) {
            source.status = SourceStatus.ACTIVE;
            source.healthStatus = HealthStatus.HEALTHY;
            source.persist();
        } else {
            source.updatedAt = Instant.now();
        }

        return source;
    }

    // ============ Enums ============

    /**
     * Source system operational status.
     */
    public enum SourceStatus {
        /**
         * Source is active and being monitored.
         */
        ACTIVE,

        /**
         * Source is paused by administrator.
         */
        PAUSED,

        /**
         * Source path is not accessible.
         */
        UNAVAILABLE,

        /**
         * Source has errors but still trying.
         */
        ERROR,

        /**
         * Source is disabled in configuration.
         */
        DISABLED,

        /**
         * Source is in maintenance mode.
         */
        MAINTENANCE
    }

    /**
     * Health status based on recent performance.
     */
    public enum HealthStatus {
        /**
         * Operating normally.
         */
        HEALTHY,

        /**
         * Minor issues detected.
         */
        WARNING,

        /**
         * Performance degraded but operational.
         */
        DEGRADED,

        /**
         * Critical issues, may stop processing.
         */
        CRITICAL,

        /**
         * Unknown state (new source).
         */
        UNKNOWN
    }
}

