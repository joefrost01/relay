package com.lbg.markets.surveillance.relay.repository;

import com.lbg.markets.surveillance.relay.dto.SourceSystemStats;
import com.lbg.markets.surveillance.relay.model.SourceSystem;
import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Repository for additional complex queries.
 */
@ApplicationScoped
class SourceSystemRepository implements PanacheRepository<SourceSystem> {

    /**
     * Get sources that need scanning based on poll interval.
     */
    public List<SourceSystem> findSourcesNeedingScanning(Duration defaultInterval) {
        Instant cutoff = Instant.now().minus(defaultInterval);
        return list(
                "enabled = true AND status = 'ACTIVE' AND " +
                        "(lastScanTime IS NULL OR lastScanTime < ?1) " +
                        "ORDER BY priority, lastScanTime",
                cutoff
        );
    }

    /**
     * Get source statistics for dashboard.
     */
    public List<SourceSystemStats> getStatistics() {
        return streamAll()
                .map(SourceSystemStats::from)
                .toList();
    }

    /**
     * Find sources with high error rates.
     */
    public List<SourceSystem> findHighErrorRateSources(double threshold) {
        return streamAll()
                .filter(s -> s.getSuccessRate() < (100 - threshold))
                .toList();
    }

    /**
     * Update all sources to reset daily counters.
     */
    @Transactional
    public void resetAllDailyCounters() {
        update("filesToday = 0, updatedAt = ?1", Instant.now());
    }

    /**
     * Mark sources as unavailable if path doesn't exist.
     */
    @Transactional
    public void markUnavailableByPath(String path, String reason) {
        update(
                "status = ?1, healthStatus = ?2, lastError = ?3, lastErrorTime = ?4 " +
                        "WHERE currentPath = ?5",
                SourceSystem.SourceStatus.UNAVAILABLE,
                SourceSystem.HealthStatus.CRITICAL,
                reason,
                Instant.now(),
                path
        );
    }
}
