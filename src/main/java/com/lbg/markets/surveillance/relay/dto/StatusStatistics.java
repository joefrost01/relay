package com.lbg.markets.surveillance.relay.dto;

import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.model.TransferStatus;

import java.time.Duration;
import java.util.List;

/**
 * Status statistics DTO.
 */
class StatusStatistics {
    public TransferStatus status;
    public Long count;
    public Double percentage;
    public Long avgProcessingTimeMs;
    public Long minProcessingTimeMs;
    public Long maxProcessingTimeMs;

    public static StatusStatistics calculate(TransferStatus status, List<FileTransfer> transfers) {
        StatusStatistics stats = new StatusStatistics();
        stats.status = status;

        List<FileTransfer> statusTransfers = transfers.stream()
                .filter(t -> t.status == status)
                .toList();

        stats.count = (long) statusTransfers.size();
        stats.percentage = transfers.isEmpty() ? 0.0
                : (double) stats.count / transfers.size() * 100;

        // Calculate timing statistics for completed transfers
        if (status == TransferStatus.COMPLETED) {
            var timings = statusTransfers.stream()
                    .filter(t -> t.startedAt != null && t.completedAt != null)
                    .map(t -> Duration.between(t.startedAt, t.completedAt).toMillis())
                    .toList();

            if (!timings.isEmpty()) {
                stats.avgProcessingTimeMs = (long) timings.stream()
                        .mapToLong(Long::longValue)
                        .average()
                        .orElse(0);
                stats.minProcessingTimeMs = timings.stream()
                        .mapToLong(Long::longValue)
                        .min()
                        .orElse(0);
                stats.maxProcessingTimeMs = timings.stream()
                        .mapToLong(Long::longValue)
                        .max()
                        .orElse(0);
            }
        }

        return stats;
    }
}
