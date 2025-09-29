package com.lbg.markets.surveillance.relay.routes;

import com.google.cloud.bigquery.*;
import com.lbg.markets.surveillance.relay.config.RelayConfiguration;
import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.model.SourceSystem;
import com.lbg.markets.surveillance.relay.model.TransferStatus;
import com.lbg.markets.surveillance.relay.model.TransferStatusHistory;
import com.lbg.markets.surveillance.relay.service.MonitoringService;
import com.lbg.markets.surveillance.relay.service.TransferOrchestrator;
import io.quarkus.logging.Log;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.sql.SqlConstants;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.*;

/**
 * Camel routes for handling file reprocessing.
 * Supports manual requests, automatic retries, and batch reprocessing.
 */
@ApplicationScoped
public class ReprocessingRoute extends RouteBuilder {

    @Inject RelayConfiguration config;
    @Inject TransferOrchestrator orchestrator;
    @Inject MonitoringService monitoringService;
    @Inject EntityManager entityManager;
    @Inject BigQuery bigQuery;

    @ConfigProperty(name = "relay.node-name")
    String nodeName;

    @ConfigProperty(name = "relay.reprocessing.enabled", defaultValue = "true")
    boolean reprocessingEnabled;

    @ConfigProperty(name = "relay.reprocessing.max-retries", defaultValue = "3")
    int maxRetries;

    @ConfigProperty(name = "relay.reprocessing.retry-delay-minutes", defaultValue = "5")
    int retryDelayMinutes;

    @ConfigProperty(name = "relay.reprocessing.batch-size", defaultValue = "100")
    int batchSize;

    @ConfigProperty(name = "relay.reprocessing.bigquery-poll-interval-seconds", defaultValue = "60")
    int bigQueryPollInterval;

    @ConfigProperty(name = "relay.reprocessing.auto-retry-enabled", defaultValue = "true")
    boolean autoRetryEnabled;

    @ConfigProperty(name = "relay.reprocessing.quarantine-after-failures", defaultValue = "5")
    int quarantineThreshold;

    // Track reprocessing statistics
    private final Map<Long, ReprocessingStats> reprocessingStats = new ConcurrentHashMap<>();
    private final AtomicInteger activeReprocessingCount = new AtomicInteger(0);

    @Override
    public void configure() throws Exception {

        if (!reprocessingEnabled) {
            Log.info("Reprocessing routes disabled");
            return;
        }

        // Configure error handling
        configureErrorHandling();

        // Configure manual reprocessing routes
        configureManualReprocessing();

        // Configure automatic retry routes
        configureAutomaticRetries();

        // Configure BigQuery-triggered reprocessing
        configureBigQueryReprocessing();

        // Configure batch reprocessing
        configureBatchReprocessing();

        // Configure scheduled reprocessing
        configureScheduledReprocessing();

        // Configure reprocessing monitoring
        configureReprocessingMonitoring();

        // Configure cleanup routes
        configureCleanupRoutes();
    }

    /**
     * Configure error handling for reprocessing routes.
     */
    private void configureErrorHandling() {

        onException(Exception.class)
                .maximumRedeliveries(2)
                .redeliveryDelay(1000)
                .handled(true)
                .process(exchange -> {
                    Exception cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    Long transferId = exchange.getIn().getHeader("TransferId", Long.class);

                    Log.errorf(cause, "Reprocessing failed for transfer: %d", transferId);

                    if (transferId != null) {
                        markReprocessingFailed(transferId, cause.getMessage());
                    }

                    monitoringService.recordError("reprocessing_error",
                            transferId != null ? transferId.toString() : "unknown",
                            cause.getMessage());
                })
                .to("direct:reprocessing-dlq");

        // Dead letter queue for failed reprocessing
        from(direct("reprocessing-dlq"))
                .routeId("reprocessing-dlq")
                .log(LoggingLevel.ERROR, "Reprocessing failed: ${header.TransferId}")
                .process(exchange -> {
                    Long transferId = exchange.getIn().getHeader("TransferId", Long.class);
                    if (transferId != null) {
                        ReprocessingStats stats = reprocessingStats.get(transferId);
                        if (stats != null) {
                            stats.failures++;

                            // Quarantine if too many failures
                            if (stats.failures >= quarantineThreshold) {
                                quarantineTransfer(transferId);
                            }
                        }
                    }
                });
    }

    /**
     * Configure manual reprocessing triggered by API or UI.
     */
    private void configureManualReprocessing() {

        // Single file reprocessing
        from(direct("reprocess-single"))
                .routeId("reprocess-single-file")
                .process(new ReprocessingValidator())
                .choice()
                .when(header("ValidationPassed").isEqualTo(true))
                .process(exchange -> {
                    Long transferId = exchange.getIn().getHeader("TransferId", Long.class);
                    String reason = exchange.getIn().getHeader("Reason", String.class);
                    String requestedBy = exchange.getIn().getHeader("RequestedBy", String.class);

                    FileTransfer transfer = markForReprocessing(transferId, reason, requestedBy);

                    exchange.getIn().setBody(transfer);
                    exchange.getIn().setHeader("SourceSystem", transfer.sourceSystem);
                })
                .to("direct:execute-reprocess")
                .otherwise()
                .log(LoggingLevel.WARN, "Reprocessing validation failed: ${header.ValidationError}")
                .end();

        // Bulk reprocessing
        from(direct("reprocess-bulk"))
                .routeId("reprocess-bulk-files")
                .split(body())
                .parallelProcessing()
                .threads(5)
                .process(exchange -> {
                    Long transferId = exchange.getIn().getBody(Long.class);
                    exchange.getIn().setHeader("TransferId", transferId);
                })
                .to("direct:reprocess-single")
                .end()
                .process(exchange -> {
                    List<?> results = exchange.getIn().getBody(List.class);
                    Log.infof("Bulk reprocessing completed: %d files", results.size());
                });

        // Execute reprocessing
        from(direct("execute-reprocess"))
                .routeId("execute-reprocessing")
                .process(exchange -> {
                    FileTransfer transfer = exchange.getIn().getBody(FileTransfer.class);

                    // Initialize stats
                    reprocessingStats.put(transfer.id, new ReprocessingStats(transfer.id));
                    activeReprocessingCount.incrementAndGet();

                    // Queue for processing
                    orchestrator.queueTransfer(transfer.id);

                    // Record event
                    monitoringService.recordEvent("reprocess_initiated", Map.of(
                            "transfer_id", transfer.id,
                            "source", transfer.sourceSystem,
                            "filename", transfer.filename,
                            "retry_count", transfer.retryCount,
                            "node", nodeName
                    ));
                })
                .log(LoggingLevel.INFO, "Reprocessing initiated for: ${header.TransferId}");
    }

    /**
     * Configure automatic retry for failed transfers.
     */
    private void configureAutomaticRetries() {

        if (!autoRetryEnabled) {
            Log.info("Automatic retries disabled");
            return;
        }

        // Check for failed transfers eligible for retry
        from(timer("auto-retry-check")
                .period(Duration.ofMinutes(retryDelayMinutes).toMillis())
                .delay(60000))
                .routeId("check-auto-retry")
                .process(exchange -> {
                    Instant retryAfter = Instant.now().minus(Duration.ofMinutes(retryDelayMinutes));

                    List<FileTransfer> eligibleForRetry = FileTransfer.find(
                            "status = ?1 and retryCount < ?2 and updatedAt < ?3",
                            TransferStatus.FAILED, maxRetries, retryAfter
                    ).list();

                    if (!eligibleForRetry.isEmpty()) {
                        Log.infof("Found %d transfers eligible for automatic retry",
                                eligibleForRetry.size());
                        exchange.getIn().setBody(eligibleForRetry);
                    }
                })
                .split(body())
                .parallelProcessing()
                .process(exchange -> {
                    FileTransfer transfer = exchange.getIn().getBody(FileTransfer.class);

                    // Check if should retry based on error type
                    if (shouldAutoRetry(transfer)) {
                        exchange.getIn().setHeader("TransferId", transfer.id);
                        exchange.getIn().setHeader("AutoRetry", true);
                    } else {
                        exchange.getIn().setHeader("SkipRetry", true);
                    }
                })
                .choice()
                .when(header("SkipRetry").isNull())
                .to("direct:auto-retry")
                .end()
                .end();

        // Process automatic retry
        from(direct("auto-retry"))
                .routeId("process-auto-retry")
                .process(exchange -> {
                    Long transferId = exchange.getIn().getHeader("TransferId", Long.class);

                    FileTransfer transfer = FileTransfer.findById(transferId);
                    if (transfer != null) {
                        transfer.status = TransferStatus.RETRYING;
                        transfer.retryCount++;
                        transfer.errorMessage = null;
                        transfer.persist();

                        // Record retry attempt
                        TransferStatusHistory.recordTransition(
                                transfer.id,
                                TransferStatus.FAILED,
                                TransferStatus.RETRYING,
                                "SYSTEM",
                                "Automatic retry attempt " + transfer.retryCount
                        );

                        // Queue for processing
                        orchestrator.queueTransfer(transfer.id);

                        monitoringService.recordEvent("auto_retry", Map.of(
                                "transfer_id", transfer.id,
                                "retry_count", transfer.retryCount,
                                "source", transfer.sourceSystem
                        ));
                    }
                })
                .log(LoggingLevel.INFO, "Auto-retry initiated for: ${header.TransferId}");
    }

    /**
     * Configure BigQuery-triggered reprocessing.
     */
    private void configureBigQueryReprocessing() {

        // Poll BigQuery for reprocess requests
        from(timer("bigquery-reprocess-poll")
                .period(bigQueryPollInterval * 1000)
                .delay(30000))
                .routeId("poll-bigquery-reprocess")
                .process(exchange -> {
                    String query = String.format("""
                    SELECT 
                        file_id,
                        source_system,
                        filename,
                        reason,
                        requested_by,
                        requested_at
                    FROM `%s.%s.%s`
                    WHERE processed = false
                    AND requested_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
                    ORDER BY requested_at
                    LIMIT 100
                    """,
                            config.monitoring().bigQueryDataset().replace("`", ""),
                            config.monitoring().bigQueryDataset().replace("`", ""),
                            config.monitoring().reprocessTable()
                    );

                    try {
                        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                                .setUseLegacySql(false)
                                .build();

                        TableResult result = bigQuery.query(queryConfig);

                        List<ReprocessRequest> requests = new ArrayList<>();
                        for (FieldValueList row : result.iterateAll()) {
                            ReprocessRequest request = new ReprocessRequest();
                            request.fileId = row.get("file_id").getLongValue();
                            request.sourceSystem = row.get("source_system").getStringValue();
                            request.filename = row.get("filename").getStringValue();
                            request.reason = row.get("reason").getStringValue();
                            request.requestedBy = row.get("requested_by").getStringValue();
                            requests.add(request);
                        }

                        if (!requests.isEmpty()) {
                            exchange.getIn().setBody(requests);
                            Log.infof("Found %d reprocess requests from BigQuery", requests.size());
                        }

                    } catch (Exception e) {
                        Log.error("Failed to query BigQuery for reprocess requests", e);
                    }
                })
                .choice()
                .when(body().isNotNull())
                .to("direct:process-bigquery-requests")
                .end();

        // Process BigQuery requests
        from(direct("process-bigquery-requests"))
                .routeId("process-bigquery-reprocess")
                .split(body())
                .parallelProcessing()
                .process(exchange -> {
                    ReprocessRequest request = exchange.getIn().getBody(ReprocessRequest.class);

                    // Validate and mark for reprocessing
                    FileTransfer transfer = FileTransfer.findById(request.fileId);
                    if (transfer != null) {
                        markForReprocessing(transfer.id, request.reason, request.requestedBy);
                        orchestrator.queueTransfer(transfer.id);

                        // Mark as processed in BigQuery
                        markBigQueryRequestProcessed(request.fileId);

                        monitoringService.recordEvent("bigquery_reprocess", Map.of(
                                "transfer_id", request.fileId,
                                "requested_by", request.requestedBy,
                                "reason", request.reason
                        ));
                    } else {
                        Log.warnf("Transfer not found for BigQuery reprocess request: %d",
                                request.fileId);
                    }
                })
                .end();
    }

    /**
     * Configure batch reprocessing for multiple files.
     */
    private void configureBatchReprocessing() {

        // Reprocess by source system
        from(direct("reprocess-by-source"))
                .routeId("reprocess-source-system")
                .process(exchange -> {
                    String sourceSystem = exchange.getIn().getHeader("SourceSystem", String.class);
                    String dateRange = exchange.getIn().getHeader("DateRange", String.class);

                    List<FileTransfer> transfers = findTransfersForReprocess(sourceSystem, dateRange);

                    if (!transfers.isEmpty()) {
                        Log.infof("Found %d transfers for source %s to reprocess",
                                transfers.size(), sourceSystem);
                        exchange.getIn().setBody(transfers);
                    } else {
                        exchange.getIn().setHeader("NoTransfers", true);
                    }
                })
                .choice()
                .when(header("NoTransfers").isNull())
                .to("direct:batch-reprocess")
                .otherwise()
                .log("No transfers found for reprocessing")
                .end();

        // Reprocess by status
        from(direct("reprocess-by-status"))
                .routeId("reprocess-by-status")
                .process(exchange -> {
                    String status = exchange.getIn().getHeader("Status", String.class);
                    Integer limit = exchange.getIn().getHeader("Limit", Integer.class);

                    TransferStatus targetStatus = TransferStatus.fromString(status);

                    List<FileTransfer> transfers = FileTransfer.find(
                            "status = ?1",
                            Sort.by("createdAt").descending(),
                            targetStatus
                    ).page(0, limit != null ? limit : batchSize).list();

                    exchange.getIn().setBody(transfers);
                })
                .to("direct:batch-reprocess");

        // Process batch reprocessing
        from(direct("batch-reprocess"))
                .routeId("process-batch-reprocess")
                .process(exchange -> {
                    List<FileTransfer> transfers = exchange.getIn().getBody(List.class);
                    String reason = exchange.getIn().getHeader("Reason", String.class);
                    String requestedBy = exchange.getIn().getHeader("RequestedBy", String.class);

                    Log.infof("Starting batch reprocess for %d transfers", transfers.size());

                    // Process in batches
                    List<List<FileTransfer>> batches = partition(transfers, batchSize);
                    exchange.getIn().setBody(batches);
                    exchange.getIn().setHeader("TotalBatches", batches.size());
                })
                .split(body())
                .process(exchange -> {
                    List<FileTransfer> batch = exchange.getIn().getBody(List.class);
                    Integer batchNumber = exchange.getProperty(Exchange.SPLIT_INDEX, Integer.class);
                    Integer totalBatches = exchange.getIn().getHeader("TotalBatches", Integer.class);

                    Log.infof("Processing batch %d/%d with %d transfers",
                            batchNumber + 1, totalBatches, batch.size());

                    processBatch(batch);
                })
                .delay(2000) // Delay between batches
                .end()
                .process(exchange -> {
                    Log.info("Batch reprocessing completed");

                    monitoringService.recordEvent("batch_reprocess_completed", Map.of(
                            "total_batches", exchange.getIn().getHeader("TotalBatches"),
                            "node", nodeName
                    ));
                });
    }

    /**
     * Configure scheduled reprocessing tasks.
     */
    private void configureScheduledReprocessing() {

        // Daily cleanup of stuck transfers
        from(cron("daily-stuck-cleanup")
                .schedule("0 0 2 * * ?"))  // 2 AM daily
                .routeId("cleanup-stuck-transfers")
                .process(exchange -> {
                    Instant cutoff = Instant.now().minus(Duration.ofHours(24));

                    List<FileTransfer> stuckTransfers = FileTransfer.find(
                            "status = ?1 and startedAt < ?2",
                            TransferStatus.PROCESSING, cutoff
                    ).list();

                    if (!stuckTransfers.isEmpty()) {
                        Log.infof("Found %d stuck transfers for cleanup", stuckTransfers.size());

                        for (FileTransfer transfer : stuckTransfers) {
                            transfer.status = TransferStatus.FAILED;
                            transfer.errorMessage = "Transfer stuck in processing for over 24 hours";
                            transfer.persist();

                            // Mark for reprocessing
                            markForReprocessing(transfer.id, "Stuck transfer cleanup", "SYSTEM");
                        }
                    }
                });

        // Weekly reprocess of quarantined files
        from(cron("weekly-quarantine-review")
                .schedule("0 0 0 ? * SUN"))  // Sunday midnight
                .routeId("review-quarantined")
                .process(exchange -> {
                    List<FileTransfer> quarantined = FileTransfer.find(
                            "status = ?1", TransferStatus.QUARANTINED
                    ).list();

                    if (!quarantined.isEmpty()) {
                        Log.infof("Reviewing %d quarantined transfers", quarantined.size());

                        // Generate report for manual review
                        Map<String, Object> report = generateQuarantineReport(quarantined);
                        exportQuarantineReport(report);
                    }
                });

        // Hourly retry of validation failures
        from(timer("validation-retry")
                .period(Duration.ofHours(1).toMillis()))
                .routeId("retry-validation-failures")
                .process(exchange -> {
                    List<FileTransfer> validationFailures = FileTransfer.find(
                            "status = ?1 and retryCount < ?2",
                            TransferStatus.VALIDATION_FAILED, 3
                    ).list();

                    for (FileTransfer transfer : validationFailures) {
                        exchange.getIn().setHeader("TransferId", transfer.id);
                        exchange.getIn().setHeader("Reason", "Scheduled validation retry");
                    }
                })
                .choice()
                .when(header("TransferId").isNotNull())
                .to("direct:reprocess-single")
                .end();
    }

    /**
     * Configure reprocessing monitoring.
     */
    private void configureReprocessingMonitoring() {

        // Monitor reprocessing progress
        from(timer("monitor-reprocessing")
                .period(30000))  // Every 30 seconds
                .routeId("monitor-reprocessing-progress")
                .process(exchange -> {
                    if (activeReprocessingCount.get() > 0) {
                        Map<String, Object> stats = new HashMap<>();
                        stats.put("active_count", activeReprocessingCount.get());

                        // Collect stats from active reprocessing
                        List<Map<String, Object>> activeStats = reprocessingStats.entrySet().stream()
                                .filter(e -> !e.getValue().completed)
                                .map(e -> Map.<String, Object>of(
                                        "transfer_id", e.getKey(),
                                        "attempts", e.getValue().attempts,
                                        "failures", e.getValue().failures,
                                        "duration_ms", Duration.between(
                                                e.getValue().startTime, Instant.now()
                                        ).toMillis()
                                ))
                                .collect(Collectors.toList());

                        stats.put("active_transfers", activeStats);

                        monitoringService.recordEvent("reprocessing_progress", stats);
                    }
                });

        // Alert on high reprocessing failure rate
        from(timer("reprocess-failure-monitor")
                .period(Duration.ofMinutes(10).toMillis()))
                .routeId("monitor-reprocess-failures")
                .process(exchange -> {
                    long totalReprocessed = reprocessingStats.size();
                    long failed = reprocessingStats.values().stream()
                            .filter(s -> s.failures > 0)
                            .count();

                    if (totalReprocessed > 0) {
                        double failureRate = (double) failed / totalReprocessed * 100;

                        if (failureRate > 20) {
                            exchange.getIn().setHeader("AlertType", "HIGH_REPROCESS_FAILURE_RATE");
                            exchange.getIn().setHeader("FailureRate", failureRate);
                            exchange.getIn().setHeader("TotalReprocessed", totalReprocessed);
                            exchange.getIn().setHeader("Failed", failed);
                        }
                    }
                })
                .choice()
                .when(header("AlertType").isNotNull())
                .log(LoggingLevel.WARN,
                        "High reprocessing failure rate: ${header.FailureRate}%")
                .to("direct:send-alert")
                .end();
    }

    /**
     * Configure cleanup routes.
     */
    private void configureCleanupRoutes() {

        // Clean up old reprocessing stats
        from(timer("cleanup-stats")
                .period(Duration.ofHours(1).toMillis()))
                .routeId("cleanup-reprocessing-stats")
                .process(exchange -> {
                    Instant cutoff = Instant.now().minus(Duration.ofHours(24));

                    List<Long> toRemove = reprocessingStats.entrySet().stream()
                            .filter(e -> e.getValue().completed &&
                                    e.getValue().completedTime.isBefore(cutoff))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    toRemove.forEach(reprocessingStats::remove);

                    if (!toRemove.isEmpty()) {
                        Log.debugf("Cleaned up %d old reprocessing stats", toRemove.size());
                    }
                });

        // Archive old reprocessing history
        from(cron("archive-history")
                .schedule("0 0 3 * * ?"))  // 3 AM daily
                .routeId("archive-reprocess-history")
                .process(exchange -> {
                    Instant archiveCutoff = Instant.now().minus(Duration.ofDays(30));

                    // Archive to BigQuery
                    List<TransferStatusHistory> oldHistory = TransferStatusHistory.find(
                            "changedAt < ?1", archiveCutoff
                    ).list();

                    if (!oldHistory.isEmpty()) {
                        archiveHistoryToBigQuery(oldHistory);

                        // Delete from database
                        TransferStatusHistory.delete("changedAt < ?1", archiveCutoff);

                        Log.infof("Archived %d reprocessing history records", oldHistory.size());
                    }
                });
    }

    // ============ Helper Methods ============

    /**
     * Validator for reprocessing requests.
     */
    private class ReprocessingValidator implements Processor {
        @Override
        public void process(Exchange exchange) throws Exception {
            Long transferId = exchange.getIn().getHeader("TransferId", Long.class);

            if (transferId == null) {
                exchange.getIn().setHeader("ValidationPassed", false);
                exchange.getIn().setHeader("ValidationError", "Transfer ID is required");
                return;
            }

            FileTransfer transfer = FileTransfer.findById(transferId);
            if (transfer == null) {
                exchange.getIn().setHeader("ValidationPassed", false);
                exchange.getIn().setHeader("ValidationError", "Transfer not found: " + transferId);
                return;
            }

            // Check if already processing
            if (transfer.status == TransferStatus.PROCESSING ||
                    transfer.status == TransferStatus.REPROCESSING) {
                exchange.getIn().setHeader("ValidationPassed", false);
                exchange.getIn().setHeader("ValidationError", "Transfer is already processing");
                return;
            }

            // Check retry limit
            if (transfer.retryCount >= maxRetries * 2) { // Double limit for manual
                exchange.getIn().setHeader("ValidationPassed", false);
                exchange.getIn().setHeader("ValidationError",
                        "Transfer has exceeded maximum retry attempts");
                return;
            }

            exchange.getIn().setHeader("ValidationPassed", true);
        }
    }

    @Transactional
    private FileTransfer markForReprocessing(Long transferId, String reason, String requestedBy) {
        FileTransfer transfer = FileTransfer.findById(transferId);
        if (transfer == null) {
            throw new IllegalArgumentException("Transfer not found: " + transferId);
        }

        TransferStatus previousStatus = transfer.status;
        transfer.status = TransferStatus.REPROCESS_REQUESTED;
        transfer.errorMessage = null;
        transfer.processingNode = null;
        transfer.persist();

        // Record status change
        TransferStatusHistory.recordTransition(
                transferId,
                previousStatus,
                TransferStatus.REPROCESS_REQUESTED,
                requestedBy != null ? requestedBy : "SYSTEM",
                reason != null ? reason : "Manual reprocess request"
        );

        return transfer;
    }

    @Transactional
    private void markReprocessingFailed(Long transferId, String error) {
        FileTransfer transfer = FileTransfer.findById(transferId);
        if (transfer != null) {
            transfer.status = TransferStatus.FAILED;
            transfer.errorMessage = "Reprocessing failed: " + error;
            transfer.persist();

            ReprocessingStats stats = reprocessingStats.get(transferId);
            if (stats != null) {
                stats.completed = true;
                stats.completedTime = Instant.now();
                stats.failures++;
            }

            activeReprocessingCount.decrementAndGet();
        }
    }

    @Transactional
    private void quarantineTransfer(Long transferId) {
        FileTransfer transfer = FileTransfer.findById(transferId);
        if (transfer != null) {
            transfer.status = TransferStatus.QUARANTINED;
            transfer.errorMessage = "Quarantined after " + quarantineThreshold + " reprocessing failures";
            transfer.persist();

            monitoringService.recordEvent("transfer_quarantined", Map.of(
                    "transfer_id", transferId,
                    "source", transfer.sourceSystem,
                    "filename", transfer.filename,
                    "retry_count", transfer.retryCount
            ));
        }
    }

    private boolean shouldAutoRetry(FileTransfer transfer) {
        // Don't retry certain error types
        if (transfer.errorMessage != null) {
            String error = transfer.errorMessage.toLowerCase();
            if (error.contains("validation") ||
                    error.contains("rejected") ||
                    error.contains("quarantine")) {
                return false;
            }
        }

        // Check source system health
        SourceSystem source = SourceSystem.findBySystemId(transfer.sourceSystem).orElse(null);
        if (source != null && source.healthStatus == SourceSystem.HealthStatus.CRITICAL) {
            return false;
        }

        return true;
    }

    private List<FileTransfer> findTransfersForReprocess(String sourceSystem, String dateRange) {
        if (dateRange != null) {
            // Parse date range
            String[] dates = dateRange.split(",");
            LocalDate startDate = LocalDate.parse(dates[0]);
            LocalDate endDate = dates.length > 1 ? LocalDate.parse(dates[1]) : startDate;

            Instant start = startDate.atStartOfDay().toInstant(ZoneOffset.UTC);
            Instant end = endDate.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC);

            return FileTransfer.find(
                    "sourceSystem = ?1 and createdAt >= ?2 and createdAt < ?3",
                    sourceSystem, start, end
            ).list();
        } else {
            // Last 24 hours by default
            Instant since = Instant.now().minus(Duration.ofDays(1));
            return FileTransfer.find(
                    "sourceSystem = ?1 and createdAt > ?2",
                    sourceSystem, since
            ).list();
        }
    }

    @Transactional
    private void processBatch(List<FileTransfer> batch) {
        for (FileTransfer transfer : batch) {
            try {
                markForReprocessing(transfer.id, "Batch reprocess", "SYSTEM");
                orchestrator.queueTransfer(transfer.id);

                ReprocessingStats stats = new ReprocessingStats(transfer.id);
                reprocessingStats.put(transfer.id, stats);

            } catch (Exception e) {
                Log.errorf(e, "Failed to reprocess transfer: %d", transfer.id);
            }
        }
    }

    private void markBigQueryRequestProcessed(Long fileId) {
        try {
            String updateQuery = String.format("""
                UPDATE `%s.%s.%s`
                SET processed = true,
                    processed_at = CURRENT_TIMESTAMP(),
                    processed_by = '%s'
                WHERE file_id = %d
                """,
                    config.monitoring().bigQueryDataset().replace("`", ""),
                    config.monitoring().bigQueryDataset().replace("`", ""),
                    config.monitoring().reprocessTable(),
                    nodeName,
                    fileId
            );

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(updateQuery)
                    .setUseLegacySql(false)
                    .build();

            bigQuery.query(queryConfig);

        } catch (Exception e) {
            Log.errorf(e, "Failed to mark BigQuery request as processed: %d", fileId);
        }
    }

    private Map<String, Object> generateQuarantineReport(List<FileTransfer> quarantined) {
        Map<String, Object> report = new HashMap<>();
        report.put("total_quarantined", quarantined.size());
        report.put("report_date", Instant.now());

        Map<String, Long> bySource = quarantined.stream()
                .collect(Collectors.groupingBy(
                        t -> t.sourceSystem,
                        Collectors.counting()
                ));
        report.put("by_source", bySource);

        List<Map<String, Object>> details = quarantined.stream()
                .map(t -> Map.<String, Object>of(
                        "id", t.id,
                        "source", t.sourceSystem,
                        "filename", t.filename,
                        "error", t.errorMessage,
                        "retry_count", t.retryCount,
                        "created", t.createdAt
                ))
                .collect(Collectors.toList());
        report.put("details", details);

        return report;
    }

    private void exportQuarantineReport(Map<String, Object> report) {
        try {
            TableId tableId = TableId.of(
                    config.monitoring().bigQueryDataset(),
                    "quarantine_reports"
            );

            InsertAllRequest.RowToInsert row = InsertAllRequest.RowToInsert.of(
                    UUID.randomUUID().toString(), report
            );

            InsertAllResponse response = bigQuery.insertAll(
                    InsertAllRequest.newBuilder(tableId)
                            .setRows(List.of(row))
                            .build()
            );

            if (response.hasErrors()) {
                Log.error("Failed to export quarantine report to BigQuery");
            }

        } catch (Exception e) {
            Log.error("Error exporting quarantine report", e);
        }
    }

    private void archiveHistoryToBigQuery(List<TransferStatusHistory> history) {
        try {
            TableId tableId = TableId.of(
                    config.monitoring().bigQueryDataset(),
                    "transfer_status_history_archive"
            );

            List<InsertAllRequest.RowToInsert> rows = history.stream()
                    .map(h -> InsertAllRequest.RowToInsert.of(
                            UUID.randomUUID().toString(),
                            Map.of(
                                    "transfer_id", h.transferId,
                                    "from_status", h.fromStatus != null ? h.fromStatus.toString() : "",
                                    "to_status", h.toStatus.toString(),
                                    "changed_at", h.changedAt.toString(),
                                    "changed_by", h.changedBy,
                                    "reason", h.reason
                            )
                    ))
                    .collect(Collectors.toList());

            // Process in batches of 500
            List<List<InsertAllRequest.RowToInsert>> batches = partition(rows, 500);

            for (List<InsertAllRequest.RowToInsert> batch : batches) {
                InsertAllResponse response = bigQuery.insertAll(
                        InsertAllRequest.newBuilder(tableId)
                                .setRows(batch)
                                .build()
                );

                if (response.hasErrors()) {
                    Log.error("Errors archiving history to BigQuery");
                }
            }

        } catch (Exception e) {
            Log.error("Failed to archive history to BigQuery", e);
        }
    }

    private <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }

    // ============ Inner Classes ============

    /**
     * Reprocessing statistics tracker.
     */
    private static class ReprocessingStats {
        final Long transferId;
        final Instant startTime;
        int attempts = 0;
        int failures = 0;
        boolean completed = false;
        Instant completedTime;

        ReprocessingStats(Long transferId) {
            this.transferId = transferId;
            this.startTime = Instant.now();
        }
    }

    /**
     * BigQuery reprocess request.
     */
    private static class ReprocessRequest {
        Long fileId;
        String sourceSystem;
        String filename;
        String reason;
        String requestedBy;
    }
}