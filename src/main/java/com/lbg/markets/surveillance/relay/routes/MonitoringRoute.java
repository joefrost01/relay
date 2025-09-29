package com.lbg.markets.surveillance.relay.routes;

import com.google.cloud.bigquery.*;
import com.lbg.markets.surveillance.relay.config.RelayConfiguration;
import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.model.SourceSystem;
import com.lbg.markets.surveillance.relay.model.TransferStatus;
import com.lbg.markets.surveillance.relay.service.MonitoringService;
import io.micrometer.core.instrument.*;
import io.quarkus.logging.Log;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.micrometer.MicrometerConstants;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.*;

/**
 * Camel routes for monitoring, metrics collection, and alerting.
 * Integrates with BigQuery, Prometheus, and notification systems.
 */
@ApplicationScoped
public class MonitoringRoute extends RouteBuilder {

    @Inject MonitoringService monitoringService;
    @Inject RelayConfiguration config;
    @Inject MeterRegistry meterRegistry;
    @Inject BigQuery bigQuery;

    @ConfigProperty(name = "relay.node-name")
    String nodeName;

    @ConfigProperty(name = "relay.monitoring.enabled", defaultValue = "true")
    boolean monitoringEnabled;

    @ConfigProperty(name = "relay.monitoring.metrics-interval-seconds", defaultValue = "60")
    int metricsIntervalSeconds;

    @ConfigProperty(name = "relay.monitoring.bigquery-batch-size", defaultValue = "500")
    int bigQueryBatchSize;

    @ConfigProperty(name = "relay.monitoring.alert-cooldown-minutes", defaultValue = "15")
    int alertCooldownMinutes;

    // Metrics collectors
    private final Map<String, Counter> transferCounters = new ConcurrentHashMap<>();
    private final Map<String, Timer> processingTimers = new ConcurrentHashMap<>();
    private final Map<String, Gauge> queueGauges = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> sourceMetrics = new ConcurrentHashMap<>();

    // Alert tracking
    private final Map<String, Instant> lastAlertTime = new ConcurrentHashMap<>();
    private final Map<String, Integer> alertCounts = new ConcurrentHashMap<>();

    @Override
    public void configure() throws Exception {

        if (!monitoringEnabled) {
            Log.info("Monitoring routes disabled");
            return;
        }

        // Initialize metrics
        initializeMetrics();

        // Configure metric collection routes
        configureMetricsCollection();

        // Configure BigQuery export
        configureBigQueryExport();

        // Configure health monitoring
        configureHealthMonitoring();

        // Configure alerting
        configureAlerting();

        // Configure performance monitoring
        configurePerformanceMonitoring();

        // Configure SLA monitoring
        configureSlaMonitoring();

        // Configure dashboard data endpoints
        configureDashboardRoutes();
    }

    /**
     * Initialize Micrometer metrics.
     */
    private void initializeMetrics() {
        // Create counters for each status
        for (TransferStatus status : TransferStatus.values()) {
            transferCounters.put(status.name(), Counter.builder("relay.transfers")
                    .description("Number of file transfers")
                    .tag("status", status.name())
                    .tag("node", nodeName)
                    .register(meterRegistry));
        }

        // Create timers for processing stages
        processingTimers.put("detection", Timer.builder("relay.processing.time")
                .description("Time taken for file detection")
                .tag("stage", "detection")
                .tag("node", nodeName)
                .register(meterRegistry));

        processingTimers.put("validation", Timer.builder("relay.processing.time")
                .description("Time taken for validation")
                .tag("stage", "validation")
                .tag("node", nodeName)
                .register(meterRegistry));

        processingTimers.put("transfer", Timer.builder("relay.processing.time")
                .description("Time taken for GCS transfer")
                .tag("stage", "transfer")
                .tag("node", nodeName)
                .register(meterRegistry));

        // Create gauges for queue sizes
        Gauge.builder("relay.queue.size", () ->
                        FileTransfer.count("status = ?1", TransferStatus.DETECTED))
                .description("Number of files pending processing")
                .tag("node", nodeName)
                .register(meterRegistry);

        Gauge.builder("relay.queue.processing", () ->
                        FileTransfer.count("status = ?1", TransferStatus.PROCESSING))
                .description("Number of files currently processing")
                .tag("node", nodeName)
                .register(meterRegistry);

        // Source-specific metrics
        for (RelayConfiguration.SourceSystem source : config.sources()) {
            sourceMetrics.put(source.id() + "_total", new AtomicLong(0));
            sourceMetrics.put(source.id() + "_success", new AtomicLong(0));
            sourceMetrics.put(source.id() + "_failed", new AtomicLong(0));

            Gauge.builder("relay.source.backlog",
                            sourceMetrics.get(source.id() + "_total"))
                    .description("Files pending from source")
                    .tag("source", source.id())
                    .tag("node", nodeName)
                    .register(meterRegistry);
        }
    }

    /**
     * Configure metrics collection routes.
     */
    private void configureMetricsCollection() {

        // Collect transfer metrics
        from(timer("collect-metrics")
                .period(metricsIntervalSeconds * 1000)
                .delay(10000))
                .routeId("collect-transfer-metrics")
                .process(exchange -> {
                    // Collect current metrics
                    Map<String, Long> statusCounts = FileTransfer.stream(
                                    "createdAt > ?1",
                                    Instant.now().minus(Duration.ofMinutes(5)))
                            .collect(Collectors.groupingBy(
                                    t -> t.status.toString(),
                                    Collectors.counting()
                            ));

                    // Update counters
                    statusCounts.forEach((status, count) -> {
                        Counter counter = transferCounters.get(status);
                        if (counter != null && count > 0) {
                            counter.increment(count);
                        }
                    });

                    // Collect source metrics
                    for (RelayConfiguration.SourceSystem source : config.sources()) {
                        long pending = FileTransfer.count(
                                "sourceSystem = ?1 and status = ?2",
                                source.id(), TransferStatus.DETECTED
                        );
                        sourceMetrics.get(source.id() + "_total").set(pending);
                    }

                    // Record to monitoring service
                    monitoringService.recordEvent("metrics_collected", Map.of(
                            "status_counts", statusCounts,
                            "timestamp", Instant.now()
                    ));
                })
                .to(micrometer("timer:relay.metrics.collection?action=stop"));

        // Collect processing times
        from(seda("record-processing-time"))
                .routeId("record-processing-time")
                .process(exchange -> {
                    String stage = exchange.getIn().getHeader("stage", String.class);
                    Long duration = exchange.getIn().getHeader("duration", Long.class);

                    Timer timer = processingTimers.get(stage);
                    if (timer != null && duration != null) {
                        timer.record(duration, TimeUnit.MILLISECONDS);
                    }
                });

        // Collect error metrics
        from(seda("record-error"))
                .routeId("record-error-metrics")
                .process(exchange -> {
                    String source = exchange.getIn().getHeader("source", String.class);
                    String errorType = exchange.getIn().getHeader("error_type", String.class);

                    Counter.builder("relay.errors")
                            .description("Error count")
                            .tag("source", source != null ? source : "unknown")
                            .tag("type", errorType != null ? errorType : "general")
                            .tag("node", nodeName)
                            .register(meterRegistry)
                            .increment();
                });
    }

    /**
     * Configure BigQuery export for long-term storage.
     */
    private void configureBigQueryExport() {

        // Batch events for BigQuery
        from(seda("bigquery-export")
                .concurrentConsumers(2)
                .size(1000))
                .routeId("bigquery-batch-export")
                .aggregate(constant(true), new EventAggregationStrategy())
                .completionSize(bigQueryBatchSize)
                .completionTimeout(30000)
                .process(exchange -> {
                    List<Map<String, Object>> events = exchange.getIn().getBody(List.class);
                    if (events != null && !events.isEmpty()) {
                        exportToBigQuery(events);
                    }
                });

        // Export transfer events
        from(timer("export-transfers")
                .period(Duration.ofMinutes(5).toMillis()))
                .routeId("export-transfer-events")
                .process(exchange -> {
                    Instant since = Instant.now().minus(Duration.ofMinutes(5));

                    List<FileTransfer> transfers = FileTransfer.find(
                            "updatedAt > ?1", since
                    ).list();

                    if (!transfers.isEmpty()) {
                        List<Map<String, Object>> events = transfers.stream()
                                .map(this::transferToEvent)
                                .collect(Collectors.toList());

                        exportTransfersToBigQuery(events);
                    }
                });

        // Export system metrics
        from(timer("export-system-metrics")
                .period(Duration.ofMinutes(1).toMillis()))
                .routeId("export-system-metrics")
                .process(exchange -> {
                    Map<String, Object> systemMetrics = collectSystemMetrics();
                    exportSystemMetricsToBigQuery(systemMetrics);
                });
    }

    /**
     * Configure health monitoring routes.
     */
    private void configureHealthMonitoring() {

        // Monitor source system health
        from(timer("source-health")
                .period(Duration.ofMinutes(2).toMillis()))
                .routeId("monitor-source-health")
                .process(exchange -> {
                    for (SourceSystem source : SourceSystem.<SourceSystem>listAll()) {
                        checkSourceHealth(source);
                    }
                });

        // Monitor processing health
        from(timer("processing-health")
                .period(Duration.ofMinutes(1).toMillis()))
                .routeId("monitor-processing-health")
                .process(exchange -> {
                    // Check for stuck transfers
                    Instant stuckCutoff = Instant.now().minus(Duration.ofHours(1));
                    List<FileTransfer> stuckTransfers = FileTransfer.find(
                            "status = ?1 and startedAt < ?2",
                            TransferStatus.PROCESSING, stuckCutoff
                    ).list();

                    if (!stuckTransfers.isEmpty()) {
                        exchange.getIn().setHeader("AlertType", "STUCK_TRANSFERS");
                        exchange.getIn().setHeader("AlertCount", stuckTransfers.size());
                        exchange.getIn().setBody(stuckTransfers);
                    }
                })
                .choice()
                .when(header("AlertType").isNotNull())
                .to("direct:send-alert")
                .end();

        // Monitor error rates
        from(timer("error-rate-monitor")
                .period(Duration.ofMinutes(5).toMillis()))
                .routeId("monitor-error-rates")
                .process(exchange -> {
                    Instant since = Instant.now().minus(Duration.ofHours(1));

                    long totalCount = FileTransfer.count("createdAt > ?1", since);
                    long errorCount = FileTransfer.count(
                            "createdAt > ?1 and status in ?2",
                            since,
                            List.of(TransferStatus.FAILED, TransferStatus.REJECTED)
                    );

                    if (totalCount > 0) {
                        double errorRate = (double) errorCount / totalCount * 100;

                        if (errorRate > 10) {
                            exchange.getIn().setHeader("AlertType", "HIGH_ERROR_RATE");
                            exchange.getIn().setHeader("ErrorRate", errorRate);
                            exchange.getIn().setHeader("ErrorCount", errorCount);
                            exchange.getIn().setHeader("TotalCount", totalCount);
                        }
                    }
                })
                .choice()
                .when(header("AlertType").isNotNull())
                .to("direct:send-alert")
                .end();
    }

    /**
     * Configure alerting routes.
     */
    private void configureAlerting() {

        from(direct("send-alert"))
                .routeId("send-alert")
                .process(exchange -> {
                    String alertType = exchange.getIn().getHeader("AlertType", String.class);

                    // Check cooldown period
                    if (!shouldSendAlert(alertType)) {
                        exchange.getIn().setHeader("SkipAlert", true);
                        return;
                    }

                    // Build alert message
                    Map<String, Object> alert = buildAlert(exchange);
                    exchange.getIn().setBody(alert);

                    // Track alert
                    lastAlertTime.put(alertType, Instant.now());
                    alertCounts.merge(alertType, 1, Integer::sum);
                })
                .choice()
                .when(header("SkipAlert").isNull())
                .multicast()
                .to("direct:alert-email",
                        "direct:alert-slack",
                        "direct:alert-pagerduty",
                        "direct:alert-bigquery")
                .end();

        // Email alerts
        from(direct("alert-email"))
                .routeId("send-email-alert")
                .process(exchange -> {
                    Map<String, Object> alert = exchange.getIn().getBody(Map.class);
                    // Email sending logic would go here
                    Log.infof("Email alert: %s", alert.get("message"));
                })
                .onException(Exception.class)
                .handled(true)
                .log(LoggingLevel.ERROR, "Failed to send email alert: ${exception.message}")
                .end();

        // Slack alerts
        from(direct("alert-slack"))
                .routeId("send-slack-alert")
                .process(exchange -> {
                    Map<String, Object> alert = exchange.getIn().getBody(Map.class);
                    sendSlackAlert(alert);
                })
                .onException(Exception.class)
                .handled(true)
                .log(LoggingLevel.ERROR, "Failed to send Slack alert: ${exception.message}")
                .end();

        // PagerDuty alerts
        from(direct("alert-pagerduty"))
                .routeId("send-pagerduty-alert")
                .process(exchange -> {
                    Map<String, Object> alert = exchange.getIn().getBody(Map.class);
                    String severity = (String) alert.get("severity");

                    if ("CRITICAL".equals(severity)) {
                        sendPagerDutyAlert(alert);
                    }
                })
                .onException(Exception.class)
                .handled(true)
                .log(LoggingLevel.ERROR, "Failed to send PagerDuty alert: ${exception.message}")
                .end();

        // Log alerts to BigQuery
        from(direct("alert-bigquery"))
                .routeId("log-alert-bigquery")
                .process(exchange -> {
                    Map<String, Object> alert = exchange.getIn().getBody(Map.class);
                    logAlertToBigQuery(alert);
                });
    }

    /**
     * Configure performance monitoring.
     */
    private void configurePerformanceMonitoring() {

        // Monitor transfer throughput
        from(timer("throughput-monitor")
                .period(Duration.ofMinutes(1).toMillis()))
                .routeId("monitor-throughput")
                .process(exchange -> {
                    Instant oneMinuteAgo = Instant.now().minus(Duration.ofMinutes(1));

                    long completedCount = FileTransfer.count(
                            "status = ?1 and completedAt > ?2",
                            TransferStatus.COMPLETED, oneMinuteAgo
                    );

                    Long totalBytes = FileTransfer.find(
                                    "status = ?1 and completedAt > ?2",
                                    TransferStatus.COMPLETED, oneMinuteAgo)
                            .stream()
                            .mapToLong(t -> t.fileSize != null ? t.fileSize : 0)
                            .sum();

                    double throughputMBps = totalBytes / (1024.0 * 1024.0 * 60.0);

                    // Record metrics
                    meterRegistry.gauge("relay.throughput.files_per_minute", completedCount);
                    meterRegistry.gauge("relay.throughput.mbps", throughputMBps);

                    // Check if throughput is too low
                    if (completedCount == 0 && hasActiveSources()) {
                        exchange.getIn().setHeader("AlertType", "NO_THROUGHPUT");
                    }
                })
                .choice()
                .when(header("AlertType").isNotNull())
                .to("direct:send-alert")
                .end();

        // Monitor processing latency
        from(timer("latency-monitor")
                .period(Duration.ofMinutes(5).toMillis()))
                .routeId("monitor-latency")
                .process(exchange -> {
                    List<FileTransfer> recent = FileTransfer.find(
                            "status = ?1 and completedAt > ?2",
                            TransferStatus.COMPLETED,
                            Instant.now().minus(Duration.ofMinutes(5))
                    ).list();

                    if (!recent.isEmpty()) {
                        DoubleSummaryStatistics stats = recent.stream()
                                .filter(t -> t.startedAt != null && t.completedAt != null)
                                .mapToDouble(t -> Duration.between(t.startedAt, t.completedAt).toMillis())
                                .summaryStatistics();

                        meterRegistry.gauge("relay.latency.avg_ms", stats.getAverage());
                        meterRegistry.gauge("relay.latency.max_ms", stats.getMax());
                        meterRegistry.gauge("relay.latency.min_ms", stats.getMin());

                        // Alert on high latency
                        if (stats.getAverage() > 60000) { // > 1 minute average
                            exchange.getIn().setHeader("AlertType", "HIGH_LATENCY");
                            exchange.getIn().setHeader("AvgLatency", stats.getAverage());
                        }
                    }
                })
                .choice()
                .when(header("AlertType").isNotNull())
                .to("direct:send-alert")
                .end();
    }

    /**
     * Configure SLA monitoring.
     */
    private void configureSlaMonitoring() {

        from(timer("sla-monitor")
                .period(Duration.ofMinutes(15).toMillis()))
                .routeId("monitor-sla")
                .process(exchange -> {
                    List<Map<String, Object>> slaBreaches = new ArrayList<>();

                    for (SourceSystem source : SourceSystem.<SourceSystem>listAll()) {
                        if (source.enabled && source.expectedNextFile != null) {
                            if (source.isOverdue(Duration.ofHours(1))) {
                                Map<String, Object> breach = Map.of(
                                        "source", source.systemId,
                                        "expected", source.expectedNextFile,
                                        "last_received", source.lastFileReceived,
                                        "hours_overdue", Duration.between(
                                                source.expectedNextFile, Instant.now()
                                        ).toHours()
                                );
                                slaBreaches.add(breach);
                            }
                        }
                    }

                    if (!slaBreaches.isEmpty()) {
                        exchange.getIn().setHeader("AlertType", "SLA_BREACH");
                        exchange.getIn().setBody(slaBreaches);
                    }
                })
                .choice()
                .when(header("AlertType").isNotNull())
                .to("direct:send-alert")
                .end();

        // Daily SLA report
        from(cron("sla-daily-report")
                .schedule("0 0 8 * * ?"))  // 8 AM daily
                .routeId("sla-daily-report")
                .process(exchange -> {
                    Map<String, Object> slaReport = generateSlaReport();
                    exportSlaReportToBigQuery(slaReport);
                    sendDailySlaEmail(slaReport);
                });
    }

    /**
     * Configure dashboard data endpoints.
     */
    private void configureDashboardRoutes() {

        // Real-time dashboard feed via Server-Sent Events
        from(timer("dashboard-feed")
                .period(5000))  // Every 5 seconds
                .routeId("dashboard-real-time-feed")
                .process(exchange -> {
                    Map<String, Object> dashboard = Map.of(
                            "timestamp", Instant.now(),
                            "node", nodeName,
                            "stats", getCurrentStats(),
                            "sources", getSourceStats(),
                            "recent_errors", getRecentErrors(5),
                            "throughput", getCurrentThroughput()
                    );

                    exchange.getIn().setBody(dashboard);
                })
                .marshal().json()
                .to("seda:dashboard-updates");

        // Aggregate dashboard metrics
        from(timer("dashboard-aggregates")
                .period(Duration.ofMinutes(1).toMillis()))
                .routeId("dashboard-aggregates")
                .process(exchange -> {
                    Map<String, Object> aggregates = Map.of(
                            "hourly_stats", getHourlyStats(),
                            "daily_trends", getDailyTrends(),
                            "top_errors", getTopErrors(),
                            "source_rankings", getSourceRankings()
                    );

                    exchange.getIn().setBody(aggregates);
                })
                .to("seda:dashboard-aggregates");
    }

    // ============ Helper Methods ============

    private Map<String, Object> transferToEvent(FileTransfer transfer) {
        Map<String, Object> event = new HashMap<>();
        event.put("transfer_id", transfer.id);
        event.put("source_system", transfer.sourceSystem);
        event.put("filename", transfer.filename);
        event.put("file_size", transfer.fileSize);
        event.put("status", transfer.status.toString());
        event.put("created_at", transfer.createdAt);
        event.put("completed_at", transfer.completedAt);
        event.put("processing_node", transfer.processingNode);
        event.put("retry_count", transfer.retryCount);

        if (transfer.startedAt != null && transfer.completedAt != null) {
            event.put("processing_time_ms",
                    Duration.between(transfer.startedAt, transfer.completedAt).toMillis());
        }

        return event;
    }

    private void exportToBigQuery(List<Map<String, Object>> events) {
        try {
            TableId tableId = TableId.of(
                    config.monitoring().bigQueryDataset(),
                    config.monitoring().eventsTable()
            );

            List<InsertAllRequest.RowToInsert> rows = events.stream()
                    .map(event -> InsertAllRequest.RowToInsert.of(
                            UUID.randomUUID().toString(), event))
                    .collect(Collectors.toList());

            InsertAllResponse response = bigQuery.insertAll(
                    InsertAllRequest.newBuilder(tableId)
                            .setRows(rows)
                            .build()
            );

            if (response.hasErrors()) {
                Log.errorf("BigQuery insert errors: %s", response.getInsertErrors());
            }

        } catch (Exception e) {
            Log.error("Failed to export to BigQuery", e);
        }
    }

    private void exportTransfersToBigQuery(List<Map<String, Object>> events) {
        try {
            TableId tableId = TableId.of(
                    config.monitoring().bigQueryDataset(),
                    "transfer_events"
            );

            // Same as exportToBigQuery but to transfer-specific table
            exportToBigQuery(events);

        } catch (Exception e) {
            Log.error("Failed to export transfers to BigQuery", e);
        }
    }

    private void exportSystemMetricsToBigQuery(Map<String, Object> metrics) {
        metrics.put("timestamp", Instant.now().toString());
        metrics.put("node", nodeName);

        exportToBigQuery(List.of(metrics));
    }

    private Map<String, Object> collectSystemMetrics() {
        Runtime runtime = Runtime.getRuntime();

        return Map.of(
                "memory_used_mb", (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024,
                "memory_total_mb", runtime.totalMemory() / 1024 / 1024,
                "memory_max_mb", runtime.maxMemory() / 1024 / 1024,
                "threads", Thread.activeCount(),
                "processors", runtime.availableProcessors(),
                "uptime_minutes", Duration.between(
                        Instant.ofEpochMilli(ManagementFactory.getRuntimeMXBean().getStartTime()),
                        Instant.now()
                ).toMinutes()
        );
    }

    private void checkSourceHealth(SourceSystem source) {
        // Calculate health metrics
        double successRate = source.getSuccessRate();
        boolean isOverdue = source.isOverdue(Duration.ofHours(2));

        SourceSystem.HealthStatus previousHealth = source.healthStatus;

        // Update health status
        if (source.consecutiveErrors >= 10 || successRate < 50) {
            source.healthStatus = SourceSystem.HealthStatus.CRITICAL;
        } else if (source.consecutiveErrors >= 5 || successRate < 80 || isOverdue) {
            source.healthStatus = SourceSystem.HealthStatus.DEGRADED;
        } else if (source.consecutiveErrors >= 3 || successRate < 90) {
            source.healthStatus = SourceSystem.HealthStatus.WARNING;
        } else {
            source.healthStatus = SourceSystem.HealthStatus.HEALTHY;
        }

        // Send alert if health degraded
        if (source.healthStatus != previousHealth &&
                source.healthStatus != SourceSystem.HealthStatus.HEALTHY) {

            sendSourceHealthAlert(source);
        }

        source.persist();
    }

    private boolean shouldSendAlert(String alertType) {
        Instant lastAlert = lastAlertTime.get(alertType);
        if (lastAlert == null) {
            return true;
        }

        return Duration.between(lastAlert, Instant.now()).toMinutes() >= alertCooldownMinutes;
    }

    private Map<String, Object> buildAlert(Exchange exchange) {
        String alertType = exchange.getIn().getHeader("AlertType", String.class);

        Map<String, Object> alert = new HashMap<>();
        alert.put("type", alertType);
        alert.put("timestamp", Instant.now().toString());
        alert.put("node", nodeName);
        alert.put("severity", determineSeverity(alertType));

        // Add type-specific details
        switch (alertType) {
            case "STUCK_TRANSFERS" -> {
                List<FileTransfer> stuck = exchange.getIn().getBody(List.class);
                alert.put("message", String.format("%d transfers stuck in processing", stuck.size()));
                alert.put("details", stuck.stream()
                        .limit(5)
                        .map(t -> Map.of("id", t.id, "file", t.filename))
                        .collect(Collectors.toList()));
            }
            case "HIGH_ERROR_RATE" -> {
                Double errorRate = exchange.getIn().getHeader("ErrorRate", Double.class);
                alert.put("message", String.format("High error rate: %.2f%%", errorRate));
                alert.put("error_count", exchange.getIn().getHeader("ErrorCount"));
                alert.put("total_count", exchange.getIn().getHeader("TotalCount"));
            }
            case "SLA_BREACH" -> {
                List<Map<String, Object>> breaches = exchange.getIn().getBody(List.class);
                alert.put("message", String.format("%d sources breaching SLA", breaches.size()));
                alert.put("breaches", breaches);
            }
            case "NO_THROUGHPUT" -> {
                alert.put("message", "No files processed in the last minute");
            }
            case "HIGH_LATENCY" -> {
                Double avgLatency = exchange.getIn().getHeader("AvgLatency", Double.class);
                alert.put("message", String.format("High processing latency: %.2f ms", avgLatency));
            }
            default -> alert.put("message", "Unknown alert: " + alertType);
        }

        return alert;
    }

    private String determineSeverity(String alertType) {
        return switch (alertType) {
            case "STUCK_TRANSFERS", "SLA_BREACH" -> "CRITICAL";
            case "HIGH_ERROR_RATE", "NO_THROUGHPUT" -> "HIGH";
            case "HIGH_LATENCY" -> "MEDIUM";
            default -> "LOW";
        };
    }

    private void sendSlackAlert(Map<String, Object> alert) {
        // Slack webhook implementation
        Log.infof("Slack alert: %s", alert);
    }

    private void sendPagerDutyAlert(Map<String, Object> alert) {
        // PagerDuty implementation
        Log.infof("PagerDuty alert: %s", alert);
    }

    private void logAlertToBigQuery(Map<String, Object> alert) {
        alert.put("alert_count", alertCounts.getOrDefault(alert.get("type"), 0));
        exportToBigQuery(List.of(alert));
    }

    private void sendSourceHealthAlert(SourceSystem source) {
        Map<String, Object> alert = Map.of(
                "type", "SOURCE_HEALTH_DEGRADED",
                "source", source.systemId,
                "health", source.healthStatus.toString(),
                "consecutive_errors", source.consecutiveErrors,
                "success_rate", source.getSuccessRate()
        );

        monitoringService.recordEvent("source_health_alert", alert);
    }

    private boolean hasActiveSources() {
        return SourceSystem.count("enabled = true and status = 'ACTIVE'") > 0;
    }

    private Map<String, Object> getCurrentStats() {
        return Map.of(
                "pending", FileTransfer.count("status = ?1", TransferStatus.DETECTED),
                "processing", FileTransfer.count("status = ?1", TransferStatus.PROCESSING),
                "completed_today", FileTransfer.count(
                        "status = ?1 and completedAt > ?2",
                        TransferStatus.COMPLETED,
                        LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC)
                ),
                "failed_today", FileTransfer.count(
                        "status = ?1 and createdAt > ?2",
                        TransferStatus.FAILED,
                        LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC)
                )
        );
    }

    private List<Map<String, Object>> getSourceStats() {
        return SourceSystem.<SourceSystem>listAll().stream()
                .map(source -> Map.<String, Object>of(
                        "id", source.systemId,
                        "health", source.healthStatus.toString(),
                        "files_today", source.filesToday,
                        "last_file", source.lastFileReceived,
                        "success_rate", source.getSuccessRate()
                ))
                .collect(Collectors.toList());
    }

    private List<Map<String, Object>> getRecentErrors(int limit) {
        return FileTransfer.find(
                        "status = ?1 and errorMessage is not null",
                        Sort.by("createdAt").descending(),
                        TransferStatus.FAILED)
                .page(0, limit)
                .list()
                .stream()
                .map(t -> Map.<String, Object>of(
                        "id", t.id,
                        "file", t.filename,
                        "error", t.errorMessage,
                        "time", t.createdAt
                ))
                .collect(Collectors.toList());
    }

    private Map<String, Object> getCurrentThroughput() {
        Instant oneMinuteAgo = Instant.now().minus(Duration.ofMinutes(1));

        long count = FileTransfer.count(
                "completedAt > ?1", oneMinuteAgo
        );

        return Map.of(
                "files_per_minute", count,
                "trend", calculateTrend()
        );
    }

    private String calculateTrend() {
        // Simple trend calculation
        return "stable";
    }

    private Map<String, List<Object>> getHourlyStats() {
        // Implementation for hourly statistics
        return new HashMap<>();
    }

    private Map<String, Object> getDailyTrends() {
        // Implementation for daily trends
        return new HashMap<>();
    }

    private List<Map<String, Object>> getTopErrors() {
        // Implementation for top errors
        return new ArrayList<>();
    }

    private List<Map<String, Object>> getSourceRankings() {
        // Implementation for source rankings
        return new ArrayList<>();
    }

    private Map<String, Object> generateSlaReport() {
        // Implementation for SLA report
        return new HashMap<>();
    }

    private void exportSlaReportToBigQuery(Map<String, Object> report) {
        // Implementation for SLA report export
    }

    private void sendDailySlaEmail(Map<String, Object> report) {
        // Implementation for daily SLA email
    }

    /**
     * Event aggregation strategy for batching.
     */
    private static class EventAggregationStrategy implements org.apache.camel.AggregationStrategy {
        @Override
        public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
            if (oldExchange == null) {
                List<Map<String, Object>> events = new ArrayList<>();
                events.add(newExchange.getIn().getBody(Map.class));
                newExchange.getIn().setBody(events);
                return newExchange;
            }

            List<Map<String, Object>> events = oldExchange.getIn().getBody(List.class);
            events.add(newExchange.getIn().getBody(Map.class));
            return oldExchange;
        }
    }
}