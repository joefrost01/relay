package com.lbg.markets.surveillance.relay.service;

import com.google.cloud.bigquery.*;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.lbg.markets.surveillance.relay.config.RelayConfiguration;
import com.lbg.markets.surveillance.relay.model.TransferStatus;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Centralized monitoring service for the Relay application.
 * Handles metrics collection, event logging, BigQuery export, and alerting.
 */
@ApplicationScoped
@Startup
public class MonitoringService {

    @Inject
    RelayConfiguration config;
    @Inject
    MeterRegistry meterRegistry;
    @Inject
    BigQuery bigQuery;

    @ConfigProperty(name = "relay.node-name")
    String nodeName;

    @ConfigProperty(name = "relay.monitoring.enabled", defaultValue = "true")
    boolean monitoringEnabled;

    @ConfigProperty(name = "relay.monitoring.buffer-size", defaultValue = "10000")
    int bufferSize;

    @ConfigProperty(name = "relay.monitoring.flush-interval-seconds", defaultValue = "30")
    int flushIntervalSeconds;

    @ConfigProperty(name = "relay.monitoring.batch-size", defaultValue = "500")
    int batchSize;

    @ConfigProperty(name = "relay.monitoring.retention-days", defaultValue = "90")
    int retentionDays;

    @ConfigProperty(name = "relay.monitoring.enable-pubsub", defaultValue = "false")
    boolean enablePubSub;

    private final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .create();

    // Event buffer for batch processing
    private final BlockingQueue<MonitoringEvent> eventBuffer = new LinkedBlockingQueue<>(bufferSize);

    // Metrics collectors
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> gaugeValues = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> summaries = new ConcurrentHashMap<>();

    // Alert tracking
    private final Map<String, AlertState> activeAlerts = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastAlertTime = new ConcurrentHashMap<>();

    // BigQuery table references
    private TableId eventsTableId;
    private TableId metricsTableId;
    private TableId alertsTableId;
    private TableId slaTableId;

    // Background services
    private ScheduledExecutorService scheduler;
    private ExecutorService executor;
    private Publisher pubsubPublisher;

    // Statistics
    private final AtomicLong totalEventsRecorded = new AtomicLong(0);
    private final AtomicLong totalEventsExported = new AtomicLong(0);
    private final AtomicLong totalEventsFailed = new AtomicLong(0);

    @PostConstruct
    void init() {
        if (!monitoringEnabled) {
            Log.info("Monitoring service disabled");
            return;
        }

        Log.info("Initializing monitoring service");

        // Initialize BigQuery tables
        initializeBigQueryTables();

        // Initialize Pub/Sub if enabled
        if (enablePubSub) {
            initializePubSub();
        }

        // Initialize metrics
        initializeMetrics();

        // Start background services
        startBackgroundServices();

        Log.info("Monitoring service initialized successfully");
    }

    @PreDestroy
    void shutdown() {
        Log.info("Shutting down monitoring service");

        try {
            // Flush remaining events
            flushEvents();

            // Shutdown executors
            if (scheduler != null) {
                scheduler.shutdown();
                scheduler.awaitTermination(10, TimeUnit.SECONDS);
            }

            if (executor != null) {
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            }

            // Close Pub/Sub publisher
            if (pubsubPublisher != null) {
                pubsubPublisher.shutdown();
                pubsubPublisher.awaitTermination(10, TimeUnit.SECONDS);
            }

        } catch (Exception e) {
            Log.error("Error during monitoring service shutdown", e);
        }
    }

    // ============ Public API ============

    /**
     * Record a monitoring event.
     */
    public void recordEvent(String eventType, Map<String, Object> data) {
        if (!monitoringEnabled) return;

        try {
            MonitoringEvent event = new MonitoringEvent(
                    eventType,
                    data,
                    nodeName,
                    Instant.now()
            );

            // Add to buffer
            if (!eventBuffer.offer(event)) {
                // Buffer full, log and increment counter
                Log.warn("Event buffer full, dropping event: " + eventType);
                totalEventsFailed.incrementAndGet();
                incrementCounter("events.dropped", "type", eventType);
            } else {
                totalEventsRecorded.incrementAndGet();
                incrementCounter("events.recorded", "type", eventType);
            }

            // Update metrics
            updateMetricsForEvent(eventType, data);

            // Check for alert conditions
            checkAlertConditions(eventType, data);

        } catch (Exception e) {
            Log.error("Failed to record event: " + eventType, e);
            totalEventsFailed.incrementAndGet();
        }
    }

    /**
     * Record an error event.
     */
    public void recordError(String errorType, String source, String message) {
        Map<String, Object> errorData = new HashMap<>();
        errorData.put("error_type", errorType);
        errorData.put("source", source);
        errorData.put("message", message);
        errorData.put("timestamp", Instant.now());

        recordEvent("error", errorData);

        // Increment error counter
        incrementCounter("errors",
                "type", errorType,
                "source", source);
    }

    /**
     * Record processing progress.
     */
    public void recordProgress(Long transferId, long bytesProcessed, Long totalBytes) {
        Map<String, Object> progressData = new HashMap<>();
        progressData.put("transfer_id", transferId);
        progressData.put("bytes_processed", bytesProcessed);
        progressData.put("total_bytes", totalBytes);

        if (totalBytes != null && totalBytes > 0) {
            double percentage = (double) bytesProcessed / totalBytes * 100;
            progressData.put("percentage", percentage);
        }

        recordEvent("transfer_progress", progressData);
    }

    /**
     * Get current metrics snapshot.
     */
    public MetricsSnapshot getMetricsSnapshot() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        snapshot.timestamp = Instant.now();
        snapshot.node = nodeName;

        // Collect counter values
        snapshot.counters = new HashMap<>();
        meterRegistry.getMeters().stream()
                .filter(meter -> meter instanceof Counter)
                .forEach(meter -> {
                    Counter counter = (Counter) meter;
                    snapshot.counters.put(
                            meter.getId().getName(),
                            counter.count()
                    );
                });

        // Collect gauge values
        snapshot.gauges = new HashMap<>();
        meterRegistry.getMeters().stream()
                .filter(meter -> meter instanceof Gauge)
                .forEach(meter -> {
                    Gauge gauge = (Gauge) meter;
                    snapshot.gauges.put(
                            meter.getId().getName(),
                            gauge.value()
                    );
                });

        // Collect timer statistics
        snapshot.timers = new HashMap<>();
        meterRegistry.getMeters().stream()
                .filter(meter -> meter instanceof Timer)
                .forEach(meter -> {
                    Timer timer = (Timer) meter;
                    Map<String, Object> timerStats = new HashMap<>();
                    timerStats.put("count", timer.count());
                    timerStats.put("mean", timer.mean(TimeUnit.MILLISECONDS));
                    timerStats.put("max", timer.max(TimeUnit.MILLISECONDS));
                    snapshot.timers.put(meter.getId().getName(), timerStats);
                });

        // Add service statistics
        snapshot.statistics = Map.of(
                "events_recorded", totalEventsRecorded.get(),
                "events_exported", totalEventsExported.get(),
                "events_failed", totalEventsFailed.get(),
                "buffer_size", eventBuffer.size(),
                "active_alerts", activeAlerts.size()
        );

        return snapshot;
    }


    /**
     * Send an alert.
     */
    public void sendAlert(String alertType, String severity, String message, Map<String, Object> details) {
        try {
            Alert alert = new Alert();
            alert.type = alertType;
            alert.severity = severity;
            alert.message = message;
            alert.details = details;
            alert.timestamp = Instant.now();
            alert.node = nodeName;

            // Check if alert is already active
            AlertState existingAlert = activeAlerts.get(alertType);
            if (existingAlert != null && existingAlert.isActive()) {
                // Update existing alert
                existingAlert.updateCount++;
                existingAlert.lastUpdated = Instant.now();
                Log.debugf("Alert already active: %s (count: %d)", alertType, existingAlert.updateCount);
                return;
            }

            // Create new alert
            AlertState newAlert = new AlertState(alert);
            activeAlerts.put(alertType, newAlert);

            // Send notifications
            sendAlertNotifications(alert);

            // Record alert event
            recordEvent("alert_triggered", Map.of(
                    "alert_type", alertType,
                    "severity", severity,
                    "message", message
            ));

            // Export to BigQuery
            exportAlertToBigQuery(alert);

        } catch (Exception e) {
            Log.error("Failed to send alert", e);
        }
    }


    // ============ Private Methods ============

    private void initializeBigQueryTables() {
        String dataset = config.monitoring().bigQueryDataset();

        eventsTableId = TableId.of(dataset, "transfer_events");
        metricsTableId = TableId.of(dataset, "system_metrics");
        alertsTableId = TableId.of(dataset, "alerts");
        slaTableId = TableId.of(dataset, "sla_reports");

        // Create tables if they don't exist
        createTableIfNotExists(eventsTableId, getEventsTableSchema());
        createTableIfNotExists(metricsTableId, getMetricsTableSchema());
        createTableIfNotExists(alertsTableId, getAlertsTableSchema());
        createTableIfNotExists(slaTableId, getSlaTableSchema());
    }

    private void initializePubSub() {
        try {
            config.notifications().ifPresent(notif -> {
                notif.pubsubTopic().ifPresent(topic -> {
                    TopicName topicName = TopicName.parse(topic);
                    try {
                        pubsubPublisher = Publisher.newBuilder(topicName).build();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    Log.infof("Initialized Pub/Sub publisher for topic: %s", topic);
                });
            });
        } catch (Exception e) {
            Log.error("Failed to initialize Pub/Sub", e);
            enablePubSub = false;
        }
    }

    private void initializeMetrics() {
        // Register JVM metrics
        new ClassLoaderMetrics().bindTo(meterRegistry);
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new JvmGcMetrics().bindTo(meterRegistry);
        new ProcessorMetrics().bindTo(meterRegistry);
        new JvmThreadMetrics().bindTo(meterRegistry);

        // Register custom metrics
        Gauge.builder("monitoring.buffer.size", eventBuffer, Queue::size)
                .description("Monitoring event buffer size")
                .register(meterRegistry);

        Gauge.builder("monitoring.events.total", totalEventsRecorded, AtomicLong::get)
                .description("Total events recorded")
                .register(meterRegistry);

        Gauge.builder("monitoring.events.exported", totalEventsExported, AtomicLong::get)
                .description("Total events exported")
                .register(meterRegistry);

        Gauge.builder("monitoring.events.failed", totalEventsFailed, AtomicLong::get)
                .description("Total events failed")
                .register(meterRegistry);
    }

    private void startBackgroundServices() {
        scheduler = Executors.newScheduledThreadPool(3, r -> {
            Thread thread = new Thread(r);
            thread.setName("monitoring-scheduler");
            thread.setDaemon(true);
            return thread;
        });

        executor = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r);
            thread.setName("monitoring-executor");
            thread.setDaemon(true);
            return thread;
        });

        // Schedule event flushing
        scheduler.scheduleWithFixedDelay(
                this::flushEvents,
                flushIntervalSeconds,
                flushIntervalSeconds,
                TimeUnit.SECONDS
        );

        // Schedule metrics export
        scheduler.scheduleWithFixedDelay(
                this::exportMetrics,
                60,
                60,
                TimeUnit.SECONDS
        );

        // Schedule cleanup
        scheduler.scheduleWithFixedDelay(
                this::performCleanup,
                1,
                24,
                TimeUnit.HOURS
        );
    }

    private void flushEvents() {
        try {
            List<MonitoringEvent> events = new ArrayList<>(batchSize);
            eventBuffer.drainTo(events, batchSize);

            if (!events.isEmpty()) {
                exportEventsToBigQuery(events);

                if (enablePubSub) {
                    publishEventsToPubSub(events);
                }

                totalEventsExported.addAndGet(events.size());
                Log.debugf("Flushed %d monitoring events", events.size());
            }
        } catch (Exception e) {
            Log.error("Failed to flush monitoring events", e);
            totalEventsFailed.incrementAndGet();
        }
    }

    private void exportEventsToBigQuery(List<MonitoringEvent> events) {
        try {
            List<InsertAllRequest.RowToInsert> rows = events.stream()
                    .map(event -> {
                        Map<String, Object> row = new HashMap<>();
                        row.put("timestamp", event.timestamp.toString());
                        row.put("event_type", event.type);
                        row.put("node", event.node);
                        row.put("data", gson.toJson(event.data));
                        return InsertAllRequest.RowToInsert.of(UUID.randomUUID().toString(), row);
                    })
                    .collect(Collectors.toList());

            InsertAllResponse response = bigQuery.insertAll(
                    InsertAllRequest.newBuilder(eventsTableId)
                            .setRows(rows)
                            .build()
            );

            if (response.hasErrors()) {
                Log.errorf("BigQuery insert errors: %s", response.getInsertErrors());
                response.getInsertErrors().forEach((k, v) ->
                        v.forEach(error -> Log.errorf("Row %s: %s", k, error.getMessage()))
                );
            }

        } catch (Exception e) {
            Log.error("Failed to export events to BigQuery", e);
            throw e;
        }
    }

    private void publishEventsToPubSub(List<MonitoringEvent> events) {
        if (pubsubPublisher == null) return;

        try {
            for (MonitoringEvent event : events) {
                String messageData = gson.toJson(event);
                PubsubMessage message = PubsubMessage.newBuilder()
                        .setData(ByteString.copyFromUtf8(messageData))
                        .putAttributes("event_type", event.type)
                        .putAttributes("node", event.node)
                        .build();

                pubsubPublisher.publish(message);
            }
        } catch (Exception e) {
            Log.error("Failed to publish events to Pub/Sub", e);
        }
    }

    private void exportMetrics() {
        try {
            MetricsSnapshot snapshot = getMetricsSnapshot();

            Map<String, Object> metricsRow = new HashMap<>();
            metricsRow.put("timestamp", snapshot.timestamp.toString());
            metricsRow.put("node", snapshot.node);
            metricsRow.put("counters", gson.toJson(snapshot.counters));
            metricsRow.put("gauges", gson.toJson(snapshot.gauges));
            metricsRow.put("timers", gson.toJson(snapshot.timers));
            metricsRow.put("statistics", gson.toJson(snapshot.statistics));

            InsertAllResponse response = bigQuery.insertAll(
                    InsertAllRequest.newBuilder(metricsTableId)
                            .addRow(UUID.randomUUID().toString(), metricsRow)
                            .build()
            );

            if (response.hasErrors()) {
                Log.error("Failed to export metrics to BigQuery");
            }

        } catch (Exception e) {
            Log.error("Failed to export metrics", e);
        }
    }

    private void exportAlertToBigQuery(Alert alert) {
        try {
            Map<String, Object> alertRow = new HashMap<>();
            alertRow.put("timestamp", alert.timestamp.toString());
            alertRow.put("alert_type", alert.type);
            alertRow.put("severity", alert.severity);
            alertRow.put("message", alert.message);
            alertRow.put("details", gson.toJson(alert.details));
            alertRow.put("node", alert.node);

            InsertAllResponse response = bigQuery.insertAll(
                    InsertAllRequest.newBuilder(alertsTableId)
                            .addRow(UUID.randomUUID().toString(), alertRow)
                            .build()
            );

            if (response.hasErrors()) {
                Log.error("Failed to export alert to BigQuery");
            }

        } catch (Exception e) {
            Log.error("Failed to export alert", e);
        }
    }

    private void performCleanup() {
        try {
            // Clean up old data from BigQuery
            Instant cutoff = Instant.now().minus(Duration.ofDays(retentionDays));
            String cutoffStr = DateTimeFormatter.ISO_INSTANT.format(cutoff);

            String deleteQuery = String.format(
                    "DELETE FROM `%s` WHERE timestamp < '%s'",
                    eventsTableId.getTable(),
                    cutoffStr
            );

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(deleteQuery)
                    .setUseLegacySql(false)
                    .build();

            bigQuery.query(queryConfig);

            Log.debugf("Cleaned up monitoring data older than %s", cutoffStr);

        } catch (Exception e) {
            Log.error("Failed to perform cleanup", e);
        }
    }

    private void updateMetricsForEvent(String eventType, Map<String, Object> data) {
        // Update counters based on event type
        switch (eventType) {
            case "file_detected" -> {
                incrementCounter("files.detected");
                String source = (String) data.get("source");
                if (source != null) {
                    incrementCounter("files.by_source", "source", source);
                }
            }
            case "transfer_completed" -> {
                incrementCounter("transfers.completed");
                Long duration = (Long) data.get("duration_ms");
                if (duration != null) {
                    recordDistribution("transfer.duration", duration);
                }
            }
            case "transfer_failed" -> {
                incrementCounter("transfers.failed");
            }
            case "error" -> {
                incrementCounter("errors.total");
            }
        }
    }

    private void checkAlertConditions(String eventType, Map<String, Object> data) {
        // Check for alert conditions based on event type
        if ("error".equals(eventType)) {
            String errorType = (String) data.get("error_type");

            // Count recent errors
            double recentErrors = Counter.builder("recent.errors")
                    .tag("type", errorType)
                    .register(meterRegistry)
                    .count();

            if (recentErrors > 10) {
                sendAlert(
                        "HIGH_ERROR_RATE",
                        "WARNING",
                        "High error rate detected: " + errorType,
                        data
                );
            }
        }
    }

    private void sendAlertNotifications(Alert alert) {
        // Send email notification
        executor.execute(() -> sendEmailAlert(alert));

        // Send Slack notification
        if ("CRITICAL".equals(alert.severity) || "HIGH".equals(alert.severity)) {
            executor.execute(() -> sendSlackAlert(alert));
        }

        // Send PagerDuty for critical alerts
        if ("CRITICAL".equals(alert.severity)) {
            executor.execute(() -> sendPagerDutyAlert(alert));
        }
    }

    private void sendEmailAlert(Alert alert) {
        // Email implementation
        Log.infof("Email alert: %s - %s", alert.type, alert.message);
    }

    private void sendSlackAlert(Alert alert) {
        // Slack implementation
        Log.infof("Slack alert: %s - %s", alert.type, alert.message);
    }

    private void sendPagerDutyAlert(Alert alert) {
        // PagerDuty implementation
        Log.infof("PagerDuty alert: %s - %s", alert.type, alert.message);
    }

    private void createTableIfNotExists(TableId tableId, Schema schema) {
        try {
            Table table = bigQuery.getTable(tableId);
            if (table == null) {
                TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                        .setSchema(schema)
                        .setTimePartitioning(
                                TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                                        .setField("timestamp")
                                        .build()
                        )
                        .build();

                TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
                bigQuery.create(tableInfo);

                Log.infof("Created BigQuery table: %s", tableId);
            }
        } catch (Exception e) {
            Log.errorf(e, "Failed to create table: %s", tableId);
        }
    }

    private Schema getEventsTableSchema() {
        return Schema.of(
                Field.of("timestamp", StandardSQLTypeName.TIMESTAMP),
                Field.of("event_type", StandardSQLTypeName.STRING),
                Field.of("node", StandardSQLTypeName.STRING),
                Field.of("data", StandardSQLTypeName.JSON)
        );
    }

    private Schema getMetricsTableSchema() {
        return Schema.of(
                Field.of("timestamp", StandardSQLTypeName.TIMESTAMP),
                Field.of("node", StandardSQLTypeName.STRING),
                Field.of("counters", StandardSQLTypeName.JSON),
                Field.of("gauges", StandardSQLTypeName.JSON),
                Field.of("timers", StandardSQLTypeName.JSON),
                Field.of("statistics", StandardSQLTypeName.JSON)
        );
    }

    private Schema getAlertsTableSchema() {
        return Schema.of(
                Field.of("timestamp", StandardSQLTypeName.TIMESTAMP),
                Field.of("alert_type", StandardSQLTypeName.STRING),
                Field.of("severity", StandardSQLTypeName.STRING),
                Field.of("message", StandardSQLTypeName.STRING),
                Field.of("details", StandardSQLTypeName.JSON),
                Field.of("node", StandardSQLTypeName.STRING)
        );
    }

    private Schema getSlaTableSchema() {
        return Schema.of(
                Field.of("date", StandardSQLTypeName.DATE),
                Field.of("node", StandardSQLTypeName.STRING),
                Field.of("overall_compliance", StandardSQLTypeName.FLOAT64),
                Field.of("source_details", StandardSQLTypeName.JSON)
        );
    }

    private void incrementCounter(String name, String... tags) {
        String key = name + Arrays.toString(tags);
        counters.computeIfAbsent(key, k ->
                Counter.builder(name)
                        .tags(tags)
                        .register(meterRegistry)
        ).increment();
    }

    private Timer getOrCreateTimer(String name, String... tags) {
        String key = name + Arrays.toString(tags);
        return timers.computeIfAbsent(key, k ->
                Timer.builder(name)
                        .tags(tags)
                        .register(meterRegistry)
        );
    }

    private void recordDistribution(String name, double value, String... tags) {
        String key = name + Arrays.toString(tags);
        summaries.computeIfAbsent(key, k ->
                DistributionSummary.builder(name)
                        .tags(tags)
                        .register(meterRegistry)
        ).record(value);
    }

    private String[] concatenateTags(String key, String value, String... additionalTags) {
        String[] result = new String[additionalTags.length + 2];
        result[0] = key;
        result[1] = value;
        System.arraycopy(additionalTags, 0, result, 2, additionalTags.length);
        return result;
    }

    // ============ Inner Classes ============

    /**
     * Monitoring event.
     */
    private static class MonitoringEvent {
        final String type;
        final Map<String, Object> data;
        final String node;
        final Instant timestamp;

        MonitoringEvent(String type, Map<String, Object> data, String node, Instant timestamp) {
            this.type = type;
            this.data = data;
            this.node = node;
            this.timestamp = timestamp;
        }
    }

    /**
     * Alert definition.
     */
    private static class Alert {
        String type;
        String severity;
        String message;
        Map<String, Object> details;
        Instant timestamp;
        String node;
    }

    /**
     * Alert state tracking.
     */
    private static class AlertState {
        final Alert alert;
        final Instant triggeredAt;
        Instant lastUpdated;
        Instant clearedAt;
        int updateCount = 1;

        AlertState(Alert alert) {
            this.alert = alert;
            this.triggeredAt = Instant.now();
            this.lastUpdated = this.triggeredAt;
        }

        boolean isActive() {
            return clearedAt == null;
        }
    }

    /**
     * Metrics snapshot.
     */
    public static class MetricsSnapshot {
        public Instant timestamp;
        public String node;
        public Map<String, Double> counters;
        public Map<String, Double> gauges;
        public Map<String, Map<String, Object>> timers;
        public Map<String, Object> statistics;
    }

    /**
     * Transfer statistics.
     */
    public static class TransferStatistics {
        public Duration period;
        public Instant startTime;
        public Instant endTime;
        public Map<TransferStatus, Long> byStatus;
        public Map<String, Long> bySource;
        public double successRate;
        public long totalBytesTransferred;
        public double throughputMBps;
        public double avgProcessingTimeMs;
        public long minProcessingTimeMs;
        public long maxProcessingTimeMs;
    }

    /**
     * SLA report.
     */
    public static class SlaReport {
        public LocalDate date;
        public String node;
        public double overallSlaCompliance;
        public Map<String, SlaStatus> sourceSlaStatus;
    }

    /**
     * SLA status for a source.
     */
    public static class SlaStatus {
        public String sourceSystem;
        public Instant expectedTime;
        public Instant actualTime;
        public long delayMinutes;
        public boolean metSla;
        public long filesProcessed;
        public long filesFailed;
        public double successRate;
    }
}