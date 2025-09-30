package com.lbg.markets.surveillance.relay.routes;

import com.lbg.markets.surveillance.relay.config.RelayConfiguration;
import com.lbg.markets.surveillance.relay.enums.TransferStatus;
import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.model.SourceSystem;
import com.lbg.markets.surveillance.relay.repository.FileTransferRepository;
import com.lbg.markets.surveillance.relay.service.FileDetectionService;
import com.lbg.markets.surveillance.relay.service.MonitoringService;
import com.lbg.markets.surveillance.relay.service.TransferOrchestrator;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.file.GenericFile;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.*;

/**
 * Camel routes for file ingestion from source systems.
 * Handles file detection, validation, and queuing for transfer.
 */
@ApplicationScoped
public class FileIngestionRoute extends RouteBuilder {

    @Inject
    RelayConfiguration config;
    @Inject
    FileDetectionService detectionService;
    @Inject
    TransferOrchestrator orchestrator;
    @Inject
    MonitoringService monitoring;
    @Inject
    FileTransferRepository repository;

    @ConfigProperty(name = "relay.node-name")
    String nodeName;

    @ConfigProperty(name = "relay.ingestion.max-files-per-poll", defaultValue = "100")
    int maxFilesPerPoll;

    @ConfigProperty(name = "relay.ingestion.concurrent-consumers", defaultValue = "3")
    int concurrentConsumers;

    @ConfigProperty(name = "relay.ingestion.error-delay-ms", defaultValue = "30000")
    long errorDelayMs;

    private final Map<String, AtomicInteger> sourceFileCounters = new HashMap<>();

    @Override
    public void configure() throws Exception {

        // Configure error handling
        configureErrorHandling();

        // Configure file watchers for each source system
        configureSourceWatchers();

        // Configure file processing routes
        configureFileProcessing();

        // Configure monitoring routes
        configureMonitoring();

        // Configure scheduled tasks
        configureScheduledTasks();
    }

    /**
     * Configure global error handling for all routes.
     */
    private void configureErrorHandling() {

        // Global error handler
        errorHandler(deadLetterChannel("direct:ingestion-dlq")
                .maximumRedeliveries(3)
                .redeliveryDelay(5000)
                .backOffMultiplier(2)
                .useExponentialBackOff()
                .retryAttemptedLogLevel(LoggingLevel.WARN)
                .logStackTrace(true)
                .logExhausted(true)
                .logHandled(false));

        // Dead letter queue processing
        from(direct("ingestion-dlq"))
                .routeId("ingestion-dlq")
                .log(LoggingLevel.ERROR,
                        "Failed to process file after retries: ${header.CamelFileName} - ${exception.message}")
                .process(exchange -> {
                    String fileName = exchange.getIn().getHeader("CamelFileName", String.class);
                    String sourceSystem = exchange.getIn().getHeader("SourceSystem", String.class);
                    Exception cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);

                    monitoring.recordError("ingestion_failed", sourceSystem,
                            String.format("File: %s, Error: %s", fileName, cause.getMessage()));

                    // Update source system error count
                    SourceSystem.findBySystemId(sourceSystem)
                            .ifPresent(source -> {
                                source.recordFailure(cause.getMessage());
                                source.persist();
                            });
                });
    }

    /**
     * Configure file watchers for each source system.
     */
    private void configureSourceWatchers() {

        for (RelayConfiguration.SourceSystem source : config.sources()) {
            if (!source.enabled()) {
                Log.infof("Skipping disabled source: %s", source.id());
                continue;
            }

            String routeId = "watch-" + source.id();
            sourceFileCounters.put(source.id(), new AtomicInteger(0));

            try {
                // Validate source path exists
                Path sourcePath = Paths.get(source.path());
                if (!Files.exists(sourcePath)) {
                    Log.warnf("Source path does not exist: %s for %s", source.path(), source.id());

                    // Try fallback paths if configured
                    if (source.fallbackPaths().isPresent()) {
                        for (String fallback : source.fallbackPaths().get()) {
                            Path fallbackPath = Paths.get(fallback);
                            if (Files.exists(fallbackPath)) {
                                Log.infof("Using fallback path for %s: %s", source.id(), fallback);
                                sourcePath = fallbackPath;
                                break;
                            }
                        }
                    }

                    if (!Files.exists(sourcePath)) {
                        Log.errorf("No valid path found for source: %s", source.id());
                        continue;
                    }
                }

                // Build file endpoint based on configuration
                from(file(sourcePath.toString())
                        .include(source.filePattern())
                        .exclude(buildExcludePattern(source))
                        .recursive(source.recursive())
                        .maxDepth(source.recursive() ? source.maxDepth() : 1)
                        .noop(true)  // Don't move/delete files
                        .idempotent(true)
                        .idempotentKey("${file:name}-${file:size}-${file:modified}")
                        .idempotentRepository("file-ingestion-" + source.id())
                        .readLock("changed")
                        .readLockCheckInterval(5000)
                        .readLockMinAge(getReadLockAge(source))
                        .readLockTimeout(60000)
                        .maxMessagesPerPoll(maxFilesPerPoll)
                        .delay(source.pollInterval().toMillis())
                        .backoffMultiplier(2)
                        .backoffIdleThreshold(3)
                        .backoffErrorThreshold(5))
                        .routeId(routeId)
                        .autoStartup(source.enabled())
                        .setHeader("SourceSystem", constant(source.id()))
                        .setHeader("SourcePriority", constant(source.priority()))
                        .setHeader("ReadyStrategy", constant(source.readyStrategy().toString()))
                        .process(new FileValidationProcessor(source))
                        .choice()
                        .when(header("FileReady").isEqualTo(true))
                        .to("direct:process-file")
                        .when(header("FileSkipped").isEqualTo(true))
                        .log(LoggingLevel.DEBUG, "File skipped: ${header.CamelFileName}")
                        .otherwise()
                        .to("direct:file-waiting")
                        .end();

                Log.infof("Configured file watcher for %s: %s", source.id(), sourcePath);

            } catch (Exception e) {
                Log.errorf(e, "Failed to configure watcher for source: %s", source.id());
            }
        }
    }

    /**
     * Configure file processing routes.
     */
    private void configureFileProcessing() {

        // Main file processing route
        from(direct("process-file"))
                .routeId("process-file")
                .threads(concurrentConsumers)
                .process(exchange -> {
                    GenericFile<?> file = exchange.getIn().getBody(GenericFile.class);
                    String sourceSystem = exchange.getIn().getHeader("SourceSystem", String.class);

                    if (file == null) {
                        throw new IllegalArgumentException("No file found in exchange");
                    }

                    Path filePath = Paths.get(file.getAbsoluteFilePath());

                    // Check if already processed
                    if (isAlreadyProcessed(sourceSystem, file.getFileName())) {
                        exchange.getIn().setHeader("AlreadyProcessed", true);
                        Log.debugf("File already processed: %s", file.getFileName());
                        return;
                    }

                    // Register the file
                    FileTransfer transfer = registerFile(sourceSystem, filePath);
                    exchange.getIn().setHeader("TransferId", transfer.id);

                    // Update source system stats
                    updateSourceStats(sourceSystem, file);
                })
                .choice()
                .when(header("AlreadyProcessed").isEqualTo(true))
                .log(LoggingLevel.DEBUG, "Skipping already processed file: ${header.CamelFileName}")
                .otherwise()
                .to("direct:queue-transfer")
                .end();

        // Queue transfer for processing
        from(direct("queue-transfer"))
                .routeId("queue-transfer")
                .process(exchange -> {
                    Long transferId = exchange.getIn().getHeader("TransferId", Long.class);
                    String sourceSystem = exchange.getIn().getHeader("SourceSystem", String.class);

                    // Queue for processing
                    orchestrator.queueTransfer(transferId);

                    // Record metrics
                    monitoring.recordEvent("file_queued", Map.of(
                            "source", sourceSystem,
                            "transfer_id", transferId,
                            "node", nodeName
                    ));

                    sourceFileCounters.get(sourceSystem).incrementAndGet();
                })
                .log(LoggingLevel.INFO, "File queued for transfer: ${header.CamelFileName}");

        // Handle files waiting for ready condition
        from(direct("file-waiting"))
                .routeId("file-waiting")
                .process(exchange -> {
                    String fileName = exchange.getIn().getHeader("CamelFileName", String.class);
                    String sourceSystem = exchange.getIn().getHeader("SourceSystem", String.class);
                    String reason = exchange.getIn().getHeader("WaitReason", String.class);

                    Log.debugf("File waiting for %s: %s - %s", sourceSystem, fileName, reason);

                    // Could track waiting files if needed
                    monitoring.recordEvent("file_waiting", Map.of(
                            "source", sourceSystem,
                            "filename", fileName,
                            "reason", reason
                    ));
                });
    }

    /**
     * Configure monitoring and health check routes.
     */
    private void configureMonitoring() {

        // Source system health monitoring
        from(timer("source-health")
                .period(Duration.ofMinutes(5).toMillis())
                .delay(60000))
                .routeId("monitor-source-health")
                .process(exchange -> {
                    for (RelayConfiguration.SourceSystem source : config.sources()) {
                        if (!source.enabled()) continue;

                        checkSourceHealth(source);
                    }
                });

        // File count monitoring
        from(timer("file-count-monitor")
                .period(Duration.ofMinutes(1).toMillis()))
                .routeId("monitor-file-counts")
                .process(exchange -> {
                    Map<String, Object> counts = new HashMap<>();
                    sourceFileCounters.forEach((source, counter) -> {
                        int count = counter.getAndSet(0);
                        counts.put(source, count);
                    });

                    if (!counts.isEmpty()) {
                        monitoring.recordEvent("ingestion_rates", counts);
                    }
                });

        // Stuck file detection
        from(timer("stuck-file-check")
                .period(Duration.ofMinutes(15).toMillis()))
                .routeId("check-stuck-files")
                .process(exchange -> {
                    Instant cutoff = Instant.now().minus(Duration.ofHours(1));

                    List<FileTransfer> stuckTransfers = FileTransfer.find(
                            "status = ?1 and createdAt < ?2",
                            TransferStatus.DETECTED, cutoff
                    ).list();

                    if (!stuckTransfers.isEmpty()) {
                        Log.warnf("Found %d stuck transfers", stuckTransfers.size());

                        stuckTransfers.forEach(transfer -> {
                            monitoring.recordEvent("stuck_transfer", Map.of(
                                    "transfer_id", transfer.id,
                                    "source", transfer.sourceSystem,
                                    "filename", transfer.filename,
                                    "age_hours", Duration.between(transfer.createdAt, Instant.now()).toHours()
                            ));
                        });
                    }
                });
    }

    /**
     * Configure scheduled tasks.
     */
    private void configureScheduledTasks() {

        // Daily statistics reset
        from(cron("daily-reset")
                .schedule("0 0 0 * * ?"))  // Midnight daily
                .routeId("daily-statistics-reset")
                .process(exchange -> {
                    Log.info("Resetting daily statistics");

                    SourceSystem.streamAll()
                            .map(s -> (SourceSystem) s)
                            .forEach(source -> {
                                source.resetDailyCounters();
                                source.persist();
                            });

                    monitoring.recordEvent("daily_reset", Map.of(
                            "node", nodeName,
                            "timestamp", Instant.now()
                    ));
                });

        // SLA monitoring
        from(timer("sla-check")
                .period(Duration.ofMinutes(30).toMillis()))
                .routeId("sla-monitoring")
                .process(exchange -> {
                    for (RelayConfiguration.SourceSystem source : config.sources()) {
                        if (!source.enabled()) continue;

                        source.sla().ifPresent(sla -> {
                            checkSlaCompliance(source.id(), sla);
                        });
                    }
                });

        // Scheduled file processing for SCHEDULED strategy sources
        config.sources().stream()
                .filter(s -> s.enabled() && s.readyStrategy() == RelayConfiguration.FileReadyStrategy.SCHEDULED)
                .forEach(source -> {
                    source.scheduledTime().ifPresent(scheduledTime -> {
                        String cronExpression = String.format("0 %d %d * * ?",
                                scheduledTime.getMinute(),
                                scheduledTime.getHour());

                        from(cron("scheduled-" + source.id())
                                .schedule(cronExpression))
                                .routeId("scheduled-process-" + source.id())
                                .process(exchange -> {
                                    Log.infof("Scheduled processing for %s", source.id());
                                    processScheduledSource(source);
                                });
                    });
                });
    }

    /**
     * File validation processor.
     */
    private class FileValidationProcessor implements Processor {
        private final RelayConfiguration.SourceSystem source;

        public FileValidationProcessor(RelayConfiguration.SourceSystem source) {
            this.source = source;
        }

        @Override
        public void process(Exchange exchange) throws Exception {
            GenericFile<?> file = exchange.getIn().getBody(GenericFile.class);

            if (file == null) {
                exchange.getIn().setHeader("FileReady", false);
                exchange.getIn().setHeader("WaitReason", "No file found");
                return;
            }

            // Check if file matches exclude patterns
            if (source.excludePatterns().isPresent()) {
                for (String excludePattern : source.excludePatterns().get()) {
                    if (file.getFileName().matches(excludePattern)) {
                        exchange.getIn().setHeader("FileSkipped", true);
                        return;
                    }
                }
            }

            // Check file size limits
            source.processing().ifPresent(proc -> {
                long fileSize = file.getFileLength();

                if (proc.minFileSize() > 0 && fileSize < proc.minFileSize()) {
                    exchange.getIn().setHeader("FileSkipped", true);
                    exchange.getIn().setHeader("SkipReason", "File too small");
                    return;
                }

                if (proc.maxFileSize() > 0 && fileSize > proc.maxFileSize()) {
                    exchange.getIn().setHeader("FileSkipped", true);
                    exchange.getIn().setHeader("SkipReason", "File too large");
                    return;
                }

                // Check file age
                proc.fileAge().ifPresent(age -> {
                    Instant fileModified = Instant.ofEpochMilli(file.getLastModified());
                    Duration fileAge = Duration.between(fileModified, Instant.now());

                    age.minAge().ifPresent(minAge -> {
                        if (fileAge.compareTo(minAge) < 0) {
                            exchange.getIn().setHeader("FileReady", false);
                            exchange.getIn().setHeader("WaitReason", "File too new");
                        }
                    });

                    age.maxAge().ifPresent(maxAge -> {
                        if (fileAge.compareTo(maxAge) > 0) {
                            exchange.getIn().setHeader("FileSkipped", true);
                            exchange.getIn().setHeader("SkipReason", "File too old");
                        }
                    });
                });
            });

            // Check readiness based on strategy
            boolean isReady = checkFileReadiness(file, source);
            exchange.getIn().setHeader("FileReady", isReady);

            if (!isReady) {
                exchange.getIn().setHeader("WaitReason",
                        "Strategy: " + source.readyStrategy());
            }
        }
    }

    /**
     * Check if file is ready based on configured strategy.
     */
    private boolean checkFileReadiness(GenericFile<?> file, RelayConfiguration.SourceSystem source) {
        return switch (source.readyStrategy()) {
            case DONE_FILE -> {
                String doneFile = file.getAbsoluteFilePath() + source.doneFileSuffix();
                yield Files.exists(Paths.get(doneFile));
            }
            case FILE_AGE -> {
                long lastModified = file.getLastModified();
                long stableTime = source.stabilityPeriod().toMillis();
                yield (System.currentTimeMillis() - lastModified) > stableTime;
            }
            case IMMEDIATE -> true;
            case SCHEDULED -> {
                // Check if we're within the scheduled window
                LocalTime now = LocalTime.now();
                LocalTime scheduled = source.scheduledTime().orElse(LocalTime.MIDNIGHT);
                LocalTime windowEnd = scheduled.plusMinutes(30);
                yield now.isAfter(scheduled) && now.isBefore(windowEnd);
            }
            case NOT_LOCKED -> {
                // Try to open file exclusively
                try {
                    File f = new File(file.getAbsoluteFilePath());
                    if (f.canWrite()) {
                        // File is not locked
                        yield true;
                    }
                } catch (Exception e) {
                    // File is likely locked
                }
                yield false;
            }
            case CUSTOM -> {
                // Could call external service or custom logic
                yield checkCustomReadiness(file, source);
            }
            default -> false;
        };
    }

    /**
     * Custom readiness check - could be extended.
     */
    private boolean checkCustomReadiness(GenericFile<?> file, RelayConfiguration.SourceSystem source) {
        // Implement custom logic here
        // Could call external API, check database, etc.
        return true;
    }

    /**
     * Check if file has already been processed.
     */
    private boolean isAlreadyProcessed(String sourceSystem, String filename) {
        return repository.findBySourceAndFilename(sourceSystem, filename).isPresent();
    }

    /**
     * Register a new file for processing.
     */
    @Transactional
    private FileTransfer registerFile(String sourceSystem, Path filePath) {
        try {
            String filename = filePath.getFileName().toString();
            long fileSize = Files.size(filePath);
            String fileHash = detectionService.calculateHash(filePath);

            FileTransfer transfer = new FileTransfer();
            transfer.sourceSystem = sourceSystem;
            transfer.filename = filename;
            transfer.filePath = filePath.toString();
            transfer.fileSize = fileSize;
            transfer.fileHash = fileHash;
            transfer.status = TransferStatus.DETECTED;
            transfer.processingNode = nodeName;
            transfer.persist();

            Log.infof("Registered file: %s from %s (ID: %d)",
                    filename, sourceSystem, transfer.id);

            return transfer;

        } catch (Exception e) {
            throw new RuntimeException("Failed to register file: " + filePath, e);
        }
    }

    /**
     * Update source system statistics.
     */
    @Transactional
    private void updateSourceStats(String sourceSystem, GenericFile<?> file) {
        SourceSystem.findBySystemId(sourceSystem).ifPresent(source -> {
            source.recordFileDetected(file.getFileName(), file.getFileLength());
            source.markAvailable();
            source.persist();
        });
    }

    /**
     * Check source system health.
     */
    private void checkSourceHealth(RelayConfiguration.SourceSystem source) {
        try {
            Path sourcePath = Paths.get(source.path());
            boolean isAccessible = Files.exists(sourcePath) && Files.isReadable(sourcePath);

            SourceSystem.findBySystemId(source.id()).ifPresent(sys -> {
                if (isAccessible) {
                    sys.markAvailable();
                } else {
                    sys.markUnavailable("Path not accessible: " + source.path());
                }
                sys.persist();
            });

        } catch (Exception e) {
            Log.errorf(e, "Health check failed for source: %s", source.id());
        }
    }

    /**
     * Check SLA compliance for a source.
     */
    private void checkSlaCompliance(String sourceId, RelayConfiguration.SlaConfig sla) {
        SourceSystem.findBySystemId(sourceId).ifPresent(source -> {
            boolean isOverdue = source.isOverdue(sla.gracePeriod());

            if (isOverdue) {
                monitoring.recordEvent("sla_breach", Map.of(
                        "source", sourceId,
                        "expected", source.expectedNextFile,
                        "last_received", source.lastFileReceived
                ));

                // Could trigger alerts here
            }
        });
    }

    /**
     * Process files for scheduled sources.
     */
    private void processScheduledSource(RelayConfiguration.SourceSystem source) {
        Path sourcePath = Paths.get(source.path());

        try {
            Files.walk(sourcePath, source.recursive() ? source.maxDepth() : 1)
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().matches(source.filePattern()))
                    .filter(path -> !isAlreadyProcessed(source.id(), path.getFileName().toString()))
                    .forEach(path -> {
                        FileTransfer transfer = registerFile(source.id(), path);
                        orchestrator.queueTransfer(transfer.id);
                    });

        } catch (Exception e) {
            Log.errorf(e, "Failed to process scheduled source: %s", source.id());
        }
    }

    /**
     * Build exclude pattern from configuration.
     */
    private String buildExcludePattern(RelayConfiguration.SourceSystem source) {
        if (source.excludePatterns().isEmpty()) {
            return null;
        }

        return String.join("|", source.excludePatterns().get());
    }

    /**
     * Get read lock age based on configuration.
     */
    private long getReadLockAge(RelayConfiguration.SourceSystem source) {
        if (source.readyStrategy() == RelayConfiguration.FileReadyStrategy.FILE_AGE) {
            return source.stabilityPeriod().toMillis();
        }
        return 10000; // Default 10 seconds
    }
}