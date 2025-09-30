package com.lbg.markets.surveillance.relay.service;

import com.lbg.markets.surveillance.relay.config.RelayConfiguration;
import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.model.SourceSystem;
import com.lbg.markets.surveillance.relay.model.TransferStatus;
import com.lbg.markets.surveillance.relay.model.TransferStatusHistory;
import com.lbg.markets.surveillance.relay.repository.FileTransferRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;

import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Orchestrates file transfer processing from detection to completion.
 * Manages queue, processing threads, and state transitions.
 */
@ApplicationScoped
@Startup
public class TransferOrchestrator {

    @Inject FileTransferRepository repository;
    @Inject GcsStreamingService gcsService;
    @Inject FileValidationService validationService;
    @Inject MonitoringService monitoringService;
    @Inject RelayConfiguration config;
    @Inject MeterRegistry meterRegistry;

    @ConfigProperty(name = "relay.node-name")
    String nodeName;

    @ConfigProperty(name = "relay.orchestrator.enabled", defaultValue = "true")
    boolean orchestratorEnabled;

    @ConfigProperty(name = "relay.orchestrator.worker-threads", defaultValue = "10")
    int workerThreads;

    @ConfigProperty(name = "relay.orchestrator.priority-threads", defaultValue = "3")
    int priorityThreads;

    @ConfigProperty(name = "relay.orchestrator.queue-capacity", defaultValue = "1000")
    int queueCapacity;

    @ConfigProperty(name = "relay.orchestrator.batch-size", defaultValue = "20")
    int batchSize;

    @ConfigProperty(name = "relay.orchestrator.poll-interval-ms", defaultValue = "5000")
    long pollIntervalMs;

    @ConfigProperty(name = "relay.orchestrator.max-concurrent-per-source", defaultValue = "5")
    int maxConcurrentPerSource;

    @ConfigProperty(name = "relay.orchestrator.processing-timeout-minutes", defaultValue = "30")
    int processingTimeoutMinutes;

    @ConfigProperty(name = "relay.orchestrator.retry-delay-minutes", defaultValue = "5")
    int retryDelayMinutes;

    @ConfigProperty(name = "relay.orchestrator.max-retries", defaultValue = "3")
    int maxRetries;

    // Thread pools
    private ThreadPoolExecutor workerPool;
    private ThreadPoolExecutor priorityPool;
    private ScheduledExecutorService schedulerPool;

    // Processing queues
    private final PriorityBlockingQueue<TransferTask> pendingQueue =
            new PriorityBlockingQueue<>(queueCapacity, new TransferTaskComparator());
    private final Map<Long, TransferTask> activeTransfers = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> sourceConcurrentCounts = new ConcurrentHashMap<>();

    // Rate limiting
    private final Map<String, RateLimiter> sourceRateLimiters = new ConcurrentHashMap<>();

    // Statistics
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalSuccessful = new AtomicLong(0);
    private final AtomicLong totalFailed = new AtomicLong(0);
    private final AtomicInteger currentQueueSize = new AtomicInteger(0);
    private final Map<String, SourceStatistics> sourceStats = new ConcurrentHashMap<>();

    // Circuit breakers for each source
    private final Map<String, CircuitBreakerState> circuitBreakers = new ConcurrentHashMap<>();

    // Shutdown flag
    private volatile boolean shutdownRequested = false;

    @PostConstruct
    void init() {
        if (!orchestratorEnabled) {
            Log.info("Transfer orchestrator disabled");
            return;
        }

        Log.info("Initializing transfer orchestrator");

        // Initialize thread pools
        initializeThreadPools();

        // Initialize rate limiters
        initializeRateLimiters();

        // Initialize statistics
        initializeStatistics();

        // Start processing threads
        startProcessingThreads();

        // Recover pending transfers
        recoverPendingTransfers();

        Log.infof("Transfer orchestrator initialized with %d worker threads", workerThreads);
    }

    @PreDestroy
    void shutdown() {
        Log.info("Shutting down transfer orchestrator");
        shutdownRequested = true;

        try {
            // Stop accepting new tasks
            schedulerPool.shutdown();

            // Wait for active transfers to complete
            waitForActiveTransfers();

            // Shutdown worker pools
            shutdownThreadPool(workerPool, "worker");
            shutdownThreadPool(priorityPool, "priority");

            // Final statistics
            logFinalStatistics();

        } catch (Exception e) {
            Log.error("Error during orchestrator shutdown", e);
        }
    }

    // ============ Public API ============

    /**
     * Queue a transfer for processing.
     */
    @Transactional
    public CompletableFuture<TransferResult> queueTransfer(Long transferId) {
        if (shutdownRequested) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Orchestrator is shutting down"));
        }

        try {
            FileTransfer transfer = repository.findById(transferId);
            if (transfer == null) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException("Transfer not found: " + transferId));
            }

            // Check if already processing
            if (activeTransfers.containsKey(transferId)) {
                Log.debugf("Transfer already processing: %d", transferId);
                return activeTransfers.get(transferId).future;
            }

            // Check if already queued
            if (pendingQueue.stream().anyMatch(t -> t.transferId.equals(transferId))) {
                Log.debugf("Transfer already queued: %d", transferId);
                return CompletableFuture.completedFuture(
                        new TransferResult(transferId, TransferStatus.QUEUED, "Already queued"));
            }

            // Create transfer task
            TransferTask task = new TransferTask(transfer);

            // Check source circuit breaker
            CircuitBreakerState breaker = circuitBreakers.get(transfer.sourceSystem);
            if (breaker != null && breaker.isOpen()) {
                Log.warnf("Circuit breaker open for source: %s", transfer.sourceSystem);
                return CompletableFuture.failedFuture(
                        new CircuitBreakerOpenException(transfer.sourceSystem));
            }

            // Add to queue
            if (pendingQueue.offer(task)) {
                currentQueueSize.incrementAndGet();
                updateTransferStatus(transfer, TransferStatus.QUEUED, "Queued for processing");

                monitoringService.recordEvent("transfer_queued", Map.of(
                        "transfer_id", transferId,
                        "source", transfer.sourceSystem,
                        "queue_size", currentQueueSize.get()
                ));

                return task.future;
            } else {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Queue is full"));
            }

        } catch (Exception e) {
            Log.errorf(e, "Failed to queue transfer: %d", transferId);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Process multiple transfers in batch.
     */
    public List<CompletableFuture<TransferResult>> queueBatch(List<Long> transferIds) {
        return transferIds.stream()
                .map(this::queueTransfer)
                .collect(Collectors.toList());
    }

    /**
     * Cancel a transfer.
     */
    @Transactional
    public boolean cancelTransfer(Long transferId) {
        // Remove from queue if pending
        boolean removedFromQueue = pendingQueue.removeIf(t -> t.transferId.equals(transferId));

        // Cancel if active
        TransferTask activeTask = activeTransfers.get(transferId);
        if (activeTask != null) {
            activeTask.cancel();
            return true;
        }

        if (removedFromQueue) {
            FileTransfer transfer = repository.findById(transferId);
            if (transfer != null) {
                updateTransferStatus(transfer, TransferStatus.CANCELLED, "Cancelled by user");
            }
            return true;
        }

        return false;
    }

    /**
     * Get current orchestrator statistics.
     */
    public OrchestratorStatistics getStatistics() {
        OrchestratorStatistics stats = new OrchestratorStatistics();
        stats.queueSize = currentQueueSize.get();
        stats.activeTransfers = activeTransfers.size();
        stats.totalProcessed = totalProcessed.get();
        stats.totalSuccessful = totalSuccessful.get();
        stats.totalFailed = totalFailed.get();
        stats.successRate = stats.totalProcessed > 0
                ? (double) stats.totalSuccessful / stats.totalProcessed * 100 : 0;

        // Worker pool stats
        stats.workerPoolSize = workerPool.getPoolSize();
        stats.workerActiveCount = workerPool.getActiveCount();
        stats.workerQueueSize = workerPool.getQueue().size();

        // Source statistics
        stats.sourceStatistics = new HashMap<>(sourceStats);

        // Circuit breaker status
        stats.circuitBreakerStatus = circuitBreakers.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().getStatus()
                ));

        return stats;
    }

    /**
     * Pause processing for a source.
     */
    public void pauseSource(String sourceSystem) {
        Log.infof("Pausing processing for source: %s", sourceSystem);
        CircuitBreakerState breaker = circuitBreakers.computeIfAbsent(
                sourceSystem, k -> new CircuitBreakerState(k));
        breaker.open();
    }

    /**
     * Resume processing for a source.
     */
    public void resumeSource(String sourceSystem) {
        Log.infof("Resuming processing for source: %s", sourceSystem);
        CircuitBreakerState breaker = circuitBreakers.get(sourceSystem);
        if (breaker != null) {
            breaker.close();
        }
    }

    // ============ Scheduled Tasks ============

    @Scheduled(every = "30s")
    void pollPendingTransfers() {
        if (!orchestratorEnabled || shutdownRequested) {
            return;
        }

        try {
            // Find transfers that need processing
            List<FileTransfer> pending = repository.findPendingTransfers(batchSize);

            if (!pending.isEmpty()) {
                Log.debugf("Found %d pending transfers", pending.size());

                for (FileTransfer transfer : pending) {
                    // Try to acquire lock
                    if (repository.tryAcquireLock(transfer.id, transfer.rowVersion)) {
                        queueTransfer(transfer.id);
                    }
                }
            }

        } catch (Exception e) {
            Log.error("Error polling pending transfers", e);
        }
    }

    @Scheduled(every = "1m")
    void checkStuckTransfers() {
        if (!orchestratorEnabled) {
            return;
        }

        Instant stuckCutoff = Instant.now().minus(Duration.ofMinutes(processingTimeoutMinutes));

        activeTransfers.values().stream()
                .filter(task -> task.startTime.isBefore(stuckCutoff))
                .forEach(task -> {
                    Log.warnf("Transfer stuck: %d (processing for %d minutes)",
                            task.transferId,
                            Duration.between(task.startTime, Instant.now()).toMinutes());

                    // Cancel and requeue
                    task.cancel();
                    handleTransferTimeout(task.transferId);
                });
    }

    @Scheduled(every = "5m")
    void reportStatistics() {
        if (!orchestratorEnabled) {
            return;
        }

        OrchestratorStatistics stats = getStatistics();

        Log.infof("Orchestrator stats - Queue: %d, Active: %d, Success rate: %.2f%%",
                stats.queueSize, stats.activeTransfers, stats.successRate);

        // Report to monitoring
        monitoringService.recordEvent("orchestrator_stats", Map.of(
                "queue_size", stats.queueSize,
                "active_transfers", stats.activeTransfers,
                "total_processed", stats.totalProcessed,
                "success_rate", stats.successRate
        ));

        // Check for issues
        if (stats.queueSize > queueCapacity * 0.8) {
            monitoringService.sendAlert(
                    "QUEUE_HIGH",
                    "WARNING",
                    "Transfer queue is near capacity",
                    Map.of("queue_size", stats.queueSize, "capacity", queueCapacity)
            );
        }
    }

    // ============ Processing Logic ============

    private void startProcessingThreads() {
        // Start worker threads
        for (int i = 0; i < workerThreads; i++) {
            workerPool.submit(new WorkerThread(false));
        }

        // Start priority threads
        for (int i = 0; i < priorityThreads; i++) {
            priorityPool.submit(new WorkerThread(true));
        }

        Log.infof("Started %d worker threads and %d priority threads",
                workerThreads, priorityThreads);
    }

    /**
     * Worker thread for processing transfers.
     */
    private class WorkerThread implements Runnable {
        private final boolean isPriority;

        WorkerThread(boolean isPriority) {
            this.isPriority = isPriority;
        }

        @Override
        public void run() {
            Thread.currentThread().setName(
                    isPriority ? "orchestrator-priority" : "orchestrator-worker");

            Log.debugf("Worker thread started: %s", Thread.currentThread().getName());

            while (!shutdownRequested) {
                try {
                    // Get next task from queue
                    TransferTask task = pendingQueue.poll(1, TimeUnit.SECONDS);

                    if (task != null) {
                        // Check if should process based on priority
                        if (!isPriority || task.priority <= 2) {
                            processTransfer(task);
                        } else {
                            // Return to queue for normal workers
                            pendingQueue.offer(task);
                        }
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    Log.error("Error in worker thread", e);
                }
            }

            Log.debugf("Worker thread stopped: %s", Thread.currentThread().getName());
        }
    }

    private void processTransfer(TransferTask task) {
        Long transferId = task.transferId;
        Timer.Sample timer = Timer.start(meterRegistry);

        try {
            // Move to active transfers
            activeTransfers.put(transferId, task);
            currentQueueSize.decrementAndGet();
            task.startTime = Instant.now();

            // Check rate limit
            if (!checkRateLimit(task.sourceSystem)) {
                Log.debugf("Rate limit exceeded for source: %s", task.sourceSystem);
                requeueTransfer(task, Duration.ofSeconds(30));
                return;
            }

            // Increment concurrent count for source
            sourceConcurrentCounts.computeIfAbsent(task.sourceSystem, k -> new AtomicInteger())
                    .incrementAndGet();

            // Load full transfer details
            FileTransfer transfer = repository.findById(transferId);
            if (transfer == null) {
                throw new IllegalStateException("Transfer not found: " + transferId);
            }

            // Update status to processing
            updateTransferStatus(transfer, TransferStatus.PROCESSING, "Processing started");

            // Execute transfer pipeline
            TransferResult result = executeTransferPipeline(transfer);

            // Complete task
            task.complete(result);

            // Update statistics
            totalProcessed.incrementAndGet();
            if (result.status == TransferStatus.COMPLETED) {
                totalSuccessful.incrementAndGet();
                updateSourceStatistics(task.sourceSystem, true, task.getDuration());

                // Reset circuit breaker on success
                CircuitBreakerState breaker = circuitBreakers.get(task.sourceSystem);
                if (breaker != null) {
                    breaker.recordSuccess();
                }
            } else {
                totalFailed.incrementAndGet();
                updateSourceStatistics(task.sourceSystem, false, task.getDuration());

                // Record failure in circuit breaker
                CircuitBreakerState breaker = circuitBreakers.get(task.sourceSystem);
                if (breaker != null) {
                    breaker.recordFailure();
                }
            }

        } catch (Exception e) {
            Log.errorf(e, "Failed to process transfer: %d", transferId);

            // Handle failure
            handleTransferFailure(task, e);

            totalProcessed.incrementAndGet();
            totalFailed.incrementAndGet();

        } finally {
            // Remove from active transfers
            activeTransfers.remove(transferId);

            // Decrement concurrent count
            AtomicInteger count = sourceConcurrentCounts.get(task.sourceSystem);
            if (count != null) {
                count.decrementAndGet();
            }

            // Record timing
            timer.stop(Timer.builder("transfer.processing.time")
                    .tag("source", task.sourceSystem)
                    .tag("status", task.future.isCompletedExceptionally() ? "failed" : "success")
                    .register(meterRegistry));
        }
    }

    @Timeout(value = 30, unit = ChronoUnit.MINUTES)
    @Retry(maxRetries = 3, delay = 1000, delayUnit = ChronoUnit.MILLIS)
    @CircuitBreaker(requestVolumeThreshold = 10, failureRatio = 0.5, delay = 60000)
    private TransferResult executeTransferPipeline(FileTransfer transfer) throws Exception {

        // Step 1: Validate file exists and is accessible
        validateFileAccess(transfer);

        // Step 2: Perform validation if configured
        if (shouldValidate(transfer)) {
            performValidation(transfer);
        }

        // Step 3: Apply transformations if configured
        Path sourceFile = Paths.get(transfer.filePath);
        if (shouldTransform(transfer)) {
            sourceFile = applyTransformations(transfer, sourceFile);
        }

        // Step 4: Stream to GCS
        streamToGcs(transfer, sourceFile);

        // Step 5: Post-processing
        performPostProcessing(transfer);

        // Update transfer status
        updateTransferStatus(transfer, TransferStatus.COMPLETED, "Transfer completed successfully");

        return new TransferResult(transfer.id, TransferStatus.COMPLETED, "Success");
    }

    private void validateFileAccess(FileTransfer transfer) throws IOException {
        Path filePath = Paths.get(transfer.filePath);

        if (!Files.exists(filePath)) {
            throw new IOException("File not found: " + transfer.filePath);
        }

        if (!Files.isReadable(filePath)) {
            throw new IOException("File not readable: " + transfer.filePath);
        }

        // Verify file size matches
        long actualSize = Files.size(filePath);
        if (transfer.fileSize != null && Math.abs(actualSize - transfer.fileSize) > 1024) {
            Log.warnf("File size mismatch for %s: expected %d, actual %d",
                    transfer.filename, transfer.fileSize, actualSize);
        }
    }

    private boolean shouldValidate(FileTransfer transfer) {
        return config.sources().stream()
                .filter(s -> s.id().equals(transfer.sourceSystem))
                .findFirst()
                .flatMap(s -> s.validation())
                .map(v -> v.enabled())
                .orElse(false);
    }

    private void performValidation(FileTransfer transfer) throws ValidationException {
        updateTransferStatus(transfer, TransferStatus.VALIDATING, "Performing validation");

        FileValidationService.ValidationResult result = validationService.validateFile(
                transfer.sourceSystem,
                Paths.get(transfer.filePath)
        );

        if (!result.isValid()) {
            throw new ValidationException(result.getErrors());
        }

        Log.debugf("Validation passed for transfer: %d", transfer.id);
    }

    private boolean shouldTransform(FileTransfer transfer) {
        return config.sources().stream()
                .filter(s -> s.id().equals(transfer.sourceSystem))
                .findFirst()
                .flatMap(s -> s.processing())
                .flatMap(p -> p.transform())
                .isPresent();
    }

    private Path applyTransformations(FileTransfer transfer, Path sourceFile) throws Exception {
        updateTransferStatus(transfer, TransferStatus.TRANSFORMING, "Applying transformations");

        // Implementation would apply configured transformations
        // For now, return original file
        return sourceFile;
    }

    private void streamToGcs(FileTransfer transfer, Path sourceFile) throws IOException {
        Timer.Sample timer = Timer.start(meterRegistry);

        try {
            gcsService.streamToGcs(transfer);

            timer.stop(Timer.builder("gcs.upload.time")
                    .tag("source", transfer.sourceSystem)
                    .register(meterRegistry));

        } catch (IOException e) {
            timer.stop(Timer.builder("gcs.upload.time")
                    .tag("source", transfer.sourceSystem)
                    .tag("status", "failed")
                    .register(meterRegistry));
            throw e;
        }
    }

    private void performPostProcessing(FileTransfer transfer) {
        // Archive or delete source file if configured
        config.sources().stream()
                .filter(s -> s.id().equals(transfer.sourceSystem))
                .findFirst()
                .flatMap(s -> s.archive())
                .ifPresent(archive -> {
                    if (archive.enabled()) {
                        archiveSourceFile(transfer, archive);
                    }
                });
    }

    private void archiveSourceFile(FileTransfer transfer, RelayConfiguration.ArchiveConfig archive) {
        try {
            Path sourceFile = Paths.get(transfer.filePath);
            Path archivePath = Paths.get(archive.path())
                    .resolve(transfer.sourceSystem)
                    .resolve(generateArchivePath(transfer));

            Files.createDirectories(archivePath.getParent());

            switch (archive.strategy()) {
                case MOVE -> Files.move(sourceFile, archivePath);
                case COPY -> Files.copy(sourceFile, archivePath);
                case COPY_THEN_DELETE -> {
                    Files.copy(sourceFile, archivePath);
                    Files.delete(sourceFile);
                }
            }

            Log.debugf("Archived file: %s -> %s", sourceFile, archivePath);

        } catch (Exception e) {
            Log.errorf(e, "Failed to archive file: %s", transfer.filePath);
        }
    }

    private String generateArchivePath(FileTransfer transfer) {
        Instant now = Instant.now();
        return String.format("%d/%02d/%02d/%s",
                now.atZone(ZoneOffset.UTC).getYear(),
                now.atZone(ZoneOffset.UTC).getMonthValue(),
                now.atZone(ZoneOffset.UTC).getDayOfMonth(),
                transfer.filename
        );
    }

    private void handleTransferFailure(TransferTask task, Exception error) {
        try {
            FileTransfer transfer = repository.findById(task.transferId);
            if (transfer == null) {
                return;
            }

            transfer.retryCount++;

            if (transfer.retryCount < maxRetries && isRetryableError(error)) {
                // Schedule retry
                updateTransferStatus(transfer, TransferStatus.RETRYING,
                        "Scheduled for retry (attempt " + transfer.retryCount + ")");

                scheduleRetry(task, Duration.ofMinutes(retryDelayMinutes));
            } else {
                // Mark as failed
                updateTransferStatus(transfer, TransferStatus.FAILED, error.getMessage());

                task.completeExceptionally(error);

                // Send alert for repeated failures
                if (transfer.retryCount >= maxRetries) {
                    monitoringService.sendAlert(
                            "TRANSFER_MAX_RETRIES",
                            "HIGH",
                            String.format("Transfer %d failed after %d retries",
                                    transfer.id, maxRetries),
                            Map.of(
                                    "transfer_id", transfer.id,
                                    "source", transfer.sourceSystem,
                                    "error", error.getMessage()
                            )
                    );
                }
            }

        } catch (Exception e) {
            Log.errorf(e, "Error handling transfer failure for: %d", task.transferId);
            task.completeExceptionally(e);
        }
    }

    private void handleTransferTimeout(Long transferId) {
        FileTransfer transfer = repository.findById(transferId);
        if (transfer != null) {
            updateTransferStatus(transfer, TransferStatus.FAILED,
                    "Processing timeout after " + processingTimeoutMinutes + " minutes");

            monitoringService.recordError("transfer_timeout",
                    transfer.sourceSystem,
                    "Transfer " + transferId + " timed out");
        }
    }

    @Transactional
    private void updateTransferStatus(FileTransfer transfer, TransferStatus newStatus, String message) {
        TransferStatus previousStatus = transfer.status;

        transfer.status = newStatus;
        transfer.processingNode = nodeName;

        switch (newStatus) {
            case PROCESSING, REPROCESSING -> transfer.startedAt = Instant.now();
            case COMPLETED -> transfer.completedAt = Instant.now();
            case FAILED, REJECTED -> {
                transfer.errorMessage = message;
                transfer.completedAt = Instant.now();
            }
        }

        transfer.persist();

        // Record status history
        TransferStatusHistory.recordTransition(
                transfer.id,
                previousStatus,
                newStatus,
                nodeName,
                message
        );

        // Update source system
        SourceSystem.findBySystemId(transfer.sourceSystem).ifPresent(source -> {
            if (newStatus == TransferStatus.COMPLETED) {
                long processingTime = Duration.between(
                        transfer.startedAt, transfer.completedAt
                ).toMillis();
                source.recordSuccess(processingTime);
            } else if (newStatus == TransferStatus.FAILED) {
                source.recordFailure(message);
            }
            source.persist();
        });
    }

    // ============ Helper Methods ============

    private void initializeThreadPools() {
        // Worker pool for normal processing
        workerPool = new ThreadPoolExecutor(
                workerThreads,
                workerThreads,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("orchestrator-worker");
                    thread.setDaemon(false);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // Priority pool for high-priority transfers
        priorityPool = new ThreadPoolExecutor(
                priorityThreads,
                priorityThreads,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("orchestrator-priority");
                    thread.setDaemon(false);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // Scheduler for periodic tasks
        schedulerPool = Executors.newScheduledThreadPool(2, r -> {
            Thread thread = new Thread(r);
            thread.setName("orchestrator-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    private void initializeRateLimiters() {
        for (RelayConfiguration.SourceSystem source : config.sources()) {
            source.processing().ifPresent(processing -> {
                int maxConcurrent = processing.maxConcurrent();
                sourceRateLimiters.put(source.id(),
                        new RateLimiter(maxConcurrent, Duration.ofMinutes(1)));
            });
        }
    }

    private void initializeStatistics() {
        for (RelayConfiguration.SourceSystem source : config.sources()) {
            sourceStats.put(source.id(), new SourceStatistics(source.id()));
        }
    }

    private void recoverPendingTransfers() {
        try {
            // Find transfers that were processing when shutdown
            List<FileTransfer> stuck = FileTransfer.find(
                    "status = ?1 and processingNode = ?2",
                    TransferStatus.PROCESSING, nodeName
            ).list();

            if (!stuck.isEmpty()) {
                Log.infof("Recovering %d stuck transfers", stuck.size());

                for (FileTransfer transfer : stuck) {
                    transfer.status = TransferStatus.DETECTED;
                    transfer.processingNode = null;
                    transfer.persist();

                    queueTransfer(transfer.id);
                }
            }

        } catch (Exception e) {
            Log.error("Error recovering pending transfers", e);
        }
    }

    private boolean checkRateLimit(String sourceSystem) {
        RateLimiter limiter = sourceRateLimiters.get(sourceSystem);
        if (limiter != null) {
            return limiter.tryAcquire();
        }

        // Check concurrent limit
        AtomicInteger concurrent = sourceConcurrentCounts.get(sourceSystem);
        if (concurrent != null) {
            return concurrent.get() < maxConcurrentPerSource;
        }

        return true;
    }

    private void requeueTransfer(TransferTask task, Duration delay) {
        schedulerPool.schedule(() -> {
            if (!shutdownRequested) {
                pendingQueue.offer(task);
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void scheduleRetry(TransferTask task, Duration delay) {
        schedulerPool.schedule(() -> {
            if (!shutdownRequested) {
                task.retryCount++;
                pendingQueue.offer(task);
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private boolean isRetryableError(Exception error) {
        // Don't retry validation errors
        if (error instanceof ValidationException) {
            return false;
        }

        // Don't retry if file not found
        if (error instanceof IOException &&
                error.getMessage() != null &&
                error.getMessage().contains("not found")) {
            return false;
        }

        // Retry most other errors
        return true;
    }

    private void updateSourceStatistics(String sourceSystem, boolean success, Duration duration) {
        SourceStatistics stats = sourceStats.computeIfAbsent(
                sourceSystem, k -> new SourceStatistics(k));

        stats.totalProcessed++;
        if (success) {
            stats.successful++;
        } else {
            stats.failed++;
        }

        stats.totalProcessingTime = stats.totalProcessingTime.plus(duration);
        stats.avgProcessingTime = Duration.ofMillis(
                stats.totalProcessingTime.toMillis() / stats.totalProcessed
        );
    }

    private void waitForActiveTransfers() {
        int waitSeconds = 30;
        while (!activeTransfers.isEmpty() && waitSeconds > 0) {
            Log.infof("Waiting for %d active transfers to complete...", activeTransfers.size());
            try {
                Thread.sleep(1000);
                waitSeconds--;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        if (!activeTransfers.isEmpty()) {
            Log.warnf("Forcing shutdown with %d active transfers", activeTransfers.size());
        }
    }

    private void shutdownThreadPool(ExecutorService pool, String name) {
        try {
            pool.shutdown();
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                Log.warnf("Forcing shutdown of %s pool", name);
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
        }
    }

    private void logFinalStatistics() {
        Log.infof("Final statistics - Processed: %d, Successful: %d, Failed: %d",
                totalProcessed.get(), totalSuccessful.get(), totalFailed.get());

        sourceStats.values().forEach(stats ->
                Log.infof("Source %s - Processed: %d, Success rate: %.2f%%",
                        stats.sourceSystem, stats.totalProcessed, stats.getSuccessRate())
        );
    }

    // ============ Inner Classes ============

    /**
     * Transfer task wrapper.
     */
    private static class TransferTask {
        final Long transferId;
        final String sourceSystem;
        final int priority;
        final CompletableFuture<TransferResult> future;
        Instant startTime;
        int retryCount = 0;

        TransferTask(FileTransfer transfer) {
            this.transferId = transfer.id;
            this.sourceSystem = transfer.sourceSystem;
            this.priority = determinePriority(transfer);
            this.future = new CompletableFuture<>();
        }

        void complete(TransferResult result) {
            future.complete(result);
        }

        void completeExceptionally(Throwable error) {
            future.completeExceptionally(error);
        }

        void cancel() {
            future.cancel(true);
        }

        Duration getDuration() {
            return Duration.between(startTime, Instant.now());
        }

        private static int determinePriority(FileTransfer transfer) {
            // Priority based on status and retry count
            if (transfer.status == TransferStatus.REPROCESS_REQUESTED) {
                return 1; // Highest
            }
            if (transfer.retryCount > 0) {
                return 2;
            }
            // Could also use source system priority
            return 5; // Default
        }
    }

    /**
     * Transfer task comparator for priority queue.
     */
    private static class TransferTaskComparator implements Comparator<TransferTask> {
        @Override
        public int compare(TransferTask t1, TransferTask t2) {
            // Lower priority value = higher priority
            int priorityCompare = Integer.compare(t1.priority, t2.priority);
            if (priorityCompare != 0) {
                return priorityCompare;
            }
            // For same priority, use transfer ID (FIFO)
            return Long.compare(t1.transferId, t2.transferId);
        }
    }

    /**
     * Transfer result.
     */
    public static class TransferResult {
        public final Long transferId;
        public final TransferStatus status;
        public final String message;

        public TransferResult(Long transferId, TransferStatus status, String message) {
            this.transferId = transferId;
            this.status = status;
            this.message = message;
        }
    }

    /**
     * Source statistics.
     */
    private static class SourceStatistics {
        final String sourceSystem;
        long totalProcessed = 0;
        long successful = 0;
        long failed = 0;
        Duration totalProcessingTime = Duration.ZERO;
        Duration avgProcessingTime = Duration.ZERO;

        SourceStatistics(String sourceSystem) {
            this.sourceSystem = sourceSystem;
        }

        double getSuccessRate() {
            return totalProcessed > 0 ? (double) successful / totalProcessed * 100 : 0;
        }
    }

    /**
     * Orchestrator statistics.
     */
    public static class OrchestratorStatistics {
        public int queueSize;
        public int activeTransfers;
        public long totalProcessed;
        public long totalSuccessful;
        public long totalFailed;
        public double successRate;
        public int workerPoolSize;
        public int workerActiveCount;
        public int workerQueueSize;
        public Map<String, SourceStatistics> sourceStatistics;
        public Map<String, String> circuitBreakerStatus;
    }

    /**
     * Circuit breaker state for a source.
     */
    private static class CircuitBreakerState {
        private final String sourceSystem;
        private volatile boolean open = false;
        private volatile Instant openedAt;
        private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
        private final AtomicInteger consecutiveSuccesses = new AtomicInteger(0);

        CircuitBreakerState(String sourceSystem) {
            this.sourceSystem = sourceSystem;
        }

        void recordSuccess() {
            consecutiveSuccesses.incrementAndGet();
            consecutiveFailures.set(0);
            if (open && consecutiveSuccesses.get() >= 3) {
                close();
            }
        }

        void recordFailure() {
            consecutiveFailures.incrementAndGet();
            consecutiveSuccesses.set(0);
            if (!open && consecutiveFailures.get() >= 5) {
                open();
            }
        }

        void open() {
            open = true;
            openedAt = Instant.now();
            Log.warnf("Circuit breaker opened for source: %s", sourceSystem);
        }

        void close() {
            open = false;
            openedAt = null;
            consecutiveFailures.set(0);
            Log.infof("Circuit breaker closed for source: %s", sourceSystem);
        }

        boolean isOpen() {
            // Auto-close after 5 minutes
            if (open && openedAt != null &&
                    Duration.between(openedAt, Instant.now()).toMinutes() > 5) {
                close();
            }
            return open;
        }

        String getStatus() {
            return open ? "OPEN" : "CLOSED";
        }
    }

    /**
     * Simple rate limiter.
     */
    private static class RateLimiter {
        private final int maxPermits;
        private final Duration refillPeriod;
        private final AtomicInteger permits;
        private volatile Instant lastRefill;

        RateLimiter(int maxPermits, Duration refillPeriod) {
            this.maxPermits = maxPermits;
            this.refillPeriod = refillPeriod;
            this.permits = new AtomicInteger(maxPermits);
            this.lastRefill = Instant.now();
        }

        boolean tryAcquire() {
            refillIfNeeded();
            return permits.getAndUpdate(p -> p > 0 ? p - 1 : 0) > 0;
        }

        private void refillIfNeeded() {
            Instant now = Instant.now();
            if (Duration.between(lastRefill, now).compareTo(refillPeriod) > 0) {
                permits.set(maxPermits);
                lastRefill = now;
            }
        }
    }

    /**
     * Custom exceptions.
     */
    public static class ValidationException extends Exception {
        private final List<String> errors;

        public ValidationException(List<String> errors) {
            super("Validation failed: " + String.join(", ", errors));
            this.errors = errors;
        }

        public List<String> getErrors() {
            return errors;
        }
    }

    public static class CircuitBreakerOpenException extends Exception {
        public CircuitBreakerOpenException(String sourceSystem) {
            super("Circuit breaker is open for source: " + sourceSystem);
        }
    }
}