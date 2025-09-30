package com.lbg.markets.surveillance.relay.service;

import com.lbg.markets.surveillance.relay.enums.TransferStatus;
import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.repository.FileTransferRepository;
import com.lbg.markets.surveillance.relay.service.metrics.MetricsService;
import com.lbg.markets.surveillance.relay.service.storage.StorageService;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class TransferService {

    @Inject
    FileTransferRepository repository;

    @Inject
    StorageService storage;

    @Inject
    MetricsService metrics;

    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    @Scheduled(every = "30s")
    void processPendingTransfers() {
        List<FileTransfer> pending = repository.findPendingTransfers(20);

        for (FileTransfer transfer : pending) {
            if (repository.tryAcquireLock(transfer.id, transfer.rowVersion)) {
                CompletableFuture.runAsync(() -> processTransfer(transfer), executor)
                        .exceptionally(error -> {
                            handleError(transfer, error);
                            return null;
                        });
            }
        }
    }

    @Scheduled(every = "5m")
    void checkStuckTransfers() {
        Instant cutoff = Instant.now().minus(Duration.ofMinutes(30));
        List<FileTransfer> stuck = repository.findStuckTransfers(cutoff);

        for (FileTransfer transfer : stuck) {
            Log.warnf("Transfer stuck: %d", transfer.id);
            resetTransfer(transfer);
        }
    }

    @Transactional
    public void processTransfer(FileTransfer transfer) {
        try {
            Path sourcePath = Paths.get(transfer.filePath);

            // Validate file exists
            if (!Files.exists(sourcePath)) {
                throw new IllegalStateException("File not found: " + transfer.filePath);
            }

            // Upload to storage
            storage.uploadFile(transfer, sourcePath);

            // Update status
            transfer.status = TransferStatus.COMPLETED;
            transfer.completedAt = Instant.now();
            repository.persist(transfer);

            metrics.recordEvent("transfer_completed", Map.of(
                    "id", transfer.id,
                    "source", transfer.sourceSystem,
                    "duration", Duration.between(transfer.startedAt, transfer.completedAt).toMillis()
            ));

            Log.infof("Transfer completed: %d", transfer.id);

        } catch (Exception e) {
            throw new RuntimeException("Transfer failed: " + transfer.id, e);
        }
    }

    @Transactional
    void handleError(FileTransfer transfer, Throwable error) {
        transfer.status = TransferStatus.FAILED;
        transfer.errorMessage = error.getMessage();
        transfer.completedAt = Instant.now();
        transfer.retryCount++;

        repository.persist(transfer);

        metrics.recordError("transfer_failed", transfer.sourceSystem, error.getMessage());
        Log.errorf(error, "Transfer failed: %d", transfer.id);
    }

    @Transactional
    void resetTransfer(FileTransfer transfer) {
        transfer.status = TransferStatus.DETECTED;
        transfer.startedAt = null;
        repository.persist(transfer);
    }
}