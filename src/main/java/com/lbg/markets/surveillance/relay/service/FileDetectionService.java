package com.lbg.markets.surveillance.relay.service;

import com.lbg.markets.surveillance.relay.config.RelayConfiguration;
import com.lbg.markets.surveillance.relay.repository.FileTransferRepository;
import com.lbg.markets.surveillance.relay.repository.SourceSystemRepository;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;

@ApplicationScoped
public class FileDetectionService {

    @Inject
    RelayConfiguration config;  // Use RelayConfiguration instead of SourceConfig

    @Inject
    FileTransferRepository transferRepository;

    @Inject
    SourceSystemRepository sourceRepository;

    @Scheduled(every = "30s")
    void scanSources() {
        config.sources().stream()
                .filter(RelayConfiguration.SourceSystem::enabled)
                .forEach(this::scanSource);
    }

    // Keep this method for compatibility with existing code
    public void scanSourceSystem(RelayConfiguration.SourceSystem source) {
        scanSource(source);
    }

    private void scanSource(RelayConfiguration.SourceSystem source) {
        try {
            Path sourcePath = Paths.get(source.path());
            if (!Files.exists(sourcePath)) {
                Log.warnf("Source path not found: %s", source.path());
                return;
            }

            scanPath(source, sourcePath);

        } catch (Exception e) {
            Log.errorf(e, "Failed to scan source: %s", source.id());
        }
    }

    private void scanPath(RelayConfiguration.SourceSystem source, Path path) throws IOException {
        PathMatcher matcher = FileSystems.getDefault()
                .getPathMatcher("glob:" + source.filePattern());

        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (matcher.matches(file.getFileName())) {
                    if (isFileReady(file, source)) {
                        registerFile(source.id(), file);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private boolean isFileReady(Path file, RelayConfiguration.SourceSystem source) {
        return switch (source.readyStrategy()) {
            case IMMEDIATE -> true;
            case DONE_FILE -> {
                String doneFileSuffix = source.doneFileSuffix().orElse(".done");
                yield Files.exists(Paths.get(file + doneFileSuffix));
            }
            case FILE_AGE -> {
                try {
                    var attrs = Files.readAttributes(file, BasicFileAttributes.class);
                    var age = Duration.between(attrs.lastModifiedTime().toInstant(), Instant.now());
                    yield age.compareTo(source.stabilityPeriod()) > 0;
                } catch (IOException e) {
                    yield false;
                }
            }
            case SCHEDULED -> {
                // Check if within scheduled window
                if (source.scheduledTime().isPresent()) {
                    var now = java.time.LocalTime.now();
                    var scheduled = source.scheduledTime().get();
                    var windowEnd = scheduled.plusMinutes(30);
                    yield now.isAfter(scheduled) && now.isBefore(windowEnd);
                }
                yield false;
            }
            case NOT_LOCKED -> {
                // Try to check if file is locked
                try {
                    // Try to open file for writing to check if locked
                    try (var channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
                        yield true;
                    }
                } catch (IOException e) {
                    // File is locked
                    yield false;
                }
            }
            case CUSTOM -> {
                // Custom logic - default to true for now
                yield true;
            }
            default -> false;
        };
    }

    @Transactional
    void registerFile(String sourceId, Path file) {
        String filename = file.getFileName().toString();

        // Check if already registered
        if (transferRepository.findBySourceAndFilename(sourceId, filename).isPresent()) {
            return;
        }

        try {
            long size = Files.size(file);
            String hash = calculateHash(file);

            transferRepository.createTransfer(
                    sourceId,
                    filename,
                    file.toString(),
                    size,
                    hash
            );

            Log.infof("Registered file: %s from %s", filename, sourceId);

        } catch (IOException e) {
            Log.errorf(e, "Failed to register file: %s", filename);
        }
    }

    public String calculateHash(Path file) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] bytes = Files.readAllBytes(file);
            return HexFormat.of().formatHex(md.digest(bytes));
        } catch (Exception e) {
            return file.getFileName().toString(); // Fallback to filename
        }
    }
}