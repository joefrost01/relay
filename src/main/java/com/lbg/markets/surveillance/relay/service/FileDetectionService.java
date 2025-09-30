package com.lbg.markets.surveillance.relay.service;

import com.lbg.markets.surveillance.relay.config.SourceConfig;
import com.lbg.markets.surveillance.relay.repository.FileTransferRepository;
import com.lbg.markets.surveillance.relay.repository.SourceSystemRepository;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;

@ApplicationScoped
public class FileDetectionService {

    @Inject
    SourceConfig config;

    @Inject
    FileTransferRepository transferRepository;

    @Inject
    SourceSystemRepository sourceRepository;

    @Scheduled(every = "30s")
    void scanSources() {
        config.sources().stream()  // FIXED - use sources() not getSources()
                .filter(source -> source.enabled())  // FIXED
                .forEach(this::scanSource);
    }

    // Add this method for compatibility with existing code
    public void scanSourceSystem(com.lbg.markets.surveillance.relay.config.RelayConfiguration.SourceSystem source) {
        // Convert to simple source scan
        try {
            Path sourcePath = Paths.get(source.path());
            if (Files.exists(sourcePath)) {
                scanPath(source.id(), sourcePath, source.filePattern());
            }
        } catch (Exception e) {
            Log.errorf(e, "Failed to scan source system: %s", source.id());
        }
    }

    private void scanSource(SourceConfig.Source source) {
        try {
            Path sourcePath = Paths.get(source.path());
            if (!Files.exists(sourcePath)) {
                Log.warnf("Source path not found: %s", source.path());
                return;
            }

            scanPath(source.id(), sourcePath, source.filePattern());

        } catch (Exception e) {
            Log.errorf(e, "Failed to scan source: %s", source.id());
        }
    }

    private void scanPath(String sourceId, Path path, String pattern) throws IOException {
        PathMatcher matcher = FileSystems.getDefault()
                .getPathMatcher("glob:" + pattern);

        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (matcher.matches(file.getFileName())) {
                    registerFile(sourceId, file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private boolean isFileReady(Path file, SourceConfig.Source source) {
        return switch (source.readyStrategy()) {
            case "IMMEDIATE" -> true;
            case "DONE_FILE" -> Files.exists(Paths.get(file + ".done"));
            case "FILE_AGE" -> {
                try {
                    var attrs = Files.readAttributes(file, BasicFileAttributes.class);
                    var age = Duration.between(attrs.lastModifiedTime().toInstant(), Instant.now());
                    yield age.compareTo(source.stabilityPeriod()) > 0;
                } catch (IOException e) {
                    yield false;
                }
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

    public String calculateHash(Path file) throws IOException {  // CHANGED to public
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] bytes = Files.readAllBytes(file);
            return HexFormat.of().formatHex(md.digest(bytes));
        } catch (Exception e) {
            return file.getFileName().toString(); // Fallback to filename
        }
    }
}