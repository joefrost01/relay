package com.lbg.markets.surveillance.relay.service;

import com.lbg.markets.surveillance.relay.config.RelayConfiguration;
import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.model.TransferStatus;
import com.lbg.markets.surveillance.relay.repository.FileTransferRepository;
import io.quarkus.logging.Log;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;

@ApplicationScoped
public class FileDetectionService {

    @Inject RelayConfiguration config;
    @Inject FileTransferRepository repository;
    @Inject MonitoringService monitoring;

    @ConfigProperty(name = "relay.node-name")
    String nodeName;

    public void scanSourceSystem(RelayConfiguration.SourceSystem source) {
        try {
            Path sourcePath = Paths.get(source.path());

            if (!Files.exists(sourcePath)) {
                Log.warnf("Source path does not exist: %s", source.path());
                monitoring.recordEvent("source_unavailable", Map.of(
                        "source", source.id(),
                        "path", source.path()
                ));
                return;
            }

            Files.walk(sourcePath, 1)
                    .filter(Files::isRegularFile)
                    .filter(path -> matchesPattern(path, source.filePattern()))
                    .filter(path -> isFileReady(path, source))
                    .forEach(path -> registerFileIfNew(path, source));

        } catch (IOException e) {
            Log.errorf(e, "Failed to scan source system: %s", source.id());
            monitoring.recordError("scan_failed", source.id(), e.getMessage());
        }
    }

    private boolean matchesPattern(Path path, String pattern) {
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
        return matcher.matches(path.getFileName());
    }

    private boolean isFileReady(Path file, RelayConfiguration.SourceSystem source) {
        return switch (source.readyStrategy()) {
            case DONE_FILE -> Files.exists(
                    Paths.get(file.toString() + source.doneFileSuffix().orElse(".done"))
            );
            case FILE_AGE -> isFileStable(file, Duration.ofMinutes(5));
            case IMMEDIATE -> true;
            case SCHEDULED -> LocalTime.now().isAfter(LocalTime.of(6, 0)); // 6 AM
        };
    }

    private boolean isFileStable(Path file, Duration stability) {
        try {
            FileTime lastModified = Files.getLastModifiedTime(file);
            return Duration.between(lastModified.toInstant(), Instant.now())
                    .compareTo(stability) > 0;
        } catch (IOException e) {
            return false;
        }
    }

    @Transactional
    protected void registerFileIfNew(Path file, RelayConfiguration.SourceSystem source) {
        String filename = file.getFileName().toString();

        try {
            String fileHash = calculateHash(file);
            Long fileSize = Files.size(file);

            FileTransfer transfer = repository.registerFileIfNew(
                    source.id(),
                    filename,
                    file.toString(),
                    fileSize,
                    fileHash
            );

            if (transfer != null) {
                Log.infof("Registered new file: %s from %s", filename, source.id());
                monitoring.recordEvent("file_detected", Map.of(
                        "source", source.id(),
                        "filename", filename,
                        "size", fileSize,
                        "hash", fileHash
                ));
            }

        } catch (IOException e) {
            Log.errorf(e, "Failed to register file: %s", filename);
            monitoring.recordError("registration_failed", source.id(), e.getMessage());
        }
    }

    private String calculateHash(Path file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[1024 * 1024];

            raf.read(buffer);
            md.update(buffer);

            if (raf.length() > buffer.length) {
                raf.seek(raf.length() - buffer.length);
                raf.read(buffer);
                md.update(buffer);
            }

            md.update(ByteBuffer.allocate(8).putLong(raf.length()).array());
            return HexFormat.of().formatHex(md.digest());
        } catch (Exception e) {
            return UUID.randomUUID().toString();
        }
    }
}