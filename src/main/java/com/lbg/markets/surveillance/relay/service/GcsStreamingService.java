package com.lbg.markets.surveillance.relay.service;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.api.gax.retrying.RetrySettings;
import com.lbg.markets.surveillance.relay.config.GcsConfig;
import com.lbg.markets.surveillance.relay.model.FileTransfer;
import io.quarkus.logging.Log;
import org.threeten.bp.Duration;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class GcsStreamingService {

    @Inject GcsConfig config;
    @Inject MonitoringService monitoring;

    private Storage storage;

    @PostConstruct
    void init() {
        StorageOptions.Builder builder = StorageOptions.newBuilder()
                .setProjectId(config.projectId());

        // Configure credentials if provided
        if (config.credentialsPath().isPresent()) {
            builder.setCredentialsPath(config.credentialsPath().get());
        }

        // Configure emulator for local development
        if (config.emulatorHost().isPresent()) {
            builder.setHost("http://" + config.emulatorHost().get());
        }

        // Configure retry settings
        RetrySettings retrySettings = RetrySettings.newBuilder()
                .setMaxAttempts(config.retryAttempts())
                .setInitialRetryDelay(ThreetenDuration.ofMillis(
                        config.retryInitialDelay().toMillis()))
                .setMaxRetryDelay(ThreetenDuration.ofMillis(
                        config.retryMaxDelay().toMillis()))
                .setRetryDelayMultiplier(2.0)
                .setTotalTimeout(ThreetenDuration.ofMillis(
                        config.uploadTimeout().toMillis()))
                .build();

        builder.setStorageRetryStrategy(StorageRetryStrategy.newBuilder()
                .setRetrySettings(retrySettings)
                .build());

        this.storage = builder.build().getService();

        Log.infof("GCS service initialized for bucket: %s", config.bucket());
    }

    public void streamToGcs(FileTransfer transfer) throws IOException {
        Path sourcePath = Path.of(transfer.filePath);
        String gcsPath = buildGcsPath(transfer);

        BlobId blobId = BlobId.of(config.bucket(), gcsPath);

        // Build blob info with metadata
        BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(blobId)
                .setStorageClass(StorageClass.valueOf(config.storageClass()))
                .setContentType(detectContentType(sourcePath));

        // Add metadata
        Map<String, String> metadata = new HashMap<>();
        metadata.put("source_system", transfer.sourceSystem);
        metadata.put("original_filename", transfer.filename);
        metadata.put("relay_node", config.nodeName());
        metadata.put("transfer_id", transfer.id.toString());
        metadata.put("file_hash", transfer.fileHash);

        // Add default metadata if configured
        if (config.defaultMetadata().isPresent()) {
            metadata.putAll(config.defaultMetadata().get());
        }

        blobBuilder.setMetadata(metadata);

        // Configure encryption if enabled
        if (config.encryption().isPresent() && config.encryption().get().enabled()) {
            config.encryption().get().kmsKeyName().ifPresent(blobBuilder::setKmsKeyName);
        }

        BlobInfo blobInfo = blobBuilder.build();

        // Perform the upload
        if (shouldUseParallelUpload(transfer.fileSize)) {
            parallelUpload(sourcePath, blobInfo, transfer);
        } else if (config.resumableUpload()) {
            resumableUpload(sourcePath, blobInfo, transfer);
        } else {
            simpleUpload(sourcePath, blobInfo, transfer);
        }

        // Send notification if configured
        sendUploadNotification(transfer, true);
    }

    private void resumableUpload(Path sourcePath, BlobInfo blobInfo,
                                 FileTransfer transfer) throws IOException {

        try (WriteChannel writer = storage.writer(blobInfo);
             ReadableByteChannel reader = Files.newByteChannel(sourcePath)) {

            writer.setChunkSize(config.resumableChunkSize());

            ByteBuffer buffer = ByteBuffer.allocate(config.uploadBufferSize());
            long totalBytes = 0;
            long lastProgress = 0;

            while (reader.read(buffer) != -1) {
                buffer.flip();
                writer.write(buffer);
                totalBytes += buffer.position();
                buffer.clear();

                // Report progress
                if (totalBytes - lastProgress > 100 * 1024 * 1024) {
                    monitoring.recordProgress(transfer.id, totalBytes, transfer.fileSize);
                    lastProgress = totalBytes;
                }
            }

            transfer.gcsPath = blobInfo.getName();

            Log.infof("Uploaded %s to GCS: %d bytes", transfer.filename, totalBytes);
        }
    }

    private boolean shouldUseParallelUpload(Long fileSize) {
        return config.parallel().isPresent()
                && config.parallel().get().enabled()
                && fileSize != null
                && fileSize > config.parallel().get().minFileSize();
    }

    private String detectContentType(Path path) {
        try {
            String contentType = Files.probeContentType(path);
            return contentType != null ? contentType : "application/octet-stream";
        } catch (IOException e) {
            return "application/octet-stream";
        }
    }

    private void sendUploadNotification(FileTransfer transfer, boolean success) {
        config.notifications().ifPresent(notif -> {
            if ((success && notif.onSuccess()) || (!success && notif.onFailure())) {
                // Implementation would send to Pub/Sub
                Log.infof("Notification sent for transfer %d: success=%b",
                        transfer.id, success);
            }
        });
    }

    // Additional helper methods...
}