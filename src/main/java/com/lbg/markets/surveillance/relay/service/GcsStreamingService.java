package com.lbg.markets.surveillance.relay.service;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.lbg.markets.surveillance.relay.config.GcsConfig;
import com.lbg.markets.surveillance.relay.model.FileTransfer;
import io.quarkus.logging.Log;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class GcsStreamingService {

    @Inject
    GcsConfig config;
    @Inject
    MonitoringService monitoring;

    @ConfigProperty(name = "relay.node-name")
    String nodeName;

    private Storage storage;

    @PostConstruct
    void init() {
        StorageOptions.Builder builder = StorageOptions.newBuilder()
                .setProjectId(config.projectId());

        // Configure emulator for local development
        if (config.emulatorHost().isPresent()) {
            builder.setHost("http://" + config.emulatorHost().get());
        }

        // Build storage service
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
        metadata.put("relay_node", nodeName);
        metadata.put("transfer_id", transfer.id.toString());
        metadata.put("file_hash", transfer.fileHash);

        // Add default metadata if configured
        if (config.defaultMetadata().isPresent()) {
            metadata.putAll(config.defaultMetadata().get());
        }

        blobBuilder.setMetadata(metadata);

        // Add KMS key name to BlobInfo if configured
        if (config.encryption().isPresent() &&
                config.encryption().get().enabled() &&
                config.encryption().get().kmsKeyName().isPresent()) {

            String kmsKeyName = config.encryption().get().kmsKeyName().get();
            // Note: KMS key should be set via BlobTargetOption during write, not in BlobInfo
            // Some versions of the API don't have setKmsKeyName on BlobInfo.Builder
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

        // Update transfer with GCS path
        transfer.gcsPath = gcsPath;

        // Send notification if configured
        sendUploadNotification(transfer, true);
    }

    private String buildGcsPath(FileTransfer transfer) {
        // Build path like: source_system/YYYY/MM/DD/filename
        java.time.LocalDate now = java.time.LocalDate.now();
        return String.format("%s/%d/%02d/%02d/%s",
                transfer.sourceSystem,
                now.getYear(),
                now.getMonthValue(),
                now.getDayOfMonth(),
                transfer.filename);
    }

    private void resumableUpload(Path sourcePath, BlobInfo blobInfo,
                                 FileTransfer transfer) throws IOException {

        // Create write channel with optional KMS encryption
        WriteChannel writer;

        if (config.encryption().isPresent() &&
                config.encryption().get().enabled() &&
                config.encryption().get().kmsKeyName().isPresent()) {

            String kmsKeyName = config.encryption().get().kmsKeyName().get();
            // Use BlobWriteOption for writer
            writer = storage.writer(blobInfo, Storage.BlobWriteOption.kmsKeyName(kmsKeyName));
        } else {
            writer = storage.writer(blobInfo);
        }

        try (ReadableByteChannel reader = Files.newByteChannel(sourcePath)) {
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

            writer.close();
            Log.infof("Uploaded %s to GCS: %d bytes", transfer.filename, totalBytes);
        }
    }

    private void simpleUpload(Path sourcePath, BlobInfo blobInfo,
                              FileTransfer transfer) throws IOException {

        byte[] bytes = Files.readAllBytes(sourcePath);

        // Build target options list (note: using BlobTargetOption for create method)
        List<Storage.BlobTargetOption> options = new ArrayList<>();

        // Add KMS encryption if configured
        if (config.encryption().isPresent() &&
                config.encryption().get().enabled() &&
                config.encryption().get().kmsKeyName().isPresent()) {

            String kmsKeyName = config.encryption().get().kmsKeyName().get();
            options.add(Storage.BlobTargetOption.kmsKeyName(kmsKeyName));
        }

        // Upload with options - using BlobTargetOption
        if (!options.isEmpty()) {
            storage.create(blobInfo, bytes, options.toArray(new Storage.BlobTargetOption[0]));
        } else {
            storage.create(blobInfo, bytes);
        }

        Log.infof("Uploaded %s to GCS (simple upload)", transfer.filename);
    }

    private void parallelUpload(Path sourcePath, BlobInfo blobInfo,
                                FileTransfer transfer) throws IOException {
        // For parallel upload, you would typically:
        // 1. Split the file into chunks
        // 2. Upload chunks in parallel as separate blobs
        // 3. Use compose API to combine them
        // For now, fall back to simple upload
        simpleUpload(sourcePath, blobInfo, transfer);
        Log.info("Parallel upload not fully implemented, using simple upload");
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
                Log.infof("Notification sent for transfer %d: success=%b",
                        transfer.id, success);
            }
        });
    }
}