package com.lbg.markets.surveillance.relay.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Google Cloud Storage configuration for the Relay service.
 * Handles both production GCS and local development with emulators.
 */
@ConfigMapping(prefix = "relay.gcs")
public interface GcsConfig {

    /**
     * The GCS bucket name where files will be uploaded.
     * In dev mode, this could be a local directory name.
     */
    @WithName("bucket")
    String bucket();

    /**
     * The GCP project ID.
     */
    @WithName("project-id")
    String projectId();

    /**
     * Buffer size for streaming uploads (in bytes).
     * Default: 8MB for optimal performance with large files.
     */
    @WithName("upload-buffer-size")
    @WithDefault("8388608")
    int uploadBufferSize();

    /**
     * Timeout for upload operations.
     * Default: 30 minutes for large files.
     */
    @WithName("upload-timeout")
    @WithDefault("PT30M")
    Duration uploadTimeout();

    /**
     * Number of retry attempts for failed uploads.
     * Default: 3 attempts with exponential backoff.
     */
    @WithName("retry-attempts")
    @WithDefault("3")
    int retryAttempts();

    /**
     * Initial retry delay for failed uploads.
     * Default: 1 second, will be exponentially increased.
     */
    @WithName("retry-initial-delay")
    @WithDefault("PT1S")
    Duration retryInitialDelay();

    /**
     * Maximum retry delay for failed uploads.
     * Default: 32 seconds.
     */
    @WithName("retry-max-delay")
    @WithDefault("PT32S")
    Duration retryMaxDelay();

    /**
     * Storage class for uploaded objects.
     * Options: STANDARD, NEARLINE, COLDLINE, ARCHIVE
     * Default: STANDARD for immediate access.
     */
    @WithName("storage-class")
    @WithDefault("STANDARD")
    String storageClass();

    /**
     * Whether to use resumable uploads.
     * Default: true for reliability with large files.
     */
    @WithName("resumable-upload")
    @WithDefault("true")
    boolean resumableUpload();

    /**
     * Chunk size for resumable uploads (in bytes).
     * Must be a multiple of 256KB (262144).
     * Default: 16MB.
     */
    @WithName("resumable-chunk-size")
    @WithDefault("16777216")
    int resumableChunkSize();

    /**
     * Service account key file path for authentication.
     * Optional - if not provided, uses Application Default Credentials.
     */
    @WithName("credentials-path")
    Optional<String> credentialsPath();

    /**
     * Custom endpoint for GCS API.
     * Used for local emulators like fake-gcs-server.
     */
    @WithName("emulator-host")
    Optional<String> emulatorHost();

    /**
     * Whether to enable GCS request/response logging.
     * Default: false in production, true in dev.
     */
    @WithName("enable-logging")
    @WithDefault("false")
    boolean enableLogging();

    /**
     * Whether to validate checksums after upload.
     * Default: true for data integrity.
     */
    @WithName("validate-checksums")
    @WithDefault("true")
    boolean validateChecksums();

    /**
     * Custom metadata to add to all uploaded objects.
     * Can include tags for cost tracking, environment, etc.
     */
    @WithName("default-metadata")
    Optional<Map<String, String>> defaultMetadata();

    /**
     * Encryption configuration for uploaded objects.
     */
    @WithName("encryption")
    Optional<EncryptionConfig> encryption();

    /**
     * Lifecycle configuration for uploaded objects.
     */
    @WithName("lifecycle")
    Optional<LifecycleConfig> lifecycle();

    /**
     * Parallel upload configuration for performance optimization.
     */
    @WithName("parallel")
    Optional<ParallelConfig> parallel();

    /**
     * Encryption configuration for GCS objects.
     */
    interface EncryptionConfig {
        /**
         * Whether to use customer-managed encryption keys (CMEK).
         */
        @WithName("enabled")
        @WithDefault("false")
        boolean enabled();

        /**
         * The Cloud KMS key name for CMEK.
         * Format: projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{cryptoKey}
         */
        @WithName("kms-key-name")
        Optional<String> kmsKeyName();

        /**
         * Whether to use client-side encryption before upload.
         */
        @WithName("client-side")
        @WithDefault("false")
        boolean clientSide();
    }

    /**
     * Lifecycle configuration for automatic object management.
     */
    interface LifecycleConfig {
        /**
         * Number of days before moving to nearline storage.
         * Default: -1 (disabled).
         */
        @WithName("nearline-after-days")
        @WithDefault("-1")
        int nearlineAfterDays();

        /**
         * Number of days before moving to coldline storage.
         * Default: -1 (disabled).
         */
        @WithName("coldline-after-days")
        @WithDefault("-1")
        int coldlineAfterDays();

        /**
         * Number of days before moving to archive storage.
         * Default: -1 (disabled).
         */
        @WithName("archive-after-days")
        @WithDefault("-1")
        int archiveAfterDays();

        /**
         * Number of days before deleting objects.
         * Default: -1 (disabled).
         */
        @WithName("delete-after-days")
        @WithDefault("-1")
        int deleteAfterDays();
    }

    /**
     * Parallel upload configuration for large files.
     */
    interface ParallelConfig {
        /**
         * Whether to enable parallel composite uploads for large files.
         * Default: false.
         */
        @WithName("enabled")
        @WithDefault("false")
        boolean enabled();

        /**
         * Minimum file size to trigger parallel upload (in bytes).
         * Default: 100MB.
         */
        @WithName("min-file-size")
        @WithDefault("104857600")
        long minFileSize();

        /**
         * Number of parallel upload threads.
         * Default: 4.
         */
        @WithName("thread-count")
        @WithDefault("4")
        int threadCount();

        /**
         * Size of each part in parallel upload (in bytes).
         * Default: 32MB.
         */
        @WithName("part-size")
        @WithDefault("33554432")
        int partSize();
    }

    /**
     * Notification configuration for upload events.
     */
    @WithName("notifications")
    Optional<NotificationConfig> notifications();

    interface NotificationConfig {
        /**
         * Pub/Sub topic for upload notifications.
         */
        @WithName("pubsub-topic")
        Optional<String> pubsubTopic();

        /**
         * Whether to send notifications on successful uploads.
         */
        @WithName("on-success")
        @WithDefault("true")
        boolean onSuccess();

        /**
         * Whether to send notifications on failed uploads.
         */
        @WithName("on-failure")
        @WithDefault("true")
        boolean onFailure();
    }

    /**
     * Performance tuning configuration.
     */
    @WithName("performance")
    Optional<PerformanceConfig> performance();

    interface PerformanceConfig {
        /**
         * Connection pool size for GCS client.
         * Default: 20.
         */
        @WithName("connection-pool-size")
        @WithDefault("20")
        int connectionPoolSize();

        /**
         * Connection timeout.
         * Default: 20 seconds.
         */
        @WithName("connection-timeout")
        @WithDefault("PT20S")
        Duration connectionTimeout();

        /**
         * Read timeout for GCS operations.
         * Default: 60 seconds.
         */
        @WithName("read-timeout")
        @WithDefault("PT60S")
        Duration readTimeout();

        /**
         * Write timeout for GCS operations.
         * Default: 60 seconds.
         */
        @WithName("write-timeout")
        @WithDefault("PT60S")
        Duration writeTimeout();

        /**
         * Whether to use HTTP/2 for better performance.
         * Default: true.
         */
        @WithName("use-http2")
        @WithDefault("true")
        boolean useHttp2();

        /**
         * Node name for identifying which instance uploaded the file.
         * Defaults to hostname or configured node name.
         */
        @WithName("node-name")
        Optional<String> nodeName();
    }
}