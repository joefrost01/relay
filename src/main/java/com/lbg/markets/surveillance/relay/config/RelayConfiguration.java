package com.lbg.markets.surveillance.relay.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.time.Duration;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "relay")
public interface RelayConfiguration {

    @WithName("node-name")
    String nodeName();

    @WithName("sources")
    List<SourceSystem> sources();

    @WithName("gcs")
    GcsConfig gcs();

    @WithName("monitoring")
    MonitoringConfig monitoring();

    @WithName("orchestrator")
    Optional<OrchestratorConfig> orchestrator();

    @WithName("notifications")
    Optional<NotificationConfig> notifications();

    interface SourceSystem {
        String id();

        @WithName("display-name")
        Optional<String> displayName();

        String path();

        @WithName("fallback-paths")
        Optional<List<String>> fallbackPaths();

        @WithName("file-pattern")
        @WithDefault("*")
        String filePattern();

        @WithName("exclude-patterns")
        Optional<List<String>> excludePatterns();

        @WithName("ready-strategy")
        @WithDefault("FILE_AGE")
        FileReadyStrategy readyStrategy();

        @WithName("done-file-suffix")
        Optional<String> doneFileSuffix();

        @WithName("stability-period")
        @WithDefault("PT5M")
        Duration stabilityPeriod();

        @WithName("scheduled-time")
        Optional<LocalTime> scheduledTime();

        @WithName("poll-interval")
        @WithDefault("PT60S")
        Duration pollInterval();

        @WithName("enabled")
        @WithDefault("true")
        boolean enabled();

        @WithName("recursive")
        @WithDefault("false")
        boolean recursive();

        @WithName("max-depth")
        @WithDefault("5")
        int maxDepth();

        @WithName("priority")
        @WithDefault("5")
        int priority();

        @WithName("processing")
        Optional<ProcessingConfig> processing();

        @WithName("validation")
        Optional<ValidationConfig> validation();

        @WithName("archive")
        Optional<ArchiveConfig> archive();

        @WithName("sla")
        Optional<SlaConfig> sla();

        @WithName("notifications")
        Optional<NotificationConfig> notifications();
    }

    enum FileReadyStrategy {
        DONE_FILE,
        FILE_AGE,
        IMMEDIATE,
        SCHEDULED,
        CUSTOM,
        NOT_LOCKED
    }

    interface GcsConfig {
        String bucket();

        @WithName("project-id")
        String projectId();

        @WithName("upload-buffer-size")
        @WithDefault("8388608")
        int uploadBufferSize();

        @WithName("upload-timeout")
        @WithDefault("PT30M")
        Duration uploadTimeout();

        @WithName("node-name")
        Optional<String> nodeName();

        @WithName("credentials-path")
        Optional<String> credentialsPath();

        @WithName("emulator-host")
        Optional<String> emulatorHost();

        @WithName("storage-class")
        @WithDefault("STANDARD")
        String storageClass();

        @WithName("resumable-upload")
        @WithDefault("true")
        boolean resumableUpload();

        @WithName("resumable-chunk-size")
        @WithDefault("16777216")
        int resumableChunkSize();

        @WithName("retry-attempts")
        @WithDefault("3")
        int retryAttempts();

        @WithName("retry-initial-delay")
        @WithDefault("PT1S")
        Duration retryInitialDelay();

        @WithName("retry-max-delay")
        @WithDefault("PT32S")
        Duration retryMaxDelay();

        @WithName("validate-checksums")
        @WithDefault("true")
        boolean validateChecksums();

        @WithName("default-metadata")
        Optional<Map<String, String>> defaultMetadata();

        @WithName("encryption")
        Optional<EncryptionConfig> encryption();

        @WithName("parallel")
        Optional<ParallelConfig> parallel();

        @WithName("notifications")
        Optional<GcsNotificationConfig> notifications();
    }

    interface MonitoringConfig {
        @WithName("big-query-dataset")
        String bigQueryDataset();

        @WithName("events-table")
        String eventsTable();

        @WithName("reprocess-table")
        String reprocessTable();

        @WithName("heartbeat-interval")
        @WithDefault("PT60S")
        Duration heartbeatInterval();

        @WithName("enabled")
        @WithDefault("true")
        boolean enabled();

        @WithName("metrics-interval-seconds")
        @WithDefault("60")
        int metricsIntervalSeconds();

        @WithName("buffer-size")
        @WithDefault("10000")
        int bufferSize();

        @WithName("batch-size")
        @WithDefault("500")
        int batchSize();

        @WithName("retention-days")
        @WithDefault("90")
        int retentionDays();
    }

    interface ProcessingConfig {
        @WithName("priority")
        @WithDefault("5")
        int priority();

        @WithName("max-concurrent")
        @WithDefault("3")
        int maxConcurrent();

        @WithName("max-file-size")
        @WithDefault("-1")
        long maxFileSize();

        @WithName("min-file-size")
        @WithDefault("0")
        long minFileSize();

        @WithName("transform")
        Optional<TransformConfig> transform();

        @WithName("file-age")
        Optional<FileAgeConfig> fileAge();
    }

    interface ValidationConfig {
        @WithName("enabled")
        @WithDefault("true")
        boolean enabled();

        @WithName("checksum")
        Optional<ChecksumConfig> checksum();

        @WithName("format")
        Optional<FormatConfig> format();

        @WithName("content")
        Optional<ContentConfig> content();
    }

    interface TransformConfig {
        @WithName("type")
        TransformType type();

        @WithName("compression")
        Optional<String> compression();

        @WithName("encryption")
        Optional<EncryptionConfig> encryption();

        enum TransformType {
            NONE,
            COMPRESS,
            ENCRYPT,
            COMPRESS_AND_ENCRYPT
        }
    }

    interface ArchiveConfig {
        @WithName("enabled")
        @WithDefault("false")
        boolean enabled();

        @WithName("path")
        String path();

        @WithName("strategy")
        @WithDefault("MOVE")
        ArchiveStrategy strategy();

        @WithName("retention")
        @WithDefault("P90D")
        Duration retention();

        @WithName("structure")
        @WithDefault("YYYY/MM/DD")
        String structure();

        enum ArchiveStrategy {
            MOVE,
            COPY,
            COPY_THEN_DELETE
        }
    }

    interface SlaConfig {
        @WithName("grace-period")
        @WithDefault("PT30M")
        Duration gracePeriod();

        @WithName("expected-time")
        Optional<LocalTime> expectedTime();

        @WithName("max-processing-time")
        @WithDefault("PT10M")
        Duration maxProcessingTime();
    }

    interface NotificationConfig {
        @WithName("email")
        Optional<EmailConfig> email();

        @WithName("slack")
        Optional<SlackConfig> slack();

        @WithName("pubsub-topic")
        Optional<String> pubsubTopic();
    }

    interface OrchestratorConfig {
        @WithName("enabled")
        @WithDefault("true")
        boolean enabled();

        @WithName("worker-threads")
        @WithDefault("10")
        int workerThreads();

        @WithName("priority-threads")
        @WithDefault("3")
        int priorityThreads();

        @WithName("queue-capacity")
        @WithDefault("1000")
        int queueCapacity();

        @WithName("batch-size")
        @WithDefault("20")
        int batchSize();

        @WithName("max-concurrent-per-source")
        @WithDefault("5")
        int maxConcurrentPerSource();

        @WithName("processing-timeout-minutes")
        @WithDefault("30")
        int processingTimeoutMinutes();

        @WithName("max-retries")
        @WithDefault("3")
        int maxRetries();

        @WithName("retry-delay-minutes")
        @WithDefault("5")
        int retryDelayMinutes();
    }

    interface EmailConfig {
        @WithName("recipients")
        List<String> recipients();

        @WithName("on-success")
        @WithDefault("false")
        boolean onSuccess();

        @WithName("on-failure")
        @WithDefault("true")
        boolean onFailure();

        @WithName("daily-summary")
        @WithDefault("true")
        boolean dailySummary();
    }

    interface SlackConfig {
        @WithName("webhook-url")
        String webhookUrl();

        @WithName("channel")
        String channel();

        @WithName("on-success")
        @WithDefault("false")
        boolean onSuccess();

        @WithName("on-failure")
        @WithDefault("true")
        boolean onFailure();
    }

    interface ChecksumConfig {
        @WithName("algorithm")
        @WithDefault("SHA256")
        String algorithm();

        @WithName("source")
        ChecksumSource source();

        @WithName("file-suffix")
        @WithDefault(".sha256")
        String fileSuffix();

        enum ChecksumSource {
            SEPARATE_FILE,
            FILENAME,
            FILE_HEADER,
            MANIFEST
        }
    }

    interface FormatConfig {
        @WithName("type")
        String type();

        @WithName("schema")
        Optional<String> schema();

        @WithName("csv")
        Optional<CsvConfig> csv();
    }

    interface CsvConfig {
        @WithName("delimiter")
        @WithDefault(",")
        String delimiter();

        @WithName("has-headers")
        @WithDefault("true")
        boolean hasHeaders();

        @WithName("expected-headers")
        Optional<List<String>> expectedHeaders();

        @WithName("column-count")
        Optional<Integer> columnCount();
    }

    interface ContentConfig {
        @WithName("must-contain")
        Optional<List<String>> mustContain();

        @WithName("must-not-contain")
        Optional<List<String>> mustNotContain();

        @WithName("row-count")
        Optional<RowCountConfig> rowCount();
    }

    interface RowCountConfig {
        @WithName("min")
        Optional<Integer> min();

        @WithName("max")
        Optional<Integer> max();
    }

    interface FileAgeConfig {
        @WithName("min-age")
        Optional<Duration> minAge();

        @WithName("max-age")
        Optional<Duration> maxAge();
    }

    interface EncryptionConfig {
        @WithName("enabled")
        @WithDefault("false")
        boolean enabled();

        @WithName("kms-key-name")
        Optional<String> kmsKeyName();

        @WithName("algorithm")
        @WithDefault("AES-256-GCM")
        String algorithm();
    }

    interface ParallelConfig {
        @WithName("enabled")
        @WithDefault("false")
        boolean enabled();

        @WithName("min-file-size")
        @WithDefault("104857600")
        long minFileSize();

        @WithName("thread-count")
        @WithDefault("4")
        int threadCount();

        @WithName("part-size")
        @WithDefault("33554432")
        int partSize();
    }

    interface GcsNotificationConfig {
        @WithName("pubsub-topic")
        Optional<String> pubsubTopic();

        @WithName("on-success")
        @WithDefault("true")
        boolean onSuccess();

        @WithName("on-failure")
        @WithDefault("true")
        boolean onFailure();
    }
}