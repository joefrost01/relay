package com.lbg.markets.surveillance.relay.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.Map;

/**
 * Configuration for individual source systems that provide files to relay.
 * Each source system can have different file detection strategies, patterns,
 * and processing rules.
 */
@ConfigMapping(prefix = "relay.sources")
public interface SourceSystemConfig {

    /**
     * List of all configured source systems.
     */
    List<SourceSystem> systems();

    /**
     * Global defaults that apply to all source systems unless overridden.
     */
    @WithName("defaults")
    Optional<SourceDefaults> defaults();

    /**
     * Configuration for an individual source system.
     */
    interface SourceSystem {

        /**
         * Unique identifier for this source system.
         * Used in logging, monitoring, and database records.
         */
        @WithName("id")
        String id();

        /**
         * Display name for this source system.
         * Used in UI and reports.
         */
        @WithName("display-name")
        Optional<String> displayName();

        /**
         * File system path to monitor.
         * Can be a local path or UNC path for Windows shares.
         * Examples: C:/feeds/trading, //fileserver/feeds/risk
         */
        @WithName("path")
        String path();

        /**
         * Alternative paths to check if primary path is unavailable.
         * Useful for failover scenarios.
         */
        @WithName("fallback-paths")
        Optional<List<String>> fallbackPaths();

        /**
         * File pattern to match (glob syntax).
         * Examples: *.csv, TRADE_*.dat, ????_*.xml
         */
        @WithName("file-pattern")
        @WithDefault("*")
        String filePattern();

        /**
         * Additional patterns to exclude from processing.
         * Useful for excluding temp files or backups.
         */
        @WithName("exclude-patterns")
        Optional<List<String>> excludePatterns();

        /**
         * Strategy for determining when a file is ready to process.
         */
        @WithName("ready-strategy")
        @WithDefault("FILE_AGE")
        FileReadyStrategy readyStrategy();

        /**
         * Suffix for done/marker files when using DONE_FILE strategy.
         * Examples: .done, .ready, .go
         */
        @WithName("done-file-suffix")
        @WithDefault(".done")
        String doneFileSuffix();

        /**
         * How long a file must be unchanged before considering it stable.
         * Used with FILE_AGE strategy.
         */
        @WithName("stability-period")
        @WithDefault("PT5M")
        Duration stabilityPeriod();

        /**
         * Scheduled time for processing when using SCHEDULED strategy.
         * Format: HH:mm (24-hour)
         */
        @WithName("scheduled-time")
        Optional<LocalTime> scheduledTime();

        /**
         * How often to poll this source for new files.
         */
        @WithName("poll-interval")
        @WithDefault("PT60S")
        Duration pollInterval();

        /**
         * Whether this source is currently enabled.
         */
        @WithName("enabled")
        @WithDefault("true")
        boolean enabled();

        /**
         * Whether to process existing files on startup.
         */
        @WithName("process-existing")
        @WithDefault("true")
        boolean processExisting();

        /**
         * Whether to recursively scan subdirectories.
         */
        @WithName("recursive")
        @WithDefault("false")
        boolean recursive();

        /**
         * Maximum recursion depth if recursive is enabled.
         */
        @WithName("max-depth")
        @WithDefault("5")
        int maxDepth();

        /**
         * File processing configuration.
         */
        @WithName("processing")
        Optional<ProcessingConfig> processing();

        /**
         * Validation rules for files from this source.
         */
        @WithName("validation")
        Optional<ValidationConfig> validation();

        /**
         * Notification settings specific to this source.
         */
        @WithName("notifications")
        Optional<NotificationConfig> notifications();

        /**
         * Archive configuration for processed files.
         */
        @WithName("archive")
        Optional<ArchiveConfig> archive();

        /**
         * Custom metadata for this source system.
         */
        @WithName("metadata")
        Optional<Map<String, String>> metadata();

        /**
         * SLA configuration for monitoring.
         */
        @WithName("sla")
        Optional<SlaConfig> sla();
    }

    /**
     * File readiness detection strategies.
     */
    enum FileReadyStrategy {
        /**
         * Wait for a marker file (.done, .ready, etc).
         */
        DONE_FILE,

        /**
         * Wait for file to be unchanged for a period.
         */
        FILE_AGE,

        /**
         * Process immediately upon detection.
         */
        IMMEDIATE,

        /**
         * Process at a scheduled time.
         */
        SCHEDULED,

        /**
         * Custom readiness check via external system.
         */
        CUSTOM,

        /**
         * Check file is not locked by another process.
         */
        NOT_LOCKED,

        /**
         * Combination of strategies (all must pass).
         */
        COMBINED
    }

    /**
     * Processing configuration for files.
     */
    interface ProcessingConfig {
        /**
         * Priority for processing (1-10, 1 is highest).
         */
        @WithName("priority")
        @WithDefault("5")
        int priority();

        /**
         * Maximum number of concurrent transfers for this source.
         */
        @WithName("max-concurrent")
        @WithDefault("3")
        int maxConcurrent();

        /**
         * Maximum file size to process (in bytes).
         * -1 means no limit.
         */
        @WithName("max-file-size")
        @WithDefault("-1")
        long maxFileSize();

        /**
         * Minimum file size to process (in bytes).
         * Helps filter out empty or marker files.
         */
        @WithName("min-file-size")
        @WithDefault("0")
        long minFileSize();

        /**
         * File age limits for processing.
         */
        @WithName("file-age")
        Optional<FileAgeConfig> fileAge();

        /**
         * Batch processing configuration.
         */
        @WithName("batch")
        Optional<BatchConfig> batch();

        /**
         * Retry configuration specific to this source.
         */
        @WithName("retry")
        Optional<RetryConfig> retry();

        /**
         * Transformation to apply before uploading.
         */
        @WithName("transform")
        Optional<TransformConfig> transform();
    }

    /**
     * File age configuration.
     */
    interface FileAgeConfig {
        /**
         * Maximum age of files to process.
         * Older files are ignored.
         */
        @WithName("max-age")
        Optional<Duration> maxAge();

        /**
         * Minimum age of files to process.
         * Ensures files are complete.
         */
        @WithName("min-age")
        Optional<Duration> minAge();

        /**
         * Action for files exceeding max age.
         */
        @WithName("expired-action")
        @WithDefault("IGNORE")
        ExpiredAction expiredAction();

        enum ExpiredAction {
            IGNORE,
            ARCHIVE,
            DELETE,
            ALERT
        }
    }

    /**
     * Batch processing configuration.
     */
    interface BatchConfig {
        /**
         * Whether to batch files together.
         */
        @WithName("enabled")
        @WithDefault("false")
        boolean enabled();

        /**
         * Maximum batch size.
         */
        @WithName("max-size")
        @WithDefault("100")
        int maxSize();

        /**
         * Maximum wait time for batch to fill.
         */
        @WithName("max-wait")
        @WithDefault("PT5M")
        Duration maxWait();

        /**
         * Whether to zip batched files.
         */
        @WithName("compress")
        @WithDefault("false")
        boolean compress();
    }

    /**
     * Retry configuration.
     */
    interface RetryConfig {
        /**
         * Maximum retry attempts.
         */
        @WithName("max-attempts")
        @WithDefault("3")
        int maxAttempts();

        /**
         * Initial retry delay.
         */
        @WithName("initial-delay")
        @WithDefault("PT1M")
        Duration initialDelay();

        /**
         * Maximum retry delay.
         */
        @WithName("max-delay")
        @WithDefault("PT1H")
        Duration maxDelay();

        /**
         * Backoff multiplier.
         */
        @WithName("multiplier")
        @WithDefault("2.0")
        double multiplier();
    }

    /**
     * Transformation configuration.
     */
    interface TransformConfig {
        /**
         * Type of transformation.
         */
        @WithName("type")
        TransformType type();

        /**
         * Compression type if applicable.
         */
        @WithName("compression")
        Optional<String> compression();

        /**
         * Encryption settings if applicable.
         */
        @WithName("encryption")
        Optional<EncryptionConfig> encryption();

        /**
         * Character encoding conversion.
         */
        @WithName("encoding")
        Optional<EncodingConfig> encoding();

        enum TransformType {
            NONE,
            COMPRESS,
            ENCRYPT,
            COMPRESS_AND_ENCRYPT,
            ENCODING_CONVERSION
        }
    }

    /**
     * File validation configuration.
     */
    interface ValidationConfig {
        /**
         * Whether validation is enabled.
         */
        @WithName("enabled")
        @WithDefault("true")
        boolean enabled();

        /**
         * Checksum validation type.
         */
        @WithName("checksum")
        Optional<ChecksumConfig> checksum();

        /**
         * File format validation.
         */
        @WithName("format")
        Optional<FormatConfig> format();

        /**
         * Content validation rules.
         */
        @WithName("content")
        Optional<ContentConfig> content();

        /**
         * Action on validation failure.
         */
        @WithName("failure-action")
        @WithDefault("REJECT")
        ValidationFailureAction failureAction();

        enum ValidationFailureAction {
            REJECT,
            QUARANTINE,
            PROCESS_ANYWAY,
            ALERT_AND_PROCESS
        }
    }

    /**
     * Checksum validation configuration.
     */
    interface ChecksumConfig {
        /**
         * Checksum algorithm (MD5, SHA1, SHA256).
         */
        @WithName("algorithm")
        @WithDefault("SHA256")
        String algorithm();

        /**
         * Where to find the checksum.
         */
        @WithName("source")
        ChecksumSource source();

        /**
         * File suffix for separate checksum files.
         */
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

    /**
     * Format validation configuration.
     */
    interface FormatConfig {
        /**
         * Expected file format.
         */
        @WithName("type")
        String type();

        /**
         * Schema file for validation (XML, JSON).
         */
        @WithName("schema")
        Optional<String> schema();

        /**
         * CSV-specific settings.
         */
        @WithName("csv")
        Optional<CsvConfig> csv();
    }

    /**
     * CSV format configuration.
     */
    interface CsvConfig {
        /**
         * Field delimiter.
         */
        @WithName("delimiter")
        @WithDefault(",")
        String delimiter();

        /**
         * Whether file has headers.
         */
        @WithName("has-headers")
        @WithDefault("true")
        boolean hasHeaders();

        /**
         * Expected headers (for validation).
         */
        @WithName("expected-headers")
        Optional<List<String>> expectedHeaders();

        /**
         * Expected number of columns.
         */
        @WithName("column-count")
        Optional<Integer> columnCount();

        /**
         * Quote character.
         */
        @WithName("quote-char")
        @WithDefault("\"")
        String quoteChar();
    }

    /**
     * Content validation configuration.
     */
    interface ContentConfig {
        /**
         * Regular expression patterns that must match.
         */
        @WithName("must-contain")
        Optional<List<String>> mustContain();

        /**
         * Regular expression patterns that must not match.
         */
        @WithName("must-not-contain")
        Optional<List<String>> mustNotContain();

        /**
         * Expected row count range.
         */
        @WithName("row-count")
        Optional<RowCountConfig> rowCount();
    }

    /**
     * Row count validation.
     */
    interface RowCountConfig {
        @WithName("min")
        Optional<Integer> min();

        @WithName("max")
        Optional<Integer> max();

        @WithName("exact")
        Optional<Integer> exact();
    }

    /**
     * Notification configuration for this source.
     */
    interface NotificationConfig {
        /**
         * Email notifications.
         */
        @WithName("email")
        Optional<EmailConfig> email();

        /**
         * Slack notifications.
         */
        @WithName("slack")
        Optional<SlackConfig> slack();

        /**
         * PagerDuty integration.
         */
        @WithName("pagerduty")
        Optional<PagerDutyConfig> pagerduty();
    }

    /**
     * Email notification configuration.
     */
    interface EmailConfig {
        /**
         * Recipients for notifications.
         */
        @WithName("recipients")
        List<String> recipients();

        /**
         * Send on successful processing.
         */
        @WithName("on-success")
        @WithDefault("false")
        boolean onSuccess();

        /**
         * Send on failed processing.
         */
        @WithName("on-failure")
        @WithDefault("true")
        boolean onFailure();

        /**
         * Send daily summary.
         */
        @WithName("daily-summary")
        @WithDefault("true")
        boolean dailySummary();
    }

    /**
     * Slack notification configuration.
     */
    interface SlackConfig {
        /**
         * Webhook URL for Slack.
         */
        @WithName("webhook-url")
        String webhookUrl();

        /**
         * Channel to post to.
         */
        @WithName("channel")
        String channel();

        /**
         * Notification conditions.
         */
        @WithName("conditions")
        Optional<List<String>> conditions();
    }

    /**
     * PagerDuty configuration.
     */
    interface PagerDutyConfig {
        /**
         * Integration key.
         */
        @WithName("integration-key")
        String integrationKey();

        /**
         * Severity for alerts.
         */
        @WithName("severity")
        @WithDefault("warning")
        String severity();
    }

    /**
     * Archive configuration for processed files.
     */
    interface ArchiveConfig {
        /**
         * Whether to archive processed files.
         */
        @WithName("enabled")
        @WithDefault("false")
        boolean enabled();

        /**
         * Archive location.
         */
        @WithName("path")
        String path();

        /**
         * Archive strategy.
         */
        @WithName("strategy")
        @WithDefault("MOVE")
        ArchiveStrategy strategy();

        /**
         * Retention period for archived files.
         */
        @WithName("retention")
        @WithDefault("P90D")
        Duration retention();

        /**
         * Whether to compress archived files.
         */
        @WithName("compress")
        @WithDefault("false")
        boolean compress();

        /**
         * Directory structure for archives.
         */
        @WithName("structure")
        @WithDefault("YYYY/MM/DD")
        String structure();

        enum ArchiveStrategy {
            MOVE,
            COPY,
            COPY_THEN_DELETE
        }
    }

    /**
     * SLA monitoring configuration.
     */
    interface SlaConfig {
        /**
         * Expected file arrival time.
         */
        @WithName("expected-time")
        LocalTime expectedTime();

        /**
         * Grace period after expected time.
         */
        @WithName("grace-period")
        @WithDefault("PT30M")
        Duration gracePeriod();

        /**
         * Expected file count per day.
         */
        @WithName("expected-files")
        Optional<Integer> expectedFiles();

        /**
         * Alert if no files received for this duration.
         */
        @WithName("no-file-alert")
        @WithDefault("PT2H")
        Duration noFileAlert();

        /**
         * Processing time SLA.
         */
        @WithName("max-processing-time")
        @WithDefault("PT10M")
        Duration maxProcessingTime();
    }

    /**
     * Default settings for all sources.
     */
    interface SourceDefaults {
        @WithName("poll-interval")
        @WithDefault("PT60S")
        Duration pollInterval();

        @WithName("ready-strategy")
        @WithDefault("FILE_AGE")
        FileReadyStrategy readyStrategy();

        @WithName("stability-period")
        @WithDefault("PT5M")
        Duration stabilityPeriod();

        @WithName("max-file-size")
        @WithDefault("-1")
        long maxFileSize();

        @WithName("retry-attempts")
        @WithDefault("3")
        int retryAttempts();
    }

    /**
     * Encryption configuration.
     */
    interface EncryptionConfig {
        @WithName("algorithm")
        String algorithm();

        @WithName("key-source")
        KeySource keySource();

        @WithName("key-id")
        Optional<String> keyId();

        enum KeySource {
            KMS,
            LOCAL,
            VAULT
        }
    }

    /**
     * Encoding configuration.
     */
    interface EncodingConfig {
        @WithName("from")
        String from();

        @WithName("to")
        String to();
    }
}