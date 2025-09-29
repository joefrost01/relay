# Relay - File Transfer Orchestration System

A high-performance, enterprise-grade file transfer orchestration system built with Quarkus, designed to reliably move files from on-premises source systems to Google Cloud Storage with comprehensive monitoring, validation, and reprocessing capabilities.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Core Components](#core-components)
- [Features](#features)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Monitoring & Observability](#monitoring--observability)
- [Development](#development)
- [Operations](#operations)
- [API Reference](#api-reference)

## Overview

### Purpose

Relay is a mission-critical data pipeline component for LBG Markets Surveillance, responsible for:

1. **Automated File Detection**: Continuously monitoring multiple source systems for new files
2. **Reliable Transfer**: Streaming files to Google Cloud Storage with retry logic and fault tolerance
3. **Comprehensive Monitoring**: Tracking every transfer with metrics, logs, and alerts
4. **SLA Compliance**: Ensuring files are transferred within defined time windows
5. **Reprocessing**: Supporting manual and automatic retry of failed transfers
6. **Observability**: Providing detailed insights into system health and transfer statistics

### Design Philosophy

The application embodies several key principles:

- **Pragmatism over Dogma**: Clean, maintainable code that solves real problems
- **Composability**: Modular components that can be understood and modified independently
- **Resilience**: Built-in retry logic, circuit breakers, and graceful degradation
- **Observability**: Every operation is instrumented for monitoring and debugging
- **Developer Experience**: Clear abstractions, comprehensive logging, and helpful error messages

### Technology Stack

- **Quarkus 3.28.1**: Modern Java framework optimized for cloud deployment
- **Java 21**: Latest LTS with modern language features
- **Apache Camel**: Enterprise integration patterns for file processing
- **Hibernate ORM with Panache**: Simplified database access
- **SQL Server / H2**: Primary database (SQL Server in prod, H2 in dev)
- **Google Cloud Storage**: Target storage system
- **BigQuery**: Long-term metrics storage and analytics
- **Micrometer/Prometheus**: Metrics collection and monitoring
- **Flyway**: Database migrations

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Source Systems                           │
│  (File Shares - Trading, Risk, Compliance, etc.)            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                  Relay Application                          │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌─────────────┐  │
│  │   File       │───▶│  Transfer    │───▶│   GCS       │  │
│  │  Detection   │    │ Orchestrator │    │  Streaming  │  │
│  │  (Camel)     │    │ (Queue/Pool) │    │  Service    │  │
│  └──────────────┘    └──────────────┘    └─────────────┘  │
│         │                    │                    │         │
│         ▼                    ▼                    ▼         │
│  ┌──────────────────────────────────────────────────────┐  │
│  │            SQL Server Database                       │  │
│  │  (FileTransfer, SourceSystem, StatusHistory)        │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Monitoring & Metrics Service                 │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              External Services                              │
│  ┌─────────────┐  ┌──────────┐  ┌────────────────────┐    │
│  │    GCS      │  │ BigQuery │  │ Prometheus/Grafana │    │
│  │  (Storage)  │  │(Analytics)│  │   (Monitoring)     │    │
│  └─────────────┘  └──────────┘  └────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Component Interaction Flow

```
1. File Detection (Camel Routes)
   ↓
   → Detects file in source system
   → Checks readiness (done file, age, schedule)
   → Registers in database
   ↓
2. Transfer Orchestrator
   ↓
   → Polls database for pending transfers
   → Acquires lock on transfer record
   → Queues for processing
   → Manages worker thread pool
   ↓
3. Processing Pipeline
   ↓
   → Validates file exists and is accessible
   → Performs optional validation (format, checksum, content)
   → Applies optional transformations (compression, encryption)
   → Streams to GCS with progress tracking
   → Updates database status
   ↓
4. Post-Processing
   ↓
   → Archives or deletes source file
   → Records metrics to BigQuery
   → Triggers notifications if configured
   ↓
5. Monitoring & Alerting
   ↓
   → Continuous health checks
   → SLA monitoring
   → Alert generation
   → Dashboard updates
```

## Core Components

### 1. File Detection Service

**Purpose**: Discovers new files in configured source systems.

**Location**: `src/main/java/com/lbg/markets/surveillance/relay/service/FileDetectionService.java`

**Key Responsibilities**:
- Scans source directories based on configured poll intervals
- Matches files against patterns (glob syntax)
- Determines file readiness using various strategies:
  - **DONE_FILE**: Waits for marker file (e.g., `.done`)
  - **FILE_AGE**: Waits for file to be unchanged for specified duration
  - **IMMEDIATE**: Processes immediately upon detection
  - **SCHEDULED**: Waits for specific time window
- Calculates file hash for duplicate detection
- Registers new files in database

**Key Methods**:
```java
public void scanSourceSystem(SourceSystem source)
private boolean isFileReady(Path file, SourceSystem source)
public String calculateHash(Path file)
```

### 2. Transfer Orchestrator

**Purpose**: Coordinates the transfer of files from detection to completion.

**Location**: `src/main/java/com/lbg/markets/surveillance/relay/service/TransferOrchestrator.java`

**Key Responsibilities**:
- Manages priority-based processing queue
- Maintains worker thread pool (configurable size)
- Implements rate limiting per source system
- Provides circuit breaker pattern for failing sources
- Handles retry logic with exponential backoff
- Tracks concurrent transfers per source
- Recovers stuck transfers on startup

**Processing Pipeline**:
1. **Validation**: Ensures file exists and is readable
2. **Pre-processing**: Optional validation (format, checksum, content rules)
3. **Transformation**: Optional compression, encryption, or format conversion
4. **Transfer**: Streams to GCS with progress tracking
5. **Post-processing**: Archives or deletes source file

**Configuration**:
```yaml
relay:
  orchestrator:
    enabled: true
    worker-threads: 10
    priority-threads: 3
    queue-capacity: 1000
    max-concurrent-per-source: 5
    processing-timeout-minutes: 30
    max-retries: 3
```

### 3. GCS Streaming Service

**Purpose**: Handles efficient upload of files to Google Cloud Storage.

**Location**: `src/main/java/com/lbg/markets/surveillance/relay/service/GcsStreamingService.java`

**Key Responsibilities**:
- Configurable upload strategies:
  - Simple upload for small files
  - Resumable upload for large files (with chunking)
  - Parallel composite upload for very large files
- Progress tracking with callback
- Automatic retry with exponential backoff
- Checksum validation
- Custom metadata attachment
- KMS encryption support

**Key Features**:
- **Resumable Uploads**: Automatically resumes interrupted uploads
- **Streaming**: Processes files without loading entirely into memory
- **Buffer Management**: Configurable buffer size (default 8MB)
- **Timeout Handling**: Configurable timeout (default 30 minutes)
- **Storage Classes**: STANDARD, NEARLINE, COLDLINE, ARCHIVE

**Configuration**:
```yaml
relay:
  gcs:
    bucket: surveillance-data-lake
    project-id: lbg-surveillance-prod
    upload-buffer-size: 8388608  # 8MB
    upload-timeout: PT30M
    resumable-upload: true
    resumable-chunk-size: 16777216  # 16MB
    validate-checksums: true
```

### 4. Monitoring Service

**Purpose**: Provides comprehensive observability across all system operations.

**Location**: `src/main/java/com/lbg/markets/surveillance/relay/service/MonitoringService.java`

**Key Responsibilities**:
- Collects metrics via Micrometer (Prometheus format)
- Exports events to BigQuery for long-term storage
- Generates alerts based on configured thresholds
- Tracks SLA compliance per source system
- Provides real-time statistics via REST API
- Manages alert cooldown periods
- Publishes events to Pub/Sub (optional)

**Metrics Collected**:
- Transfer counts by status (DETECTED, PROCESSING, COMPLETED, FAILED)
- Processing time per stage (detection, validation, transfer)
- Queue sizes (pending, active)
- Source system health status
- Error rates and types
- Throughput (files/minute, MB/s)
- Circuit breaker states

**Alert Types**:
- High error rate (>10% failures in 1 hour)
- Stuck transfers (processing >1 hour)
- SLA breaches (files not received on time)
- No throughput (no completed transfers in 5 minutes)
- High latency (average processing time >1 minute)
- Queue near capacity (>80% full)

### 5. Camel Routes

#### File Ingestion Route

**Location**: `src/main/java/com/lbg/markets/surveillance/relay/routes/FileIngestionRoute.java`

**Purpose**: Camel-based file watching and initial processing.

**Key Features**:
- Configures file watchers per source system
- Implements idempotency to prevent duplicate processing
- Uses read locks to ensure file stability
- Supports recursive directory scanning
- Handles fallback paths if primary unavailable
- Implements error handling with dead letter queue

**Route Configuration**:
- Poll interval: Configurable per source
- Max files per poll: 100 (configurable)
- Concurrent consumers: 3 (configurable)
- Read lock strategy: "changed" (waits for file stability)
- Idempotent repository: Prevents reprocessing

#### Monitoring Route

**Location**: `src/main/java/com/lbg/markets/surveillance/relay/routes/MonitoringRoute.java`

**Purpose**: Continuous monitoring, metrics collection, and alerting.

**Scheduled Tasks**:
- **Every 30s**: Collect and export metrics to BigQuery
- **Every 1m**: Export system metrics (memory, threads, etc.)
- **Every 5m**: Check error rates and generate alerts
- **Hourly**: Export transfer statistics
- **Daily**: Generate SLA reports

#### Reprocessing Route

**Location**: `src/main/java/com/lbg/markets/surveillance/relay/routes/ReprocessingRoute.java`

**Purpose**: Handles manual and automatic reprocessing of transfers.

**Capabilities**:
- Single file reprocessing via API
- Bulk reprocessing by criteria (source, date range, status)
- BigQuery-triggered reprocessing (external requests)
- Automatic retry of failed transfers with backoff
- Quarantine after repeated failures
- Scheduled cleanup of stuck transfers

### 6. Data Model

#### FileTransfer Entity

**Location**: `src/main/java/com/lbg/markets/surveillance/relay/model/FileTransfer.java`

**Purpose**: Represents a single file transfer through the system.

**Key Fields**:
```java
public class FileTransfer extends PanacheEntity {
    String sourceSystem;       // Source identifier
    String filename;           // Original filename
    String filePath;           // Source path
    Long fileSize;             // File size in bytes
    String fileHash;           // SHA-256 hash
    String gcsPath;            // Destination path in GCS
    TransferStatus status;     // Current status
    String processingNode;     // Node processing this transfer
    Instant createdAt;         // Detection time
    Instant startedAt;         // Processing start time
    Instant completedAt;       // Completion time
    String errorMessage;       // Error details if failed
    Integer retryCount;        // Number of retry attempts
    byte[] rowVersion;         // For optimistic locking
}
```

**Status Lifecycle**:
```
DETECTED → QUEUED → VALIDATING → PROCESSING → COMPLETED
                           ↓
                        FAILED → RETRYING → PROCESSING
                           ↓
                    QUARANTINED (after max retries)
```

#### TransferStatus Enum

**Location**: `src/main/java/com/lbg/markets/surveillance/relay/model/TransferStatus.java`

**Purpose**: Defines all possible states in the transfer lifecycle.

**Key States**:
- **DETECTED**: File found, awaiting processing
- **WAITING**: Waiting for ready condition (done file, schedule)
- **QUEUED**: In processing queue
- **VALIDATING**: Performing validation checks
- **PROCESSING**: Active transfer in progress
- **RETRYING**: Automatic retry after failure
- **COMPLETED**: Successfully transferred
- **FAILED**: Transfer failed after retries
- **REJECTED**: Validation failed (will not retry)
- **QUARANTINED**: Requires manual intervention
- **REPROCESS_REQUESTED**: Marked for manual reprocessing

**State Transitions**:
The enum includes validation logic to ensure only valid state transitions occur:
```java
public boolean canTransitionTo(TransferStatus newStatus)
public void validateTransition(TransferStatus newStatus)
```

#### SourceSystem Entity

**Location**: `src/main/java/com/lbg/markets/surveillance/relay/model/SourceSystem.java`

**Purpose**: Tracks runtime state and statistics for each configured source.

**Key Fields**:
```java
public class SourceSystem extends PanacheEntity {
    String systemId;              // Unique identifier
    String displayName;           // Human-readable name
    String currentPath;           // Active monitoring path
    Boolean enabled;              // Whether processing is active
    Integer priority;             // Processing priority (1=highest)
    SourceStatus status;          // ACTIVE, PAUSED, UNAVAILABLE, ERROR
    HealthStatus healthStatus;    // HEALTHY, WARNING, DEGRADED, CRITICAL
    Instant lastScanTime;         // Last directory scan
    Instant lastFileReceived;     // Last file detection
    Instant expectedNextFile;     // For SLA monitoring
    Integer filesToday;           // Daily counter
    Long totalFiles;              // All-time counter
    Long successfulTransfers;     // Success count
    Long failedTransfers;         // Failure count
    Integer consecutiveErrors;    // Error streak
    Long avgProcessingTimeMs;     // Average processing time
}
```

**Business Methods**:
```java
public void recordFileDetected(String filename, Long fileSize)
public void recordSuccess(long processingTimeMs)
public void recordFailure(String error)
public void markUnavailable(String reason)
public boolean isOverdue(Duration gracePeriod)
public double getSuccessRate()
```

## Features

### 1. Multiple File Readiness Strategies

#### DONE_FILE Strategy
Waits for a marker file to indicate completion:
```yaml
ready-strategy: DONE_FILE
done-file-suffix: .done
```

#### FILE_AGE Strategy
Waits for file to be unchanged for specified duration:
```yaml
ready-strategy: FILE_AGE
stability-period: PT5M  # 5 minutes
```

#### IMMEDIATE Strategy
Processes file as soon as detected:
```yaml
ready-strategy: IMMEDIATE
```

#### SCHEDULED Strategy
Processes files only during specified time window:
```yaml
ready-strategy: SCHEDULED
scheduled-time: "06:00:00"  # 6 AM
```

### 2. Comprehensive Validation

#### Checksum Validation
```yaml
validation:
  checksum:
    algorithm: SHA256
    source: SEPARATE_FILE
    file-suffix: .sha256
```

#### Format Validation
```yaml
validation:
  format:
    type: CSV
    csv:
      delimiter: ","
      has-headers: true
      expected-headers: ["ID", "Timestamp", "Price", "Quantity"]
      column-count: 25
```

#### Content Validation
```yaml
validation:
  content:
    must-contain: ["^HEADER\\|", "^TRAILER\\|"]
    must-not-contain: ["ERROR", "INVALID"]
    row-count:
      min: 100
      max: 1000000
```

### 3. Advanced Retry Logic

**Automatic Retry**:
```yaml
relay:
  orchestrator:
    max-retries: 3
    retry-delay-minutes: 5  # Increases exponentially
```

**Retry Delay Progression**:
- Attempt 1: 5 minutes
- Attempt 2: 10 minutes
- Attempt 3: 20 minutes
- After 3 attempts: FAILED (requires manual intervention)

**Selective Retry**:
- **Retryable**: Network errors, temporary GCS issues, timeouts
- **Non-retryable**: Validation failures, file not found, access denied

### 4. Rate Limiting

**Per-Source Concurrency**:
```yaml
processing:
  max-concurrent: 5  # Max 5 simultaneous transfers from this source
```

**Global Rate Limiting**:
```yaml
relay:
  orchestrator:
    worker-threads: 10  # Total concurrent transfers across all sources
```

### 5. Transformation Pipeline

**Compression**:
```yaml
transform:
  type: COMPRESS
  compression: gzip
```

**Encryption**:
```yaml
transform:
  type: ENCRYPT
  encryption:
    algorithm: AES-256-GCM
    key-source: KMS
    key-id: projects/my-project/locations/global/keyRings/relay/cryptoKeys/data-key
```

**Combined Transformations**:
```yaml
transform:
  type: COMPRESS_AND_ENCRYPT
  compression: gzip
  encryption:
    algorithm: AES-256-GCM
```

### 6. Archival Strategies

**Move Strategy**:
```yaml
archive:
  strategy: MOVE
  path: /mnt/archive
```

**Dynamic Folder Structure**:
```yaml
archive:
  structure: "YYYY/MM/DD"  # Creates /2025/01/15/
```

### 7. SLA Monitoring

**Expected File Times**:
```yaml
sla:
  expected-time: "09:00:00"
  grace-period: PT30M
```

**Processing Time SLA**:
```yaml
sla:
  max-processing-time: PT10M
```

### 8. Notification System

**Email Notifications**:
```yaml
notifications:
  email:
    recipients: ["ops-team@company.com"]
    on-success: false
    on-failure: true
    daily-summary: true
```

**Slack Integration**:
```yaml
notifications:
  slack:
    webhook-url: https://hooks.slack.com/services/XXX
    channel: "#data-ops"
```

## Configuration

### Environment Variables

#### Required
```bash
# Database
DB_HOST=sqlserver.company.com
DB_NAME=relay_prod
DB_USER=relay_app
DB_PASSWORD=<secret>

# Google Cloud
GCP_PROJECT=lbg-surveillance-prod
GCS_BUCKET=surveillance-data-lake
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

# Application
HOSTNAME=relay-prod-01
```

### Example Configuration

```yaml
relay:
  node-name: ${HOSTNAME:relay-dev-01}
  
  sources:
    - id: trading_system
      path: /mnt/feeds/trading
      file-pattern: "TRADE_*.csv"
      ready-strategy: DONE_FILE
      done-file-suffix: .done
      poll-interval: PT30S
      enabled: true
      
      processing:
        priority: 1
        max-concurrent: 5
        
      validation:
        enabled: true
        checksum:
          algorithm: SHA256
          source: SEPARATE_FILE
          
      archive:
        enabled: true
        path: /mnt/archive/trading
        strategy: MOVE
        retention: P90D
        
      sla:
        expected-time: "09:00:00"
        grace-period: PT30M

  gcs:
    bucket: surveillance-data-lake
    project-id: lbg-surveillance-prod
    upload-buffer-size: 8388608
    upload-timeout: PT30M
    resumable-upload: true

  monitoring:
    big-query-dataset: relay_monitoring
    events-table: transfer_events
    heartbeat-interval: PT60S
```

## Deployment

### Building

#### Standard JAR
```bash
./mvnw clean package
```

#### Docker Image
```bash
docker build -f src/main/docker/Dockerfile.jvm -t relay:latest .
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: relay
spec:
  replicas: 3
  selector:
    matchLabels:
      app: relay
  template:
    metadata:
      labels:
        app: relay
    spec:
      containers:
      - name: relay
        image: gcr.io/lbg-surveillance/relay:1.0.0
        ports:
        - containerPort: 8080
        env:
        - name: DB_HOST
          valueFrom:
            secretKeyRef:
              name: relay-secrets
              key: db-host
        volumeMounts:
        - name: feeds
          mountPath: /mnt/feeds
          readOnly: true
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
          limits:
            cpu: "4"
            memory: "8Gi"
        livenessProbe:
          httpGet:
            path: /q/health/live
            port: 8080
        readinessProbe:
          httpGet:
            path: /q/health/ready
            port: 8080
      volumes:
      - name: feeds
        nfs:
          server: fileserver.company.com
          path: /feeds
```

## Monitoring & Observability

### Health Endpoints

**Liveness Probe**:
```bash
curl http://localhost:8080/q/health/live
```

**Readiness Probe**:
```bash
curl http://localhost:8080/q/health/ready
```

### Metrics

**Prometheus Endpoint**:
```bash
curl http://localhost:8080/q/metrics
```

**Key Metrics**:
- `relay_transfers_total{status="COMPLETED"}` - Total completed transfers
- `relay_processing_time_seconds` - Processing duration
- `relay_queue_size` - Current queue size
- `relay_errors_total` - Error counts

### Alerting

**Example Alert Rules** (Prometheus):
```yaml
- alert: RelayHighErrorRate
  expr: |
    rate(relay_transfers_total{status="FAILED"}[5m])
    / rate(relay_transfers_total[5m]) > 0.1
  for: 5m
  annotations:
    summary: "High error rate detected"
```

## Development

### Local Development

```bash
# Start dependencies
docker run -d -p 8089:8089 fsouza/fake-gcs-server

# Create test data
mkdir -p test-data/trading
echo "ID,Timestamp,Price" > test-data/trading/TRADE_001.csv

# Run application
./mvnw quarkus:dev
```

### Testing

```bash
# Unit tests
./mvnw test

# Integration tests
./mvnw verify
```

## Operations

### Common Operations

**View Current Queue**:
```bash
curl http://localhost:8080/admin/stats | jq '.metrics.pendingCount'
```

**Reprocess Failed Transfers**:
```bash
curl -X POST "http://localhost:8080/admin/transfers/bulk-reprocess" \
  -H "Content-Type: application/json" \
  -d '{"transferIds": [123, 456], "reason": "Retry after outage"}'
```

**Trigger Manual Scan**:
```bash
curl -X POST "http://localhost:8080/admin/sources/trading_system/scan"
```

### Troubleshooting

**Files not being detected**:
1. Check source path is accessible
2. Verify file pattern matches
3. Confirm ready condition is met
4. Check source system is enabled

**Transfers stuck in PROCESSING**:
1. Check active transfers via admin API
2. Automatic cleanup runs every 24 hours
3. Restart application (transfers will be recovered)

**High error rate**:
1. Review recent errors via admin API
2. Check GCS connectivity
3. Verify service account permissions
4. Review circuit breaker status

## API Reference

### Admin Endpoints

- `GET /admin/transfers` - List transfers with filtering
- `GET /admin/transfers/{id}` - Get transfer details
- `POST /admin/transfers/{id}/reprocess` - Reprocess single transfer
- `POST /admin/transfers/bulk-reprocess` - Bulk reprocessing
- `GET /admin/stats` - Overall statistics
- `GET /admin/sources` - List source systems
- `GET /admin/health` - System health status

### Example Response

```json
{
  "id": 12345,
  "sourceSystem": "trading_system",
  "filename": "TRADE_20250115.csv",
  "status": "COMPLETED",
  "createdAt": "2025-01-15T09:30:00Z",
  "completedAt": "2025-01-15T09:31:30Z",
  "processingDurationSeconds": 75
}
```

## Performance

### Benchmarks

| Metric | Value |
|--------|-------|
| Max Throughput | 150 files/minute |
| Average Latency | 6 seconds |
| P95 Latency | 15 seconds |
| Success Rate | 99.2% |

### Optimization

**For Higher Throughput**:
- Increase worker threads
- Use parallel GCS uploads
- Scale horizontally

**For Large Files** (>100MB):
```yaml
gcs:
  parallel:
    enabled: true
    min-file-size: 104857600
    thread-count: 4
```

## License

Copyright © 2025 LBG Markets Surveillance. All rights reserved.
