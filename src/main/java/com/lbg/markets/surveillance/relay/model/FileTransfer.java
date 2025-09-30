package com.lbg.markets.surveillance.relay.model;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;

import java.time.Instant;
import java.util.Optional;

@Entity
@Table(name = "file_transfers")
@NamedNativeQuery(
        name = "FileTransfer.tryAcquireLock",
        query = """
                UPDATE file_transfers 
                SET status = :status, 
                    processing_node = :node, 
                    started_at = GETUTCDATE()
                OUTPUT INSERTED.id
                WHERE id = :id 
                  AND status = 'DETECTED'
                  AND row_version = :version
                """
)
public class FileTransfer extends PanacheEntity {

    @Column(name = "source_system", nullable = false, length = 100)
    public String sourceSystem;

    @Column(nullable = false, length = 255)
    public String filename;

    @Column(name = "file_path", nullable = false, length = 500)
    public String filePath;

    @Column(name = "file_size")
    public Long fileSize;

    @Column(name = "file_hash", length = 64)
    public String fileHash;

    @Column(name = "gcs_path", length = 500)
    public String gcsPath;

    @Enumerated(EnumType.STRING)
    @Column(length = 50)
    public TransferStatus status = TransferStatus.DETECTED;

    @Column(name = "processing_node", length = 100)
    public String processingNode;

    @Column(name = "created_at", columnDefinition = "DATETIME2")
    public Instant createdAt;

    @Column(name = "started_at", columnDefinition = "DATETIME2")
    public Instant startedAt;

    @Column(name = "completed_at", columnDefinition = "DATETIME2")
    public Instant completedAt;

    @Column(name = "error_message", length = 2000)
    public String errorMessage;

    @Column(name = "retry_count")
    public Integer retryCount = 0;

    @Column(name = "row_version", columnDefinition = "ROWVERSION")
    @Version
    public byte[] rowVersion;

    @PrePersist
    public void prePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public static Optional<FileTransfer> findBySourceAndFilename(String source, String filename) {
        return find("sourceSystem = ?1 and filename = ?2", source, filename)
                .firstResultOptional();
    }
}