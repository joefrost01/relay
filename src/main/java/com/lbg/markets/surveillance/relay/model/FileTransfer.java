package com.lbg.markets.surveillance.relay.model;

import com.lbg.markets.surveillance.relay.enums.TransferStatus;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "file_transfers")
public class FileTransfer extends PanacheEntity {

    @Column(name = "source_system", nullable = false)
    public String sourceSystem;

    @Column(nullable = false)
    public String filename;

    @Column(name = "file_path", nullable = false)
    public String filePath;

    @Column(name = "file_size")
    public Long fileSize;

    @Column(name = "file_hash")
    public String fileHash;

    @Column(name = "gcs_path")
    public String gcsPath;

    @Enumerated(EnumType.STRING)
    public TransferStatus status = TransferStatus.DETECTED;

    @Column(name = "processing_node")
    public String processingNode;  // ADDED

    @Column(name = "created_at")
    public Instant createdAt;

    @Column(name = "started_at")
    public Instant startedAt;

    @Column(name = "completed_at")
    public Instant completedAt;

    @Column(name = "error_message", length = 2000)
    public String errorMessage;

    @Column(name = "retry_count")
    public Integer retryCount = 0;

    @Version
    @Column(name = "row_version")
    public Long rowVersion;  // ADDED - Using Long instead of byte[] for simplicity

    @PrePersist
    public void prePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }
}