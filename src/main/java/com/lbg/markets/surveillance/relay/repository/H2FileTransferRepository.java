package com.lbg.markets.surveillance.relay.repository;

import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.model.TransferStatus;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.util.List;

@ApplicationScoped
@IfBuildProfile("dev")  // H2 implementation for development
public class H2FileTransferRepository implements PanacheRepository<FileTransfer>, FileTransferRepository {

    @ConfigProperty(name = "relay.node-name")
    String nodeName;

    @Override
    public boolean tryAcquireLock(Long id, byte[] rowVersion) {
        // Simplified locking for H2
        int updated = update(
                "status = ?1, processingNode = ?2, startedAt = ?3, rowVersion = rowVersion + 1 " +
                        "where id = ?4 and status = ?5",
                TransferStatus.PROCESSING, nodeName, Instant.now(),
                id, TransferStatus.DETECTED
        );
        return updated > 0;
    }

    @Override
    public List<FileTransfer> findPendingTransfers(int limit) {
        // H2 compatible query (no WITH hints, uses LIMIT instead of TOP)
        return find("""
                        SELECT * FROM FileTransfer
                        WHERE status IN (?1, ?2)
                           OR (status = ?3 AND retryCount < 3)
                        ORDER BY 
                            CASE status 
                                WHEN ?2 THEN 0 
                                WHEN ?1 THEN 1 
                                ELSE 2 
                            END,
                            createdAt
                        """,
                TransferStatus.DETECTED,
                TransferStatus.REPROCESS_REQUESTED,
                TransferStatus.FAILED)
                .page(0, limit)
                .list();
    }

    @Override
    @Transactional
    public FileTransfer registerFileIfNew(String sourceSystem, String filename,
                                          String filePath, Long fileSize, String fileHash) {
        // H2 compatible - use regular insert with duplicate check
        FileTransfer existing = find("sourceSystem = ?1 and filename = ?2",
                sourceSystem, filename)
                .firstResult();

        if (existing == null) {
            FileTransfer transfer = new FileTransfer();
            transfer.sourceSystem = sourceSystem;
            transfer.filename = filename;
            transfer.filePath = filePath;
            transfer.fileSize = fileSize;
            transfer.fileHash = fileHash;
            transfer.persist();
            return transfer;
        }
        return null;
    }
}
