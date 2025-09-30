package com.lbg.markets.surveillance.relay.repository;

import com.lbg.markets.surveillance.relay.enums.TransferStatus;
import com.lbg.markets.surveillance.relay.model.FileTransfer;
import io.quarkus.hibernate.orm.panache.PanacheRepository;
import io.quarkus.panache.common.Page;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.LockModeType;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class FileTransferRepository implements PanacheRepository<FileTransfer> {

    @ConfigProperty(name = "relay.node-name", defaultValue = "relay-node-01")
    String nodeName;

    public Optional<FileTransfer> findBySourceAndFilename(String source, String filename) {
        return find("sourceSystem = ?1 and filename = ?2", source, filename)
                .firstResultOptional();
    }

    public List<FileTransfer> findPendingTransfers(int limit) {
        return find("status in ?1",
                List.of(TransferStatus.DETECTED, TransferStatus.QUEUED))
                .page(Page.ofSize(limit))
                .list();
    }

    public List<FileTransfer> findByStatus(TransferStatus status) {
        return list("status", status);
    }

    public List<FileTransfer> findStuckTransfers(Instant cutoff) {
        return list("status = ?1 and startedAt < ?2",
                TransferStatus.PROCESSING, cutoff);
    }

    @Transactional
    public boolean tryAcquireLock(Long id, Long rowVersion) {  // UPDATED signature
        try {
            FileTransfer transfer = findById(id, LockModeType.PESSIMISTIC_WRITE);
            if (transfer != null &&
                    transfer.status == TransferStatus.DETECTED &&
                    transfer.rowVersion.equals(rowVersion)) {
                transfer.status = TransferStatus.PROCESSING;
                transfer.startedAt = Instant.now();
                transfer.processingNode = nodeName;
                return true;
            }
        } catch (Exception e) {
            // Lock acquisition failed
        }
        return false;
    }

    @Transactional
    public FileTransfer createTransfer(String source, String filename,
                                       String path, Long size, String hash) {
        FileTransfer transfer = new FileTransfer();
        transfer.sourceSystem = source;
        transfer.filename = filename;
        transfer.filePath = path;
        transfer.fileSize = size;
        transfer.fileHash = hash;
        transfer.status = TransferStatus.DETECTED;
        persist(transfer);
        return transfer;
    }

}