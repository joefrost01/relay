package com.lbg.markets.surveillance.relay.repository;

import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.model.TransferStatus;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.List;

@ApplicationScoped
@UnlessBuildProfile("dev")  // SQL Server implementation for prod/test
public class SqlServerFileTransferRepository implements PanacheRepository<FileTransfer>, FileTransferRepository {

    @ConfigProperty(name = "relay.node-name")
    String nodeName;

    @Override
    public boolean tryAcquireLock(Long id, byte[] rowVersion) {
        try {
            Query query = getEntityManager().createNativeQuery("""
                
                    UPDATE file_transfers 
                SET status = :status, 
                    processing_node = :node, 
                    started_at = GETUTCDATE()
                OUTPUT INSERTED.id
                WHERE id = :id 
                  AND status = 'DETECTED'
                  AND row_version = :version
                """);

            query.setParameter("status", TransferStatus.PROCESSING.toString());
            query.setParameter("node", nodeName);
            query.setParameter("id", id);
            query.setParameter("version", rowVersion);

            List<?> result = query.getResultList();
            return !result.isEmpty();

        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public List<FileTransfer> findPendingTransfers(int limit) {
        return getEntityManager().createNativeQuery
                        ("""
            
                        SELECT TOP (:limit) * 
            FROM
                        file_transfers WITH (READPAST, ROWLOCK)
            WHERE
                        status IN ('DETECTED', 'REPROCESS_RE
                                    OR (status = 'FAILED
                                    ' AND retry_count < 3 
                   AND DATEADD(MINUTE, POWER(2, retry_count) * 5, started_at) < GETUTCDATE())
            ORDER BY 
                CASE status 
                    WHEN 'REPROCESS_REQUESTED
                                    ' THEN 0 
                    WHEN 'DETECTED
                        ' T
                                      ELSE 2 
                END,
                created_at
            """, FileTransfer.class)
                .setParameter("limit", limit)
                .getResultList();
    }

    @Override
    @Transactional
    public FileTransfer registerFileIfNew(String sourceSystem, String filename,
                                          String filePath, Long fileSize, String fileHash) {
        // SQL Server MERGE implementation
        // ... (as before)
    }
}
