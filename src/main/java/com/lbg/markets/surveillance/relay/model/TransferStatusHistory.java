package com.lbg.markets.surveillance.relay.model;

import java.time.Instant;
import java.util.List;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

/**
 * Status transition record for audit trail.
 */
@Entity
@Table(name = "transfer_status_history")
public class TransferStatusHistory extends PanacheEntity {

    @Column(name = "transfer_id", nullable = false)
    public Long transferId;

    @Column(name = "from_status", length = 50)
    @Enumerated(EnumType.STRING)
    public TransferStatus fromStatus;

    @Column(name = "to_status", nullable = false, length = 50)
    @Enumerated(EnumType.STRING)
    public TransferStatus toStatus;

    @Column(name = "changed_at", nullable = false)
    public Instant changedAt = Instant.now();

    @Column(name = "changed_by", length = 100)
    public String changedBy;

    @Column(name = "reason", length = 500)
    public String reason;

    @Column(name = "node_name", length = 100)
    public String nodeName;

    /**
     * Record a status transition.
     */
    @Transactional
    public static void recordTransition(Long transferId,
                                        TransferStatus from,
                                        TransferStatus to,
                                        String changedBy,
                                        String reason) {
        TransferStatusHistory history = new TransferStatusHistory();
        history.transferId = transferId;
        history.fromStatus = from;
        history.toStatus = to;
        history.changedBy = changedBy;
        history.reason = reason;
        history.persist();
    }

    /**
     * Get status history for a transfer.
     */
    public static List<TransferStatusHistory> getHistory(Long transferId) {
        return list("transferId = ?1 ORDER BY changedAt DESC", transferId);
    }
}
