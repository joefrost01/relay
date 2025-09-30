package com.lbg.markets.surveillance.relay.repository;

import com.lbg.markets.surveillance.relay.model.SourceSystem;
import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class SourceSystemRepository implements PanacheRepository<SourceSystem> {

    public Optional<SourceSystem> findBySystemId(String systemId) {
        return find("systemId", systemId).firstResultOptional();
    }

    public List<SourceSystem> findEnabled() {
        return list("enabled = true ORDER BY priority");
    }

    public List<SourceSystem> findOverdue(Duration gracePeriod) {
        Instant cutoff = Instant.now().minus(gracePeriod);
        return list("expectedNextFile < ?1 and lastFileReceived < ?2",
                Instant.now(), cutoff);
    }

    @Transactional
    public SourceSystem updateOrCreate(String systemId, String displayName,
                                       String path, int priority) {
        SourceSystem source = findBySystemId(systemId)
                .orElse(new SourceSystem());

        source.systemId = systemId;
        source.displayName = displayName;
        source.currentPath = path;
        source.priority = priority;
        source.enabled = true;

        if (source.id == null) {
            persist(source);
        }

        return source;
    }
}