package com.lbg.markets.surveillance.relay.health;

import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.model.TransferStatus;
import com.lbg.markets.surveillance.relay.repository.FileTransferRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom wellness check for monitoring non-critical issues.
 */
@ApplicationScoped
@Readiness
class DatabaseWellnessCheck implements HealthCheck {

    @Inject
    FileTransferRepository repository;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder response = HealthCheckResponse.named("database-wellness");

        try {
            // Check for performance issues
            Map<String, Object> wellness = new HashMap<>();

            // Check backlog size
            long backlog = FileTransfer.count("status = ?1", TransferStatus.DETECTED);
            wellness.put("backlog_size", backlog);

            // Check failure rate
            long failures = FileTransfer.count(
                    "status = ?1 and createdAt > ?2",
                    TransferStatus.FAILED,
                    Instant.now().minus(Duration.ofHours(1))
            );
            wellness.put("recent_failures", failures);

            // Determine wellness
            boolean isWell = backlog < 1000 && failures < 10;

            response.status(isWell);
            wellness.forEach((k, v) -> response.withData(k, v.toString()));

            return response.build();

        } catch (Exception e) {
            return response.down()
                    .withData("error", e.getMessage())
                    .build();
        }
    }
}
