package com.lbg.markets.surveillance.relay.health;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Separate readiness check that only validates if the service is ready to accept traffic.
 */
@ApplicationScoped
@Readiness
class DatabaseReadinessCheck implements HealthCheck {

    @Inject
    AgroalDataSource dataSource;

    @ConfigProperty(name = "relay.health.db.min-connections", defaultValue = "2")
    int minConnections;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder response = HealthCheckResponse.named("database-readiness");

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            // Simple query to verify database is responsive
            ResultSet rs = stmt.executeQuery("SELECT 1");
            if (rs.next()) {
                // Check connection pool has minimum connections
                var metrics = dataSource.getMetrics();
                if (metrics.availableCount() >= minConnections) {
                    return response.up()
                            .withData("available_connections", metrics.availableCount())
                            .build();
                } else {
                    return response.down()
                            .withData("reason", "Insufficient connections available")
                            .withData("available", metrics.availableCount())
                            .withData("required", minConnections)
                            .build();
                }
            }

            return response.down()
                    .withData("reason", "Database query failed")
                    .build();

        } catch (Exception e) {
            return response.down()
                    .withData("reason", "Database connection failed")
                    .withData("error", e.getMessage())
                    .build();
        }
    }
}
