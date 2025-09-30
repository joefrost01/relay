package com.lbg.markets.surveillance.relay.health;

import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.model.TransferStatus;
import com.lbg.markets.surveillance.relay.repository.FileTransferRepository;
import io.agroal.api.AgroalDataSource;
import io.quarkus.logging.Log;
import io.smallrye.health.api.Wellness;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.health.*;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;


/**
 * Database health check for SQL Server / H2.
 * Provides both liveness and readiness probes with detailed metrics.
 */
@ApplicationScoped
@Liveness
@Readiness
public class DatabaseHealthCheck implements HealthCheck {

    @Inject
    AgroalDataSource dataSource;

    @Inject
    EntityManager entityManager;

    @Inject
    FileTransferRepository repository;

    @ConfigProperty(name = "relay.node-name")
    String nodeName;

    @ConfigProperty(name = "relay.health.db.timeout-seconds", defaultValue = "5")
    int timeoutSeconds;

    @ConfigProperty(name = "relay.health.db.warning-threshold-ms", defaultValue = "1000")
    long warningThresholdMs;

    @ConfigProperty(name = "relay.health.db.critical-threshold-ms", defaultValue = "3000")
    long criticalThresholdMs;

    @ConfigProperty(name = "quarkus.datasource.db-kind")
    String dbKind;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder responseBuilder = HealthCheckResponse.named("database");

        try {
            // Run all checks with timeout
            CompletableFuture<Map<String, Object>> future = CompletableFuture.supplyAsync(this::performHealthChecks);
            Map<String, Object> healthData = future.get(timeoutSeconds, TimeUnit.SECONDS);

            // Determine overall status
            String status = (String) healthData.get("status");
            boolean isHealthy = "UP".equals(status);

            // Build response
            responseBuilder.status(isHealthy);
            healthData.forEach((key, value) -> {
                if (value != null) {
                    responseBuilder.withData(key, value.toString());
                }
            });

            return responseBuilder.build();

        } catch (TimeoutException e) {
            Log.error("Database health check timed out");
            return responseBuilder
                    .down()
                    .withData("error", "Health check timed out after " + timeoutSeconds + " seconds")
                    .withData("node", nodeName)
                    .build();

        } catch (Exception e) {
            Log.error("Database health check failed", e);
            return responseBuilder
                    .down()
                    .withData("error", e.getMessage())
                    .withData("exception", e.getClass().getSimpleName())
                    .withData("node", nodeName)
                    .build();
        }
    }

    private Map<String, Object> performHealthChecks() {
        Map<String, Object> health = new LinkedHashMap<>();
        health.put("node", nodeName);
        health.put("timestamp", Instant.now().toString());
        health.put("database_type", dbKind);

        // Track overall status
        boolean isHealthy = true;
        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        // 1. Basic connectivity check
        long connectionStart = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection()) {
            long connectionTime = System.currentTimeMillis() - connectionStart;
            health.put("connection_time_ms", connectionTime);

            if (connectionTime > criticalThresholdMs) {
                warnings.add("Connection time exceeds critical threshold");
                health.put("connection_status", "SLOW");
            } else if (connectionTime > warningThresholdMs) {
                warnings.add("Connection time exceeds warning threshold");
                health.put("connection_status", "WARNING");
            } else {
                health.put("connection_status", "OK");
            }

            // 2. Database-specific checks
            if ("mssql".equals(dbKind)) {
                performSqlServerChecks(conn, health, warnings, errors);
            } else if ("h2".equals(dbKind)) {
                performH2Checks(conn, health, warnings, errors);
            }

            // 3. Connection pool stats
            checkConnectionPool(health, warnings);

        } catch (SQLException e) {
            isHealthy = false;
            errors.add("Database connection failed: " + e.getMessage());
            health.put("connection_status", "DOWN");
            health.put("sql_state", e.getSQLState());
            health.put("error_code", e.getErrorCode());
        }

        // 4. Application-level checks
        try {
            performApplicationChecks(health, warnings, errors);
        } catch (Exception e) {
            warnings.add("Application checks failed: " + e.getMessage());
        }

        // 5. Performance metrics
        try {
            performPerformanceChecks(health, warnings);
        } catch (Exception e) {
            warnings.add("Performance checks failed: " + e.getMessage());
        }

        // Determine final status
        if (!errors.isEmpty()) {
            health.put("status", "DOWN");
            health.put("errors", String.join("; ", errors));
            isHealthy = false;
        } else if (!warnings.isEmpty()) {
            health.put("status", "DEGRADED");
            health.put("warnings", String.join("; ", warnings));
        } else {
            health.put("status", "UP");
        }

        health.put("healthy", isHealthy);

        return health;
    }

    private void performSqlServerChecks(Connection conn, Map<String, Object> health,
                                        List<String> warnings, List<String> errors) throws SQLException {

        try (Statement stmt = conn.createStatement()) {
            // SQL Server specific health query
            String healthQuery = """
                SELECT 
                    @@VERSION as version,
                    @@SERVERNAME as server_name,
                    DB_NAME() as database_name,
                    SUSER_SNAME() as login_name,
                    (SELECT state_desc FROM sys.databases WHERE name = DB_NAME()) as db_state,
                    (SELECT COUNT(*) FROM sys.dm_exec_connections WHERE session_id = @@SPID) as connections,
                    (SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id > 0) as blocked_processes,
                    (SELECT COUNT(*) FROM sys.dm_tran_active_transactions) as active_transactions,
                    CAST(DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS VARCHAR(20)) as updateability,
                    CAST(SERVERPROPERTY('IsHadrEnabled') AS INT) as ha_enabled,
                    GETUTCDATE() as current_time
                """;

            ResultSet rs = stmt.executeQuery(healthQuery);
            if (rs.next()) {
                String dbState = rs.getString("db_state");
                health.put("server_name", rs.getString("server_name"));
                health.put("database_name", rs.getString("database_name"));
                health.put("database_state", dbState);
                health.put("login_name", rs.getString("login_name"));
                health.put("active_connections", rs.getInt("connections"));
                health.put("blocked_processes", rs.getInt("blocked_processes"));
                health.put("active_transactions", rs.getInt("active_transactions"));
                health.put("updateability", rs.getString("updateability"));
                health.put("ha_enabled", rs.getBoolean("ha_enabled"));

                // Check database state
                if (!"ONLINE".equals(dbState)) {
                    errors.add("Database is not online: " + dbState);
                }

                // Check for blocked processes
                int blockedProcesses = rs.getInt("blocked_processes");
                if (blockedProcesses > 0) {
                    warnings.add("Found " + blockedProcesses + " blocked processes");
                }

                // Extract version info
                String version = rs.getString("version");
                if (version != null && version.length() > 50) {
                    version = version.substring(0, version.indexOf('\n'));
                }
                health.put("sql_server_version", version);
            }

            // Check table accessibility and row counts
            checkTableHealth(stmt, health, warnings);

            // Check disk space (SQL Server specific)
            checkDiskSpace(stmt, health, warnings);

        } catch (SQLException e) {
            warnings.add("SQL Server specific checks failed: " + e.getMessage());
        }
    }

    private void performH2Checks(Connection conn, Map<String, Object> health,
                                 List<String> warnings, List<String> errors) throws SQLException {

        try (Statement stmt = conn.createStatement()) {
            // H2 specific health query
            String healthQuery = """
                SELECT 
                    H2VERSION() as version,
                    DATABASE() as database_name,
                    USER() as user_name,
                    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.SESSIONS) as session_count,
                    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.LOCKS WHERE NOT TABLE_NAME IS NULL) as lock_count,
                    CURRENT_TIMESTAMP as current_time
                """;

            ResultSet rs = stmt.executeQuery(healthQuery);
            if (rs.next()) {
                health.put("h2_version", rs.getString("version"));
                health.put("database_name", rs.getString("database_name"));
                health.put("user_name", rs.getString("user_name"));
                health.put("session_count", rs.getInt("session_count"));
                health.put("lock_count", rs.getInt("lock_count"));

                int lockCount = rs.getInt("lock_count");
                if (lockCount > 10) {
                    warnings.add("High lock count: " + lockCount);
                }
            }

            // Check table health
            checkTableHealth(stmt, health, warnings);

            // H2 memory usage
            checkH2Memory(stmt, health, warnings);

        } catch (SQLException e) {
            warnings.add("H2 specific checks failed: " + e.getMessage());
        }
    }

    private void checkTableHealth(Statement stmt, Map<String, Object> health,
                                  List<String> warnings) throws SQLException {

        // Check main table exists and get row counts
        String tableCheckQuery = """
            SELECT COUNT(*) as total_count,
                   SUM(CASE WHEN status = 'DETECTED' THEN 1 ELSE 0 END) as pending,
                   SUM(CASE WHEN status = 'PROCESSING' THEN 1 ELSE 0 END) as processing,
                   SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
                   SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                   MAX(created_at) as last_created
            FROM file_transfers
            """;

        ResultSet rs = stmt.executeQuery(tableCheckQuery);
        if (rs.next()) {
            long totalCount = rs.getLong("total_count");
            health.put("total_transfers", totalCount);
            health.put("pending_transfers", rs.getLong("pending"));
            health.put("processing_transfers", rs.getLong("processing"));
            health.put("completed_transfers", rs.getLong("completed"));
            health.put("failed_transfers", rs.getLong("failed"));

            Timestamp lastCreated = rs.getTimestamp("last_created");
            if (lastCreated != null) {
                health.put("last_file_detected", lastCreated.toString());

                // Check if we haven't received files recently (could indicate source issues)
                long hoursSinceLastFile = Duration.between(
                        lastCreated.toInstant(),
                        Instant.now()
                ).toHours();

                if (hoursSinceLastFile > 24) {
                    warnings.add("No new files detected in " + hoursSinceLastFile + " hours");
                }
            }

            // Warn if too many pending
            long pendingCount = rs.getLong("pending");
            if (pendingCount > 1000) {
                warnings.add("High number of pending transfers: " + pendingCount);
            }

            // Warn if too many failed
            long failedCount = rs.getLong("failed");
            if (failedCount > 100) {
                warnings.add("High number of failed transfers: " + failedCount);
            }
        }
    }

    private void checkDiskSpace(Statement stmt, Map<String, Object> health,
                                List<String> warnings) {
        try {
            // SQL Server disk space check
            String diskQuery = """
                SELECT 
                    volume_mount_point,
                    total_bytes/1024/1024/1024 as total_gb,
                    available_bytes/1024/1024/1024 as available_gb,
                    (available_bytes * 100.0 / total_bytes) as percent_free
                FROM sys.master_files AS f
                CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id)
                WHERE f.database_id = DB_ID()
                """;

            ResultSet rs = stmt.executeQuery(diskQuery);
            while (rs.next()) {
                String mountPoint = rs.getString("volume_mount_point");
                double percentFree = rs.getDouble("percent_free");
                long availableGb = rs.getLong("available_gb");

                health.put("disk_" + mountPoint.replace(":\\", "") + "_free_gb", availableGb);
                health.put("disk_" + mountPoint.replace(":\\", "") + "_free_percent", percentFree);

                if (percentFree < 10) {
                    warnings.add("Low disk space on " + mountPoint + ": " +
                            String.format("%.1f%%", percentFree) + " free");
                }
            }
        } catch (SQLException e) {
            // Not critical - might not have permissions
            Log.debug("Could not check disk space: " + e.getMessage());
        }
    }

    private void checkH2Memory(Statement stmt, Map<String, Object> health,
                               List<String> warnings) {
        try {
            String memoryQuery = "SELECT MEMORY_FREE(), MEMORY_USED() FROM DUAL";
            ResultSet rs = stmt.executeQuery(memoryQuery);
            if (rs.next()) {
                long freeMemory = rs.getLong(1);
                long usedMemory = rs.getLong(2);
                health.put("h2_memory_used_mb", usedMemory / 1024 / 1024);
                health.put("h2_memory_free_mb", freeMemory / 1024 / 1024);

                if (freeMemory < 50 * 1024 * 1024) { // Less than 50MB free
                    warnings.add("Low H2 memory: " + (freeMemory / 1024 / 1024) + "MB free");
                }
            }
        } catch (SQLException e) {
            Log.debug("Could not check H2 memory: " + e.getMessage());
        }
    }

    private void checkConnectionPool(Map<String, Object> health, List<String> warnings) {
        try {
            // Agroal pool metrics
            var metrics = dataSource.getMetrics();

            health.put("pool_active_connections", metrics.activeCount());
            health.put("pool_available_connections", metrics.availableCount());
            health.put("pool_max_used", metrics.maxUsedCount());
            health.put("pool_await_count", metrics.awaitingCount());
            health.put("pool_create_count", metrics.creationCount());
            health.put("pool_destroy_count", metrics.destroyCount());

            // Check if pool is exhausted
            if (metrics.availableCount() == 0 && metrics.awaitingCount() > 0) {
                warnings.add("Connection pool exhausted - " + metrics.awaitingCount() + " waiting");
            }

            // Check for connection leaks (high destroy count)
            if (metrics.destroyCount() > metrics.creationCount() * 0.1) {
                warnings.add("Possible connection leak - high destroy count: " + metrics.destroyCount());
            }

        } catch (Exception e) {
            Log.debug("Could not get pool metrics: " + e.getMessage());
        }
    }

    private void performApplicationChecks(Map<String, Object> health,
                                          List<String> warnings, List<String> errors) {

        // Check if we can perform queries through JPA
        try {
            long start = System.currentTimeMillis();
            long count = FileTransfer.count();
            long queryTime = System.currentTimeMillis() - start;

            health.put("jpa_query_time_ms", queryTime);
            health.put("jpa_accessible", true);

            if (queryTime > warningThresholdMs) {
                warnings.add("JPA query slow: " + queryTime + "ms");
            }

        } catch (Exception e) {
            errors.add("JPA query failed: " + e.getMessage());
            health.put("jpa_accessible", false);
        }

        // Check for stuck transfers
        try {
            List<FileTransfer> stuckTransfers = FileTransfer.find(
                    "status = ?1 and startedAt < ?2",
                    TransferStatus.PROCESSING,
                    Instant.now().minus(Duration.ofHours(1))
            ).list();

            if (!stuckTransfers.isEmpty()) {
                health.put("stuck_transfers", stuckTransfers.size());
                warnings.add("Found " + stuckTransfers.size() + " transfers stuck in PROCESSING for over 1 hour");

                // Include details of stuck transfers
                List<String> stuckDetails = stuckTransfers.stream()
                        .limit(5)
                        .map(t -> t.id + ":" + t.filename)
                        .toList();
                health.put("stuck_transfer_samples", String.join(", ", stuckDetails));
            }

        } catch (Exception e) {
            Log.debug("Could not check for stuck transfers: " + e.getMessage());
        }

        // Check error rate
        try {
            Instant oneDayAgo = Instant.now().minus(Duration.ofDays(1));
            long recentErrors = FileTransfer.count(
                    "status = ?1 and createdAt > ?2",
                    TransferStatus.FAILED,
                    oneDayAgo
            );
            long recentTotal = FileTransfer.count("createdAt > ?1", oneDayAgo);

            if (recentTotal > 0) {
                double errorRate = (double) recentErrors / recentTotal * 100;
                health.put("24h_error_rate", String.format("%.2f%%", errorRate));

                if (errorRate > 10) {
                    warnings.add("High error rate in last 24 hours: " + String.format("%.2f%%", errorRate));
                }
            }

        } catch (Exception e) {
            Log.debug("Could not calculate error rate: " + e.getMessage());
        }
    }

    private void performPerformanceChecks(Map<String, Object> health, List<String> warnings) {
        try {
            // Test write performance
            long writeStart = System.currentTimeMillis();
            testWritePerformance();
            long writeTime = System.currentTimeMillis() - writeStart;
            health.put("write_test_ms", writeTime);

            if (writeTime > criticalThresholdMs) {
                warnings.add("Write performance degraded: " + writeTime + "ms");
            }

            // Test read performance
            long readStart = System.currentTimeMillis();
            testReadPerformance();
            long readTime = System.currentTimeMillis() - readStart;
            health.put("read_test_ms", readTime);

            if (readTime > criticalThresholdMs) {
                warnings.add("Read performance degraded: " + readTime + "ms");
            }

        } catch (Exception e) {
            Log.debug("Performance checks failed: " + e.getMessage());
        }
    }

    private void testWritePerformance() {
        // Simple write test - insert and rollback
        entityManager.getTransaction().begin();
        try {
            Query query = entityManager.createNativeQuery(
                    "INSERT INTO file_transfers (source_system, filename, file_path, status, created_at) " +
                            "VALUES ('HEALTH_CHECK', 'test.txt', '/test', 'DETECTED', CURRENT_TIMESTAMP)"
            );
            query.executeUpdate();
        } finally {
            entityManager.getTransaction().rollback();
        }
    }

    private void testReadPerformance() {
        // Simple read test
        entityManager.createQuery(
                        "SELECT COUNT(f) FROM FileTransfer f WHERE f.status = :status"
                )
                .setParameter("status", TransferStatus.COMPLETED)
                .setMaxResults(1)
                .getSingleResult();
    }
}

