package com.lbg.markets.surveillance.relay.api;

import com.lbg.markets.surveillance.relay.config.RelayConfiguration;
import com.lbg.markets.surveillance.relay.enums.TransferStatus;
import com.lbg.markets.surveillance.relay.model.FileTransfer;
import com.lbg.markets.surveillance.relay.repository.FileTransferRepository;
import com.lbg.markets.surveillance.relay.service.FileDetectionService;
import com.lbg.markets.surveillance.relay.service.MonitoringService;
import com.lbg.markets.surveillance.relay.service.TransferOrchestrator;
import io.quarkus.logging.Log;
import io.quarkus.panache.common.Page;
import io.quarkus.panache.common.Sort;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Path("/admin")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class AdminResource {

    @Inject
    FileTransferRepository repository;
    @Inject
    MonitoringService monitoring;
    @Inject
    TransferOrchestrator orchestrator;
    @Inject
    FileDetectionService detectionService;
    @Inject
    RelayConfiguration config;

    @ConfigProperty(name = "relay.node-name")
    String nodeName;

    @ConfigProperty(name = "quarkus.application.version", defaultValue = "unknown")
    String applicationVersion;

    // ============ Transfer Management ============

    @GET
    @Path("/transfers")
    public Response listTransfers(
            @QueryParam("status") TransferStatus status,
            @QueryParam("source") String source,
            @QueryParam("filename") String filename,
            @QueryParam("date") String date,
            @QueryParam("page") @DefaultValue("0") int page,
            @QueryParam("size") @DefaultValue("50") int size,
            @QueryParam("sort") @DefaultValue("createdAt") String sortField,
            @QueryParam("direction") @DefaultValue("DESC") String sortDirection) {

        try {
            // Build query dynamically
            StringBuilder query = new StringBuilder("1=1");
            Map<String, Object> params = new HashMap<>();

            if (status != null) {
                query.append(" and status = :status");
                params.put("status", status);
            }

            if (source != null && !source.isEmpty()) {
                query.append(" and sourceSystem = :source");
                params.put("source", source);
            }

            if (filename != null && !filename.isEmpty()) {
                query.append(" and filename like :filename");
                params.put("filename", "%" + filename + "%");
            }

            if (date != null && !date.isEmpty()) {
                LocalDateTime startOfDay = LocalDateTime.parse(date + "T00:00:00");
                LocalDateTime endOfDay = startOfDay.plusDays(1);
                query.append(" and createdAt >= :startDate and createdAt < :endDate");
                params.put("startDate", startOfDay.toInstant(ZoneOffset.UTC));
                params.put("endDate", endOfDay.toInstant(ZoneOffset.UTC));
            }

            // Execute query with pagination
            Sort.Direction direction = "ASC".equalsIgnoreCase(sortDirection)
                    ? Sort.Direction.Ascending
                    : Sort.Direction.Descending;

            var panacheQuery = FileTransfer.find(query.toString(),
                    Sort.by(sortField, direction), params);

            List<FileTransfer> transfers = panacheQuery
                    .page(Page.of(page, size))
                    .list();

            long totalCount = panacheQuery.count();

            // Transform to DTOs
            List<TransferDto> transferDtos = transfers.stream()
                    .map(this::toDto)
                    .collect(Collectors.toList());

            return Response.ok(Map.of(
                    "transfers", transferDtos,
                    "page", page,
                    "size", size,
                    "totalElements", totalCount,
                    "totalPages", (totalCount + size - 1) / size
            )).build();

        } catch (Exception e) {
            Log.error("Error listing transfers", e);
            return Response.serverError()
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @GET
    @Path("/transfers/{id}")
    public Response getTransfer(@PathParam("id") Long id) {
        FileTransfer transfer = FileTransfer.findById(id);
        if (transfer == null) {
            return Response.status(404)
                    .entity(Map.of("error", "Transfer not found"))
                    .build();
        }
        return Response.ok(toDetailedDto(transfer)).build();
    }

    @POST
    @Path("/transfers/{id}/reprocess")
    @Transactional
    public Response reprocessTransfer(
            @PathParam("id") Long id,
            @QueryParam("reason") String reason) {

        FileTransfer transfer = FileTransfer.findById(id);
        if (transfer == null) {
            return Response.status(404)
                    .entity(Map.of("error", "Transfer not found"))
                    .build();
        }

        if (transfer.status == TransferStatus.PROCESSING) {
            return Response.status(400)
                    .entity(Map.of("error", "Cannot reprocess - transfer is currently processing"))
                    .build();
        }

        // Reset for reprocessing
        transfer.status = TransferStatus.DETECTED;
        transfer.processingNode = null;
        transfer.retryCount = 0;
        transfer.errorMessage = null;
        transfer.startedAt = null;
        transfer.completedAt = null;
        transfer.persist();

        // Log the reprocess request
        monitoring.recordEvent("manual_reprocess", Map.of(
                "file_id", id,
                "source", transfer.sourceSystem,
                "filename", transfer.filename,
                "reason", reason != null ? reason : "Manual request",
                "requested_by", getCurrentUser()
        ));

        Log.infof("Transfer %d marked for reprocessing: %s", id, reason);

        return Response.ok(Map.of(
                "status", "success",
                "message", "Transfer marked for reprocessing",
                "transferId", id
        )).build();
    }

    @POST
    @Path("/transfers/bulk-reprocess")
    @Transactional
    public Response bulkReprocess(BulkReprocessRequest request) {
        if (request.transferIds == null || request.transferIds.isEmpty()) {
            return Response.status(400)
                    .entity(Map.of("error", "No transfer IDs provided"))
                    .build();
        }

        int processed = 0;
        List<Long> failed = new ArrayList<>();

        for (Long id : request.transferIds) {
            FileTransfer transfer = FileTransfer.findById(id);
            if (transfer != null && transfer.status != TransferStatus.PROCESSING) {
                transfer.status = TransferStatus.DETECTED;
                transfer.processingNode = null;
                transfer.retryCount = 0;
                transfer.persist();
                processed++;
            } else {
                failed.add(id);
            }
        }

        monitoring.recordEvent("bulk_reprocess", Map.of(
                "count", processed,
                "failed", failed.size(),
                "reason", request.reason
        ));

        return Response.ok(Map.of(
                "processed", processed,
                "failed", failed,
                "message", String.format("%d transfers marked for reprocessing", processed)
        )).build();
    }

    @DELETE
    @Path("/transfers/{id}")
    @Transactional
    public Response deleteTransfer(@PathParam("id") Long id) {
        FileTransfer transfer = FileTransfer.findById(id);
        if (transfer == null) {
            return Response.status(404)
                    .entity(Map.of("error", "Transfer not found"))
                    .build();
        }

        if (transfer.status == TransferStatus.PROCESSING) {
            return Response.status(400)
                    .entity(Map.of("error", "Cannot delete - transfer is currently processing"))
                    .build();
        }

        monitoring.recordEvent("transfer_deleted", Map.of(
                "file_id", id,
                "source", transfer.sourceSystem,
                "filename", transfer.filename
        ));

        transfer.delete();

        return Response.ok(Map.of(
                "status", "success",
                "message", "Transfer deleted"
        )).build();
    }

    // ============ Statistics & Monitoring ============

    @GET
    @Path("/stats")
    public Response getStatistics(
            @QueryParam("from") String from,
            @QueryParam("to") String to) {

        Instant fromDate = from != null
                ? LocalDateTime.parse(from).toInstant(ZoneOffset.UTC)
                : Instant.now().minus(Duration.ofDays(7));

        Instant toDate = to != null
                ? LocalDateTime.parse(to).toInstant(ZoneOffset.UTC)
                : Instant.now();

        // Get counts by status
        Map<String, Long> statusCounts = FileTransfer.find(
                        "createdAt >= ?1 and createdAt <= ?2", fromDate, toDate)
                .stream()
                .map(t -> (FileTransfer) t)
                .collect(Collectors.groupingBy(
                        t -> t.status.toString(),
                        Collectors.counting()
                ));

        // Get counts by source system
        Map<String, Long> sourceCounts = FileTransfer.find(
                        "createdAt >= ?1 and createdAt <= ?2", fromDate, toDate)
                .stream()
                .map(t -> (FileTransfer) t)
                .collect(Collectors.groupingBy(
                        t -> t.sourceSystem,
                        Collectors.counting()
                ));

        // Calculate success rate
        long completed = statusCounts.getOrDefault("COMPLETED", 0L);
        long failed = statusCounts.getOrDefault("FAILED", 0L);
        double successRate = (completed + failed) > 0
                ? (double) completed / (completed + failed) * 100
                : 0;

        // Get processing times
        List<FileTransfer> completedTransfers = FileTransfer.find(
                        "status = ?1 and completedAt is not null and startedAt is not null",
                        TransferStatus.COMPLETED)
                .list();

        OptionalDouble avgProcessingTime = completedTransfers.stream()
                .mapToLong(t -> Duration.between(t.startedAt, t.completedAt).toSeconds())
                .average();

        // Get current processing
        long currentlyProcessing = FileTransfer.count("status = ?1", TransferStatus.PROCESSING);
        long pendingCount = FileTransfer.count("status = ?1", TransferStatus.DETECTED);

        return Response.ok(Map.of(
                "period", Map.of("from", fromDate, "to", toDate),
                "statusCounts", statusCounts,
                "sourceCounts", sourceCounts,
                "metrics", Map.of(
                        "successRate", successRate,
                        "avgProcessingTimeSeconds", avgProcessingTime.orElse(0),
                        "currentlyProcessing", currentlyProcessing,
                        "pendingCount", pendingCount
                )
        )).build();
    }

    @GET
    @Path("/stats/hourly")
    public Response getHourlyStats(@QueryParam("hours") @DefaultValue("24") int hours) {
        Instant since = Instant.now().minus(Duration.ofHours(hours));

        // Group transfers by hour
        List<FileTransfer> transfers = FileTransfer.find(
                "createdAt >= ?1", since).list();

        Map<String, Map<String, Long>> hourlyStats = transfers.stream()
                .collect(Collectors.groupingBy(
                        t -> DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                                LocalDateTime.ofInstant(t.createdAt, ZoneOffset.UTC)
                                        .withMinute(0).withSecond(0).withNano(0)
                        ),
                        Collectors.groupingBy(
                                t -> t.status.toString(),
                                Collectors.counting()
                        )
                ));

        return Response.ok(hourlyStats).build();
    }

    // ============ Source System Management ============

    @GET
    @Path("/sources")
    public Response listSources() {
        List<Map<String, Object>> sources = config.sources().stream()
                .map(source -> {
                    // Get recent stats for this source
                    long totalCount = FileTransfer.count("sourceSystem = ?1", source.id());
                    long completedCount = FileTransfer.count(
                            "sourceSystem = ?1 and status = ?2",
                            source.id(), TransferStatus.COMPLETED
                    );
                    long failedCount = FileTransfer.count(
                            "sourceSystem = ?1 and status = ?2",
                            source.id(), TransferStatus.FAILED
                    );

                    return Map.of(
                            "id", source.id(),
                            "path", source.path(),
                            "filePattern", source.filePattern(),
                            "readyStrategy", source.readyStrategy().toString(),
                            "pollInterval", source.pollInterval().toString(),
                            "enabled", source.enabled(),
                            "stats", Map.of(
                                    "total", totalCount,
                                    "completed", completedCount,
                                    "failed", failedCount
                            )
                    );
                })
                .collect(Collectors.toList());

        return Response.ok(sources).build();
    }

    @POST
    @Path("/sources/{sourceId}/scan")
    public Response triggerSourceScan(@PathParam("sourceId") String sourceId) {
        Optional<RelayConfiguration.SourceSystem> source = config.sources().stream()
                .filter(s -> s.id().equals(sourceId))
                .findFirst();

        if (source.isEmpty()) {
            return Response.status(404)
                    .entity(Map.of("error", "Source system not found"))
                    .build();
        }

        if (!source.get().enabled()) {
            return Response.status(400)
                    .entity(Map.of("error", "Source system is disabled"))
                    .build();
        }

        // Trigger scan asynchronously
        detectionService.scanSourceSystem(source.get());

        monitoring.recordEvent("manual_scan", Map.of(
                "source", sourceId,
                "triggered_by", getCurrentUser()
        ));

        return Response.ok(Map.of(
                "status", "success",
                "message", "Scan triggered for " + sourceId
        )).build();
    }

    // ============ System Health & Management ============

    @GET
    @Path("/health")
    public Response getHealth() {
        Map<String, Object> health = new HashMap<>();

        health.put("status", "UP");
        health.put("node", nodeName);
        health.put("version", applicationVersion);
        health.put("timestamp", Instant.now());

        // Check database
        try {
            long count = FileTransfer.count();
            health.put("database", Map.of(
                    "status", "UP",
                    "totalRecords", count
            ));
        } catch (Exception e) {
            health.put("database", Map.of(
                    "status", "DOWN",
                    "error", e.getMessage()
            ));
            health.put("status", "DEGRADED");
        }

        // Check GCS connectivity (you'd implement a ping method)
        health.put("gcs", Map.of(
                "status", "UP",
                "bucket", config.gcs().bucket()
        ));

        // Current processing stats
        health.put("processing", Map.of(
                "active", FileTransfer.count("status = ?1", TransferStatus.PROCESSING),
                "pending", FileTransfer.count("status = ?1", TransferStatus.DETECTED),
                "failed", FileTransfer.count("status = ?1 and retryCount >= 3", TransferStatus.FAILED)
        ));

        return Response.ok(health).build();
    }

    @GET
    @Path("/errors")
    public Response getRecentErrors(
            @QueryParam("limit") @DefaultValue("50") int limit) {

        List<FileTransfer> errors = FileTransfer.find(
                        "status = ?1 and errorMessage is not null",
                        Sort.by("createdAt").descending(),
                        TransferStatus.FAILED)
                .page(0, limit)
                .list();

        List<Map<String, Object>> errorList = errors.stream()
                .map(t -> Map.<String, Object>of(
                        "id", t.id,
                        "sourceSystem", t.sourceSystem,
                        "filename", t.filename,
                        "error", t.errorMessage,
                        "retryCount", t.retryCount,
                        "failedAt", t.startedAt,
                        "node", t.processingNode
                ))
                .collect(Collectors.toList());

        return Response.ok(errorList).build();
    }

    @POST
    @Path("/maintenance/cleanup")
    @Transactional
    public Response cleanupOldRecords(
            @QueryParam("days") @DefaultValue("90") int days,
            @QueryParam("dryRun") @DefaultValue("true") boolean dryRun) {

        Instant cutoff = Instant.now().minus(Duration.ofDays(days));

        long count = FileTransfer.count(
                "status = ?1 and completedAt < ?2",
                TransferStatus.COMPLETED, cutoff
        );

        if (!dryRun) {
            FileTransfer.delete(
                    "status = ?1 and completedAt < ?2",
                    TransferStatus.COMPLETED, cutoff
            );

            monitoring.recordEvent("cleanup_executed", Map.of(
                    "records_deleted", count,
                    "cutoff_days", days
            ));
        }

        return Response.ok(Map.of(
                "recordsAffected", count,
                "dryRun", dryRun,
                "cutoffDate", cutoff
        )).build();
    }

    // ============ Helper Methods & DTOs ============

    private TransferDto toDto(FileTransfer transfer) {
        return new TransferDto(
                transfer.id,
                transfer.sourceSystem,
                transfer.filename,
                transfer.fileSize,
                transfer.status.toString(),
                transfer.createdAt,
                transfer.completedAt,
                transfer.processingNode,
                transfer.retryCount
        );
    }

    private Map<String, Object> toDetailedDto(FileTransfer transfer) {
        Map<String, Object> dto = new HashMap<>();
        dto.put("id", transfer.id);
        dto.put("sourceSystem", transfer.sourceSystem);
        dto.put("filename", transfer.filename);
        dto.put("filePath", transfer.filePath);
        dto.put("fileSize", transfer.fileSize);
        dto.put("fileHash", transfer.fileHash);
        dto.put("gcsPath", transfer.gcsPath);
        dto.put("status", transfer.status.toString());
        dto.put("processingNode", transfer.processingNode);
        dto.put("createdAt", transfer.createdAt);
        dto.put("startedAt", transfer.startedAt);
        dto.put("completedAt", transfer.completedAt);
        dto.put("errorMessage", transfer.errorMessage);
        dto.put("retryCount", transfer.retryCount);

        if (transfer.startedAt != null && transfer.completedAt != null) {
            dto.put("processingDurationSeconds",
                    Duration.between(transfer.startedAt, transfer.completedAt).toSeconds());
        }

        return dto;
    }

    private String getCurrentUser() {
        // In a real app, this would get from security context
        return "admin";
    }

    // ============ DTOs ============

    public static class TransferDto {
        public Long id;
        public String sourceSystem;
        public String filename;
        public Long fileSize;
        public String status;
        public Instant createdAt;
        public Instant completedAt;
        public String processingNode;
        public Integer retryCount;

        public TransferDto(Long id, String sourceSystem, String filename,
                           Long fileSize, String status, Instant createdAt,
                           Instant completedAt, String processingNode, Integer retryCount) {
            this.id = id;
            this.sourceSystem = sourceSystem;
            this.filename = filename;
            this.fileSize = fileSize;
            this.status = status;
            this.createdAt = createdAt;
            this.completedAt = completedAt;
            this.processingNode = processingNode;
            this.retryCount = retryCount;
        }
    }

    public static class BulkReprocessRequest {
        public List<Long> transferIds;
        public String reason;
    }
}