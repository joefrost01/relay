package com.lbg.markets.surveillance.relay.model;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Status enumeration for file transfer lifecycle.
 * Represents all possible states a file transfer can be in.
 */
public enum TransferStatus {

    /**
     * File has been detected in the source system but not yet processed.
     * Initial state for all transfers.
     */
    DETECTED("Detected", "File detected and awaiting processing", true, false, false),

    /**
     * File is waiting for dependencies or preconditions to be met.
     * E.g., waiting for a .done file or scheduled time.
     */
    WAITING("Waiting", "Waiting for ready condition", true, false, false),

    /**
     * File has been queued for processing.
     * Intermediate state between detection and processing.
     */
    QUEUED("Queued", "Queued for processing", true, false, false),

    /**
     * File validation is in progress.
     * Checking checksums, format, content rules.
     */
    VALIDATING("Validating", "Performing validation checks", false, true, false),

    /**
     * File validation failed but can be retried.
     * Temporary validation failure.
     */
    VALIDATION_FAILED("Validation Failed", "Validation checks failed", true, false, true),

    /**
     * File is currently being transferred to GCS.
     * Active transfer in progress.
     */
    PROCESSING("Processing", "Transfer in progress", false, true, false),

    /**
     * Transfer is being retried after a failure.
     * Automatic retry in progress.
     */
    RETRYING("Retrying", "Retrying transfer", false, true, false),

    /**
     * File transfer completed successfully.
     * Terminal success state.
     */
    COMPLETED("Completed", "Transfer completed successfully", false, false, false),

    /**
     * File transfer failed after all retry attempts.
     * Terminal failure state.
     */
    FAILED("Failed", "Transfer failed", false, false, true),

    /**
     * File was rejected due to validation or policy rules.
     * Terminal rejection state - will not be retried.
     */
    REJECTED("Rejected", "File rejected by validation rules", false, false, true),

    /**
     * File is in quarantine due to suspicious content or validation issues.
     * Requires manual intervention.
     */
    QUARANTINED("Quarantined", "File quarantined for review", false, false, true),

    /**
     * Transfer was cancelled by user or system.
     * Terminal cancellation state.
     */
    CANCELLED("Cancelled", "Transfer cancelled", false, false, false),

    /**
     * File has been archived after successful processing.
     * Post-processing state.
     */
    ARCHIVED("Archived", "File archived after processing", false, false, false),

    /**
     * File has been deleted from source after successful processing.
     * Post-processing cleanup state.
     */
    DELETED("Deleted", "Source file deleted after processing", false, false, false),

    /**
     * Transfer is on hold pending manual intervention.
     * Requires administrator action.
     */
    HOLD("On Hold", "Transfer on hold", false, false, false),

    /**
     * File has been marked for reprocessing.
     * Will be picked up again by the processor.
     */
    REPROCESS_REQUESTED("Reprocess Requested", "Marked for reprocessing", true, false, false),

    /**
     * File is being reprocessed after manual request.
     * Active reprocessing state.
     */
    REPROCESSING("Reprocessing", "Reprocessing in progress", false, true, false),

    /**
     * Transfer was skipped due to policy or configuration.
     * E.g., file too old, wrong format, duplicate.
     */
    SKIPPED("Skipped", "Transfer skipped", false, false, false),

    /**
     * File is being transformed before upload.
     * E.g., compression, encryption, format conversion.
     */
    TRANSFORMING("Transforming", "Applying transformations", false, true, false),

    /**
     * Unknown or undefined status.
     * Should not occur in normal operation.
     */
    UNKNOWN("Unknown", "Status unknown", false, false, true);

    // ============ Properties ============

    private final String displayName;
    private final String description;
    private final boolean isPending;
    private final boolean isActive;
    private final boolean isError;

    /**
     * Constructor for TransferStatus.
     */
    TransferStatus(String displayName, String description,
                   boolean isPending, boolean isActive, boolean isError) {
        this.displayName = displayName;
        this.description = description;
        this.isPending = isPending;
        this.isActive = isActive;
        this.isError = isError;
    }

    // ============ Getters ============

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }

    public boolean isPending() {
        return isPending;
    }

    public boolean isActive() {
        return isActive;
    }

    public boolean isError() {
        return isError;
    }

    public boolean isTerminal() {
        return !isPending && !isActive;
    }

    public boolean isSuccessful() {
        return this == COMPLETED || this == ARCHIVED || this == DELETED;
    }

    public boolean isRetryable() {
        return this == FAILED || this == VALIDATION_FAILED;
    }

    public boolean requiresIntervention() {
        return this == QUARANTINED || this == HOLD || this == REJECTED;
    }

    // ============ State Transition Validation ============

    /**
     * Check if transition to another status is valid.
     */
    public boolean canTransitionTo(TransferStatus newStatus) {
        if (this == newStatus) {
            return true; // Same status is always valid
        }

        return switch (this) {
            case DETECTED -> Set.of(
                    WAITING, QUEUED, VALIDATING, PROCESSING,
                    CANCELLED, SKIPPED, HOLD
            ).contains(newStatus);

            case WAITING -> Set.of(
                    QUEUED, VALIDATING, PROCESSING,
                    CANCELLED, SKIPPED, FAILED
            ).contains(newStatus);

            case QUEUED -> Set.of(
                    VALIDATING, PROCESSING, CANCELLED,
                    HOLD, FAILED
            ).contains(newStatus);

            case VALIDATING -> Set.of(
                    PROCESSING, VALIDATION_FAILED, REJECTED,
                    QUARANTINED, TRANSFORMING
            ).contains(newStatus);

            case VALIDATION_FAILED -> Set.of(
                    VALIDATING, REJECTED, QUARANTINED,
                    CANCELLED, FAILED
            ).contains(newStatus);

            case PROCESSING -> Set.of(
                    COMPLETED, FAILED, RETRYING,
                    CANCELLED, HOLD
            ).contains(newStatus);

            case RETRYING -> Set.of(
                    PROCESSING, COMPLETED, FAILED, CANCELLED
            ).contains(newStatus);

            case COMPLETED -> Set.of(
                    ARCHIVED, DELETED, REPROCESS_REQUESTED
            ).contains(newStatus);

            case FAILED -> Set.of(
                    RETRYING, REPROCESS_REQUESTED, CANCELLED,
                    QUARANTINED
            ).contains(newStatus);

            case REJECTED -> Set.of(
                    REPROCESS_REQUESTED, DELETED
            ).contains(newStatus);

            case QUARANTINED -> Set.of(
                    VALIDATING, PROCESSING, REJECTED,
                    DELETED, REPROCESS_REQUESTED
            ).contains(newStatus);

            case CANCELLED -> Set.of(
                    REPROCESS_REQUESTED, DELETED
            ).contains(newStatus);

            case ARCHIVED -> Set.of(
                    DELETED, REPROCESS_REQUESTED
            ).contains(newStatus);

            case DELETED -> Set.of(
                    REPROCESS_REQUESTED
            ).contains(newStatus);

            case HOLD -> Set.of(
                    QUEUED, PROCESSING, CANCELLED,
                    REJECTED, REPROCESS_REQUESTED
            ).contains(newStatus);

            case REPROCESS_REQUESTED -> Set.of(
                    QUEUED, REPROCESSING, CANCELLED
            ).contains(newStatus);

            case REPROCESSING -> Set.of(
                    COMPLETED, FAILED, CANCELLED
            ).contains(newStatus);

            case SKIPPED -> Set.of(
                    REPROCESS_REQUESTED, DELETED
            ).contains(newStatus);

            case TRANSFORMING -> Set.of(
                    PROCESSING, FAILED, CANCELLED
            ).contains(newStatus);

            case UNKNOWN -> true; // Can transition to any state
        };
    }

    /**
     * Validate a status transition and throw exception if invalid.
     */
    public void validateTransition(TransferStatus newStatus) {
        if (!canTransitionTo(newStatus)) {
            throw new IllegalStateTransitionException(this, newStatus);
        }
    }

    // ============ Status Groups ============

    /**
     * Get all statuses that represent pending work.
     */
    public static List<TransferStatus> getPendingStatuses() {
        return Arrays.stream(values())
                .filter(TransferStatus::isPending)
                .toList();
    }

    /**
     * Get all statuses that represent active processing.
     */
    public static List<TransferStatus> getActiveStatuses() {
        return Arrays.stream(values())
                .filter(TransferStatus::isActive)
                .toList();
    }

    /**
     * Get all statuses that represent errors.
     */
    public static List<TransferStatus> getErrorStatuses() {
        return Arrays.stream(values())
                .filter(TransferStatus::isError)
                .toList();
    }

    /**
     * Get all terminal statuses (no further automatic processing).
     */
    public static List<TransferStatus> getTerminalStatuses() {
        return Arrays.stream(values())
                .filter(TransferStatus::isTerminal)
                .toList();
    }

    /**
     * Get statuses that should be processed by the orchestrator.
     */
    public static List<TransferStatus> getProcessableStatuses() {
        return List.of(
                DETECTED, WAITING, QUEUED,
                REPROCESS_REQUESTED, VALIDATION_FAILED
        );
    }

    /**
     * Get statuses that count towards SLA metrics.
     */
    public static List<TransferStatus> getSlaStatuses() {
        return List.of(
                COMPLETED, FAILED, REJECTED
        );
    }

    // ============ Utility Methods ============

    /**
     * Parse status from string, case-insensitive.
     */
    public static TransferStatus fromString(String status) {
        if (status == null) {
            return UNKNOWN;
        }

        try {
            return valueOf(status.toUpperCase().replace(" ", "_"));
        } catch (IllegalArgumentException e) {
            // Try to match by display name
            return Arrays.stream(values())
                    .filter(s -> s.displayName.equalsIgnoreCase(status))
                    .findFirst()
                    .orElse(UNKNOWN);
        }
    }

    /**
     * Get color code for UI display.
     */
    public String getColorCode() {
        if (isError) return "#dc3545";      // Red
        if (isActive) return "#007bff";     // Blue
        if (isPending) return "#ffc107";    // Yellow
        if (isSuccessful()) return "#28a745"; // Green
        if (this == CANCELLED) return "#6c757d"; // Gray
        return "#17a2b8";                   // Cyan (info)
    }

    /**
     * Get icon name for UI display.
     */
    public String getIconName() {
        return switch (this) {
            case DETECTED -> "file-plus";
            case WAITING -> "clock";
            case QUEUED -> "list";
            case VALIDATING -> "check-circle";
            case VALIDATION_FAILED -> "x-circle";
            case PROCESSING -> "upload";
            case RETRYING -> "refresh-cw";
            case COMPLETED -> "check";
            case FAILED -> "x";
            case REJECTED -> "slash";
            case QUARANTINED -> "alert-triangle";
            case CANCELLED -> "x-octagon";
            case ARCHIVED -> "archive";
            case DELETED -> "trash";
            case HOLD -> "pause-circle";
            case REPROCESS_REQUESTED -> "repeat";
            case REPROCESSING -> "refresh-cw";
            case SKIPPED -> "skip-forward";
            case TRANSFORMING -> "tool";
            case UNKNOWN -> "help-circle";
        };
    }

    /**
     * Get priority for processing order.
     * Lower number = higher priority.
     */
    public int getPriority() {
        return switch (this) {
            case REPROCESS_REQUESTED -> 1;
            case RETRYING -> 2;
            case VALIDATION_FAILED -> 3;
            case QUEUED -> 4;
            case WAITING -> 5;
            case DETECTED -> 6;
            default -> 99;
        };
    }

    @Override
    public String toString() {
        return displayName;
    }
}

