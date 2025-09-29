package com.lbg.markets.surveillance.relay.model;

/**
 * Exception thrown when an invalid status transition is attempted.
 */
class IllegalStateTransitionException extends RuntimeException {
    private final TransferStatus fromStatus;
    private final TransferStatus toStatus;

    public IllegalStateTransitionException(TransferStatus from, TransferStatus to) {
        super(String.format("Invalid status transition from %s to %s",
                from.getDisplayName(), to.getDisplayName()));
        this.fromStatus = from;
        this.toStatus = to;
    }

    public TransferStatus getFromStatus() {
        return fromStatus;
    }

    public TransferStatus getToStatus() {
        return toStatus;
    }
}
