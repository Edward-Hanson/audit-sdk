package com.company.audit.core;

/**
 * Abstraction over "is a transaction in progress, and if so run this after it commits."
 *
 * <p>This is the ONLY seam where audit publishing needs to know about transactions. Keeping
 * it an interface lets the {@code audit-sdk-core} module stay completely free of Spring:
 * each starter module supplies an implementation backed by Spring's
 * {@code TransactionSynchronizationManager}, compiled against its own Spring generation.
 *
 * @see AuditClient
 */
public interface TransactionHook {

    /** @return true if a transaction is currently active on the calling thread. */
    boolean isActive();

    /**
     * Register {@code action} to run after the current transaction commits. Must only be
     * called when {@link #isActive()} is true. If the transaction rolls back, {@code action}
     * is never run.
     */
    void afterCommit(Runnable action);

    /**
     * A hook that reports no active transaction — every publish happens immediately. Used
     * when there is no transaction infrastructure to consult (e.g. the disabled client).
     */
    static TransactionHook none() {
        return NoTransactionHook.INSTANCE;
    }
}
