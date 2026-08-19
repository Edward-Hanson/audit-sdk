package com.company.audit.model;

/**
 * The controlled vocabulary of auditable actions. Auditing targets state/data
 * changes — reads are intentionally not represented.
 *
 * <p>Serialized to Kafka as its name (e.g. {@code "CREATE"}). If a new state-change
 * action is needed, add it here and release a new SDK version so the vocabulary stays
 * governed across all services.
 */
public enum AuditAction {

    // Core data lifecycle
    CREATE,
    UPDATE,
    DELETE,

    // Archival lifecycle
    ARCHIVE,
    UNARCHIVE,

    // Activation lifecycle
    ACTIVATE,
    DEACTIVATE,

    // Application / request lifecycle
    APPLY,

    // Approval / decision workflow
    SUBMIT,
    REVIEW,
    RECOMMEND,
    APPROVE,
    REJECT,
    DENY,
    DEFER,
    CANCEL,

    // Ownership / assignment
    ASSIGN,
    UNASSIGN,

    // Access / state locks
    LOCK,
    UNLOCK,
    BLOCK,
    SUSPEND,
    RESTORE,

    // Publication lifecycle
    PUBLISH,
    UNPUBLISH,

    // Permission changes
    GRANT,
    REVOKE,

    // Funds movement
    DISBURSE,
    PAY,

    // Post-issuance servicing
    SERVICE
}
