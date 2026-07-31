package com.company.audit.core;

/**
 * A {@link TransactionHook} that always reports "no transaction". {@link #afterCommit} is
 * never legally called (guarded by {@link #isActive()} returning false); if it ever is, the
 * action runs immediately as a safe fallback.
 */
final class NoTransactionHook implements TransactionHook {

    static final NoTransactionHook INSTANCE = new NoTransactionHook();

    private NoTransactionHook() {
    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public void afterCommit(Runnable action) {
        action.run();
    }
}
