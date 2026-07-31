package com.company.audit.config;

import com.company.audit.core.TransactionHook;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Spring-backed {@link TransactionHook}: consults the current thread's
 * {@code TransactionSynchronizationManager} so an audit publish is deferred until the
 * surrounding {@code @Transactional} method commits (and dropped if it rolls back).
 *
 * <p>This is the ONLY place the SDK touches Spring's transaction API. It is compiled once per
 * Spring Boot generation (this source is shared by the {@code audit-sdk-spring-boot-3} and
 * {@code audit-sdk-spring-boot-4} modules), so it always binds against the matching Spring
 * Framework version — 6.x under Boot 3, 7.x under Boot 4.
 */
public class SpringTransactionHook implements TransactionHook {

    @Override
    public boolean isActive() {
        return TransactionSynchronizationManager.isSynchronizationActive();
    }

    @Override
    public void afterCommit(Runnable action) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                action.run();
            }
        });
    }
}
