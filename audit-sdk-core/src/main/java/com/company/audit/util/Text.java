package com.company.audit.util;

/**
 * Minimal string helpers, so the framework-neutral core needs no Spring
 * {@code StringUtils} (and therefore no Spring dependency).
 */
public final class Text {

    private Text() {
    }

    /** @return true if {@code s} is non-null and contains at least one non-whitespace char. */
    public static boolean hasText(String s) {
        return s != null && !s.isBlank();
    }
}
