package com.balh.oms.fix;

/**
 * Table names for QuickFIX/J {@link quickfix.JdbcStore} (Flyway {@code V9__oms_fix_session_store.sql}).
 */
public final class FixJdbcSessionSchema {

    public static final String SESSIONS_TABLE = "oms_fix_sessions";
    public static final String MESSAGES_TABLE = "oms_fix_messages";

    private FixJdbcSessionSchema() {}
}
