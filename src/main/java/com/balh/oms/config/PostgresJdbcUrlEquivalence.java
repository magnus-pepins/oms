package com.balh.oms.config;

import java.util.Locale;

/**
 * Compare Postgres JDBC URLs for <strong>logical</strong> same-database identity so we do not open a second pool
 * when {@code spring.datasource.url} and {@code oms.fix.session-jdbc-url} differ only cosmetically (query params,
 * {@code localhost} vs {@code 127.0.0.1}).
 */
public final class PostgresJdbcUrlEquivalence {

    private static final String PREFIX = "jdbc:postgresql://";

    private PostgresJdbcUrlEquivalence() {}

    /** True when both URLs point at the same Postgres database for pool-reuse decisions. */
    public static boolean isSameLogicalDatabase(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        return logicalKey(a).equals(logicalKey(b));
    }

    static String logicalKey(String url) {
        String u = url.trim();
        if (u.isEmpty()) {
            return "";
        }
        if (!u.regionMatches(true, 0, PREFIX, 0, PREFIX.length())) {
            return u.toLowerCase(Locale.ROOT);
        }
        return keyPostgresql(u.substring(PREFIX.length()));
    }

    private static String keyPostgresql(String afterScheme) {
        int slash = afterScheme.indexOf('/');
        String authority = slash >= 0 ? afterScheme.substring(0, slash) : afterScheme;
        String dbPart = slash >= 0 ? afterScheme.substring(slash + 1) : "";
        int q = dbPart.indexOf('?');
        String database = (q >= 0 ? dbPart.substring(0, q) : dbPart).toLowerCase(Locale.ROOT);
        String auth = authority.toLowerCase(Locale.ROOT);
        if ("localhost".equals(auth)) {
            auth = "127.0.0.1";
        } else if (auth.startsWith("localhost:")) {
            auth = "127.0.0.1:" + auth.substring("localhost:".length());
        }
        return auth + "/" + database;
    }
}
