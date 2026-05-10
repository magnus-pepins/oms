package com.balh.oms.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresJdbcUrlEquivalenceTest {

    @Test
    void localhostAndLoopbackSame() {
        assertThat(PostgresJdbcUrlEquivalence.isSameLogicalDatabase(
                        "jdbc:postgresql://localhost:5432/oms",
                        "jdbc:postgresql://127.0.0.1:5432/oms"))
                .isTrue();
    }

    @Test
    void ignoresQueryParamsOnSessionUrl() {
        assertThat(PostgresJdbcUrlEquivalence.isSameLogicalDatabase(
                        "jdbc:postgresql://127.0.0.1:5432/oms",
                        "jdbc:postgresql://127.0.0.1:5432/oms?loggerLevel=OFF&currentSchema=public"))
                .isTrue();
    }

    @Test
    void differentDatabaseNotSame() {
        assertThat(PostgresJdbcUrlEquivalence.isSameLogicalDatabase(
                        "jdbc:postgresql://127.0.0.1:5432/oms",
                        "jdbc:postgresql://127.0.0.1:5432/other"))
                .isFalse();
    }
}
