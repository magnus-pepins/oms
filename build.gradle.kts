plugins {
    java
    id("org.springframework.boot") version "3.3.4"
    id("io.spring.dependency-management") version "1.1.6"
}

group = "com.balh"
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

val quickfixjVersion = "2.3.2"

/**
 * Spring Test default context cache is 32; cap growth so fewer Hikari pools pin the integration
 * Postgres. Keep (this value × primary Hikari `maximum-pool-size` in `application-test.yaml`, plus
 * any second pools such as `oms.fix.session-jdbc-pool-max-size`) under the server `max_connections`
 * budget. Override with env `SPRING_TEST_CONTEXT_CACHE_MAX_SIZE` (1–64) if needed.
 */
val springTestContextCacheMaxSize =
    System.getenv("SPRING_TEST_CONTEXT_CACHE_MAX_SIZE")?.toIntOrNull()?.coerceIn(1, 64) ?: 10

dependencies {
    // Spring Boot 3 minimal: Web + Actuator + JDBC + Flyway
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-validation")

    // Postgres + Flyway
    runtimeOnly("org.postgresql:postgresql:42.7.4")
    implementation("org.flywaydb:flyway-core:10.20.1")
    implementation("org.flywaydb:flyway-database-postgresql:10.20.1")

    // Micrometer Prometheus
    implementation("io.micrometer:micrometer-registry-prometheus")

    // Chronicle Queue OSS — durable shard-local journal.
    // 2026.2 is the latest stable on Maven Central as of this bootstrap.
    implementation("net.openhft:chronicle-queue:2026.2") {
        // Avoid the OpenHFT slf4j shim; let Spring Boot's logback own slf4j-api.
        exclude(group = "org.slf4j", module = "slf4j-api")
    }

    // LMAX Disruptor — in-process pipelining (used post-PG-commit only)
    implementation("com.lmax:disruptor:4.0.0")

    // Logging — Spring Boot brings logback-classic by default; OK for slice 1.
    implementation("org.slf4j:slf4j-api:2.0.16")

    // Hashing for PII-safe metric labels
    implementation("net.openhft:zero-allocation-hashing:0.16")

    // NATS JetStream fanout (slice 1.5+; gated by oms.events.nats.enabled)
    implementation("io.nats:jnats:2.20.5")

    // QuickFIX/J — slice 4 outbound/inbound FIX wire (gated by oms.routing.backend=fix + initiator wiring)
    implementation("org.quickfixj:quickfixj-core:$quickfixjVersion")
    implementation("org.quickfixj:quickfixj-messages-fix44:$quickfixjVersion")

    // JSON (Jackson is pulled from spring-web; explicit on the classpath for outbox payload codec)
    implementation("com.fasterxml.jackson.module:jackson-module-parameter-names")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Test
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.testcontainers:testcontainers:1.20.3")
    testImplementation("org.testcontainers:postgresql:1.20.3")
    testImplementation("org.testcontainers:junit-jupiter:1.20.3")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("org.awaitility:awaitility:4.2.2")
    // WireMock 3 core does not ship an HTTP server; Jetty 12 extension is required on the test classpath.
    testImplementation("org.wiremock:wiremock-jetty12:3.9.1")
}

tasks.withType<Test> {
    useJUnitPlatform()
    // Keep test logs deterministic: do not run integration suites in parallel until we enable Postgres pooling.
    systemProperty("junit.jupiter.execution.parallel.enabled", "false")
    systemProperty("spring.test.context.cache.maxSize", springTestContextCacheMaxSize.toString())
    testLogging {
        events("failed", "skipped")
        showExceptions = true
        showCauses = true
        showStackTraces = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    archiveBaseName.set("oms")
}
