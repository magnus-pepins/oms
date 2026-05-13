import org.gradle.api.JavaVersion

plugins {
    java
    id("com.google.protobuf") version "0.9.4"
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

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-parameters")
}

repositories {
    mavenCentral()
}

val quickfixjVersion = "2.3.2"

/**
 * JVM `--add-opens` / `--add-exports` for low-latency JDK-internals access required by
 * Aeron / Agrona (off-heap buffers, atomic ops on `Unsafe`). Without them, the relevant
 * `bootRun` / cluster-node JVM fails on JDK 21 with `IllegalAccessException` against
 * `sun.nio.ch.*`, `jdk.internal.misc.*`, or `sun.misc.Unsafe`.
 */
val lowLatencyJvmModuleOpens =
    listOf(
        "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
    )

/**
 * Spring Test default context cache is 32; cap growth so fewer Hikari pools pin the integration
 * Postgres. Keep (this value × primary Hikari `maximum-pool-size` in `application-test.yaml`, plus
 * any second pools such as `oms.fix.session-jdbc-pool-max-size`) under the server `max_connections`
 * budget. Override with env `SPRING_TEST_CONTEXT_CACHE_MAX_SIZE` (1–64) if needed.
 */
val springTestContextCacheMaxSize =
    System.getenv("SPRING_TEST_CONTEXT_CACHE_MAX_SIZE")?.toIntOrNull()?.coerceIn(1, 64) ?: 10

/** Protobuf compiler + runtime (legacy ProtoBuf descriptors used by gRPC ingress). */
val protobufJavaVersion = "4.29.3"

/** gRPC Java stack (ingress); keep aligned with protoc-gen-grpc-java. */
val grpcJavaVersion = "1.68.2"

val openTelemetryVersion = "1.51.0"
/** Must match OTel prometheus exporter transitive; Micrometer 1.13 pins older 1.2.x without this. */
val prometheusMetricsBomVersion = "1.3.8"

/**
 * Aeron family — substrate for the cluster (replaces Chronicle per ADR 0001).
 *
 * `io.aeron:aeron-cluster` transitively pulls in `aeron-archive`, `aeron-driver`, `aeron-client`,
 * and `agrona`. We pin Agrona explicitly for reproducibility and to keep IDE navigation clean.
 *
 * `1.48.0` is the latest stable on Maven Central as of the ADR. Phase 0 spike confirms or updates.
 */
val aeronVersion = "1.48.0"

/** Agrona — off-heap buffers, primitive collections (`Long2ObjectHashMap`, `DirectBuffer`). */
val agronaVersion = "2.2.1"

dependencies {
    // OpenTelemetry metrics (optional; gated by oms.otel.metrics-enabled) — Prometheus scrape for histograms (p50/p95).
    // Prometheus exporter remains on an -alpha train separate from the stable BOM line.
    implementation(platform("io.opentelemetry:opentelemetry-bom:$openTelemetryVersion"))
    implementation("io.opentelemetry:opentelemetry-api")
    implementation("io.opentelemetry:opentelemetry-sdk")
    implementation("io.opentelemetry:opentelemetry-sdk-metrics")
    implementation("io.opentelemetry:opentelemetry-exporter-prometheus:${openTelemetryVersion}-alpha")

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

    // Aeron family — substrate for the clustered state machine (ADR 0001).
    //
    // `aeron-cluster` pulls in `aeron-archive`, `aeron-driver`, `aeron-client` transitively;
    // listed explicitly here so navigation and compile-time visibility don't depend on the
    // transitive graph.
    implementation("io.aeron:aeron-cluster:$aeronVersion")
    implementation("io.aeron:aeron-archive:$aeronVersion")
    implementation("io.aeron:aeron-driver:$aeronVersion")
    implementation("io.aeron:aeron-client:$aeronVersion")
    implementation("org.agrona:agrona:$agronaVersion")

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

    implementation("com.google.protobuf:protobuf-java:$protobufJavaVersion")

    implementation("io.grpc:grpc-netty-shaded:$grpcJavaVersion")
    implementation("io.grpc:grpc-protobuf:$grpcJavaVersion")
    implementation("io.grpc:grpc-stub:$grpcJavaVersion")
    implementation("javax.annotation:javax.annotation-api:1.3.2")

    // Test
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.testcontainers:testcontainers:1.20.3")
    testImplementation("org.testcontainers:postgresql:1.20.3")
    testImplementation("org.testcontainers:junit-jupiter:1.20.3")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("org.awaitility:awaitility:4.2.2")

    testImplementation("com.google.protobuf:protobuf-java:$protobufJavaVersion")
    // WireMock 3 core does not ship an HTTP server; Jetty 12 extension is required on the test classpath.
    testImplementation("org.wiremock:wiremock-jetty12:3.9.1")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufJavaVersion"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcJavaVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
    jvmArgs(lowLatencyJvmModuleOpens)
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

tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    jvmArgs(lowLatencyJvmModuleOpens)
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    archiveBaseName.set("oms")
}

/** Horizontal ingress replica — same app as bootRun with Spring profile oms-ingress-replica (order accept; submits AcceptOrderCommand through the cluster client; see OmsIngressReplicaBootstrap). */
tasks.register<org.springframework.boot.gradle.tasks.run.BootRun>("bootRunIngressReplica") {
    group = "application"
    description =
        "Run OMS with oms-ingress-replica profile (horizontal ingress; application-oms-ingress-replica.yaml)."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.balh.oms.OmsIngressReplicaBootstrap")
    jvmArgs(lowLatencyJvmModuleOpens)
}

tasks.register<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJarIngressReplica") {
    group = "build"
    description =
        "Executable JAR for ingress-replica entry (Start-Class OmsIngressReplicaBootstrap; classifier ingress-replica)."
    mainClass.set("com.balh.oms.OmsIngressReplicaBootstrap")
    archiveClassifier.set("ingress-replica")
    archiveBaseName.set("oms")
    targetJavaVersion.set(JavaVersion.VERSION_21)
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJarIngressReplica").configure {
    shouldRunAfter(tasks.named("bootJar"))
    duplicatesStrategy = org.gradle.api.file.DuplicatesStrategy.EXCLUDE
}

/**
 * ADR 0001 / topology-aeron-cluster Phase 1: cluster-node JVM.
 *
 * Boots an Aeron MediaDriver + Archive + ConsensusModule + ClusteredServiceContainer hosting
 * `OmsAdmissionClusteredService`. **Do not** combine on the same JVM with `oms-ingress-replica`,
 * `oms-postgres-projector`, or `oms-fix-egress` (TopologyWorkerProfiles).
 */
tasks.register<org.springframework.boot.gradle.tasks.run.BootRun>("bootRunClusterNode") {
    group = "application"
    description =
        "Run OMS cluster-node JVM (Aeron Cluster, OmsAdmissionClusteredService; ADR 0001)."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.balh.oms.OmsClusterNodeBootstrap")
    jvmArgs(lowLatencyJvmModuleOpens)
}

tasks.register<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJarClusterNode") {
    group = "build"
    description =
        "Executable JAR for cluster-node entry (Start-Class OmsClusterNodeBootstrap; classifier cluster-node)."
    mainClass.set("com.balh.oms.OmsClusterNodeBootstrap")
    archiveClassifier.set("cluster-node")
    archiveBaseName.set("oms")
    targetJavaVersion.set(JavaVersion.VERSION_21)
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJarClusterNode").configure {
    shouldRunAfter(tasks.named("bootJar"))
    duplicatesStrategy = org.gradle.api.file.DuplicatesStrategy.EXCLUDE
}

/**
 * Phase 2 of `system-documentation/plans/oms-aeron-cluster-substrate.md`: oms-postgres-projector JVM.
 *
 * Subscribes to the cluster log and writes Postgres projection rows. Slice 2a is a skeleton — the bean
 * activates the role but does not consume events yet. Slice 2b wires consumption.
 */
tasks.register<org.springframework.boot.gradle.tasks.run.BootRun>("bootRunPostgresProjector") {
    group = "application"
    description =
        "Run OMS with oms-postgres-projector profile (Phase 2 cluster→Postgres projector; application-oms-postgres-projector.yaml)."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.balh.oms.OmsPostgresProjectorBootstrap")
    jvmArgs(lowLatencyJvmModuleOpens)
}

tasks.register<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJarPostgresProjector") {
    group = "build"
    description =
        "Executable JAR for postgres-projector entry (Start-Class OmsPostgresProjectorBootstrap; classifier postgres-projector)."
    mainClass.set("com.balh.oms.OmsPostgresProjectorBootstrap")
    archiveClassifier.set("postgres-projector")
    archiveBaseName.set("oms")
    targetJavaVersion.set(JavaVersion.VERSION_21)
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJarPostgresProjector").configure {
    shouldRunAfter(tasks.named("bootJar"))
    duplicatesStrategy = org.gradle.api.file.DuplicatesStrategy.EXCLUDE
}

/**
 * Phase 3 of `system-documentation/plans/oms-aeron-cluster-substrate.md`: oms-fix-egress JVM.
 *
 * Mirrors `oms-postgres-projector` on the FIX side. Subscribes to the cluster events recording via Aeron Archive
 * replay (slice 3b) and owns the QuickFIX/J SocketInitiator that ships NewOrderSingle to the broker (slices 3b/3d).
 * Slice 3a is a skeleton — the bean activates the role but does not consume events yet, mirroring the
 * postgres-projector cadence so each slice is independently shippable.
 */
tasks.register<org.springframework.boot.gradle.tasks.run.BootRun>("bootRunFixEgress") {
    group = "application"
    description =
        "Run OMS with oms-fix-egress profile (Phase 3 cluster→FIX egress; application-oms-fix-egress.yaml)."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.balh.oms.OmsFixEgressBootstrap")
    jvmArgs(lowLatencyJvmModuleOpens)
}

tasks.register<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJarFixEgress") {
    group = "build"
    description =
        "Executable JAR for fix-egress entry (Start-Class OmsFixEgressBootstrap; classifier fix-egress)."
    mainClass.set("com.balh.oms.OmsFixEgressBootstrap")
    archiveClassifier.set("fix-egress")
    archiveBaseName.set("oms")
    targetJavaVersion.set(JavaVersion.VERSION_21)
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJarFixEgress").configure {
    shouldRunAfter(tasks.named("bootJar"))
    duplicatesStrategy = org.gradle.api.file.DuplicatesStrategy.EXCLUDE
}

/** Minimal FIX 4.4 loopback acceptor for local OMS + HTTP load scripts (see docs/fix-out.md § synthetic traffic). */
tasks.register<JavaExec>("fixLoopbackAcceptor") {
    group = "development"
    description =
        "Start QuickFIX loopback acceptor (auto-fill ER). Env: FIX_ACCEPTOR_PORT, FIX_ACCEPTOR_FILE_STORE, FIX_ACCEPTOR_SESSION_*"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("com.balh.oms.fix.dev.FixLoopbackAcceptorMain")
}

/**
 * Phase 4 slice 4a (operator-driven snapshot): trigger an Aeron Cluster snapshot on a running
 * cluster member. Cluster dir resolved from `OMS_AERON_CLUSTER_DIR` (preferred) or
 * `<OMS_AERON_DIR_BASE>/consensus-module`. See `docs/runbooks/oms-cluster-node-snapshot.md` and
 * `system-documentation/plans/oms-aeron-cluster-substrate.md` § Phase 4 slice 4a.
 */
tasks.register<JavaExec>("clusterSnapshot") {
    group = "application"
    description =
        "Request an Aeron Cluster snapshot via ClusterTool.snapshot(...). Env: OMS_AERON_CLUSTER_DIR or OMS_AERON_DIR_BASE."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.balh.oms.cluster.admin.OmsClusterSnapshotAdminTool")
    jvmArgs(lowLatencyJvmModuleOpens)
}

/**
 * Micrometer's prometheus registry pins {@code io.prometheus:prometheus-metrics-*} 1.2.x while the OTel
 * Prometheus exporter brings 1.3.x textformats — mixed jars cause {@code NoSuchMethodError} at startup.
 */
configurations.configureEach {
    resolutionStrategy.eachDependency {
        if (requested.group == "io.prometheus" && requested.name.startsWith("prometheus-metrics-")) {
            useVersion(prometheusMetricsBomVersion)
            because("Align Prometheus Java metrics libraries for Micrometer registry + OTel exporter")
        }
    }
}
