import org.gradle.api.JavaVersion

plugins {
    java
    id("com.google.protobuf") version "0.9.4"
    id("org.springframework.boot") version "3.3.4"
    id("io.spring.dependency-management") version "1.1.6"
    // Phase 4 slice 4f: JMH benchmarks for cluster wire-format hot paths.
    // me.champeau.jmh discovers @Benchmark classes under src/jmh, generates the JMH harness, and
    // exposes a `jmh` Gradle task. Profilers (e.g. `gc`, `stack`) wire via `jmh.profilers`.
    id("me.champeau.jmh") version "0.7.2"
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

    // Caffeine — JVM-local cache for the (balanceId -> identityId) binding (Phase 4 Tier 2.5
    // phase D-8). Version managed by the Spring Boot 3.3 BOM (3.1.8 at the time of writing);
    // pin via the BOM rather than explicitly so future Boot upgrades carry it forward.
    implementation("com.github.ben-manes.caffeine:caffeine")

    // HdrHistogram — Phase 4 slice 4e cluster bench harness records commit-round-trip
    // latencies into a lossless high-dynamic-range histogram so p50/p95/p99/p99.9 are
    // accurate to the underlying recording resolution (no Micrometer-style summary
    // bucket truncation at the tail). Tiny (~150 KB) jar, zero transitive deps.
    implementation("org.hdrhistogram:HdrHistogram:2.2.2")

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

/**
 * Phase 4 slice 4f: JMH config. Benchmarks live under {@code src/jmh/java}. Defaults are tight
 * enough for a single-laptop-second iteration cycle (3 forks, 3+5 warmup/measurement iterations,
 * 1 s each) while still being statistically meaningful. Override per-run via `-PjmhInclude` or
 * env. The {@code -prof gc} profiler is the workhorse for slice 4f's allocation audit; engage it
 * with `./gradlew jmh -Pjmh.profilers=gc` (passed through via the {@code profilers} property).
 */
jmh {
    warmupIterations.set(3)
    iterations.set(5)
    fork.set(3)
    timeOnIteration.set("1s")
    warmup.set("1s")
    benchmarkMode.set(listOf("avgt"))
    timeUnit.set("ns")
    jvmArgs.set(lowLatencyJvmModuleOpens)
    val profilersProp = project.findProperty("jmh.profilers")?.toString()
    if (!profilersProp.isNullOrBlank()) {
        profilers.set(profilersProp.split(",").map { it.trim() }.filter { it.isNotEmpty() })
    }
    val includeProp = project.findProperty("jmhInclude")?.toString()
    if (!includeProp.isNullOrBlank()) {
        includes.set(listOf(includeProp))
    }
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("reports/jmh/results.json"))
    humanOutputFile.set(layout.buildDirectory.file("reports/jmh/human.txt"))
}

// JMH plugin builds a fat-jar including test classes, which in this project exceeds the 65535
// entry limit of standard ZIP — enable zip64 so packaging succeeds.
tasks.named<Jar>("jmhJar") {
    isZip64 = true
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
 * Phase 4 slice 4e (cluster bench harness): boot a single-node in-process Aeron Cluster, run a
 * steady-state AcceptOrderCommand offer loop at OMS_BENCH_THROUGHPUT_OPS_PER_S for
 * OMS_BENCH_DURATION_S, capture HdrHistogram of commit-round-trip latencies, write summary.md +
 * histogram.hgrm under OMS_BENCH_REPORT_DIR (default build/reports/cluster-bench/<timestamp>).
 * See system-documentation/plans/oms-aeron-cluster-substrate.md § Phase 4 slice 4e.
 */
tasks.register<JavaExec>("clusterBench") {
    group = "verification"
    description =
        "Run the OMS cluster bench harness (HdrHistogram of commit-round-trip)." +
                " Env: OMS_BENCH_DURATION_S, OMS_BENCH_THROUGHPUT_OPS_PER_S, OMS_BENCH_WARMUP_S," +
                " OMS_BENCH_TIMEOUT_MS, OMS_BENCH_REPORT_DIR, OMS_BENCH_AERON_DIR_BASE." +
                " GC selection (slice 4g): -PgcMode=g1|zgc|shenandoah." +
                " Override the JVM (e.g. for a Temurin 21 runner that ships ZGC + Shenandoah):" +
                " -PclusterBenchJava=/absolute/path/to/java."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.balh.oms.cluster.bench.OmsClusterBenchHarness")
    jvmArgs(lowLatencyJvmModuleOpens)

    // Slice 4g — GC selection. Default is "default": let the JVM pick (which is G1 on JDK 21+).
    // Each named GC sets ONLY the toggles needed to switch collector; bench-relevant tuning
    // (heap size, NUMA, AlwaysPreTouch) is intentionally NOT set here so the comparison
    // measures the GC's out-of-the-box behaviour rather than an operator-tuned configuration.
    // The first slice 4g pass is "is one of these obviously bad on the cluster apply path"
    // — tuning passes can follow once we have a baseline.
    val gcMode = (project.findProperty("gcMode") as String?)?.lowercase() ?: "default"
    when (gcMode) {
        "default" -> { /* no-op — JVM default (G1 on JDK 21+) */ }
        "g1" -> jvmArgs("-XX:+UseG1GC")
        "zgc" -> jvmArgs("-XX:+UseZGC", "-XX:+ZGenerational")
        "shenandoah" -> jvmArgs("-XX:+UseShenandoahGC")
        else -> throw GradleException(
            "Unknown gcMode='$gcMode'. Expected one of: default, g1, zgc, shenandoah.")
    }

    // Bench reports default to build/reports/cluster-bench/<ts>; under -PgcMode let the harness
    // see the GC label so summaries are self-describing without having to grep the JVM args.
    if (gcMode != "default") {
        environment("OMS_BENCH_GC_LABEL", gcMode)
    }

    // Allow swapping the JVM (without changing the project toolchain) — useful when the
    // toolchain JDK doesn't ship ZGC / Shenandoah (e.g. JetBrains Runtime). Only applied when
    // the property is set; CI / Linux runners can pass `-PclusterBenchJava=$JAVA_HOME/bin/java`
    // pointing at a Temurin 21 install that ships all three collectors.
    val clusterBenchJava = project.findProperty("clusterBenchJava") as String?
    if (!clusterBenchJava.isNullOrBlank()) {
        executable(clusterBenchJava)
    }
}

/**
 * Phase 4 slice 4k (ingress burst tool): JDK 21 HttpClient + virtual threads firing
 * concurrent {@code POST /internal/v1/orders} requests at a running OMS ingress-replica.
 * Records HTTP RTT into HdrHistogram and prints p50/p95/p99/p999. Pair with
 * {@code scripts/benchmark/burst-ingress-orders.sh} for pre/post Prometheus scrapes.
 * Env: OMS_INTERNAL_API_KEY (required), OMS_BURST_URL, OMS_BURST_TOTAL,
 * OMS_BURST_CONCURRENCY, OMS_BURST_RPS_CAP, OMS_BURST_ACCOUNT_POOL, OMS_BURST_INSTRUMENT,
 * OMS_BURST_QUANTITY, OMS_BURST_LIMIT_PRICE, OMS_BURST_REQUEST_TIMEOUT_S, OMS_BURST_WARMUP.
 * See system-documentation/plans/oms-clean-latency-story.plan.md § Slice 4k.
 */
tasks.register<JavaExec>("bootRunBurst") {
    group = "verification"
    description =
        "Burst POST /internal/v1/orders against a running ingress-replica (slice 4k)." +
                " Env: OMS_INTERNAL_API_KEY (required), OMS_BURST_URL, OMS_BURST_TOTAL," +
                " OMS_BURST_CONCURRENCY, OMS_BURST_RPS_CAP, OMS_BURST_ACCOUNT_POOL," +
                " OMS_BURST_INSTRUMENT, OMS_BURST_QUANTITY, OMS_BURST_LIMIT_PRICE," +
                " OMS_BURST_REQUEST_TIMEOUT_S, OMS_BURST_WARMUP."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.balh.oms.ingress.bench.IngressBurstMain")
    jvmArgs(lowLatencyJvmModuleOpens)
    standardInput = System.`in`
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
