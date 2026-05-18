/**
 * PM2 ecosystem for the OMS Aeron Cluster demo stack (Wed demo, 2026-05-20).
 *
 * <h2>Roles</h2>
 *
 * Mirrors the role split documented in `OmsProfiles.java`:
 *
 *   1. oms-cluster-node           — Aeron Cluster + OmsAdmissionClusteredService.
 *                                    Source of truth for OMS state; runs the
 *                                    MediaDriver, Archive, ConsensusModule, and
 *                                    ClusteredServiceContainer in one JVM.
 *
 *   2. oms-postgres-projector     — Subscribes to the cluster events recording via
 *                                    Aeron Archive replay; writes orders,
 *                                    executions, control_decisions into Postgres.
 *                                    Owns Flyway migrations.
 *
 *   3. oms-fix-egress             — Subscribes to the same events recording; sends
 *                                    NewOrderSingle / OrderCancelRequest /
 *                                    OrderCancelReplaceRequest to the broker; also
 *                                    submits inbound ExecutionReport /
 *                                    OrderCancelReject back to the cluster as
 *                                    ApplyExecutionReportCommand. Exactly one per
 *                                    FIX route.
 *
 *   4. oms-ingress                — Default profile (no role activation); runs the
 *                                    HTTP / internal/v1/orders ingress including
 *                                    the new POST /cancel + /replace endpoints and
 *                                    the LedgerInflightLifecycleReconciler that
 *                                    closes the loop on user-visible balance flips.
 *
 *   5. oms-fix-loopback-acceptor  — Standalone QuickFIX/J test acceptor
 *                                    (FixLoopbackAcceptorMain). Receives 35=D
 *                                    NewOrderSingle, replies with auto-fill 35=8
 *                                    ExecutionReports. Symbol pattern 'REJECT' on
 *                                    a 35=F cancel triggers 35=9 OrderCancelReject.
 *                                    Spans the test classpath via the gradle
 *                                    `fixLoopbackAcceptor` task so we don't have
 *                                    to ship a separate fat jar.
 *
 * <h2>Why a shared `~/.oms-bench.env` foundation + demo overrides</h2>
 *
 * Pop's slice-4p bench has been running OMS roles against a Supabase pooler at
 * 127.0.0.1:5432 and the ledger-rest-shim at 127.0.0.1:5001 for weeks. Re-deriving
 * those settings here from scratch would risk drift (e.g. wrong PG user, wrong
 * inflight destination balance id). Each app sources `~/.oms-bench.env` via
 * `env_file` so the known-good base is the foundation, and the role-specific
 * `env` block layers the demo-only flags on top:
 *
 *   - OMS_LEDGER_INFLIGHT_LIFECYCLE_RECONCILER_ENABLED=true (V32 reconciler)
 *   - OMS_FIX_AUTO_START=true on oms-fix-egress only
 *   - SPRING_PROFILES_ACTIVE per role
 *
 * <h2>Signal-propagation notes</h2>
 *
 * Cluster JVMs run as `java -jar` (NOT `gradlew bootRun`) so PM2's SIGTERM lands
 * directly on the JVM — same lesson as the 2026-05-17 ledger-cluster fix
 * (gradle wrappers don't propagate SIGTERM into JavaExec children, leaving
 * Aeron mark files locked past PM2's kill_timeout). `kill_timeout: 30000` gives
 * the shutdown hooks time to close the MediaDriver / Archive cleanly.
 *
 * The FIX loopback acceptor IS launched via gradle since it depends on the test
 * runtime classpath. That's safe: it has no Aeron SHM state, so PM2's tree-kill
 * default + a 5 s kill_timeout is enough to drain a QuickFIX SocketAcceptor.
 *
 * <h2>What this ecosystem does NOT do</h2>
 *
 *   - It does NOT start ledger-cluster (already managed by
 *     ~/ledger-cluster/ecosystem.config.cjs on Pop).
 *   - It does NOT start NATS (Pop already has obs-nats at :4222 — re-running
 *     would split the consumer group). `OMS_NATS_ENABLED` stays inherited from
 *     the bench env (false today; demo can flip to true to point at obs-nats).
 *   - It does NOT start the customer-frontend / trading-desk BFFs (those have
 *     their own PM2 entries: customer-frontend / customer-frontend-api on Pop
 *     today, trading-desk to be added separately).
 */

const path = require('path');
const fs = require('fs');

const projectRoot = path.resolve(__dirname);
const logsDir = path.join(projectRoot, 'logs');
try {
  fs.mkdirSync(logsDir, { recursive: true });
} catch (_) {
  // logsDir may already exist (re-run); proceed silently.
}

function logPath(name, suffix) {
  return path.join(logsDir, `${name}${suffix}`);
}

// Mirrors ledger-cluster/ecosystem.config.cjs ROLE_JAR helper so future Gradle
// version bumps only need OMS_VERSION set at deploy time.
const OMS_VERSION = process.env.OMS_VERSION || '0.1.0-SNAPSHOT';
const LIBS_DIR = path.join(projectRoot, 'build', 'libs');
const ROLE_JAR = (classifier) =>
  classifier === ''
    ? path.join(LIBS_DIR, `oms-${OMS_VERSION}.jar`)
    : path.join(LIBS_DIR, `oms-${OMS_VERSION}-${classifier}.jar`);
const JAVA = process.env.OMS_JAVA || 'java';

// Same low-latency module-opens as ledger-cluster — Aeron/Agrona need these on
// JDK 21 to access jdk.internal.misc.* / sun.nio.ch.* / sun.misc.Unsafe.
const LOW_LATENCY_JVM_FLAGS = [
  '--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED',
  '--add-exports=java.base/sun.nio.ch=ALL-UNNAMED',
  '--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED',
  '--add-opens=java.base/java.lang=ALL-UNNAMED',
  '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED',
  '--add-opens=java.base/java.util=ALL-UNNAMED',
  '--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED',
];

// Shared Aeron media-driver IPC root for cluster-node / projector / fix-egress
// so all three mount the same SHM tree. Pinned to OMS's build dir so it does
// NOT collide with ledger-cluster's directory (which lives under its own repo).
const AERON_DIR_BASE =
  process.env.OMS_AERON_DIR_BASE || path.join(projectRoot, 'build', 'aeron-cluster');
const AERON_MEDIA_DRIVER = path.join(AERON_DIR_BASE, 'media-driver');

// Pop's existing slice-4p bench env. Already exports OMS_PG_URL / OMS_LEDGER_BASE_URL /
// OMS_FIX_* / OMS_LEDGER_INFLIGHT_* etc. Each app inherits this then overrides what
// the demo needs. If the file is absent (laptop dev), the parser returns {} and the
// app boots against the application.yaml defaults.
//
// PM2's built-in `env_file` is dotenv-style and silently DROPS bash `export KEY=VAL`
// lines (it parses them as a key literally named `export OMS_PG_USER` rather than
// `OMS_PG_USER`, leaving the JVM with the application.yaml default — exactly the
// "Tenant or user not found" symptom we hit on the first 2026-05-18 bring-up).
// Re-formatting ~/.oms-bench.env to drop `export` would break the launch-bench-stack
// .sh path that dot-sources it. So we parse it here in JS with bash-source semantics
// and inject the result into each app's `env` block.
const BENCH_ENV_FILE =
  process.env.OMS_BENCH_ENV_FILE || path.join(process.env.HOME || '/root', '.oms-bench.env');

/**
 * Bash-source-equivalent env parser. Recognises:
 *
 *   export KEY=VALUE
 *   KEY=VALUE
 *   # comment
 *   <blank line>
 *
 * Strips matching single or double quotes around VALUE. Does NOT do variable
 * interpolation (`$FOO`, `${FOO}`) or command substitution — neither appears in
 * ~/.oms-bench.env today, and silently expanding shell features inside a Node
 * loader is the kind of "looks helpful, breaks unexpectedly" magic this rule set
 * explicitly avoids. If the file ever needs `$()`-style values, swap this for a
 * `bash -c '. file; env'` capture at PM2 start time.
 */
function loadBashEnvFile(filePath) {
  let raw;
  try {
    raw = fs.readFileSync(filePath, 'utf8');
  } catch (err) {
    if (err.code === 'ENOENT') {
      return {};
    }
    throw err;
  }
  const out = {};
  const lineRe = /^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/;
  for (const line of raw.split(/\r?\n/)) {
    if (!line || line.trim().startsWith('#')) continue;
    const m = line.match(lineRe);
    if (!m) continue;
    let value = m[2];
    if (
      (value.startsWith("'") && value.endsWith("'")) ||
      (value.startsWith('"') && value.endsWith('"'))
    ) {
      value = value.slice(1, -1);
    }
    out[m[1]] = value;
  }
  return out;
}

const BENCH_ENV = loadBashEnvFile(BENCH_ENV_FILE);

// BENCH_ENV first, then OMS_AERON_DIR_BASE (so this ecosystem's choice of dir wins
// over any stale entry in ~/.oms-bench.env), then the demo-only reconciler flag.
const COMMON_ENV = {
  ...BENCH_ENV,
  OMS_AERON_DIR_BASE: AERON_DIR_BASE,
  // V32 reconciler is the load-bearing demo bit — Wed flow needs FILLED orders to
  // flip the held debit on the Ledger. Stays off in slice-4p bench (sliced under
  // application.yaml's default `false`), so we layer it here so production deploys
  // stay safe and only the demo gets the new behavior.
  OMS_LEDGER_INFLIGHT_LIFECYCLE_RECONCILER_ENABLED: 'true',
  // Slice-4p bench applied V31 via launch-bench-stack.sh before V32 existed; this
  // PM2 stack now adds V32 alongside V31. On rebuild after we changed V31 (dropped
  // CONCURRENTLY for pgbouncer, see V31 source header), the DB checksum from the
  // pre-rebuild apply no longer matched the new file. Deleting V31 from
  // flyway_schema_history (V31 is `CREATE INDEX IF NOT EXISTS`, safely re-applies)
  // then triggers Flyway's strict ordering guard: V32 is already applied so V31
  // would be applied "out of order". outOfOrder=true is the Flyway-documented escape
  // hatch for exactly this case (slice/back-patch migrations). Demo/bench-only —
  // production deploys should re-create the schema fresh or restore from a known
  // baseline; tracked as a post-demo cleanup item.
  SPRING_FLYWAY_OUT_OF_ORDER: 'true',
};

const COMMON_PM2 = {
  cwd: projectRoot,
  // `script: java` is a binary — without interpreter: 'none' PM2 tries to exec
  // it via its bundled node and fails to parse.
  interpreter: 'none',
  exec_mode: 'fork',
  instances: 1,
  autorestart: true,
  watch: false,
  max_restarts: 10,
  restart_delay: 5000,
  // Aeron MediaDriver shutdown + Spring context teardown need ~10 s of grace.
  // ledger-cluster uses 30 s for the same reason; mirror that here.
  kill_timeout: 30000,
  time: true,
  merge_logs: true,
};

const apps = [
  {
    name: 'oms-cluster-node',
    script: JAVA,
    args: [...LOW_LATENCY_JVM_FLAGS, '-jar', ROLE_JAR('cluster-node')],
    max_memory_restart: '4G',
    min_uptime: '20s',
    output: logPath('oms-cluster-node-out', '.log'),
    error: logPath('oms-cluster-node-err', '.log'),
    log: logPath('oms-cluster-node-combined', '.log'),
    env: {
      ...COMMON_ENV,
      // Cluster-node entry point is OmsClusterNodeBootstrap — a plain `public static
      // void main` JVM (no SpringApplication.run). SPRING_PROFILES_ACTIVE is dead
      // config for this role; the JVM reads OMS_AERON_* env directly. The other
      // three roles below DO boot Spring and DO consume SPRING_PROFILES_ACTIVE.
    },
    ...COMMON_PM2,
  },
  {
    name: 'oms-postgres-projector',
    script: JAVA,
    args: [...LOW_LATENCY_JVM_FLAGS, '-jar', ROLE_JAR('postgres-projector')],
    max_memory_restart: '2G',
    min_uptime: '30s',
    output: logPath('oms-postgres-projector-out', '.log'),
    error: logPath('oms-postgres-projector-err', '.log'),
    log: logPath('oms-postgres-projector-combined', '.log'),
    env: {
      ...COMMON_ENV,
      SPRING_PROFILES_ACTIVE: 'oms-postgres-projector',
      OMS_POSTGRES_PROJECTOR_AERON_DIR: AERON_MEDIA_DRIVER,
    },
    ...COMMON_PM2,
  },
  {
    name: 'oms-fix-egress',
    script: JAVA,
    args: [...LOW_LATENCY_JVM_FLAGS, '-jar', ROLE_JAR('fix-egress')],
    max_memory_restart: '2G',
    min_uptime: '30s',
    output: logPath('oms-fix-egress-out', '.log'),
    error: logPath('oms-fix-egress-err', '.log'),
    log: logPath('oms-fix-egress-combined', '.log'),
    env: {
      ...COMMON_ENV,
      SPRING_PROFILES_ACTIVE: 'oms-fix-egress',
      OMS_FIX_EGRESS_AERON_DIR: AERON_MEDIA_DRIVER,
      OMS_FIX_EGRESS_CLUSTER_CLIENT_AERON_DIR: AERON_MEDIA_DRIVER,
      OMS_FIX_AUTO_START: 'true',
    },
    ...COMMON_PM2,
  },
  {
    name: 'oms-ingress',
    script: JAVA,
    args: [...LOW_LATENCY_JVM_FLAGS, '-jar', ROLE_JAR('')],
    max_memory_restart: '2G',
    min_uptime: '20s',
    output: logPath('oms-ingress-out', '.log'),
    error: logPath('oms-ingress-err', '.log'),
    log: logPath('oms-ingress-combined', '.log'),
    env: {
      ...COMMON_ENV,
      // Use the oms-ingress-replica profile so application-oms-ingress-replica.yaml
      // wires the cluster client (oms.cluster.client.enabled=true) and the
      // OmsClusterShardRouter bean OrderIngressService depends on. The default profile
      // leaves cluster.client.enabled at false (production assumed the future
      // CLUSTER_CLIENT role would own this), which fails the order-accept JVM with
      // "required a bean of type OmsClusterShardRouter that could not be found"
      // (observed 2026-05-18 14:04 in oms-ingress-combined.log). ORDER_ACCEPT_PROFILE
      // = "!oms-postgres-projector & !oms-fix-egress" so the ingress beans (HTTP
      // /internal/v1/orders, new /cancel + /replace endpoints, the V32 lifecycle
      // reconciler) all activate normally under this profile.
      SPRING_PROFILES_ACTIVE: 'oms-ingress-replica',
      OMS_CLUSTER_CLIENT_AERON_DIRECTORY: AERON_MEDIA_DRIVER,
    },
    ...COMMON_PM2,
  },
  {
    name: 'oms-fix-loopback-acceptor',
    // Gradle task because the acceptor's main class (FixLoopbackAcceptorMain)
    // lives under src/test/java. A separate fat jar is the cleaner long-term
    // fix but is scope creep for the Wed demo; gradle's ~5 s warm-start is
    // acceptable for a rarely-restarted process.
    script: path.join(projectRoot, 'gradlew'),
    args: ['--no-daemon', '-q', 'fixLoopbackAcceptor'],
    interpreter: 'none',
    exec_mode: 'fork',
    instances: 1,
    autorestart: true,
    watch: false,
    max_restarts: 10,
    restart_delay: 5000,
    // No Aeron SHM here; a 5 s tree-kill is plenty for QuickFIX SocketAcceptor.
    kill_timeout: 5000,
    time: true,
    merge_logs: true,
    cwd: projectRoot,
    max_memory_restart: '1G',
    min_uptime: '15s',
    output: logPath('oms-fix-loopback-acceptor-out', '.log'),
    error: logPath('oms-fix-loopback-acceptor-err', '.log'),
    log: logPath('oms-fix-loopback-acceptor-combined', '.log'),
    env: {
      // Match the slice-4p bench env (~/.oms-bench.env) so the OMS initiator and
      // this acceptor agree on port + comp ids. Listed inline (not via env_file)
      // so a fresh laptop without ~/.oms-bench.env still gets the right defaults.
      FIX_ACCEPTOR_PORT: process.env.FIX_ACCEPTOR_PORT || '9876',
      FIX_ACCEPTOR_SESSION_SENDER: process.env.FIX_ACCEPTOR_SESSION_SENDER || 'BROKER_ACCEPT',
      FIX_ACCEPTOR_SESSION_TARGET: process.env.FIX_ACCEPTOR_SESSION_TARGET || 'OMS_INIT',
      FIX_ACCEPTOR_FILE_STORE:
        process.env.FIX_ACCEPTOR_FILE_STORE ||
        path.join(projectRoot, 'build', 'fix-loopback-acceptor'),
    },
  },
];

module.exports = { apps };
