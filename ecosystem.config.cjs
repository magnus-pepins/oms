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
 *   - It does NOT start NATS (Pop already has obs-nats at :4222 with JetStream
 *     enabled; re-running would split the consumer group). The demo flips
 *     OMS_NATS_ENABLED=true in COMMON_ENV below so DomainFanoutReconciler
 *     drains the Postgres outbox into the OMS_EVENTS JetStream stream. The
 *     trading-desk BFF consumes that stream via a durable JetStream consumer
 *     and pushes deltas to the React blotter over WebSocket.
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
const OMS_JAVA_TMPDIR =
  process.env.OMS_JAVA_TMPDIR || path.join(projectRoot, 'tmp', 'java');
try {
  fs.mkdirSync(OMS_JAVA_TMPDIR, { recursive: true });
} catch (_) {
  // may already exist
}
const LOW_LATENCY_JVM_FLAGS = [
  '--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED',
  '--add-exports=java.base/sun.nio.ch=ALL-UNNAMED',
  '--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED',
  '--add-opens=java.base/java.lang=ALL-UNNAMED',
  '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED',
  '--add-opens=java.base/java.util=ALL-UNNAMED',
  '--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED',
  `-Djava.io.tmpdir=${OMS_JAVA_TMPDIR}`,
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
  // Live trading-desk blotter for the Wed demo runs off NATS JetStream rather
  // than HTTP polling. DomainFanoutReconciler (in whichever JVM picks it up via
  // @ComponentScan + @Scheduled) drains the Postgres oms_domain_outbox into
  // subject oms.events.<EventType>; NatsFanoutClient.ensureJetStreamStream()
  // auto-creates the OMS_EVENTS stream on first publish, so no out-of-band
  // `nats stream add` is needed. Pop's obs-nats runs with -js and the existing
  // EVENTS stream uses prefix events.>, so OMS_EVENTS over oms.events.> does
  // not collide. URL pinned to 127.0.0.1:4222 (matches the application.yaml
  // default) so the JVMs talk to the local broker, not whatever a stale bench
  // env might claim. Storage budget unblocked 2026-05-18: obs-nats was raised
  // from max_file=2GB to 8GB (see ops-console/docker/config/nats/nats.conf)
  // because EVENTS (1GB) + DEAD_LETTERS (1GB) already booked the full 2GB
  // budget and addStream OMS_EVENTS failed with JetStreamApiException 10047.
  OMS_NATS_ENABLED: 'true',
  OMS_NATS_URL: 'nats://127.0.0.1:4222',
  // Bounded desk read API (GET /internal/v1/desk/orders/snapshot) on
  // oms-ingress; the trading-desk BFF calls it via /api/desk/orders/snapshot
  // to seed the blotter on first load (before the SSE delta stream starts
  // pushing live OrderAccepted/Working/Filled events). Off by default in
  // OmsConfig.Desk.snapshotEnabled because production reads should go
  // through the dedicated read replica path; flipped on for the Wed demo so
  // the operator UI on Pop:5310 has rows to show.
  OMS_DESK_SNAPSHOT_ENABLED: 'true',
  // Bump the snapshot window from the OmsConfig default (24h) to 168h (7d, the
  // OmsConfig hard cap) so the trading-desk date-range presets "Yesterday" and
  // "Last 7d" actually return data. The endpoint still clamps any `since` older
  // than this floor to the floor itself; the default behaviour (no `since`) is
  // unchanged from the operator's perspective because the desk UI defaults its
  // date filter to "Today".
  OMS_DESK_SNAPSHOT_MAX_AGE_HOURS: '168',
  // Cap on active orders returned by the snapshot endpoint. OmsConfig default (500) is fine
  // for the demo deploy but pinning it here makes the operational knob explicit — bump on a
  // desk with more than 500 working orders before the snapshot starts truncating.
  OMS_DESK_SNAPSHOT_ACTIVE_LIMIT: '500',
  // Wed-demo: historical order search endpoint (GET /internal/v1/desk/orders/search). Off by
  // default in OmsConfig.Desk.searchEnabled because production should typically point operator
  // search at a read replica or a dedicated search service; flipped on here because the
  // trading-desk UI on Pop:5310 ships the search panel and would otherwise show
  // `desk_search_disabled` on every submit.
  OMS_DESK_SEARCH_ENABLED: 'true',
  // Settlement state machine auto-driver (SettlementAutoStepScheduler). Walks fresh TRADE
  // executions through executed → matched → confirmed → settling → settled at the scheduler's
  // cadence (default 5s), bypassing the broker-confirm queue. The simulator never sends a
  // settlement file, so without this every fill stays at `executed` forever and the
  // beard-admin settlement pages render an all-yellow list — un-demonstrable. MUST stay off
  // in any environment with a real broker pipe; see SettlementAutoStepScheduler.java for the
  // race-condition warning.
  OMS_SETTLEMENT_AUTO_STEP_SCHEDULER_ENABLED: 'true',
  // Cap on how old an execution can be before the auto-step scheduler ignores it (default
  // 3600s = 1h). Bumped to 24h so we can run the demo on yesterday's fills if needed.
  OMS_SETTLEMENT_AUTO_STEP_SCHEDULER_MAX_EXECUTION_AGE_SECONDS: '86400',
  // FIX session stores: JDBC on bench/prod (Flyway V9). Override to file in ~/.oms-bench.env for local file-store dev.
  OMS_FIX_SESSION_STORE_TYPE: process.env.OMS_FIX_SESSION_STORE_TYPE || 'jdbc',
  OMS_FIX_IN_SESSION_STORE_TYPE: process.env.OMS_FIX_IN_SESSION_STORE_TYPE || 'jdbc',
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

  // ---------------------------------------------------------------------------
  // FX module (M3 demo)
  // ---------------------------------------------------------------------------
  // Master switch for every endpoint under /internal/v1/fx (quotes, mids,
  // hedge submit, hedge recent, nostro snapshot). Set to 'false' to disable
  // the whole FX surface; individual sub-flags below let the operator gate
  // pieces independently.
  OMS_FX_MODULE_ENABLED: 'true',
  // Stub-quote endpoint (legacy GET /quotes). Real per-tier quotes via POST
  // /quote come from fx_pair_markups (Flyway V37) regardless of this flag —
  // kept on for the bench-stack health check.
  OMS_FX_QUOTE_STUB_ENABLED: 'true',
  // POST /hedge/submit + GET /hedge/recent. The Ledger transfer that backs
  // the hedge requires oms.ledger.enabled=true (already wired by the slice-4p
  // bench env). When false, the surface returns 404 — useful for read-only
  // demos.
  OMS_FX_HEDGE_HOOKS_ENABLED: 'true',
  // Nostro snapshot endpoint + CSV of the bank's own nostro balance ids.
  // Seeded via scripts/seed-fx-nostros.sh (issuer -> nostro pattern from
  // ledger/scripts/seed-demo-coin.ts). Three currencies for the demo.
  OMS_FX_NOSTRO_READ_ENABLED: 'true',
  OMS_FX_NOSTRO_BALANCE_IDS_CSV:
    'balance_69ca46aa-9541-4470-abc1-1fe241fcc8e5,' +
    'balance_e3a2aa4a-ed30-48b6-8480-c4e5e0f1456f,' +
    'balance_885efbde-3b17-41f3-b8ec-b6f9db33afe4',

  // FX-Suspense visibility (mirror of OMS_FX_NOSTRO_BALANCE_IDS_CSV but
  // resolved by Ledger indicator @FX-Suspense-<CCY> so we don't need to
  // pin balance ids that the seed script regenerates between rebuilds).
  // The trading-desk Treasury panel reads these via /api/desk/fx/suspense/snapshot
  // on whichever role serves /internal/v1/fx (typically ingress).
  OMS_FX_SUSPENSE_CURRENCIES_CSV: 'USD,EUR,GBP',
  // Per-currency soft limit; trips overLimit=true in the snapshot,
  // the OmsFxSuspenseOverLimit Prometheus alert (oms_fx_suspense_alerts),
  // and increments oms_fx_suspense_over_limit_total{currency}. Bench
  // sizes — production stacks will tighten these once the PB settlement
  // loop closes more reliably.
  OMS_FX_SUSPENSE_MAX_ABS_CSV: 'USD=1000000,EUR=1000000,GBP=500000',
  // Retail FX conversion pools (@Nostro-<CCY>) for customer move-money.
  OMS_FX_RETAIL_NOSTRO_CURRENCIES_CSV: 'USD,EUR,GBP,SEK',
  OMS_FX_RETAIL_NOSTRO_MAX_ABS_CSV: 'USD=1000000,EUR=1000000,GBP=500000,SEK=5000000',
  // OMS_FX_SUSPENSE_LIMIT_MONITOR_ENABLED is set only on oms-postgres-projector
  // below — same pattern as OMS_FX_AUTO_HEDGER_ENGINE_ENABLED so the gauges
  // are published from a single source and don't double-tick.

  // Phase 1.5 FX mid subscriber — default OFF in COMMON_ENV. Each EMQX subscription to
  // fx/+/+/quote receives every live FX tick; enabling this on every JVM that inherits
  // COMMON_ENV (ingress + both egress roles + fix-ingress) multiplied broker fan-out ~5×
  // and pegged EMQX CPU on pop. Enable only on JVMs that actually consume mids:
  //   oms-ingress (FxQuoteService) and oms-postgres-projector (customer-quote tick).
  OMS_FX_MID_SUBSCRIBER_ENABLED: 'false',

  // Hedge event publisher: emits fx/hedge/event on every fx_hedge_actions
  // status change so the trading-desk Treasury page streams the audit row
  // in real time instead of polling. Same broker as the mid subscriber.
  OMS_FX_HEDGE_PUBLISHER_ENABLED: 'true',

  // §8.4 quote-lock at order accept. When 'true', OrderIngressService recalls
  // CreateOrderRequest.fxQuoteId via FxQuoteService.recall() and rejects with
  // RISK_FX_QUOTE_EXPIRED (HTTP 422) on a miss/expired hit. Pairs with the
  // BFF-side OMS_FX_ACCEPT_USE_QUOTER=true so the customer-frontend mints
  // the lock pre-OMS-POST. Single-currency orders never carry a quoteId,
  // so flipping this is a no-op for them — only cross-currency BUYs (e.g.
  // EUR-funded customer buying USD AAPL) hit the recall path. Maps to
  // Spring property oms.fx.accept-use-quoter.enabled via SPRING_APPLICATION_JSON.
  OMS_FX_ACCEPT_USE_QUOTER_ENABLED: process.env.OMS_FX_ACCEPT_USE_QUOTER_ENABLED || 'true',

  // §8.3 — production stacks set false so stale vendor mids reject instead of STUB_MIDS.
  OMS_FX_STUB_MIDS_ALLOWED: process.env.OMS_FX_STUB_MIDS_ALLOWED || 'true',

  // ---------------------------------------------------------------------------
  // Settlement → Ledger money movement (Phase 1 multi-leg outbox — V39)
  // ---------------------------------------------------------------------------
  // SettlementConfirmProcessor enqueues two ledger_settlement_outbox rows when an
  // execution reaches `settled`:
  //   leg=cash → posts customer cash <-> @Nostro-<ccy>-Bank in tradeCurrency
  //              (single-currency only in Phase 1; cross-currency uses
  //              cash-base/cash-quote via @FX-Suspense-<ccy>).
  //   leg=fee  → posts customer cash → @Fees-<feeCcy> for the commission amount,
  //              fee schedule mirrored from customer-frontend STOCK_<market>.
  //
  // LedgerSettlementOutboxReconciler drains the outbox at the configured cadence
  // and translates each row to a Ledger POST /transactions via
  // LedgerSettlementLegPoster. Idempotent reference: settlement-<outboxId>-<leg>.
  //
  // Required Ledger balances (run scripts/seed-ledger-settlement.sh once on Pop;
  // @Nostro-<ccy>-Bank already exists from seed-fx-nostros.sh):
  //   @Fees-USD / @Fees-EUR / @Fees-GBP
  //
  // Off in production until the cross-currency leg + custody legs land
  // (Phase 2 / Phase 3 of plans/oms-fix-gateway-and-settlement.md §11.6).
  OMS_LEDGER_SETTLEMENT_OUTBOX_ENABLED: 'true',
  // Reconciler runs on oms-postgres-projector only (see LedgerSettlementOutboxConfiguration
  // @Profile). Keeping this false in COMMON_ENV avoids three JVMs ticking the same rows.
  OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_ENABLED: 'false',

  // Phase 1 Shape A — settlement daily close + ISK metadata sync.
  // Pop bench: set in ~/.oms-bench.env (see docs/runbooks/settlement-daily-close.md).
  OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_ENABLED:
    BENCH_ENV.OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_ENABLED
    || process.env.OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_ENABLED
    || 'false',
  OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_PATH:
    BENCH_ENV.OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_PATH
    || process.env.OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_PATH
    || '',
  OMS_SETTLEMENT_DAILY_CLOSE_ENABLED:
    BENCH_ENV.OMS_SETTLEMENT_DAILY_CLOSE_ENABLED
    || process.env.OMS_SETTLEMENT_DAILY_CLOSE_ENABLED
    || 'false',
  OMS_ISK_TAX_QUARTERLY_VALUATION_JOB_ENABLED:
    BENCH_ENV.OMS_ISK_TAX_QUARTERLY_VALUATION_JOB_ENABLED
    || process.env.OMS_ISK_TAX_QUARTERLY_VALUATION_JOB_ENABLED
    || 'false',
  OMS_ISK_TAX_PENDING_POSITION_COUNT_SYNC_ENABLED:
    BENCH_ENV.OMS_ISK_TAX_PENDING_POSITION_COUNT_SYNC_ENABLED
    || process.env.OMS_ISK_TAX_PENDING_POSITION_COUNT_SYNC_ENABLED
    || 'false',
  // ISK Slice ISK-B / Phase E 12a — flip on pop after BFF binds ISK ledgerBalanceId.
  OMS_RISK_ISK_FUNDING_CHECK_ENABLED:
    BENCH_ENV.OMS_RISK_ISK_FUNDING_CHECK_ENABLED
    || process.env.OMS_RISK_ISK_FUNDING_CHECK_ENABLED
    || 'false',
  OMS_RISK_ISK_INSTRUMENT_ELIGIBILITY_CHECK_ENABLED:
    BENCH_ENV.OMS_RISK_ISK_INSTRUMENT_ELIGIBILITY_CHECK_ENABLED
    || process.env.OMS_RISK_ISK_INSTRUMENT_ELIGIBILITY_CHECK_ENABLED
    || 'false',
  OMS_LEDGER_METADATA_SYNC_ENABLED:
    BENCH_ENV.OMS_LEDGER_METADATA_SYNC_ENABLED
    || process.env.OMS_LEDGER_METADATA_SYNC_ENABLED
    || 'false',
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
      // Pop CI / fresh-wipe boots: empty archive replay must flip the Aeron readiness
      // counter to READY (see OmsAdmissionClusteredService ENV_READINESS_ALLOW_EMPTY_REPLAY).
      OMS_READINESS_ALLOW_EMPTY_REPLAY: 'true',
      // Cluster-node entry point is OmsClusterNodeBootstrap — a plain `public static
      // void main` JVM (no SpringApplication.run). SPRING_PROFILES_ACTIVE is dead
      // config for this role; the JVM reads OMS_AERON_* env directly. The other
      // three roles below DO boot Spring and DO consume SPRING_PROFILES_ACTIVE.
    },
    ...COMMON_PM2,
    // Phase 5 (plans/oms-cluster-recovery-and-hardening.md §5.3): readiness is on
    // oms-ingress HTTP (/actuator/oms-cluster-readiness), not cluster-node. Use
    // scripts/pm2-oms-cluster-readiness-probe.sh from cron or restart-pop-oms-cluster.sh.
    // PM2 wait_ready below is OFF until pop runs Phase 2–5 jars — enable manually:
    //   wait_ready: true,
    //   listen_timeout: 120000,
    //   wait_ready_timeout: 120000,
    // and ensure oms-ingress is up before cluster-node is marked ready (start order
    // in restart-pop-oms-cluster.sh already does cluster-node → projector → egress → ingress).
    // wait_ready: false,

    // 2026-05-21 zombie-JVM hardening: with OmsClusterNodeBootstrap now
    // System.exit(1)ing on any bootstrap failure (e.g. Aeron "active Mark file
    // detected" right after a fast PM2 restart), PM2 sees a real crash and applies
    // restart_delay + max_restarts. The default max_restarts: 10 (5s delay) covers
    // ~55s of retries. Aeron's MarkFile liveness window is ~20s, so 10 attempts
    // would usually suffice; we bump to 30 (≈ 155s of attempts) so that even a
    // slow JVM warm-up still outlasts the window — critical because once PM2
    // gives up, the entire OMS cluster (projector / fix-egress / ingress) is
    // unrecoverable without manual intervention.
    max_restarts: 30,
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
      OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_ENABLED: 'true',
      // Settlement customer notifications (outbox → NATS → customer-frontend BFF).
      OMS_SETTLEMENT_CUSTOMER_NOTIFICATION_PUBLISHER_ENABLED: 'true',
      OMS_SETTLEMENT_FAIL_CUSTOMER_NOTIFICATION_ENABLED: 'true',
      // §8.3 — tier MQTT publisher runs on this role ONLY (single instance).
      OMS_FX_CUSTOMER_QUOTE_PUBLISHER_ENABLED: 'true',
      OMS_FX_CUSTOMER_QUOTE_PUBLISHER_TIERS: 'basic,premium,elite,admin,business',
      // Live vendor mids for the tier MQTT tick (pairs with customer-quote publisher).
      OMS_FX_MID_SUBSCRIBER_ENABLED: 'true',
      // Auto-hedger engine (plan B1): advisory drift + recommendations only.
      // Do NOT set on oms-ingress — duplicate ENGINE_ENABLED=true on two JVMs
      // can double-insert recommendations. auto-fire stays off (plan B1.3).
      OMS_FX_AUTO_HEDGER_ENGINE_ENABLED: 'true',
      // FX-Suspense limit monitor (FxSuspenseLimitMonitor): polls
      // @FX-Suspense-<CCY> via Ledger and publishes oms_fx_suspense_*
      // gauges + over-limit counter. Single-source-of-truth pattern —
      // pinned to this JVM only so we don't double-publish.
      OMS_FX_SUSPENSE_LIMIT_MONITOR_ENABLED: 'true',
      OMS_FX_RETAIL_NOSTRO_LIMIT_MONITOR_ENABLED: 'true',
      OMS_FX_RETAIL_NOSTRO_LIMIT_MONITOR_POLL_INTERVAL_MS: '30000',
      OMS_FX_SUSPENSE_LIMIT_MONITOR_POLL_INTERVAL_MS: '30000',
      // Corporate-action processors + record-date snapshot job (Slice 17 / §5.9).
      OMS_CORPORATE_ACTION_PROCESSOR_ENABLED: 'true',
      OMS_CORPORATE_ACTION_RECORD_DATE_SNAPSHOT_JOB_ENABLED: 'true',
      // Pop bench throughput: skip PASS control_decisions INSERT on PREDMKT/* admits.
      OMS_PROJECTOR_SKIP_VENUE_CONTROL_PASS_AUDIT: 'true',
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
      OMS_FIX_EGRESS_CLUSTER_CLIENT_AERON_DIR: AERON_MEDIA_DRIVER,
      OMS_FIX_AUTO_START: 'true',
      OMS_FIX_SESSION_STORE_TYPE: process.env.OMS_FIX_SESSION_STORE_TYPE || 'jdbc',
      OMS_VENUE_SYMBOL_PREFIX_ROUTING_ENABLED: 'true',
      OMS_VENUE_SYMBOL_PREFIX: 'PREDMKT',
    },
    ...COMMON_PM2,
  },
  {
    name: 'oms-venue-egress',
    script: JAVA,
    args: [...LOW_LATENCY_JVM_FLAGS, '-jar', ROLE_JAR('venue-egress')],
    max_memory_restart: '2G',
    min_uptime: '30s',
    output: logPath('oms-venue-egress-out', '.log'),
    error: logPath('oms-venue-egress-err', '.log'),
    log: logPath('oms-venue-egress-combined', '.log'),
    env: {
      ...COMMON_ENV,
      SPRING_PROFILES_ACTIVE: 'oms-venue-egress',
      OMS_ROUTING_BACKEND: 'internal-venue',
      OMS_VENUE_EGRESS_ENABLED: 'true',
      OMS_VENUE_EGRESS_AERON_DIR: AERON_MEDIA_DRIVER,
      OMS_VENUE_EGRESS_CLUSTER_CLIENT_AERON_DIR: AERON_MEDIA_DRIVER,
      OMS_VENUE_GRPC_TARGET: 'localhost:50051',
      OMS_VENUE_GRPC_CALL_TIMEOUT_MS: '15000',
      OMS_VENUE_SYMBOL_PREFIX_ROUTING_ENABLED: 'true',
      OMS_VENUE_SYMBOL_PREFIX: 'PREDMKT',
      // Pipelined RouteOrderStream in-flight cap (see plans/oms-venue-egress-pipelining.md).
      OMS_CLUSTER_VENUE_EGRESS_VENUE_ROUTE_MAX_IN_FLIGHT: '512',
      OMS_CLUSTER_VENUE_EGRESS_FRAGMENT_LIMIT: '256',
      // 8095/8092 are oms-fix-ingress on pop.
      OMS_VENUE_EGRESS_HTTP_PORT: '8097',
      OMS_VENUE_EGRESS_MANAGEMENT_SERVER_PORT: '8098',
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
      // Venue-egress health circuit breaker (VenueAdmissionGate): refuse PREDMKT/* accepts when
      // oms-venue-egress is behind the projector, so we never admit orders the venue can't see.
      // Reads aeron_projector_cursor + oms_venue_egress_cursor from the shared oms projector DB.
      OMS_VENUE_SYMBOL_PREFIX: 'PREDMKT',
      OMS_VENUE_ADMISSION_GATE_ENABLED: 'true',
      OMS_VENUE_ADMISSION_GATE_MAX_LAG_BYTES: '4096',
      // Must match oms-venue-egress pipelining cap so VenueAdmissionGate effective budget scales.
      OMS_CLUSTER_VENUE_EGRESS_VENUE_ROUTE_MAX_IN_FLIGHT: '512',
      // Sole HTTP order-accept JVM that needs live FX mids (FxQuoteService).
      OMS_FX_MID_SUBSCRIBER_ENABLED: 'true',
      // Phase D-6 admit-batcher: amortise cluster accept_order round-trips. Default off in
      // application-oms-ingress-replica.yaml; enable on pop where 321k resting orders make
      // per-admit cluster RTT the knee (150 RPS → ~377 ms accept_ms unbatched vs ~7 ms at 120).
      // Pop! D-6 bench (57k rps): max=8 + flush=50 µs beat max=16; at ~150 RPS the 50 µs
      // coalesce window fills ~8 admits before flush.
      OMS_CLUSTER_CLIENT_ADMIT_BATCH_ENABLED: 'true',
      OMS_CLUSTER_CLIENT_ADMIT_BATCH_MAX_SIZE: '8',
      OMS_CLUSTER_CLIENT_ADMIT_BATCH_FLUSH_INTERVAL_NANOS: '50000',
      OMS_CLUSTER_CLIENT_ADMIT_BATCH_QUEUE_CAPACITY: '8192',
    },
    ...COMMON_PM2,
  },
  {
    name: 'oms-fix-ingress',
    script: JAVA,
    args: [...LOW_LATENCY_JVM_FLAGS, '-jar', ROLE_JAR('fix-ingress')],
    max_memory_restart: '2G',
    min_uptime: '30s',
    output: logPath('oms-fix-ingress-out', '.log'),
    error: logPath('oms-fix-ingress-err', '.log'),
    log: logPath('oms-fix-ingress-combined', '.log'),
    env: {
      ...COMMON_ENV,
      SPRING_PROFILES_ACTIVE: 'oms-fix-ingress',
      OMS_FIX_INGRESS_AERON_DIR: AERON_MEDIA_DRIVER,
      OMS_FIX_INGRESS_CLUSTER_CLIENT_AERON_DIR: AERON_MEDIA_DRIVER,
      OMS_FIX_IN_AUTO_START: 'true',
      OMS_FIX_IN_SESSION_STORE_TYPE: process.env.OMS_FIX_IN_SESSION_STORE_TYPE || 'jdbc',
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
  {
    name: 'oms-fix-in-loopback-client',
    // Gradle task — main class under src/test/java (same pattern as oms-fix-loopback-acceptor).
    script: path.join(projectRoot, 'gradlew'),
    args: ['--no-daemon', '-q', 'fixInLoopbackClient'],
    interpreter: 'none',
    exec_mode: 'fork',
    instances: 1,
    autorestart: true,
    watch: false,
    max_restarts: 10,
    restart_delay: 5000,
    kill_timeout: 5000,
    time: true,
    merge_logs: true,
    cwd: projectRoot,
    max_memory_restart: '512M',
    min_uptime: '15s',
    output: logPath('oms-fix-in-loopback-client-out', '.log'),
    error: logPath('oms-fix-in-loopback-client-err', '.log'),
    log: logPath('oms-fix-in-loopback-client-combined', '.log'),
    env: {
      FIX_IN_CLIENT_CONNECT_HOST: process.env.FIX_IN_CLIENT_CONNECT_HOST || '127.0.0.1',
      FIX_IN_CLIENT_CONNECT_PORT: process.env.OMS_FIX_IN_ACCEPT_PORT || '9877',
      FIX_IN_CLIENT_SENDER: process.env.FIX_IN_CLIENT_SENDER || 'LOOPBACK_CLIENT',
      FIX_IN_CLIENT_TARGET: process.env.FIX_IN_CLIENT_TARGET || 'BALH_OMS',
      FIX_IN_CLIENT_FILE_STORE:
        process.env.FIX_IN_CLIENT_FILE_STORE ||
        path.join(projectRoot, 'build', 'fix-in-loopback-client'),
      FIX_IN_LOOPBACK_HEARTBEAT_LOG_SECS: process.env.FIX_IN_LOOPBACK_HEARTBEAT_LOG_SECS || '60',
    },
  },
];

module.exports = { apps };
