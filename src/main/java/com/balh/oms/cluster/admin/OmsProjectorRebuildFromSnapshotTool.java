package com.balh.oms.cluster.admin;

import com.balh.oms.cluster.OmsAdmissionClusteredService;
import com.balh.oms.cluster.OmsClusterSnapshotDecoder;
import io.aeron.Aeron;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.RecordingLog;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Operator-driven projector rebuild from the cluster's persisted snapshot. Closes gap 1 of
 * the post-V55/V56 stability review (see
 * {@code system-documentation/handovers/2026-05-23-oms-snapshot-magic-mismatch-and-stability-rework.md}
 * §10).
 *
 * <p><b>Problem this solves.</b> The recording-aware cursor walking the projector now does
 * after V55 only catches up from where the saved cursor points. If an event recording older
 * than the saved cursor's recording id has been deleted from the Archive (manual {@code rm},
 * disk wipe, archive recompaction) — or if the cluster's in-memory state contains orders
 * admitted before any retained recording — the projector cannot re-derive those orders by
 * replay alone. The cluster's snapshot file is the only durable place the full
 * {@code orderIndex} survives; this tool reads that snapshot and fills missing rows into
 * Postgres.
 *
 * <p><b>What it does.</b>
 *
 * <ol>
 *   <li>Connects to the cluster's local Aeron MediaDriver as a client (no consensus join).</li>
 *   <li>Opens an {@link AeronArchive} session against the same Archive the cluster writes to.</li>
 *   <li>Lists snapshot recordings on
 *       {@link ConsensusModule.Configuration#SNAPSHOT_CHANNEL_DEFAULT} /
 *       {@link ConsensusModule.Configuration#SNAPSHOT_STREAM_ID_DEFAULT} and selects the
 *       highest recording id (or {@code --snapshot-recording-id N} if explicit).</li>
 *   <li>Replays the snapshot into a {@link Subscription} wrapped with
 *       {@link ImageFragmentAssembler} so multi-fragment snapshots are reassembled before
 *       {@link OmsClusterSnapshotDecoder#onFragment} sees the magic+length header — same
 *       fragmentation invariant the cluster's own {@code SnapshotLoader} now honours after
 *       the 2026-05-23 fix.</li>
 *   <li>Per decoded {@code AdmittedOrder}: in dry-run prints a JSON line; with {@code --commit}
 *       runs {@code INSERT INTO orders ... ON CONFLICT DO NOTHING} against the projector's
 *       Postgres so missing rows materialise without overwriting anything that already
 *       exists.</li>
 * </ol>
 *
 * <p><b>What it does NOT do.</b>
 *
 * <ul>
 *   <li>Touch {@code aeron_projector_cursor} or {@code oms_fix_egress_cursor}. The cursor
 *       repair tool ({@code system-documentation/scripts/oms-cursor-repair.sh}) is the
 *       operator surface for cursor work; this tool is strictly an orders-row backfill.</li>
 *   <li>Re-insert {@code executions} or {@code control_decisions}. The cluster snapshot
 *       stores only execution refs (the {@code venueExecRef} strings), not full
 *       {@code ExecutionAppliedEvent} payloads — re-inserting executions from the snapshot
 *       alone would lose price / quantity / venue-ts. Operators who need executions back
 *       must replay the events stream through the projector (which the V55 cursor walking
 *       now does correctly once the orders rows exist).</li>
 *   <li>Restart any service. Operator decides when to restart the projector after this tool
 *       finishes; see runbook in handover §10.</li>
 * </ul>
 *
 * <h3>Exit codes</h3>
 *
 * <ul>
 *   <li>{@code 0} — replay completed, all decoded orders processed (dry-run printed or
 *       inserted in --commit mode).</li>
 *   <li>{@code 1} — replay failed: snapshot recording missing, schema mismatch, JDBC error.</li>
 *   <li>{@code 2} — argument error.</li>
 * </ul>
 *
 * <h3>Env vars (read at startup)</h3>
 *
 * <ul>
 *   <li>{@code OMS_AERON_DIR_BASE} — base directory for cluster Aeron state. Default
 *       {@code build/aeron-cluster}. Pop sets this in the env file.</li>
 *   <li>{@code OMS_AERON_MEDIA_DRIVER_DIR} — explicit aeron client dir if set; else derived
 *       from {@code OMS_AERON_DIR_BASE/media-driver} (mirrors {@code OmsClusterNodeBootstrap}).</li>
 *   <li>{@code OMS_AERON_ARCHIVE_CONTROL_REQUEST_CHANNEL} — defaults to
 *       {@code aeron:ipc?term-length=64k} (matches the cluster's local control channel).</li>
 *   <li>{@code OMS_PG_URL} / {@code OMS_PG_USER} / {@code OMS_PG_PASSWORD} — JDBC connection
 *       for {@code --commit}. Ignored in dry-run.</li>
 * </ul>
 */
public final class OmsProjectorRebuildFromSnapshotTool {

    /** Default base directory for the cluster's Aeron working state. Matches {@code OmsClusterNodeBootstrap}. */
    public static final String DEFAULT_AERON_DIR_BASE = "build/aeron-cluster";

    public static final String DEFAULT_MEDIA_DRIVER_SUBDIR = "media-driver";

    public static final String ENV_AERON_DIR_BASE = "OMS_AERON_DIR_BASE";

    public static final String ENV_AERON_MEDIA_DRIVER_DIR = "OMS_AERON_MEDIA_DRIVER_DIR";

    /**
     * Env var: explicit cluster (consensus-module) directory containing the {@code recording-log}
     * file. Default {@code <OMS_AERON_DIR_BASE>/consensus-module} — matches
     * {@code OmsClusterNodeBootstrap.resolvePaths} and {@code OmsClusterSnapshotAdminTool}.
     */
    public static final String ENV_AERON_CLUSTER_DIR = "OMS_AERON_CLUSTER_DIR";

    public static final String DEFAULT_CLUSTER_DIR_SUBDIR = "consensus-module";

    /**
     * Aeron cluster {@code RecordingLog} serviceId for the (single) clustered service in this
     * cluster — {@code OmsAdmissionClusteredService}. Service snapshots are stored under this
     * id; the consensus module's own snapshot lives under
     * {@link ConsensusModule.Configuration#SERVICE_ID} ({@code -1}). The cluster's snapshot
     * stream id (107) is shared between both kinds of snapshot, so filtering by stream id
     * alone (via {@code AeronArchive.listRecordings}) catches consensus-module marker
     * recordings whose payload bytes are not {@code OMS_SNAPSHOT_MAGIC} and the decoder
     * loud-fails on them. {@link RecordingLog#getLatestSnapshot(int)} with this serviceId
     * returns the right recordingId unambiguously.
     */
    public static final int SERVICE_ID = 0;

    public static final String ENV_ARCHIVE_CONTROL_REQUEST = "OMS_AERON_ARCHIVE_CONTROL_REQUEST_CHANNEL";

    public static final String DEFAULT_ARCHIVE_CONTROL_REQUEST = "aeron:ipc?term-length=64k";

    public static final String ENV_ARCHIVE_CONTROL_RESPONSE = "OMS_AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL";

    public static final String DEFAULT_ARCHIVE_CONTROL_RESPONSE = "aeron:ipc?term-length=64k";

    public static final String ENV_REPLAY_CHANNEL = "OMS_REBUILD_REPLAY_CHANNEL";

    public static final String DEFAULT_REPLAY_CHANNEL = "aeron:ipc?term-length=64k";

    public static final int DEFAULT_REPLAY_STREAM_ID = 200_007;

    public static final String ENV_PG_URL = "OMS_PG_URL";

    public static final String ENV_PG_USER = "OMS_PG_USER";

    public static final String ENV_PG_PASSWORD = "OMS_PG_PASSWORD";

    public static final int EXIT_OK = 0;

    public static final int EXIT_FAILURE = 1;

    public static final int EXIT_ARGS = 2;

    // Postgres scaling constants kept private to the tool — must match
    // AcceptOrderCommand.QUANTITY_SCALE / PRICE_SCALE exactly. Hard-coded here (rather than
    // depending on AcceptOrderCommand) so this tool stays a thin slice with no surprises if
    // the cluster wire-format module is refactored. A drift will be caught by
    // OmsProjectorRebuildFromSnapshotToolTest.statusCodeStringRoundTrip.
    private static final long QUANTITY_SCALE_DENOM = 100_000_000L;

    private static final long PRICE_SCALE_DENOM = 100_000_000L;

    private static final long POLL_PARK_NANOS = 1_000_000L;

    private static final int FRAGMENT_LIMIT = 64;

    /**
     * Drained-recording detection. After the inner poll returns 0, we re-check the recording's
     * current write position. If the position hasn't advanced AND we've already consumed bytes,
     * the snapshot is complete (snapshot recordings are written in one shot by the cluster).
     */
    private static final long IDLE_POLL_DEADLINE_MS = 5_000L;

    private OmsProjectorRebuildFromSnapshotTool() {}

    public static void main(String[] args) {
        Args parsed;
        try {
            parsed = Args.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("FATAL: " + e.getMessage());
            usage(System.err);
            System.exit(EXIT_ARGS);
            return;
        }
        if (parsed.printHelp) {
            usage(System.out);
            System.exit(EXIT_OK);
            return;
        }
        int code;
        try {
            code = run(parsed);
        } catch (RuntimeException | SQLException e) {
            System.err.println("FATAL: " + e.getMessage());
            e.printStackTrace(System.err);
            code = EXIT_FAILURE;
        }
        System.exit(code);
    }

    static int run(Args args) throws SQLException {
        String aeronDir = resolveAeronDirectory();
        System.out.printf(Locale.ROOT,
                "OmsProjectorRebuildFromSnapshotTool starting (dryRun=%s, aeronDir=%s)%n",
                !args.commit, aeronDir);

        Aeron aeron = null;
        AeronArchive archive = null;
        Subscription replay = null;
        Connection conn = null;
        PreparedStatement insert = null;
        try {
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(false)
                    .controlRequestChannel(envOrDefault(ENV_ARCHIVE_CONTROL_REQUEST, DEFAULT_ARCHIVE_CONTROL_REQUEST))
                    .controlResponseChannel(envOrDefault(ENV_ARCHIVE_CONTROL_RESPONSE, DEFAULT_ARCHIVE_CONTROL_RESPONSE)));

            List<SnapshotRecording> recordings = listSnapshotRecordings(archive);
            if (recordings.isEmpty()) {
                System.err.println("FATAL: no service snapshot recordings (serviceId=" + SERVICE_ID
                        + ") found in RecordingLog at " + resolveClusterDir().getAbsolutePath()
                        + ". Has the cluster ever taken a snapshot? "
                        + "Trigger one with ./gradlew clusterSnapshot first.");
                return EXIT_FAILURE;
            }
            SnapshotRecording chosen = chooseRecording(recordings, args.snapshotRecordingId);
            System.out.printf(Locale.ROOT, "Selected snapshot recordingId=%d (startPosition=%d, stopPosition=%d)%n",
                    chosen.recordingId, chosen.startPosition, chosen.stopPosition);

            if (args.commit) {
                String url = requireEnv(ENV_PG_URL);
                String user = requireEnv(ENV_PG_USER);
                String password = requireEnv(ENV_PG_PASSWORD);
                conn = DriverManager.getConnection(url, user, password);
                conn.setAutoCommit(false);
                insert = conn.prepareStatement(INSERT_SQL);
            }

            AtomicLong dryRunCount = new AtomicLong();
            AtomicLong insertedCount = new AtomicLong();
            AtomicLong skippedCount = new AtomicLong();

            final PreparedStatement insertRef = insert;
            final boolean commitMode = args.commit;
            OmsClusterSnapshotDecoder decoder = new OmsClusterSnapshotDecoder(
                    order -> {
                        try {
                            if (commitMode) {
                                int affected = applyInsert(insertRef, order);
                                if (affected > 0) {
                                    insertedCount.incrementAndGet();
                                } else {
                                    skippedCount.incrementAndGet();
                                }
                            } else {
                                System.out.println(toJsonLine(order));
                                dryRunCount.incrementAndGet();
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException("INSERT failed for order " + order.orderId(), e);
                        }
                    },
                    /* onExecutionRefs = */ null);
            // Aeron Cluster snapshot recordings are framed as:
            //   [SnapshotMarkerEncoder BEGIN] -> [service payload] -> [SnapshotMarkerEncoder END]
            // The service's onStart(Image) callback in production runs through framework
            // machinery that consumes the markers before handing the service its slice of the
            // image. When this tool replays the raw recording it sees the markers too — their
            // first 4 bytes are an SBE MessageHeader, not OMS_SNAPSHOT_MAGIC, so the decoder
            // loud-fails on them. Filter at the fragment level: anything not starting with
            // OMS_SNAPSHOT_MAGIC is treated as a marker frame and skipped.
            FragmentHandler skipMarkers = (buf, off, len, hdr) -> {
                if (len < Integer.BYTES) {
                    return;
                }
                int magic = buf.getInt(off);
                if (magic != OmsClusterSnapshotDecoder.SNAPSHOT_MAGIC) {
                    return;
                }
                decoder.onFragment(buf, off, len, hdr);
            };
            ImageFragmentAssembler assembler = new ImageFragmentAssembler(skipMarkers);

            replay = archive.replay(
                    chosen.recordingId,
                    chosen.startPosition,
                    chosen.stopPosition - chosen.startPosition,
                    envOrDefault(ENV_REPLAY_CHANNEL, DEFAULT_REPLAY_CHANNEL),
                    DEFAULT_REPLAY_STREAM_ID);

            drainReplay(replay, assembler);

            if (commitMode && conn != null) {
                conn.commit();
            }

            System.out.printf(Locale.ROOT,
                    "OmsProjectorRebuildFromSnapshotTool done: ordersDecoded=%d, executionRefBlocksDecoded=%d, inserted=%d, skipped(existing)=%d, dryRunPrinted=%d%n",
                    decoder.ordersDecoded(), decoder.executionRefBlocksDecoded(),
                    insertedCount.get(), skippedCount.get(), dryRunCount.get());
            return EXIT_OK;
        } finally {
            if (insert != null) {
                try { insert.close(); } catch (SQLException ignored) {}
            }
            if (conn != null) {
                try {
                    if (!conn.getAutoCommit()) {
                        conn.rollback();
                    }
                } catch (SQLException ignored) {}
                try { conn.close(); } catch (SQLException ignored) {}
            }
            CloseHelper.quietClose(replay);
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
        }
    }

    /**
     * Resolves all valid OMS service snapshot recordings via the cluster's {@code RecordingLog}
     * (serviceId = {@link #SERVICE_ID}). Filtering by Aeron Archive stream id alone caught
     * the consensus module's marker recordings on the same snapshot stream (107) whose payload
     * is not {@code OMS_SNAPSHOT_MAGIC} and made the decoder loud-fail. {@code RecordingLog}
     * is the authoritative source for which recording corresponds to which clustered service's
     * snapshot.
     *
     * <p>For each {@code RecordingLog} snapshot entry the Archive is queried for the recording's
     * start/stop positions (required for {@link AeronArchive#replay}).
     */
    private static List<SnapshotRecording> listSnapshotRecordings(AeronArchive archive) {
        File clusterDir = resolveClusterDir();
        List<SnapshotRecording> out = new ArrayList<>();
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, /* createIfMissing = */ false)) {
            for (RecordingLog.Entry entry : recordingLog.entries()) {
                if (entry.type != RecordingLog.ENTRY_TYPE_SNAPSHOT) {
                    continue;
                }
                if (!entry.isValid) {
                    continue;
                }
                if (entry.serviceId != SERVICE_ID) {
                    continue;
                }
                long[] startStop = lookupRecordingStartStop(archive, entry.recordingId);
                if (startStop == null) {
                    System.err.printf(Locale.ROOT,
                            "WARN: RecordingLog snapshot entry recordingId=%d (serviceId=%d) not found in Archive — skipping%n",
                            entry.recordingId, entry.serviceId);
                    continue;
                }
                out.add(new SnapshotRecording(entry.recordingId, startStop[0], startStop[1]));
            }
        }
        out.sort(Comparator.comparingLong((SnapshotRecording r) -> r.recordingId).reversed());
        return out;
    }

    /**
     * Returns {@code [startPosition, stopPosition]} for the given recording, or {@code null} if
     * the Archive does not know about it (recording deleted, catalog truncated). Defensive
     * because {@code RecordingLog} on disk can outlive the Archive catalog after a manual
     * archive wipe.
     */
    private static long[] lookupRecordingStartStop(AeronArchive archive, long recordingId) {
        long[] holder = new long[]{-1L, -1L};
        RecordingDescriptorConsumer consumer = (controlSessionId,
                correlationId,
                rid,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity) -> {
            if (rid == recordingId) {
                holder[0] = startPosition;
                holder[1] = stopPosition;
            }
        };
        int matched = archive.listRecording(recordingId, consumer);
        if (matched == 0 || holder[0] < 0) {
            return null;
        }
        return holder;
    }

    static File resolveClusterDir() {
        String explicit = System.getenv(ENV_AERON_CLUSTER_DIR);
        if (explicit != null && !explicit.isBlank()) {
            return new File(explicit);
        }
        String base = envOrDefault(ENV_AERON_DIR_BASE, DEFAULT_AERON_DIR_BASE);
        return new File(base, DEFAULT_CLUSTER_DIR_SUBDIR);
    }

    private static SnapshotRecording chooseRecording(List<SnapshotRecording> recordings, Long preferredId) {
        if (preferredId != null) {
            for (SnapshotRecording r : recordings) {
                if (r.recordingId == preferredId) {
                    return r;
                }
            }
            throw new IllegalStateException(
                    "no snapshot recording with id=" + preferredId
                            + " (available: " + recordings + ")");
        }
        return recordings.get(0); // sorted descending, so [0] is the most recent
    }

    private static void drainReplay(Subscription replay, ImageFragmentAssembler assembler) {
        long lastObservedFragmentMs = System.currentTimeMillis();
        boolean sawAnyFragment = false;
        while (true) {
            int polled = replay.poll(assembler, FRAGMENT_LIMIT);
            if (polled > 0) {
                sawAnyFragment = true;
                lastObservedFragmentMs = System.currentTimeMillis();
                continue;
            }
            // No fragments. If we've already drained some, and the subscription has no
            // active connected images anymore (snapshot replay completes when the
            // exclusive replay publication closes), we're done. Otherwise idle-poll briefly.
            if (sawAnyFragment && !replay.isConnected()) {
                return;
            }
            if (System.currentTimeMillis() - lastObservedFragmentMs > IDLE_POLL_DEADLINE_MS) {
                if (!sawAnyFragment) {
                    throw new IllegalStateException(
                            "snapshot replay produced zero fragments within " + IDLE_POLL_DEADLINE_MS
                                    + "ms — recording may be empty or the replay subscription never connected");
                }
                return;
            }
            LockSupport.parkNanos(POLL_PARK_NANOS);
        }
    }

    private static int applyInsert(PreparedStatement stmt, OmsAdmissionClusteredService.AdmittedOrder order)
            throws SQLException {
        stmt.setObject(1, order.orderId());
        stmt.setObject(2, java.util.UUID.fromString(order.accountId()));
        stmt.setString(3, order.clientIdempotencyKey());
        stmt.setInt(4, order.shardId());
        stmt.setString(5, statusCodeToName(order.statusCode()));
        // terminal_reason / terminal_at — left null; the snapshot does not carry the reject
        // code separately. Terminal orders restored without a reject_code will show in admin
        // UIs as "terminal but no reason" which is the honest signal that this row came from
        // a snapshot backfill rather than the live event stream.
        stmt.setNull(6, Types.OTHER);
        stmt.setString(7, sideName(order.side()));
        stmt.setString(8, order.instrumentSymbol());
        BigDecimal quantity = scaledToDecimal(order.quantityScaled(), QUANTITY_SCALE_DENOM);
        BigDecimal limitPrice = order.limitPriceScaledOrZero() == 0L
                ? null
                : scaledToDecimal(order.limitPriceScaledOrZero(), PRICE_SCALE_DENOM);
        stmt.setBigDecimal(9, quantity);
        if (limitPrice == null) {
            stmt.setNull(10, Types.NUMERIC);
        } else {
            stmt.setBigDecimal(10, limitPrice);
        }
        stmt.setString(11, tifName(order.timeInForceCode()));
        // ord_type — snapshot AdmittedOrder doesn't carry an ord_type byte (only limitPriceScaled
        // tells us MARKET vs LIMIT). Backfill it deterministically from the limit price.
        stmt.setString(12, order.limitPriceScaledOrZero() == 0L ? "MARKET" : "LIMIT");
        stmt.setTimestamp(13, Timestamp.from(Instant.ofEpochMilli(order.acceptedAtMillis())));
        stmt.setTimestamp(14, Timestamp.from(Instant.ofEpochMilli(order.acceptedAtMillis())));
        stmt.setNull(15, Types.TIMESTAMP);
        stmt.setString(16, order.accountIdHash());
        if (order.ledgerBalanceIdOrNull() == null) {
            stmt.setNull(17, Types.VARCHAR);
        } else {
            stmt.setString(17, order.ledgerBalanceIdOrNull());
        }
        stmt.setBigDecimal(18, scaledToDecimal(order.cumQtyScaled(), QUANTITY_SCALE_DENOM));
        return stmt.executeUpdate();
    }

    /**
     * Status codes are the ordinals of {@code com.balh.oms.domain.OrderStatus}. Kept inlined
     * (not a dependency on the domain module) so this tool stays a small standalone admin
     * surface. A drift between the cluster's status codes and the projector's enum order is
     * exactly the kind of bug the round-trip test asserts against.
     */
    static String statusCodeToName(byte statusCode) {
        return switch (statusCode) {
            case 0 -> "PENDING_NEW";
            case 1 -> "NEW";
            case 2 -> "WORKING";
            case 3 -> "PARTIALLY_FILLED";
            case 4 -> "FILLED";
            case 5 -> "CANCELLED";
            case 6 -> "REJECTED";
            case 7 -> "EXPIRED";
            default -> throw new IllegalStateException("unknown statusCode=" + statusCode);
        };
    }

    static String sideName(byte side) {
        return switch (side) {
            case 1 -> "BUY";
            case 2 -> "SELL";
            default -> throw new IllegalStateException("unknown side=" + side);
        };
    }

    static String tifName(byte tif) {
        return switch (tif) {
            case 1 -> "DAY";
            case 2 -> "GTC";
            case 3 -> "IOC";
            case 4 -> "FOK";
            default -> throw new IllegalStateException("unknown tif=" + tif);
        };
    }

    static BigDecimal scaledToDecimal(long scaled, long denom) {
        return BigDecimal.valueOf(scaled).divide(BigDecimal.valueOf(denom), 10, RoundingMode.UNNECESSARY);
    }

    static String toJsonLine(OmsAdmissionClusteredService.AdmittedOrder order) {
        // Hand-rolled JSON to keep this tool free of Jackson on the classpath (the cluster
        // bootstrap module deliberately avoids Spring/Jackson per ADR 0001). Operators that
        // want a richer dump can pipe stdout through jq.
        StringBuilder sb = new StringBuilder(256);
        sb.append('{')
                .append("\"orderId\":").append(quote(order.orderId().toString())).append(',')
                .append("\"accountId\":").append(quote(order.accountId())).append(',')
                .append("\"clientIdempotencyKey\":").append(quote(order.clientIdempotencyKey())).append(',')
                .append("\"shardId\":").append(order.shardId()).append(',')
                .append("\"status\":").append(quote(statusCodeToName(order.statusCode()))).append(',')
                .append("\"side\":").append(quote(sideName(order.side()))).append(',')
                .append("\"instrument\":").append(quote(order.instrumentSymbol())).append(',')
                .append("\"quantityScaled\":").append(order.quantityScaled()).append(',')
                .append("\"limitPriceScaledOrZero\":").append(order.limitPriceScaledOrZero()).append(',')
                .append("\"timeInForce\":").append(quote(tifName(order.timeInForceCode()))).append(',')
                .append("\"acceptedAtMillis\":").append(order.acceptedAtMillis()).append(',')
                .append("\"cumQtyScaled\":").append(order.cumQtyScaled()).append(',')
                .append("\"version\":").append(order.version());
        if (order.ledgerBalanceIdOrNull() != null) {
            sb.append(",\"ledgerBalanceId\":").append(quote(order.ledgerBalanceIdOrNull()));
        }
        sb.append('}');
        return sb.toString();
    }

    private static String quote(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default: sb.append(c);
            }
        }
        sb.append('"');
        return sb.toString();
    }

    // INSERT shape mirrors OrdersRepository.PROJECTOR_INSERT_SQL field-for-field. Hard-coded
    // here so this tool runs without bringing up the Spring application context (operators
    // often run it on pop before the projector itself is ready to start, where there is no
    // Spring context to share). A schema drift will be caught by the orders-table check in
    // OmsProjectorRebuildFromSnapshotToolTest.insertSqlMatchesOrdersRepositoryShape.
    private static final String INSERT_SQL = ""
            + "INSERT INTO orders ("
            + "  id, account_id, client_idempotency_key, shard_id, version,"
            + "  status, terminal_reason, side, instrument_symbol,"
            + "  quantity, limit_price, time_in_force, ord_type,"
            + "  received_at, accepted_at, terminal_at, account_id_hash, ledger_balance_id,"
            + "  cum_filled_quantity"
            + ") VALUES ("
            + "  ?, ?, ?, ?, 0,"
            + "  CAST(? AS order_status), CAST(? AS reject_code), CAST(? AS order_side), ?,"
            + "  ?, ?, ?, ?,"
            + "  ?, ?, ?, ?, ?,"
            + "  ?"
            + ") ON CONFLICT DO NOTHING";

    static String resolveAeronDirectory() {
        String explicit = System.getenv(ENV_AERON_MEDIA_DRIVER_DIR);
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        String base = envOrDefault(ENV_AERON_DIR_BASE, DEFAULT_AERON_DIR_BASE);
        return base + java.io.File.separator + DEFAULT_MEDIA_DRIVER_SUBDIR;
    }

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException(name + " env var is required for --commit");
        }
        return v;
    }

    private static String envOrDefault(String name, String defaultValue) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? defaultValue : v;
    }

    static void usage(java.io.PrintStream out) {
        out.println("Usage: OmsProjectorRebuildFromSnapshotTool [--commit] [--snapshot-recording-id N]");
        out.println();
        out.println("  --commit                   Apply INSERT INTO orders ON CONFLICT DO NOTHING for");
        out.println("                             each AdmittedOrder in the snapshot. Without this flag");
        out.println("                             the tool prints JSON to stdout and exits 0.");
        out.println("  --snapshot-recording-id N  Replay this specific snapshot recording id instead of");
        out.println("                             the latest. Useful for replaying a known-good snapshot");
        out.println("                             after a destructive recovery.");
        out.println("  --help, -h                 Print this usage block.");
        out.println();
        out.println("Env vars (read at startup):");
        out.println("  OMS_AERON_DIR_BASE         Base directory for cluster Aeron state (default: build/aeron-cluster)");
        out.println("  OMS_AERON_MEDIA_DRIVER_DIR Explicit Aeron media-driver dir (overrides OMS_AERON_DIR_BASE/media-driver)");
        out.println("  OMS_PG_URL/USER/PASSWORD   JDBC connection for --commit. Ignored in dry-run.");
    }

    private record SnapshotRecording(long recordingId, long startPosition, long stopPosition) {
        @Override public String toString() {
            return "Recording[id=" + recordingId + ",start=" + startPosition + ",stop=" + stopPosition + "]";
        }
    }

    /** Parsed command-line. Package-private for tests. */
    static final class Args {
        boolean commit;
        boolean printHelp;
        Long snapshotRecordingId;

        static Args parse(String[] argv) {
            Args a = new Args();
            for (int i = 0; i < argv.length; i++) {
                String arg = Objects.requireNonNull(argv[i]);
                switch (arg) {
                    case "--commit" -> a.commit = true;
                    case "--help", "-h" -> a.printHelp = true;
                    case "--snapshot-recording-id" -> {
                        if (i + 1 >= argv.length) {
                            throw new IllegalArgumentException("--snapshot-recording-id requires a value");
                        }
                        try {
                            a.snapshotRecordingId = Long.parseLong(argv[++i]);
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("invalid --snapshot-recording-id: " + argv[i], e);
                        }
                    }
                    default -> throw new IllegalArgumentException("unknown arg: " + arg);
                }
            }
            return a;
        }
    }
}
