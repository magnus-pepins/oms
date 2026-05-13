package com.balh.oms.cluster.bench;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OmsClusterWireFormat;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Phase 4 slice 4f of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: JMH
 * benchmarks for the cluster-client / cluster-service hot path on {@link AcceptOrderCommand}.
 *
 * <p>Two benchmarks live here:
 *
 * <ul>
 *   <li>{@link #encode(EncodeState, Blackhole)} — measures {@link AcceptOrderCommand#encode}, the
 *       per-submit cost on {@link com.balh.oms.cluster.OmsClusterIngressClient}'s hot path.</li>
 *   <li>{@link #decode(DecodeState, Blackhole)} — measures
 *       {@link AcceptOrderCommand#decode(org.agrona.DirectBuffer, int, int)}, the per-command cost
 *       inside {@link com.balh.oms.cluster.OmsAdmissionClusteredService#onSessionMessage}.</li>
 * </ul>
 *
 * <p>String fields use sizes that match production traffic for retail orders: a 36-char
 * idempotency key (UUID-string-shaped), a 24-char accountId hash, a 36-char accountId, and a
 * 4-char instrument symbol. ledgerBalanceIdOrNull is null in the steady-state benches; a follow-up
 * slice can add a parameterised variant that exercises the optional-string branch if it shows up
 * in the audit.
 *
 * <p>Run with {@code ./gradlew jmh -PjmhInclude=AcceptOrderCommandWireBench -Pjmh.profilers=gc}
 * to capture both wall-clock (avgt ns/op) and allocation (norm B/op via the {@code gc} profiler).
 * The {@code gc} profiler is the workhorse for slice 4f's allocation audit — bytes/op is what
 * Phase 4 slice 4f's plan-doc text targets.
 */
public class AcceptOrderCommandWireBench {

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void encode(EncodeState state, Blackhole bh) {
        // Re-encode the same command into the same reused buffer every iteration. This isolates
        // the encode allocation cost (currently 4× byte[] from String.getBytes inside
        // AcceptOrderCommand.writeString) from any caller-side allocation.
        int written = state.cmd.encode(state.buffer, 0);
        bh.consume(written);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void decode(DecodeState state, Blackhole bh) {
        // Decode the prebuilt buffer back to a record. Allocations recorded by `-prof gc` here
        // include: 4× byte[] (one per string field, from AcceptOrderCommand.readString), 4× String
        // (the resulting decoded values), 4× **redundant** byte[] from stringByteLen re-encoding
        // each just-decoded String to UTF-8 to advance the cursor, 1× UUID, 1× AcceptOrderCommand
        // record. Slice 4f's smallest-fix candidate replaces stringByteLen(String) with a
        // stringByteLenAt(buffer, offset) helper that re-reads the 4-byte length prefix from the
        // buffer (zero allocation) — eliminates 4× byte[]/op without changing semantics.
        AcceptOrderCommand cmd = AcceptOrderCommand.decode(state.buffer, 0, state.length);
        bh.consume(cmd);
    }

    @State(Scope.Thread)
    public static class EncodeState {

        AcceptOrderCommand cmd;

        MutableDirectBuffer buffer;

        @Setup
        public void setup() {
            this.cmd = sampleCommand();
            this.buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        }
    }

    @State(Scope.Thread)
    public static class DecodeState {

        UnsafeBuffer buffer;

        int length;

        @Setup
        public void setup() {
            ExpandableArrayBuffer src = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
            this.length = sampleCommand().encode(src, 0);
            byte[] copy = new byte[this.length];
            src.getBytes(0, copy);
            this.buffer = new UnsafeBuffer(copy);
        }
    }

    /**
     * Production-shaped sample command. Stable across encode and decode benches so the two
     * numbers can be compared directly. UUID is fixed (not random) so JIT optimisation effects
     * around `new UUID(...)` are consistent across forks.
     */
    static AcceptOrderCommand sampleCommand() {
        return new AcceptOrderCommand(
                /* correlationId        = */ 1234567890L,
                /* orderId              = */ UUID.fromString("11111111-1111-4111-8111-111111111111"),
                /* clientTimestampNanos = */ 1_700_000_000_000_000_000L,
                /* quantityScaled       = */ 10_000_000_000L,
                /* limitPriceScaledOrZero = */ 150_000_000L,
                /* shardId              = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                /* accountId            = */ "22222222-2222-4222-8222-222222222222",
                /* clientIdempotencyKey = */ "33333333-3333-4333-8333-333333333333",
                /* accountIdHash        = */ "abcdef0123456789abcdef01",
                /* instrumentSymbol     = */ "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
    }
}
