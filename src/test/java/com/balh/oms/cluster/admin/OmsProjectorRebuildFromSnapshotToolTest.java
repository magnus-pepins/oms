package com.balh.oms.cluster.admin;

import com.balh.oms.cluster.OmsAdmissionClusteredService;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Closes gap 1 test coverage for the operator-facing surface of
 * {@link OmsProjectorRebuildFromSnapshotTool}: arg parsing, JSON emission, and the
 * status-code ↔ enum mapping that must stay in lockstep with
 * {@link OrderStatus}'s ordinal order.
 */
class OmsProjectorRebuildFromSnapshotToolTest {

    // ---- Args parsing --------------------------------------------------------------------

    @Test
    void args_default_isDryRunWithoutSnapshotIdAndNoHelp() {
        OmsProjectorRebuildFromSnapshotTool.Args a = OmsProjectorRebuildFromSnapshotTool.Args.parse(new String[]{});
        assertThat(a.commit).isFalse();
        assertThat(a.printHelp).isFalse();
        assertThat(a.snapshotRecordingId).isNull();
    }

    @Test
    void args_commit_setsCommitFlag() {
        OmsProjectorRebuildFromSnapshotTool.Args a = OmsProjectorRebuildFromSnapshotTool.Args.parse(new String[]{"--commit"});
        assertThat(a.commit).isTrue();
    }

    @Test
    void args_snapshotRecordingId_parsesValue() {
        OmsProjectorRebuildFromSnapshotTool.Args a =
                OmsProjectorRebuildFromSnapshotTool.Args.parse(new String[]{"--snapshot-recording-id", "42"});
        assertThat(a.snapshotRecordingId).isEqualTo(42L);
    }

    @Test
    void args_snapshotRecordingId_missingValue_throws() {
        assertThatThrownBy(() ->
                OmsProjectorRebuildFromSnapshotTool.Args.parse(new String[]{"--snapshot-recording-id"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a value");
    }

    @Test
    void args_snapshotRecordingId_nonNumeric_throws() {
        assertThatThrownBy(() ->
                OmsProjectorRebuildFromSnapshotTool.Args.parse(new String[]{"--snapshot-recording-id", "not-a-number"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid --snapshot-recording-id");
    }

    @Test
    void args_unknownArg_throws() {
        assertThatThrownBy(() -> OmsProjectorRebuildFromSnapshotTool.Args.parse(new String[]{"--banana"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown arg: --banana");
    }

    @Test
    void args_help_setsPrintHelp() {
        assertThat(OmsProjectorRebuildFromSnapshotTool.Args.parse(new String[]{"--help"}).printHelp).isTrue();
        assertThat(OmsProjectorRebuildFromSnapshotTool.Args.parse(new String[]{"-h"}).printHelp).isTrue();
    }

    @Test
    void args_combo_commitAndSnapshotId() {
        OmsProjectorRebuildFromSnapshotTool.Args a = OmsProjectorRebuildFromSnapshotTool.Args.parse(
                new String[]{"--commit", "--snapshot-recording-id", "16"});
        assertThat(a.commit).isTrue();
        assertThat(a.snapshotRecordingId).isEqualTo(16L);
    }

    // ---- statusCodeToName <-> OrderStatus ordinal parity --------------------------------

    @Test
    void statusCodeToName_mappingMatchesOrderStatusOrdinalOrder() {
        // The cluster encodes statusCode as the ordinal of OrderStatus. A reorder of the
        // enum or a drift in the switch in statusCodeToName would silently corrupt every
        // restored row's status. This test pins the contract.
        for (OrderStatus s : OrderStatus.values()) {
            assertThat(OmsProjectorRebuildFromSnapshotTool.statusCodeToName((byte) s.ordinal()))
                    .as("statusCode=%d", s.ordinal())
                    .isEqualTo(s.name());
        }
    }

    @Test
    void statusCodeToName_unknownCode_throws() {
        // Picks a byte well outside any plausible OrderStatus ordinal so a future enum
        // addition doesn't silently make this test pass.
        assertThatThrownBy(() -> OmsProjectorRebuildFromSnapshotTool.statusCodeToName((byte) 99))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown statusCode=99");
    }

    @Test
    void sideName_mappingMatchesAcceptOrderCommand() {
        // Wire codes are owned by AcceptOrderCommand (SIDE_BUY=0, SIDE_SELL=1) — same constants
        // the cluster's admit path writes and the projector reads. The previous version of this
        // test asserted BUY=1/SELL=2 and was the cause of the live pop dry-run failing with
        // "unknown side=0" on the first decoded order — the test had captured the tool author's
        // wrong assumption rather than the actual wire format. Don't change these numbers
        // without also updating AcceptOrderCommand.
        assertThat(OmsProjectorRebuildFromSnapshotTool.sideName(com.balh.oms.cluster.AcceptOrderCommand.SIDE_BUY))
                .isEqualTo(Side.BUY.name());
        assertThat(OmsProjectorRebuildFromSnapshotTool.sideName(com.balh.oms.cluster.AcceptOrderCommand.SIDE_SELL))
                .isEqualTo(Side.SELL.name());
    }

    @Test
    void sideName_unknown_throws() {
        // 99 is well outside the {0,1} valid range; using a sentinel byte instead of 0
        // because 0 is the real SIDE_BUY value.
        assertThatThrownBy(() -> OmsProjectorRebuildFromSnapshotTool.sideName((byte) 99))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void tifName_knownCodes_returnHumanReadable() {
        // TIF codes owned by AcceptOrderCommand (TIF_DAY=0 .. TIF_GTC=3).
        assertThat(OmsProjectorRebuildFromSnapshotTool.tifName(com.balh.oms.cluster.AcceptOrderCommand.TIF_DAY))
                .isEqualTo("DAY");
        assertThat(OmsProjectorRebuildFromSnapshotTool.tifName(com.balh.oms.cluster.AcceptOrderCommand.TIF_IOC))
                .isEqualTo("IOC");
        assertThat(OmsProjectorRebuildFromSnapshotTool.tifName(com.balh.oms.cluster.AcceptOrderCommand.TIF_FOK))
                .isEqualTo("FOK");
        assertThat(OmsProjectorRebuildFromSnapshotTool.tifName(com.balh.oms.cluster.AcceptOrderCommand.TIF_GTC))
                .isEqualTo("GTC");
    }

    @Test
    void scaledToDecimal_dividesByDenom() {
        assertThat(OmsProjectorRebuildFromSnapshotTool.scaledToDecimal(150_000_000L, 100_000_000L))
                .isEqualByComparingTo(new BigDecimal("1.5"));
    }

    // ---- JSON line emission --------------------------------------------------------------

    @Test
    void toJsonLine_includesAllExpectedFieldsAndEscapesQuotes() {
        OmsAdmissionClusteredService.AdmittedOrder order = new OmsAdmissionClusteredService.AdmittedOrder(
                UUID.fromString("11111111-2222-3333-4444-555555555555"),
                /* accountId */ "acct-1",
                /* clientIdempotencyKey */ "key-with-\"quotes\"",
                /* accountIdHash */ "h",
                /* instrumentSymbol */ "AAPL",
                /* side */ com.balh.oms.cluster.AcceptOrderCommand.SIDE_BUY,
                /* quantityScaled */ 100_000_000L,
                /* limitPriceScaledOrZero */ 15_000_000_000L,
                /* timeInForceCode */ com.balh.oms.cluster.AcceptOrderCommand.TIF_DAY,
                /* ledgerBalanceIdOrNull */ "bal-9",
                /* version */ 7,
                /* acceptedAtMillis */ 1_700_000_000_000L,
                /* statusCode */ (byte) 2,
                /* cumQtyScaled */ 0L,
                /* shardId */ 0);

        String json = OmsProjectorRebuildFromSnapshotTool.toJsonLine(order);

        assertThat(json)
                .contains("\"orderId\":\"11111111-2222-3333-4444-555555555555\"")
                .contains("\"accountId\":\"acct-1\"")
                .contains("\"clientIdempotencyKey\":\"key-with-\\\"quotes\\\"\"")
                .contains("\"shardId\":0")
                .contains("\"status\":\"WORKING\"")
                .contains("\"side\":\"BUY\"")
                .contains("\"instrument\":\"AAPL\"")
                .contains("\"quantityScaled\":100000000")
                .contains("\"limitPriceScaledOrZero\":15000000000")
                .contains("\"timeInForce\":\"DAY\"")
                .contains("\"acceptedAtMillis\":1700000000000")
                .contains("\"cumQtyScaled\":0")
                .contains("\"version\":7")
                .contains("\"ledgerBalanceId\":\"bal-9\"");
    }

    @Test
    void toJsonLine_nullLedgerBalanceId_omitsField() {
        OmsAdmissionClusteredService.AdmittedOrder order = new OmsAdmissionClusteredService.AdmittedOrder(
                UUID.randomUUID(),
                "acct", "key", "h", "AAPL",
                com.balh.oms.cluster.AcceptOrderCommand.SIDE_BUY,
                100L, 0L,
                com.balh.oms.cluster.AcceptOrderCommand.TIF_DAY,
                /* ledgerBalanceIdOrNull */ null,
                0, 0L, (byte) 1, 0L, 0);

        String json = OmsProjectorRebuildFromSnapshotTool.toJsonLine(order);

        assertThat(json).doesNotContain("ledgerBalanceId");
    }
}
