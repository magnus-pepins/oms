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
    void sideName_mappingMatchesDomainSideName() {
        // Two-byte side wire: BUY=1, SELL=2 (see AcceptOrderCommand.sideName equivalent in
        // cluster wire format). Assert against Side enum to catch any drift.
        assertThat(OmsProjectorRebuildFromSnapshotTool.sideName((byte) 1)).isEqualTo(Side.BUY.name());
        assertThat(OmsProjectorRebuildFromSnapshotTool.sideName((byte) 2)).isEqualTo(Side.SELL.name());
    }

    @Test
    void sideName_unknown_throws() {
        assertThatThrownBy(() -> OmsProjectorRebuildFromSnapshotTool.sideName((byte) 0))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void tifName_knownCodes_returnHumanReadable() {
        assertThat(OmsProjectorRebuildFromSnapshotTool.tifName((byte) 1)).isEqualTo("DAY");
        assertThat(OmsProjectorRebuildFromSnapshotTool.tifName((byte) 2)).isEqualTo("GTC");
        assertThat(OmsProjectorRebuildFromSnapshotTool.tifName((byte) 3)).isEqualTo("IOC");
        assertThat(OmsProjectorRebuildFromSnapshotTool.tifName((byte) 4)).isEqualTo("FOK");
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
                /* side */ (byte) 1,
                /* quantityScaled */ 100_000_000L,
                /* limitPriceScaledOrZero */ 15_000_000_000L,
                /* timeInForceCode */ (byte) 1,
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
                (byte) 1, 100L, 0L, (byte) 1,
                /* ledgerBalanceIdOrNull */ null,
                0, 0L, (byte) 1, 0L, 0);

        String json = OmsProjectorRebuildFromSnapshotTool.toJsonLine(order);

        assertThat(json).doesNotContain("ledgerBalanceId");
    }
}
