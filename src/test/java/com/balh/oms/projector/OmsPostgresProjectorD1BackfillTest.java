package com.balh.oms.projector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.marketdata.MarketdataPlatformHttpClient;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.balh.oms.persistence.MarketContextRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.persistence.PositionsRepository;
import com.balh.oms.tailer.OrderControlAdmission;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Phase 4 Tier 2.5 phase D-1 (introduced) / phase D-9 (promoted to primary) — covers
 * {@link OmsPostgresProjector}'s ledger inflight outbox projection: idempotently writes a
 * {@code ledger_inflight_outbox} row from the cluster's {@link OrderAdmittedEvent} so the
 * slice 4p reconciler/compensator pipeline drives the BUY hold (or compensating cancel).
 *
 * <p>D-1 added this as a crash-window backfill (ingress was the primary writer, projector
 * filled in if the ingress JVM crashed between cluster admit and its INSERT tx commit). D-9
 * removed the ingress-side INSERT entirely, so the projector is now the only writer on the
 * BUY-async path. The gating + payload contract this test pins down is unchanged across both
 * phases — the slice 4p reconciler keeps consuming the same row shape it always has, and
 * {@code insertIfAbsent} stays idempotent on cursor-rewind / recording-replay.
 *
 * <p>Scope: gating on {@code oms.ledger.inflight-reservation-enabled},
 * {@code oms.ledger.inflight-async-enabled}, {@code side=BUY}, non-null {@code ledgerBalanceId}
 * and non-zero limit price; plus the payload shape ({@code ledgerBalanceId} /
 * {@code quantity} / {@code limitPrice} as plain-string decimals) that the reconciler parses.
 */
@ExtendWith(MockitoExtension.class)
class OmsPostgresProjectorD1BackfillTest {

    private static final long ACCEPTED_AT_MS = 1_700_000_000_000L;
    private static final long FRAGMENT_POSITION = 8192L;

    @Mock private AeronProjectorCursorRepository cursorRepository;
    @Mock private OrdersRepository ordersRepository;
    @Mock private OrderControlAdmission controlAdmission;
    @Mock private ExecutionsRepository executionsRepository;
    @Mock private DomainEventOutboxRepository domainEventOutboxRepository;
    @Mock private LedgerInflightOutboxRepository ledgerInflightOutboxRepository;
    @Mock private DomainEventEnvelopeCodec envelopeCodec;
    @Mock private MarketContextRepository marketContextRepository;
    @Mock private PositionsRepository positionsRepository;
    @Mock private ObjectProvider<MarketdataPlatformHttpClient> marketdataHttp;
    @Mock private PlatformTransactionManager txManager;

    private OmsConfig config;
    private OmsPostgresProjector projector;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        Clock pinned = Clock.fixed(Instant.ofEpochMilli(ACCEPTED_AT_MS + 1L), ZoneOffset.UTC);
        projector = new OmsPostgresProjector(
                config,
                cursorRepository,
                ordersRepository,
                controlAdmission,
                executionsRepository,
                domainEventOutboxRepository,
                ledgerInflightOutboxRepository,
                envelopeCodec,
                marketContextRepository,
                positionsRepository,
                marketdataHttp,
                meterRegistry,
                objectMapper,
                txManager,
                pinned);
    }

    @Test
    void inflightReservationDisabled_skipsProjection() {
        config.getLedger().setInflightReservationEnabled(false);
        config.getLedger().setInflightAsyncEnabled(true);

        projector.applyAdmittedEvent(buyAdmittedWithLedger(123L, "10000"), FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
    }

    @Test
    void inflightAsyncDisabled_skipsProjection() {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(false);

        projector.applyAdmittedEvent(buyAdmittedWithLedger(123L, "10000"), FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
    }

    @Test
    void sellSide_skipsProjection() {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);

        OrderAdmittedEvent ev = admitted(
                AcceptOrderCommand.SIDE_SELL,
                /* quantityScaled = */ 5_000_000_000L,
                /* limitPriceScaled = */ 100_000L,
                "ledger-bal-1");

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
    }

    @Test
    void noLedgerBalanceId_skipsProjection() {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);

        OrderAdmittedEvent ev = admitted(
                AcceptOrderCommand.SIDE_BUY,
                /* quantityScaled = */ 5_000_000_000L,
                /* limitPriceScaled = */ 100_000L,
                /* ledgerBalanceId = */ null);

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
    }

    @Test
    void marketOrder_zeroLimitPrice_skipsProjection() {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);

        OrderAdmittedEvent ev = admitted(
                AcceptOrderCommand.SIDE_BUY,
                /* quantityScaled = */ 5_000_000_000L,
                /* limitPriceScaled = */ 0L,
                "ledger-bal-1");

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository, never()).insertIfAbsent(any(), any());
    }

    @Test
    void buyAsyncWithLedgerAndLimit_writesIdempotentRowWithExpectedPayload() throws Exception {
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);

        long quantityScaled = 12_345_000_000L; // 12.345
        long limitPriceScaled = 100_500_000L;  // 100.5
        OrderAdmittedEvent ev = admitted(
                AcceptOrderCommand.SIDE_BUY,
                quantityScaled,
                limitPriceScaled,
                "ledger-bal-d1");

        when(ledgerInflightOutboxRepository.insertIfAbsent(eq(ev.orderId()), any())).thenReturn(true);

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(ledgerInflightOutboxRepository, times(1))
                .insertIfAbsent(eq(ev.orderId()), payloadCaptor.capture());

        JsonNode payload = objectMapper.readTree(payloadCaptor.getValue());
        assertThat(payload.get("ledgerBalanceId").asText()).isEqualTo("ledger-bal-d1");
        // BigDecimal value-equality is what matters to the reconciler; toPlainString round-trip
        // gives us "12.3450000000" / "100.5000000000". The reconciler parses these back via
        // new BigDecimal(...) and compareTo() the original — bit-exact representation isn't
        // required, only numeric equivalence.
        assertThat(new java.math.BigDecimal(payload.get("quantity").asText()))
                .isEqualByComparingTo(new java.math.BigDecimal("12.345"));
        assertThat(new java.math.BigDecimal(payload.get("limitPrice").asText()))
                .isEqualByComparingTo(new java.math.BigDecimal("100.5"));
    }

    @Test
    void replayPath_insertIfAbsentReturnsFalse_isStillTreatedAsSuccess() {
        // Idempotency on cursor-rewind / recording-replay: insertIfAbsent returns false (the
        // row from the original projection of this orderId still exists, ON CONFLICT DO NOTHING)
        // and the projector advances the cursor without throwing. Pre-D-9 this also covered the
        // "ingress already wrote the row" branch; D-9 removed that ingress-side INSERT, so this
        // path now only fires on operator-driven cluster cursor rewinds. Mockito stub returns
        // false by default.
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);

        OrderAdmittedEvent ev = buyAdmittedWithLedger(/* quantityScaled = */ 1_000_000_000L, /* limit = */ "100");

        projector.applyAdmittedEvent(ev, FRAGMENT_POSITION);

        verify(ledgerInflightOutboxRepository).insertIfAbsent(eq(ev.orderId()), any());
        verify(cursorRepository).advance(anyString(), anyInt(), eq(FRAGMENT_POSITION));
    }

    private static OrderAdmittedEvent admitted(
            byte side, long quantityScaled, long limitPriceScaled, String ledgerBalanceId) {
        return new OrderAdmittedEvent(
                UUID.randomUUID(),
                /* clientTimestampNanos = */ 0L,
                /* acceptedAtMillis = */ ACCEPTED_AT_MS,
                quantityScaled,
                limitPriceScaled,
                /* shardId = */ 0,
                /* version = */ 0,
                side,
                AcceptOrderCommand.TIF_DAY,
                "acct",
                "idem-" + UUID.randomUUID(),
                "hash",
                "AAPL",
                ledgerBalanceId);
    }

    private static OrderAdmittedEvent buyAdmittedWithLedger(long quantityScaled, String limitPlain) {
        long limitPriceScaled = new java.math.BigDecimal(limitPlain)
                .movePointRight(6)
                .longValueExact();
        return admitted(AcceptOrderCommand.SIDE_BUY, quantityScaled, limitPriceScaled, "ledger-bal-1");
    }
}
