package com.balh.oms.ingress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

/** Phase G slice 1b: OMS-side per-contract tick / price-bounds pre-validation at accept. */
class PredictionMarketTickGateTest {

    private static final String YES = "PREDMKT-TICK-1";
    private static final String NO = "PREDMKT-TICK-1-NO";

    private PredictionMarketContractRepository repository;
    private PredictionMarketTickGate gate;

    @BeforeEach
    void setUp() {
        OmsConfig config = new OmsConfig();
        config.getRouting().setVenueSymbolPrefix("PREDMKT");
        repository = mock(PredictionMarketContractRepository.class);
        gate = new PredictionMarketTickGate(config, repository, new SimpleMeterRegistry());
    }

    @Test
    void onTickPriceAllowed() {
        when(repository.findTickSizeBySymbol(YES)).thenReturn(Optional.of(new BigDecimal("0.01")));
        gate.assertOnTick(YES, new BigDecimal("0.66"));
    }

    @Test
    void offTickPriceRejected422() {
        when(repository.findTickSizeBySymbol(YES)).thenReturn(Optional.of(new BigDecimal("0.01")));
        assertThatThrownBy(() -> gate.assertOnTick(YES, new BigDecimal("0.655")))
                .isInstanceOf(ClusterAdmissionException.class)
                .satisfies(e -> {
                    ClusterAdmissionException ex = (ClusterAdmissionException) e;
                    assertThat(ex.getHttpStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
                    assertThat(ex.getErrorCode()).isEqualTo(PredictionMarketTickGate.REJECT_OFF_TICK);
                });
    }

    @Test
    void outOfBoundsPriceRejected422() {
        when(repository.findTickSizeBySymbol(YES)).thenReturn(Optional.of(new BigDecimal("0.01")));
        // 1.00 is on-tick but at the upper boundary (a binary leg can never rest at certainty).
        assertThatThrownBy(() -> gate.assertOnTick(YES, new BigDecimal("1.00")))
                .isInstanceOf(ClusterAdmissionException.class)
                .satisfies(e ->
                        assertThat(((ClusterAdmissionException) e).getErrorCode())
                                .isEqualTo(PredictionMarketTickGate.REJECT_OUT_OF_BOUNDS));
    }

    @Test
    void noLegEnforcedWithSameTick() {
        when(repository.findTickSizeBySymbol(NO)).thenReturn(Optional.of(new BigDecimal("0.01")));
        assertThatThrownBy(() -> gate.assertOnTick(NO, new BigDecimal("0.405")))
                .isInstanceOf(ClusterAdmissionException.class);
    }

    @Test
    void nonVenueSymbolNeverQueries() {
        gate.assertOnTick("AAPL", new BigDecimal("123.455"));
        verify(repository, times(0)).findTickSizeBySymbol("AAPL");
    }

    @Test
    void marketOrderNullPriceIsNoOp() {
        gate.assertOnTick(YES, null);
        verify(repository, times(0)).findTickSizeBySymbol(YES);
    }

    @Test
    void unknownContractDefersToVenue() {
        when(repository.findTickSizeBySymbol(YES)).thenReturn(Optional.empty());
        // No catalog tick → no OMS-side reject (venue is authoritative).
        gate.assertOnTick(YES, new BigDecimal("0.655"));
    }

    @Test
    void tickCachedAcrossRepeatedAccepts() {
        when(repository.findTickSizeBySymbol(YES)).thenReturn(Optional.of(new BigDecimal("0.01")));
        gate.assertOnTick(YES, new BigDecimal("0.66"));
        gate.assertOnTick(YES, new BigDecimal("0.40"));
        gate.assertOnTick(YES, new BigDecimal("0.55"));
        verify(repository, times(1)).findTickSizeBySymbol(YES);
    }
}
