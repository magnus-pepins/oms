package com.balh.oms.venue;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import com.balh.venue.cluster.VenueClusterWireFormat;
import com.balh.venue.grpc.v1.RegisterPredictionMarketContractRequest;
import com.balh.venue.grpc.v1.RegisterPredictionMarketContractResponse;
import com.balh.venue.grpc.v1.VenueCatalogServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Syncs OMS prediction-market catalog rows into the venue contract registry (Phase C). */
@Component
public class VenueContractRegistryClient {

    private static final Logger log = LoggerFactory.getLogger(VenueContractRegistryClient.class);

    private final OmsConfig omsConfig;
    private final ManagedChannel channel;
    private final VenueCatalogServiceGrpc.VenueCatalogServiceBlockingStub catalogStub;

    public VenueContractRegistryClient(OmsConfig omsConfig) {
        this.omsConfig = omsConfig;
        this.channel =
                ManagedChannelBuilder.forAddress(
                                omsConfig.getVenue().getGrpcHost(), omsConfig.getVenue().getGrpcPort())
                        .usePlaintext()
                        .build();
        this.catalogStub = VenueCatalogServiceGrpc.newBlockingStub(channel);
    }

    public void syncContract(PredictionMarketContractRepository.ContractRow row) {
        if (!omsConfig.getVenue().isRegistrySyncEnabled()) {
            return;
        }
        long tickScaled = tickSizeToScaled(row.tickSize());
        RegisterPredictionMarketContractRequest request =
                RegisterPredictionMarketContractRequest.newBuilder()
                        .setSlug(row.slug())
                        .setYesSymbol(row.yesSymbol())
                        .setNoSymbol(row.noSymbol())
                        .setTickSizeScaled(tickScaled)
                        .setStatus(row.status())
                        .build();
        try {
            RegisterPredictionMarketContractResponse response =
                    catalogStub.registerPredictionMarketContract(request);
            if (!response.getAccepted()) {
                throw new IllegalStateException(
                        "venue registry rejected slug="
                                + row.slug()
                                + " reason="
                                + response.getRejectReason());
            }
            log.info(
                    "venue registry synced slug={} yes={} no={} status={}",
                    row.slug(),
                    row.yesSymbol(),
                    row.noSymbol(),
                    row.status());
        } catch (RuntimeException e) {
            log.error("venue registry sync failed slug={}", row.slug(), e);
            throw e;
        }
    }

    static long tickSizeToScaled(BigDecimal tickSize) {
        if (tickSize == null || tickSize.signum() <= 0) {
            return 10_000L;
        }
        return tickSize
                .multiply(BigDecimal.valueOf(VenueClusterWireFormat.PRICE_SCALE))
                .setScale(0, RoundingMode.HALF_UP)
                .longValue();
    }

    @PreDestroy
    void shutdown() {
        channel.shutdown();
    }
}
