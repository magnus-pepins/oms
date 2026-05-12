package com.balh.oms.config;

import com.balh.oms.ingress.grpc.GrpcInternalApiKeyInterceptor;
import com.balh.oms.ingress.grpc.OrderIngressGrpcServiceImpl;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Optional gRPC server for internal order ingress (see {@link OrderIngressGrpcServiceImpl}).
 * Disabled in {@code test} profile and on worker profiles ({@value OmsProfiles#CONTROL_WORKER},
 * {@value OmsProfiles#FIX_WORKER}); bind with {@code oms.grpc.enabled=true}.
 */
@Configuration
@Profile({"!test", OmsProfiles.ORDER_ACCEPT_PROFILE})
@ConditionalOnProperty(prefix = "oms.grpc", name = "enabled", havingValue = "true")
public class OmsGrpcConfiguration {

    private static final Logger log = LoggerFactory.getLogger(OmsGrpcConfiguration.class);

    @Bean
    public OmsGrpcServerLifecycle omsGrpcServerLifecycle(OmsConfig omsConfig, OrderIngressGrpcServiceImpl orderIngressGrpcService) {
        ServerServiceDefinition intercepted = ServerInterceptors.intercept(
                orderIngressGrpcService.bindService(), new GrpcInternalApiKeyInterceptor(omsConfig));
        return new OmsGrpcServerLifecycle(omsConfig, intercepted);
    }

    /** Starts / stops the Netty gRPC server with the rest of the application context. */
    public static final class OmsGrpcServerLifecycle implements SmartLifecycle {

        private final OmsConfig omsConfig;
        private final ServerServiceDefinition serviceDefinition;
        private volatile Server server;
        private volatile boolean running;

        OmsGrpcServerLifecycle(OmsConfig omsConfig, ServerServiceDefinition serviceDefinition) {
            this.omsConfig = omsConfig;
            this.serviceDefinition = serviceDefinition;
        }

        @Override
        public void start() {
            if (running) {
                return;
            }
            int port = omsConfig.getGrpc().getPort();
            try {
                server = NettyServerBuilder.forPort(port).addService(serviceDefinition).build().start();
                running = true;
                log.info("OMS gRPC ingress listening on port {} (OrderIngress.CreateOrder)", port);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to start gRPC server on port " + port, e);
            }
        }

        @Override
        public void stop() {
            if (server != null) {
                server.shutdown();
                server = null;
            }
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public int getPhase() {
            return Integer.MAX_VALUE;
        }
    }
}
