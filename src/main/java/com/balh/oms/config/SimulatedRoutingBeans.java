package com.balh.oms.config;

import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.returnpath.ExecutionReportApplier;
import com.balh.oms.routing.SimulatedBrokerDispatcher;
import com.balh.oms.routing.SimulatedExecutionProgram;
import com.balh.oms.routing.SimulatedReturnPathProjectionWorker;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Simulated broker wiring: {@link SimulatedBrokerDispatcher} (outbound seam) +
 * {@link SimulatedReturnPathProjectionWorker} (return-path drain / projection tick).
 */
@Configuration
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "simulated")
public class SimulatedRoutingBeans {

    @Bean
    BlockingQueue<UUID> simulatedRouteOrderQueue(OmsConfig config) {
        int cap = Math.max(1, config.getRouting().getSimulated().getQueueCapacity());
        return new LinkedBlockingQueue<>(cap);
    }

    @Bean
    SimulatedBrokerDispatcher simulatedBrokerDispatcher(BlockingQueue<UUID> simulatedRouteOrderQueue) {
        return new SimulatedBrokerDispatcher(simulatedRouteOrderQueue);
    }

    @Bean
    SimulatedExecutionProgram simulatedExecutionProgram(
            OmsConfig config,
            OrdersRepository orders,
            ExecutionReportApplier applier) {
        return new SimulatedExecutionProgram(config, orders, applier);
    }

    @Bean
    SimulatedReturnPathProjectionWorker simulatedReturnPathProjectionWorker(
            BlockingQueue<UUID> simulatedRouteOrderQueue,
            SimulatedExecutionProgram program,
            OmsConfig config) {
        return new SimulatedReturnPathProjectionWorker(simulatedRouteOrderQueue, program, config);
    }
}
