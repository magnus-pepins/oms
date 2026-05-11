package com.balh.oms.config;

import com.balh.oms.fix.FixOutboundOrderDequeue;
import com.balh.oms.fix.FixRouteDispatcher;
import com.balh.oms.fix.PostgresFixHandoffRouteDispatcher;
import com.balh.oms.fix.PostgresFixOutboundOrderDequeue;
import com.balh.oms.fix.QueueFixOutboundOrderDequeue;
import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import com.balh.oms.persistence.FixOutboundHandoffRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Configuration
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixRoutingBeans {

    @Bean
    @ConditionalOnProperty(prefix = "oms.fix", name = "outbound-handoff-transport", havingValue = "memory", matchIfMissing = true)
    BlockingQueue<UUID> fixOutboundPendingOrderQueue(OmsConfig omsConfig) {
        int cap = Math.max(1, omsConfig.getFix().getOutboundQueueCapacity());
        return new LinkedBlockingQueue<>(cap);
    }

    @Bean
    @ConditionalOnProperty(prefix = "oms.fix", name = "outbound-handoff-transport", havingValue = "memory", matchIfMissing = true)
    FixRouteDispatcher fixRouteDispatcher(
            BlockingQueue<UUID> fixOutboundPendingOrderQueue,
            IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder) {
        return new FixRouteDispatcher(fixOutboundPendingOrderQueue, ingressToFixNosLatencyRecorder);
    }

    @Bean
    @ConditionalOnProperty(prefix = "oms.fix", name = "outbound-handoff-transport", havingValue = "memory", matchIfMissing = true)
    FixOutboundOrderDequeue memoryFixOutboundOrderDequeue(FixRouteDispatcher fixRouteDispatcher) {
        return new QueueFixOutboundOrderDequeue(fixRouteDispatcher);
    }

    @Bean
    @ConditionalOnProperty(prefix = "oms.fix", name = "outbound-handoff-transport", havingValue = "postgres")
    PostgresFixHandoffRouteDispatcher postgresFixHandoffRouteDispatcher(FixOutboundHandoffRepository repository) {
        return new PostgresFixHandoffRouteDispatcher(repository);
    }

    @Bean
    @ConditionalOnProperty(prefix = "oms.fix", name = "outbound-handoff-transport", havingValue = "postgres")
    FixOutboundOrderDequeue postgresFixOutboundOrderDequeue(
            FixOutboundHandoffRepository repository, PlatformTransactionManager transactionManager) {
        return new PostgresFixOutboundOrderDequeue(repository, new TransactionTemplate(transactionManager));
    }
}
