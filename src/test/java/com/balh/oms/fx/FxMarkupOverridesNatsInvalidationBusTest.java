package com.balh.oms.fx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Behaviour tests for {@link FxMarkupOverridesNatsInvalidationBus}.
 *
 * <p>We pin three things because the bus exists for one reason — closing
 * the cross-JVM window where the ingress has the new override and the
 * projector still has the old one (or vice versa for revoke / expire):
 *
 * <ol>
 *   <li>local writes publish a message that the remote side can decode,
 *   <li>inbound messages from <em>another</em> JVM call
 *       {@code service.refreshNow()},
 *   <li>inbound messages from <em>this</em> JVM never call it (otherwise
 *       every local create would double-load its own cache from DB).
 * </ol>
 */
class FxMarkupOverridesNatsInvalidationBusTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private FxMarkupOverridesNatsInvalidationBus newBus(
            Connection nats,
            FxMarkupOverridesService service,
            SimpleMeterRegistry registry) {
        return new FxMarkupOverridesNatsInvalidationBus(nats, service, mapper, registry);
    }

    @Test
    void start_subscribesAndWiresPropagator() {
        Connection nats = mock(Connection.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        when(nats.createDispatcher(any(MessageHandler.class))).thenReturn(dispatcher);
        FxMarkupOverridesService service = mock(FxMarkupOverridesService.class);

        FxMarkupOverridesNatsInvalidationBus bus = newBus(nats, service, new SimpleMeterRegistry());
        bus.start();

        verify(nats).createDispatcher(any(MessageHandler.class));
        verify(dispatcher).subscribe(eq(FxMarkupOverridesNatsInvalidationBus.SUBJECT));
        // bus registered itself as the service's propagator so create/approve/revoke
        // flow through onto NATS
        verify(service).setChangePropagator(bus);
    }

    @Test
    void localChanged_publishesPayloadWithSelfJvmIdActionAndId() throws Exception {
        Connection nats = mock(Connection.class);
        FxMarkupOverridesService service = mock(FxMarkupOverridesService.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        FxMarkupOverridesNatsInvalidationBus bus = newBus(nats, service, registry);
        bus.localChanged("create", 42L);

        ArgumentCaptor<byte[]> bodyCap = ArgumentCaptor.forClass(byte[].class);
        verify(nats, times(1)).publish(
                eq(FxMarkupOverridesNatsInvalidationBus.SUBJECT), bodyCap.capture());
        JsonNode body = mapper.readTree(bodyCap.getValue());
        assertThat(body.path("jvmId").asText()).isEqualTo(bus.getSelfJvmId());
        assertThat(body.path("action").asText()).isEqualTo("create");
        assertThat(body.path("id").asLong()).isEqualTo(42L);

        assertThat(outCounter(registry, "ok")).isEqualTo(1.0);
        assertThat(outCounter(registry, "fail")).isEqualTo(0.0);
    }

    @Test
    void localChanged_publishFailureCountsButDoesNotThrow() {
        Connection nats = mock(Connection.class);
        FxMarkupOverridesService service = mock(FxMarkupOverridesService.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        // simulate NATS down — runtime exception from publish
        org.mockito.Mockito.doThrow(new RuntimeException("nats down"))
                .when(nats).publish(eq(FxMarkupOverridesNatsInvalidationBus.SUBJECT), any(byte[].class));

        FxMarkupOverridesNatsInvalidationBus bus = newBus(nats, service, registry);
        // local refreshNow already ran in the service; bus must not bubble
        bus.localChanged("approve", 7L);

        assertThat(outCounter(registry, "fail")).isEqualTo(1.0);
        assertThat(outCounter(registry, "ok")).isEqualTo(0.0);
    }

    @Test
    void handleInbound_remoteJvm_triggersRefreshNow() throws Exception {
        Connection nats = mock(Connection.class);
        FxMarkupOverridesService service = mock(FxMarkupOverridesService.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FxMarkupOverridesNatsInvalidationBus bus = newBus(nats, service, registry);

        byte[] payload = mapper.writeValueAsBytes(
                mapper.createObjectNode()
                        .put("jvmId", "remote-jvm-uuid")
                        .put("action", "create")
                        .put("id", 99L));
        bus.handleInbound(stubMessage(payload));

        verify(service, times(1)).refreshNow();
        assertThat(inCounter(registry, "applied")).isEqualTo(1.0);
        assertThat(inCounter(registry, "self_filtered")).isEqualTo(0.0);
    }

    @Test
    void handleInbound_selfOriginated_isFilteredAndDoesNotRefresh() throws Exception {
        Connection nats = mock(Connection.class);
        FxMarkupOverridesService service = mock(FxMarkupOverridesService.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FxMarkupOverridesNatsInvalidationBus bus = newBus(nats, service, registry);

        byte[] payload = mapper.writeValueAsBytes(
                mapper.createObjectNode()
                        .put("jvmId", bus.getSelfJvmId())
                        .put("action", "create")
                        .put("id", 1L));
        bus.handleInbound(stubMessage(payload));

        verify(service, never()).refreshNow();
        assertThat(inCounter(registry, "self_filtered")).isEqualTo(1.0);
        assertThat(inCounter(registry, "applied")).isEqualTo(0.0);
    }

    @Test
    void handleInbound_garbledJsonIncrementsApplyFailAndDoesNotThrow() {
        Connection nats = mock(Connection.class);
        FxMarkupOverridesService service = mock(FxMarkupOverridesService.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FxMarkupOverridesNatsInvalidationBus bus = newBus(nats, service, registry);

        bus.handleInbound(stubMessage("not json".getBytes()));

        verify(service, never()).refreshNow();
        assertThat(inCounter(registry, "apply_fail")).isEqualTo(1.0);
    }

    private static double outCounter(SimpleMeterRegistry r, String outcome) {
        return Search.in(r)
                .name("oms_fx_markup_overrides_invalidation_total")
                .tag("direction", "out")
                .tag("outcome", outcome)
                .counter()
                .count();
    }

    private static double inCounter(SimpleMeterRegistry r, String outcome) {
        return Search.in(r)
                .name("oms_fx_markup_overrides_invalidation_total")
                .tag("direction", "in")
                .tag("outcome", outcome)
                .counter()
                .count();
    }

    /**
     * Minimal {@link Message} stub via Mockito — the bus only ever reads
     * {@link Message#getData()}, so we wire that one method and let the
     * rest of the interface stay null / default.
     */
    private static Message stubMessage(byte[] payload) {
        Message m = mock(Message.class);
        when(m.getData()).thenReturn(payload);
        return m;
    }
}
