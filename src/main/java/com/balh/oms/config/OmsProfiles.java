package com.balh.oms.config;

/**
 * Spring {@link org.springframework.context.annotation.Profile} constants shared across OMS.
 */
public final class OmsProfiles {

    private OmsProfiles() {}

    /**
     * P3 prep: Chronicle tail + control apply + FIX path without order-accept ingress (HTTP {@code OrdersController},
     * gRPC {@code OrderIngressGrpcServiceImpl}, {@link com.balh.oms.ingress.OrderIngressService}).
     */
    public static final String CONTROL_WORKER = "oms-control-worker";

    /**
     * P4 prep: same “no new-order ingress” as {@link #CONTROL_WORKER}, but this JVM is allowed to run QuickFIX
     * {@code SocketInitiator} + outbound drain (single initiator per route — do not run two replicas with the same
     * session store).
     */
    public static final String FIX_WORKER = "oms-fix-worker";

    /**
     * P5 prep: horizontal **ingress** replica — HTTP/gRPC order accept stays on; admission runs in the accept
     * transaction ({@code oms.control.postgres-write-path=ingress}); Chronicle **append** stays on but the local
     * {@code ChronicleControlTailReader} is off ({@code oms.chronicle.control-tail-enabled=false}). Do not combine
     * with {@value #CONTROL_WORKER} or {@value #FIX_WORKER} on the same JVM.
     */
    public static final String INGRESS_REPLICA = "oms-ingress-replica";

    /**
     * Spring {@code @Profile} expression: load order-accept beans only on the monolith / ingress JVM (not on
     * {@value #CONTROL_WORKER}, {@value #FIX_WORKER}, or {@value #POSTGRES_PROJECTOR}). {@value #INGRESS_REPLICA}
     * is an ingress JVM and therefore <strong>does</strong> load order-accept beans.
     */
    public static final String ORDER_ACCEPT_PROFILE =
            "!oms-control-worker & !oms-fix-worker & !oms-postgres-projector";

    // ------------------------------------------------------------------------
    // ADR 0001 / topology-aeron-cluster — new role profiles.
    //
    // Each role corresponds to one Deployment / StatefulSet in k8s. Profiles are
    // mutually exclusive on a JVM; the topology validator rejects combinations.
    // ------------------------------------------------------------------------

    /**
     * Aeron Cluster node — runs the MediaDriver, Archive, ConsensusModule, and
     * {@code ClusteredServiceContainer} hosting {@code OmsAdmissionClusteredService}.
     * StatefulSet, typically 3 replicas, per shard. Source of truth for OMS state.
     */
    public static final String CLUSTER_NODE = "oms-cluster-node";

    /**
     * Aeron Cluster client — stateless HTTP / gRPC ingress that submits commands
     * (e.g. {@code AcceptOrder}) to the cluster leader and returns the resulting
     * egress confirmation to the API caller. Deployment, scaled horizontally.
     *
     * <p>Replaces the role of {@link #INGRESS_REPLICA} once the cluster scaffold
     * is wired and Phase 1 of the plan deletes the ingress-side Chronicle path.
     */
    public static final String CLUSTER_CLIENT = "oms-cluster-client";

    /**
     * Postgres projector — subscribes to cluster egress and writes the projection
     * tables ({@code orders}, {@code executions}, {@code control_decisions}, etc.).
     * Idempotent (multiple replicas safe but redundant).
     */
    public static final String POSTGRES_PROJECTOR = "oms-postgres-projector";

    /**
     * FIX egress — subscribes to cluster egress and translates {@code EnqueueNos} /
     * {@code EnqueueOrderCancel} / {@code MassCancel} events to QuickFIX
     * {@code Session.sendToTarget}. Exactly one replica per FIX route (broker
     * constraint).
     *
     * <p>Replaces the role of {@link #FIX_WORKER} once the cluster scaffold is
     * wired and Phase 3 of the plan deletes the legacy outbound dispatch worker.
     */
    public static final String FIX_EGRESS = "oms-fix-egress";
}
