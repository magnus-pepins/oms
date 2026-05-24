package com.balh.oms.config;

/**
 * Spring {@link org.springframework.context.annotation.Profile} constants shared across OMS.
 */
public final class OmsProfiles {

    private OmsProfiles() {}

    /**
     * Horizontal ingress replica — HTTP/gRPC order accept stays on; admission is submitted to the cluster as
     * {@code AcceptOrderCommand} through {@link com.balh.oms.cluster.OmsClusterIngressClient}. Do not combine
     * with {@value #POSTGRES_PROJECTOR} or {@value #FIX_EGRESS} on the same JVM (TopologyWorkerProfiles).
     */
    public static final String INGRESS_REPLICA = "oms-ingress-replica";

    /**
     * FIX-in acceptor — QuickFIX/J {@code SocketAcceptor} for external clients; submits
     * {@code AcceptOrderCommand} and returns synchronous admission {@code ExecutionReport}s.
     * Exactly one replica per listen port / session set.
     */
    public static final String FIX_INGRESS = "oms-fix-ingress";

    /**
     * Spring {@code @Profile} expression: load order-accept beans only on ingress JVMs (not on
     * {@value #POSTGRES_PROJECTOR}, {@value #FIX_EGRESS}, or {@value #FIX_INGRESS}).
     */
    public static final String ORDER_ACCEPT_PROFILE =
            "!oms-postgres-projector & !oms-fix-egress & !oms-fix-ingress";

    /**
     * Spring {@code @Profile} expression: JVMs that submit commands through {@code OmsClusterIngressClient}.
     * Superset of {@link #ORDER_ACCEPT_PROFILE}: ingress JVMs offer {@code AcceptOrderCommand} on the HTTP /
     * gRPC accept paths, and {@value #FIX_EGRESS} offers {@code ApplyExecutionReportCommand} on inbound FIX
     * execution reports. Excludes pure cluster-internal roles ({@value #POSTGRES_PROJECTOR}) that read state
     * from the cluster events recording but never offer commands back.
     */
    public static final String CLUSTER_CLIENT_PROFILE =
            "!oms-postgres-projector";

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
     * egress confirmation to the API caller. Deployment, scaled horizontally. This
     * is the future home of the role currently implemented by {@link #INGRESS_REPLICA}.
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
     */
    public static final String FIX_EGRESS = "oms-fix-egress";
}
