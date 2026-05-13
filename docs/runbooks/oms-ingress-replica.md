# Runbook: `oms-ingress-replica` JVM (horizontal ingress)

Same **new-order** HTTP and gRPC surface as the monolith (`@Profile` **`ORDER_ACCEPT_PROFILE`** in `OmsProfiles`). Submits `AcceptOrderCommand` to the cluster leader through `OmsClusterIngressClient` — the cluster + `oms-postgres-projector` + `oms-fix-egress` JVMs handle admission, projection, and outbound FIX. This JVM never writes `orders` / `executions` directly.

## Local (Gradle)

```bash
./scripts/oms-ingress-replica.sh
# or
./gradlew bootRunIngressReplica
```

## Fat JAR (`Start-Class` = `OmsIngressReplicaBootstrap`)

```bash
./gradlew bootJarIngressReplica
java -jar build/libs/oms-*-SNAPSHOT-ingress-replica.jar
```

## Docker (optional)

```bash
./gradlew bootJarIngressReplica
docker build -f docker/Dockerfile.ingress-replica --build-arg JAR_FILE=build/libs/oms-0.1.0-SNAPSHOT-ingress-replica.jar -t oms-ingress-replica:local .
```

## Ports

- **HTTP:** `OMS_HTTP_PORT` (default `8088`).
- **Actuator / Prometheus:** `OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT` (default `8087` in `application-oms-ingress-replica.yaml`).

## Verify effective topology

After boot, **`GET http://127.0.0.1:${OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT:-8087}/actuator/info`** includes **`oms-topology`** (`routing.backend`, `grpc.enabled`, `fix.auto-start`, active Spring profiles) from `OmsTopologyInfoContributor`.

## Preconditions

- Postgres reachable for projector reads on the API path (e.g. order-by-id GET, idempotent dedupe lookup) and for the same `domain_event_outbox` writes the monolith does.
- Cluster client wiring (`oms.cluster.client.enabled=true` in the active topology) so `OrderIngressService` can submit `AcceptOrderCommand` and await the egress reply.
- Do **not** activate **`oms-ingress-replica`** together with **`oms-postgres-projector`** or **`oms-fix-egress`** on the same process (`TopologyWorkerProfiles`).
- Do **not** run QuickFIX `SocketInitiator` here (`oms.routing.backend=fix` + **`oms.fix.auto-start=true`**); use **`oms-fix-egress`** for FIX-out.

## Topology

See [plans/oms-aeron-cluster-substrate.md](../../../system-documentation/plans/oms-aeron-cluster-substrate.md) for the cluster-substrate topology (one cluster per shard, multiple ingress JVMs as cluster clients, exactly one `oms-fix-egress` per FIX route).
