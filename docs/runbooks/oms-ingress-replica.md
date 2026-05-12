# Runbook: `oms-ingress-replica` JVM (horizontal ingress)

P5 prep: same **new-order** HTTP and gRPC surface as the monolith (`@Profile` **`ORDER_ACCEPT_PROFILE`** in `OmsProfiles`). This JVM **appends** to the shared control Chronicle but **does not** run `ChronicleControlTailReader` (`oms.chronicle.control-tail-enabled=false` in `application-oms-ingress-replica.yaml`). Admission for the ingress path uses **`oms.control.postgres-write-path=ingress`** so CAS + domain fanout run in the accept transaction; **`oms-control-worker`** / **`oms-fix-worker`** consume the tail elsewhere.

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

After boot, **`GET http://127.0.0.1:${OMS_INGRESS_REPLICA_MANAGEMENT_SERVER_PORT:-8087}/actuator/info`** includes **`oms-topology`** (`control.postgres-write-path`, `chronicle.control-tail-enabled`, `routing.backend`, …) from `OmsTopologyInfoContributor` — use it to prove this JVM matches the cluster contract (same source of truth as workers for `postgres-write-path`).

## Preconditions

- Postgres + **same** Chronicle queue directory and **`postgres-write-path`** as the rest of the cluster (see [plans/oms-ingress-control-fix-topology.md](../../../system-documentation/plans/oms-ingress-control-fix-topology.md) P1).
- **`oms.chronicle.enabled=true`**, **`oms.chronicle.control-tail-enabled=false`**, **`oms.control.postgres-write-path=ingress`** (YAML default).
- Do **not** activate **`oms-ingress-replica`** together with **`oms-control-worker`** or **`oms-fix-worker`** on the same process (`TopologyWorkerProfiles`).
- Do **not** run QuickFIX **`SocketInitiator`** here (`oms.routing.backend=fix` + **`oms.fix.auto-start=true`**); use **`oms-fix-worker`** for FIX-out.

## Topology

See [`oms/docs/chronicle-tail-driver.md`](../chronicle-tail-driver.md) and [`oms-control-worker.md`](oms-control-worker.md) for **one** active tail consumer per shared queue directory unless operations explicitly isolates readers.
