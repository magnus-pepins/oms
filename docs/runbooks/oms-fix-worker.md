# Runbook: `oms-fix-worker` JVM (FIX-out)

P4 prep: same **no new-order HTTP/gRPC ingress** as [`oms-control-worker.md`](oms-control-worker.md), but this JVM **starts QuickFIX** (`oms.routing.backend=fix`, `oms.fix.auto-start=true`, **`oms.control.postgres-write-path=ingress`** per `application-oms-fix-worker.yaml`).

## Local (Gradle)

```bash
./scripts/oms-fix-worker.sh
# or
./gradlew bootRunFixWorker
```

## Fat JAR (`Start-Class` = `OmsFixWorkerBootstrap`)

```bash
./gradlew bootJarFixWorker
java -jar build/libs/oms-*-SNAPSHOT-fix-worker.jar
```

## Docker (optional)

```bash
./gradlew bootJar bootJarFixWorker
docker build -f docker/Dockerfile.fix-worker --build-arg JAR_FILE=build/libs/oms-0.1.0-SNAPSHOT-fix-worker.jar -t oms-fix-worker:local .
```

## Ports

- **HTTP:** `OMS_HTTP_PORT` (default `8088`).
- **Actuator / Prometheus:** `OMS_FIX_WORKER_MANAGEMENT_SERVER_PORT` (default `8090` in `application-oms-fix-worker.yaml`).

## Verify effective topology

`GET` **`/actuator/info`** on the management port includes **`oms-topology`** (`OmsTopologyInfoContributor`): expect **`control.postgres-write-path=ingress`**, **`routing.backend=fix`**, **`grpc.enabled=false`** for this role.

## Preconditions

- Postgres + shared Chronicle queue dir (same as control path).
- **`oms.control.postgres-write-path=ingress`** (YAML default; no `OrderIngressService` here — admission runs on ingress replicas; tail is dispatch-only + **`fix_nos_route_enqueue_claim`** dedupe for first NOS enqueue), **`oms.grpc.enabled=false`** (forced in YAML; same validators as control-worker plus FIX-specific rules in `FixWorkerTopologyValidator`).
- **Exactly one** active `oms-fix-worker` instance per FIX route / session store (two initiators = split-brain).
- Do **not** activate **`oms-control-worker`** and **`oms-fix-worker`** on the same process.

## Topology

Deploy **ingress** (monolith or `oms-control-worker` replicas for tail+apply **without** `oms.fix.auto-start`) **plus** **one** `oms-fix-worker` for outbound FIX. `ControlWorkerTopologyValidator` rejects `oms-control-worker` + `backend=fix` + `oms.fix.auto-start=true` so the initiator is not accidentally started on the wrong role.
