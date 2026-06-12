# Runbook: OMS cluster member replace (S2/S3)

**Related:** [rolling-upgrade-production.md](../../../system-documentation/plans/rolling-upgrade-production.md), [oms-cluster-node-snapshot.md](./oms-cluster-node-snapshot.md), [oms-cluster-restart.md](./oms-cluster-restart.md) (S4).

## S2 PR checklist

- [ ] Replay-equivalence review (no admission semantics change without S3).
- [ ] Same SHA: cluster-node + postgres-projector + fix-egress + ingress + venue-egress.
- [ ] `./gradlew test` green.

## Procedure

1. Preflight: `GET :8088/actuator/oms-cluster-readiness` = READY; reconcile inSync.
2. `RUN_SNAPSHOT=1` or `./gradlew clusterSnapshot`.
3. Replace **follower** first: stop `oms-cluster-node` on host N; 35s drain; start new jar.
4. On co-located hosts: restart `oms-venue-egress` bound to that member if severed.
5. Soak; repeat; ex-leader last.
6. Rolling S1: ingress, projector, fix-egress (same SHA).
7. Smoke: ledger-end-to-end → oms-end-to-end.

## 3-node env

```bash
export OMS_AERON_CLUSTER_MEMBER_ID=0
export OMS_AERON_CLUSTER_MEMBERS='0,localhost:20110,...,8010|1,localhost:21110,...,9010|2,localhost:22110,...,10010'
export OMS_AERON_DIR_BASE=build/aeron-cluster/member-0
export OMS_AERON_ARCHIVE_CONTROL_CHANNEL=aeron:udp?endpoint=localhost:8010
```

Ingress clients: `OMS_CLUSTER_CLIENT_INGRESS_ENDPOINTS=0=localhost:20110,1=localhost:21110,2=localhost:22110`

**On pop, do NOT use these defaults** — member 0 collides with the demo single-node
OMS (20110/8010). Use `ecosystem.3node-lab.config.cjs` (25110+/13010+, per-member
metrics 18089+), which is disjoint from all demo stacks.

## FIX egress

Singleton per route — lease handoff before S1 roll ([oms-fix-gateway-and-settlement.md](../../../system-documentation/plans/oms-fix-gateway-and-settlement.md) §14.13).

## Abort

`PATCH /internal/v1/runtime-flags/global_halt` or [rolling-upgrade-runner.sh](../../../system-documentation/scripts/deploy/rolling-upgrade-runner.sh) abort.

## k8s

StatefulSet: [deploy/k8s/oms-cluster-node-statefulset.yaml](../../deploy/k8s/oms-cluster-node-statefulset.yaml). Snapshot CronJob: [deploy/k8s/oms-cluster-snapshot-cronjob.yaml](../../deploy/k8s/oms-cluster-snapshot-cronjob.yaml).
