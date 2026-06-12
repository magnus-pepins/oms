/**
 * Pop 3-node OMS lab — coexists with the demo single-node stack.
 *
 * Port plan (disjoint from demo OMS 20110-block/8010/8089, ledger lab 35110+/23010+/31099+,
 * venue lab 45110+/26010+/8293+):
 *   member 0: cluster 25110-25440, archive 13010, metrics 18089
 *   member 1: cluster 26110-26440, archive 14010, metrics 19089
 *   member 2: cluster 27110-27440, archive 15010, metrics 20089
 */
const path = require('path');

const repoRoot = path.resolve(__dirname);
const jar = path.join(repoRoot, 'build/libs/oms-cluster-node.jar');

const LAB_BASE_INGRESS = 25110;
const LAB_BASE_ARCHIVE = 13010;
const LAB_BASE_METRICS = 18089;
const PORT_STEP = 1000;

function memberTuple(memberId) {
  const o = memberId * PORT_STEP;
  return (
    `${memberId},localhost:${LAB_BASE_INGRESS + o},localhost:${25220 + o},` +
    `localhost:${25330 + o},localhost:${25440 + o},localhost:${LAB_BASE_ARCHIVE + o}`
  );
}

const THREE_NODE_MEMBERS = [0, 1, 2].map(memberTuple).join('|');

function memberApp(memberId) {
  const o = memberId * PORT_STEP;
  return {
    name: `oms-cluster-node-lab-${memberId}`,
    script: 'java',
    args: ['-jar', jar],
    cwd: repoRoot,
    env: {
      OMS_AERON_CLUSTER_MEMBER_ID: String(memberId),
      OMS_AERON_CLUSTER_MEMBERS: THREE_NODE_MEMBERS,
      OMS_AERON_DIR_BASE: path.join(repoRoot, `build/aeron-cluster-lab/member-${memberId}`),
      OMS_AERON_ARCHIVE_CONTROL_CHANNEL: `aeron:udp?endpoint=localhost:${LAB_BASE_ARCHIVE + o}`,
      OMS_CLUSTER_NODE_METRICS_PORT: String(LAB_BASE_METRICS + memberId * PORT_STEP),
      OMS_CLUSTER_SNAPSHOT_INTERVAL_MS: '300000',
    },
    autorestart: true,
    kill_timeout: 30000,
    max_restarts: 10,
  };
}

module.exports = {
  apps: [memberApp(0), memberApp(1), memberApp(2)],
};
