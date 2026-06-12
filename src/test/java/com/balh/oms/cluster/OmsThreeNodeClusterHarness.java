package com.balh.oms.cluster;

import com.balh.oms.OmsClusterNodeBootstrap;
import com.balh.oms.cluster.OmsAdmissionClusteredService;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.IoUtil;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

final class OmsThreeNodeClusterHarness implements AutoCloseable {

    static final String CLUSTER_MEMBERS = OmsClusterNodeBootstrap.DEFAULT_CLUSTER_MEMBERS_THREE_NODE;
    static final String INGRESS_ENDPOINTS =
            OmsClusterNodeBootstrap.ingressEndpointsFromClusterMembers(CLUSTER_MEMBERS);

    private final List<Member> members = new ArrayList<>(3);

    static OmsThreeNodeClusterHarness start(Path tempDir) {
        OmsThreeNodeClusterHarness harness = new OmsThreeNodeClusterHarness();
        for (int memberId = 0; memberId < 3; memberId++) {
            harness.members.add(Member.boot(tempDir, memberId));
        }
        return harness;
    }

    OmsClusterNodeBootstrap.ClusterNodePaths pathsForClient() {
        return members.get(1).paths;
    }

    OmsAdmissionClusteredService serviceOnMember(int memberId) {
        return members.get(memberId).service;
    }

    void stopMember(int memberId) {
        members.get(memberId).close();
    }

    @Override
    public void close() {
        for (int i = members.size() - 1; i >= 0; i--) {
            members.get(i).close();
        }
    }

    private static final class Member implements AutoCloseable {
        final OmsClusterNodeBootstrap.ClusterNodePaths paths;
        final OmsAdmissionClusteredService service;
        private ClusteredMediaDriver driver;
        private OmsClusterNodeBootstrap.EventsRecordingHandle recording;
        private ClusteredServiceContainer container;

        static Member boot(Path tempDir, int memberId) {
            OmsClusterNodeBootstrap.ClusterNodePaths paths =
                    OmsClusterNodeBootstrap.pathsForMember(tempDir, memberId);
            ensureDirsFirstBoot(paths);

            OmsAdmissionClusteredService service = new OmsAdmissionClusteredService();
            int archivePort = OmsClusterNodeBootstrap.archiveControlPortForMember(memberId);
            ClusteredMediaDriver driver =
                    ClusteredMediaDriver.launch(
                            OmsClusterNodeBootstrap.buildMediaDriverContext(paths),
                            OmsClusterNodeBootstrap.buildArchiveContext(paths, archivePort),
                            OmsClusterNodeBootstrap.buildConsensusModuleContext(
                                    paths, memberId, CLUSTER_MEMBERS));
            OmsClusterNodeBootstrap.EventsRecordingHandle recording =
                    OmsClusterNodeBootstrap.startEventsRecording(paths);
            ClusteredServiceContainer container =
                    ClusteredServiceContainer.launch(
                            OmsClusterNodeBootstrap.buildServiceContainerContext(paths, service));
            return new Member(paths, service, driver, recording, container);
        }

        private Member(
                OmsClusterNodeBootstrap.ClusterNodePaths paths,
                OmsAdmissionClusteredService service,
                ClusteredMediaDriver driver,
                OmsClusterNodeBootstrap.EventsRecordingHandle recording,
                ClusteredServiceContainer container) {
            this.paths = paths;
            this.service = service;
            this.driver = driver;
            this.recording = recording;
            this.container = container;
        }

        @Override
        public void close() {
            if (container != null) {
                container.close();
                container = null;
            }
            if (recording != null) {
                recording.close();
                recording = null;
            }
            if (driver != null) {
                driver.close();
                driver = null;
            }
        }
    }

    private static void ensureDirsFirstBoot(OmsClusterNodeBootstrap.ClusterNodePaths paths) {
        for (String dir :
                new String[] {paths.aeronDirBase(), paths.archiveDir(), paths.clusterDir(), paths.clusterServicesDir()}) {
            File f = new File(dir);
            if (!f.exists() && !f.mkdirs()) {
                throw new IllegalStateException("could not create test dir: " + dir);
            }
        }
        IoUtil.delete(new File(paths.aeronDirectory()), true);
    }
}
