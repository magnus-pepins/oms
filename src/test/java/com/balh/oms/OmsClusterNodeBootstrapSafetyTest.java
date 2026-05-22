package com.balh.oms;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatCode;

class OmsClusterNodeBootstrapSafetyTest {

    @TempDir
    Path temp;

    @Test
    void buildContexts_onlyMediaDriverDeletesOnStart() {
        OmsClusterNodeBootstrap.ClusterNodePaths paths =
                new OmsClusterNodeBootstrap.ClusterNodePaths(
                        temp.toString(),
                        temp.resolve("media-driver").toString(),
                        temp.resolve("archive").toString(),
                        temp.resolve("consensus").toString(),
                        temp.resolve("services").toString());

        assertThatCode(() -> OmsClusterNodeBootstrap.assertDestructiveDirPolicy(
                        OmsClusterNodeBootstrap.buildMediaDriverContext(paths),
                        OmsClusterNodeBootstrap.buildArchiveContext(paths)))
                .doesNotThrowAnyException();
    }
}
