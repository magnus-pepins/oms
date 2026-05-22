package com.balh.oms.docs.runbook;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runbook hygiene — primary pop restart must not be bare {@code pm2 restart oms-cluster-node}.
 * See plans/oms-cluster-recovery-and-hardening.md §1.3.
 */
class OmsRunbookContentTest {

    private static final Path RESTART_RUNBOOK =
            Path.of("docs/runbooks/oms-cluster-restart.md");

    @Test
    void normalRestartSection_pointsAtSanctionedScript() throws Exception {
        String content = Files.readString(RESTART_RUNBOOK);
        int normalIdx = content.indexOf("## Normal rolling restart");
        assertThat(normalIdx).isGreaterThanOrEqualTo(0);
        String normalSection = content.substring(normalIdx);
        int nextSection = normalSection.indexOf("\n## ", 1);
        if (nextSection > 0) {
            normalSection = normalSection.substring(0, nextSection);
        }
        assertThat(normalSection).contains("restart-pop-oms-cluster.sh");
        assertThat(normalSection).doesNotContain("pm2 restart oms-cluster-node");
    }
}
