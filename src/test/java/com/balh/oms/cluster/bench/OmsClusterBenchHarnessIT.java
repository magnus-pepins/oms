package com.balh.oms.cluster.bench;

import static org.assertj.core.api.Assertions.assertThat;

import com.balh.oms.cluster.bench.OmsClusterBenchHarness.BenchResult;
import com.balh.oms.cluster.bench.OmsClusterBenchHarness.Config;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Phase 4 slice 4e — smoke IT for {@link OmsClusterBenchHarness}.
 *
 * <p>Drives the harness end-to-end via {@link OmsClusterBenchHarness#run(Config)} with a
 * deliberately tiny configuration (1 s warmup + 2 s steady at 200 ops/s) so the test runs in
 * seconds while still exercising every code path: cluster boot, client connect, warmup phase,
 * steady-state phase, commit-round-trip recording, output writing.
 *
 * <p>Asserts:
 * <ul>
 *   <li>{@code summary.md} exists, parses, and reports a non-zero commit count and at least p50
 *       and p99 lines.</li>
 *   <li>{@code histogram.hgrm} exists and contains the HdrHistogram percentile-distribution
 *       header.</li>
 *   <li>The {@link BenchResult} returned from {@code run} matches the on-disk summary — defends
 *       against a future refactor that accidentally writes a stale histogram to the file.</li>
 * </ul>
 *
 * <p>Slow IT (~5–8 s wall clock, dominated by cluster boot). Generous explicit budgets are
 * inherited from {@link OmsClusterBenchHarness} itself; we don't second-guess them here.
 */
class OmsClusterBenchHarnessIT {

    /** Tiny enough to fit comfortably in CI but large enough to record real samples. */
    private static final int WARMUP_S = 1;

    private static final int DURATION_S = 2;

    private static final int THROUGHPUT_OPS_PER_S = 200;

    private static final int TIMEOUT_MS = 5_000;

    @Test
    void mainEndToEnd_writesSummaryAndHistogramWithCommits(@TempDir Path tmp) throws IOException {
        Path reportDir = tmp.resolve("report");
        Path aeronDir = tmp.resolve("aeron");

        Config config = new Config(
                WARMUP_S, DURATION_S, THROUGHPUT_OPS_PER_S, TIMEOUT_MS, reportDir, aeronDir);

        BenchResult result = OmsClusterBenchHarness.run(config);

        assertThat(result.commits())
                .as("at least 1 commit must be recorded in steady state")
                .isGreaterThanOrEqualTo(1L);

        Path summary = reportDir.resolve("summary.md");
        Path histogram = reportDir.resolve("histogram.hgrm");

        assertThat(summary).as("summary.md must exist after a successful bench").exists();
        assertThat(histogram).as("histogram.hgrm must exist after a successful bench").exists();

        String summaryText = Files.readString(summary, StandardCharsets.UTF_8);
        String histogramText = Files.readString(histogram, StandardCharsets.UTF_8);

        long summaryCommits = extractLong(summaryText, "\\| commit \\| (\\d+) \\|");
        assertThat(summaryCommits)
                .as("summary.md commit count must match BenchResult; tail: %s", tail(summaryText))
                .isEqualTo(result.commits());

        long summaryP50 = extractLong(summaryText, "\\| p50 \\| (\\d+) \\|");
        long summaryP99 = extractLong(summaryText, "\\| p99 \\| (\\d+) \\|");
        long summaryP999 = extractLong(summaryText, "\\| p99\\.9 \\| (\\d+) \\|");
        assertThat(summaryP50)
                .as("p50 must be > 0 µs; if this is 0 the harness wrote an empty histogram")
                .isGreaterThan(0L);
        assertThat(summaryP99)
                .as("p99 must be >= p50")
                .isGreaterThanOrEqualTo(summaryP50);
        assertThat(summaryP999)
                .as("p99.9 must be >= p99")
                .isGreaterThanOrEqualTo(summaryP99);

        assertThat(histogramText)
                .as("histogram.hgrm must be the HdrHistogram percentile-distribution format")
                .contains("Value")
                .contains("Percentile")
                .contains("TotalCount");
    }

    @Test
    void newHistogram_recordsAndReportsExpectedPercentiles() {
        Histogram h = OmsClusterBenchHarness.newHistogram();
        for (long micros : List.of(100L, 200L, 300L, 400L, 500L)) {
            h.recordValue(micros);
        }
        assertThat(h.getValueAtPercentile(50.0)).isBetween(290L, 310L);
        assertThat(h.getValueAtPercentile(99.0)).isBetween(495L, 505L);
        assertThat(h.getMaxValue()).isBetween(495L, 505L);
    }

    @Test
    void clamp_lowAndHigh() {
        assertThat(OmsClusterBenchHarness.clamp(0L, 1L, 100L)).isEqualTo(1L);
        assertThat(OmsClusterBenchHarness.clamp(50L, 1L, 100L)).isEqualTo(50L);
        assertThat(OmsClusterBenchHarness.clamp(1_000L, 1L, 100L)).isEqualTo(100L);
    }

    @Test
    void config_fromEnv_appliesDefaultsWhenEnvUnset() {
        Config c = Config.fromEnv();
        assertThat(c.warmupSeconds()).isPositive();
        assertThat(c.durationSeconds()).isPositive();
        assertThat(c.throughputOpsPerSec()).isPositive();
        assertThat(c.timeoutMs()).isPositive();
        assertThat(c.reportDir()).isNotNull();
        assertThat(c.aeronDirBase()).isNotNull();
    }

    private static long extractLong(String text, String regex) {
        Matcher m = Pattern.compile(regex).matcher(text);
        if (!m.find()) {
            throw new AssertionError(
                    "Pattern not found: " + regex + " — summary tail: " + tail(text));
        }
        return Long.parseLong(m.group(1));
    }

    private static String tail(String s) {
        int len = s.length();
        return len <= 600 ? s : "..." + s.substring(len - 600);
    }
}
