package com.balh.oms.cluster.disk;

/** Snapshot of filesystem space for the Aeron data directory mount. */
public record AeronDiskSpaceSample(long usableBytes, long totalBytes) {

    public double freeRatio() {
        if (totalBytes <= 0L) {
            return 0.0d;
        }
        return (double) usableBytes / (double) totalBytes;
    }
}
