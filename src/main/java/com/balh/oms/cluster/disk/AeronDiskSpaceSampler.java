package com.balh.oms.cluster.disk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Reads usable/total space for a directory via its mount {@link java.nio.file.FileStore}. */
public final class AeronDiskSpaceSampler {

    private AeronDiskSpaceSampler() {}

    public static AeronDiskSpaceSample sample(Path dataDir) throws IOException {
        Path resolved = dataDir.toAbsolutePath().normalize();
        Files.createDirectories(resolved);
        var store = Files.getFileStore(resolved);
        return new AeronDiskSpaceSample(store.getUsableSpace(), store.getTotalSpace());
    }
}
