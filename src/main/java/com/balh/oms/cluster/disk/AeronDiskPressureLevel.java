package com.balh.oms.cluster.disk;

/** Published on the shared media driver disk-pressure Aeron counter. */
public enum AeronDiskPressureLevel {
    OK(0L),
    WARN(1L),
    REJECT(2L),
    CRITICAL(3L);

    private final long counterValue;

    AeronDiskPressureLevel(long counterValue) {
        this.counterValue = counterValue;
    }

    public long counterValue() {
        return counterValue;
    }

    public boolean blocksWrites() {
        return ordinal() >= REJECT.ordinal();
    }

    public static AeronDiskPressureLevel fromCounterValue(long value) {
        for (AeronDiskPressureLevel level : values()) {
            if (level.counterValue == value) {
                return level;
            }
        }
        return REJECT;
    }
}
