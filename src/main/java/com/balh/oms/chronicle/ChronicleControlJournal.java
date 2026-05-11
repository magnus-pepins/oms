package com.balh.oms.chronicle;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;

/**
 * Chronicle Queue OSS-backed implementation of {@link ControlJournal}.
 *
 * <p>Shares a single {@link ChronicleQueue} instance with
 * {@link ChronicleControlTailReader} (see {@code ChronicleQueueConfiguration}).
 * Excerpt bytes are {@link ControlChronicleWireFormat#CHRONICLE_PROTO_PREFIX} + protobuf
 * {@link com.balh.oms.proto.control.v1.ControlPendingEvent}, or legacy UTF-8 JSON of {@link PendingControlEvent}.
 */
public class ChronicleControlJournal implements ControlJournal {

    private final ChronicleQueue queue;
    private final ExcerptAppender appender;

    public ChronicleControlJournal(ChronicleQueue queue) {
        this.queue = queue;
        this.appender = queue.createAppender();
    }

    @Override
    public synchronized long append(byte[] payload) {
        var buf = Bytes.allocateElasticOnHeap();
        buf.write(payload);
        appender.writeBytes(buf);
        return appender.lastIndexAppended();
    }
}
