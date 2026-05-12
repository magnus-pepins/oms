package com.balh.oms.chronicle;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.tailer.ControlTailer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChronicleControlTailReaderReplayStartTest {

    @Mock
    private ChronicleQueue queue;

    @Mock
    private ExcerptTailer excerptTailer;

    @Mock
    private ControlJournal controlJournal;

    @Mock
    private ControlTailer controlTailer;

    @Mock
    private ControlChroniclePayloadCodec codec;

    @Test
    void replayFromStartOnBootTrueInvokesToStart() {
        when(queue.createTailer("tid")).thenReturn(excerptTailer);
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setControlTailId("tid");
        cfg.getChronicle().setControlTailReplayFromStartOnBoot(true);
        cfg.getChronicle().setTailDriver(ChronicleTailDriver.SCHEDULED);
        var reader = new ChronicleControlTailReader(
                queue, controlJournal, controlTailer, codec, cfg, new SimpleMeterRegistry());
        reader.startTailer();
        verify(excerptTailer).toStart();
    }

    @Test
    void replayFromStartOnBootFalseSkipsToStart() {
        when(queue.createTailer("tid")).thenReturn(excerptTailer);
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setControlTailId("tid");
        cfg.getChronicle().setControlTailReplayFromStartOnBoot(false);
        cfg.getChronicle().setTailDriver(ChronicleTailDriver.SCHEDULED);
        var reader = new ChronicleControlTailReader(
                queue, controlJournal, controlTailer, codec, cfg, new SimpleMeterRegistry());
        reader.startTailer();
        verify(excerptTailer, never()).toStart();
    }
}
