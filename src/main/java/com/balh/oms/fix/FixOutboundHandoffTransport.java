package com.balh.oms.fix;

/**
 * Where WORKING order ids wait between {@link com.balh.oms.tailer.ControlTailer} and {@link FixOutboundDispatchWorker}.
 */
public enum FixOutboundHandoffTransport {

    /** In-process {@link java.util.concurrent.BlockingQueue} (legacy slice-4 default). */
    MEMORY,

    /** Postgres {@code fix_outbound_handoff} with {@code FOR UPDATE SKIP LOCKED} pop (multi-producer, single consumer). */
    POSTGRES
}
