package com.balh.oms.fix;

import quickfix.FieldNotFound;
import quickfix.Message;

/**
 * Strategy for handling inbound FIX {@code ExecutionReport} (35=8) and {@code OrderCancelReject}
 * (35=9) messages received by {@link OmsFixApplication}.
 *
 * <p>Two implementations live in the codebase, profile-gated so exactly one is wired per JVM
 * (slice 3d of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}):
 *
 * <ul>
 *   <li>{@link FixInboundHandler} — legacy applier that translates the message into a
 *       return-path command and runs {@code ExecutionReportApplier} inside a {@code @Transactional}
 *       boundary. Loaded on every JVM <em>except</em> {@code oms-fix-egress}.</li>
 *   <li>{@code FixInboundClusterSink} — slice-3d cluster path that translates the message into an
 *       {@code ApplyExecutionReportCommand} and offers it to the cluster ingress client. Loaded
 *       only on {@code oms-fix-egress}.</li>
 * </ul>
 *
 * <p>Both methods are called from QuickFIX/J's {@code Application.fromApp} on the FIX engine
 * thread; {@code FieldNotFound} propagates back to QuickFIX so the message is rejected at the
 * session level (peer sees a {@code SessionLevelReject}). Implementations should not throw on
 * mapping failures that are expected to be silently ignored (unknown order, ER-without-orderId,
 * mid-replay broker quirks): emit a metric and return.
 */
public interface FixInboundExecutionReportSink {

    void handleExecutionReport(Message message) throws FieldNotFound;

    void handleOrderCancelReject(Message message) throws FieldNotFound;
}
