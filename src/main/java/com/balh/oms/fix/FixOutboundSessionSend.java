package com.balh.oms.fix;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;

/**
 * Single outbound call site for {@link Session#sendToTarget(quickfix.Message, SessionID)} on the
 * active initiator session (same session {@link FixOutboundDispatchWorker} uses for NOS).
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixOutboundSessionSend {

    private final FixSessionRegistry fixSessionRegistry;

    public FixOutboundSessionSend(FixSessionRegistry fixSessionRegistry) {
        this.fixSessionRegistry = fixSessionRegistry;
    }

    public boolean hasActiveSession() {
        return fixSessionRegistry.hasLoggedOnSession();
    }

    /**
     * @throws SessionNotFound when no logged-on session or QuickFIX rejects the send
     */
    public void send(Message message) throws SessionNotFound {
        SessionID sessionId = fixSessionRegistry.sessionOrNull();
        if (sessionId == null) {
            throw new SessionNotFound();
        }
        Session.sendToTarget(message, sessionId);
    }
}
