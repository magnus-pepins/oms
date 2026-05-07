package com.balh.oms.fix;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import quickfix.SessionID;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds the first logged-on FIX session id for outbound {@code sendToTarget} calls.
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixSessionRegistry {

    private final AtomicReference<SessionID> activeSession = new AtomicReference<>();

    public void setActiveSession(SessionID sessionId) {
        activeSession.set(sessionId);
    }

    public void clear() {
        activeSession.set(null);
    }

    public SessionID sessionOrNull() {
        return activeSession.get();
    }

    public boolean hasLoggedOnSession() {
        return activeSession.get() != null;
    }
}
