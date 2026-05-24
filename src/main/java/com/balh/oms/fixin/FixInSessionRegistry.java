package com.balh.oms.fixin;

import com.balh.oms.fixin.persistence.FixInSessionRow;
import com.balh.oms.config.OmsProfiles;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import quickfix.SessionID;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Maps QuickFIX {@link SessionID} to OMS {@code oms_fix_in_session.id} after logon. */
@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInSessionRegistry {

    private final Map<SessionID, FixInSessionRow> activeBySessionId = new ConcurrentHashMap<>();
    private final Map<UUID, SessionID> wireIdByFixSessionId = new ConcurrentHashMap<>();

    public void register(SessionID sessionId, FixInSessionRow row) {
        activeBySessionId.put(sessionId, row);
        wireIdByFixSessionId.put(row.id(), sessionId);
    }

    public void unregister(SessionID sessionId) {
        FixInSessionRow removed = activeBySessionId.remove(sessionId);
        if (removed != null) {
            wireIdByFixSessionId.remove(removed.id(), sessionId);
        }
    }

    public Optional<SessionID> findWireSessionId(UUID fixSessionId) {
        return Optional.ofNullable(wireIdByFixSessionId.get(fixSessionId));
    }

    public Optional<FixInSessionRow> find(SessionID sessionId) {
        return Optional.ofNullable(activeBySessionId.get(sessionId));
    }

    public Optional<UUID> fixSessionUuid(SessionID sessionId) {
        return find(sessionId).map(FixInSessionRow::id);
    }
}
