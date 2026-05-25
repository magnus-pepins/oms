package com.balh.oms.corporateaction;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Service;

/** Broker-facing election export (gap plan §5.9 Phase 2). */
@Service
public class CorporateActionBrokerElectionExportService {

    private final CorporateActionEventRepository events;
    private final CorporateActionElectionRepository elections;

    public CorporateActionBrokerElectionExportService(
            CorporateActionEventRepository events, CorporateActionElectionRepository elections) {
        this.events = events;
        this.elections = elections;
    }

    public Optional<Map<String, Object>> exportForEvent(long eventId) {
        return events.findById(eventId).map(event -> {
            List<Map<String, Object>> rows =
                    elections.listByEvent(eventId).stream()
                            .filter(r -> r.approvedAt() != null)
                            .map(
                                    r -> {
                                        Map<String, Object> m = new LinkedHashMap<>();
                                        m.put("accountId", r.accountId().toString());
                                        m.put("electionChoice", r.electionChoice());
                                        m.put("requestedBy", r.requestedBy());
                                        m.put("approvedBy", r.approvedBy());
                                        m.put("approvedAt", r.approvedAt().toString());
                                        return m;
                                    })
                            .toList();
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("eventId", eventId);
            body.put("instrumentSymbol", event.instrumentSymbol());
            body.put("actionType", event.actionType());
            body.put("effectiveDate", event.effectiveDate());
            body.put("exportedAt", Instant.now().toString());
            body.put("approvedElectionCount", rows.size());
            body.put("elections", rows);
            return body;
        });
    }
}
