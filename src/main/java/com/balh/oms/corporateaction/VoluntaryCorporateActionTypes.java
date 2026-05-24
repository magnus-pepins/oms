package com.balh.oms.corporateaction;

import java.util.Locale;
import java.util.Set;

/** Voluntary corporate actions requiring holder elections before processing (gap plan §5.9 Phase 1 manual). */
public final class VoluntaryCorporateActionTypes {

    public static final String RIGHTS_ISSUE = "RIGHTS_ISSUE";
    public static final String TENDER_OFFER = "TENDER_OFFER";

    private static final Set<String> REQUIRES_ELECTION =
            Set.of(RIGHTS_ISSUE, TENDER_OFFER);

    private VoluntaryCorporateActionTypes() {}

    public static boolean requiresElection(String actionType) {
        if (actionType == null || actionType.isBlank()) {
            return false;
        }
        return REQUIRES_ELECTION.contains(actionType.trim().toUpperCase(Locale.ROOT));
    }
}
