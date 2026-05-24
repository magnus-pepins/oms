package com.balh.oms.corporateaction;

import java.util.Locale;
import java.util.Set;

/** Normalized voluntary-election choices (gap plan §5.9 Phase 1 manual). */
public final class CorporateActionElectionChoices {

    private static final Set<String> PARTICIPATE =
            Set.of("PARTICIPATE", "TENDER", "ACCEPT", "YES", "SUBSCRIBE");

    private static final Set<String> DECLINE = Set.of("DECLINE", "REJECT", "NO", "ABSTAIN", "LAPS");

    private CorporateActionElectionChoices() {}

    public static boolean isParticipate(String rawChoice) {
        return normalized(rawChoice).filter(PARTICIPATE::contains).isPresent();
    }

    public static boolean isDecline(String rawChoice) {
        return normalized(rawChoice).filter(DECLINE::contains).isPresent();
    }

    private static java.util.Optional<String> normalized(String rawChoice) {
        if (rawChoice == null || rawChoice.isBlank()) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(rawChoice.trim().toUpperCase(Locale.ROOT));
    }
}
