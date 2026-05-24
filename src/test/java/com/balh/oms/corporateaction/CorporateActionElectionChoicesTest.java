package com.balh.oms.corporateaction;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CorporateActionElectionChoicesTest {

    @Test
    void isParticipate_recognizesSynonyms() {
        assertThat(CorporateActionElectionChoices.isParticipate("PARTICIPATE")).isTrue();
        assertThat(CorporateActionElectionChoices.isParticipate("tender")).isTrue();
        assertThat(CorporateActionElectionChoices.isParticipate("SUBSCRIBE")).isTrue();
    }

    @Test
    void isDecline_recognizesSynonyms() {
        assertThat(CorporateActionElectionChoices.isDecline("DECLINE")).isTrue();
        assertThat(CorporateActionElectionChoices.isDecline("no")).isTrue();
        assertThat(CorporateActionElectionChoices.isDecline("LAPS")).isTrue();
    }

    @Test
    void blankChoice_isNeitherParticipateNorDecline() {
        assertThat(CorporateActionElectionChoices.isParticipate("")).isFalse();
        assertThat(CorporateActionElectionChoices.isDecline(null)).isFalse();
    }
}
