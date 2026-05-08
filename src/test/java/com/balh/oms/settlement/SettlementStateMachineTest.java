package com.balh.oms.settlement;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SettlementStateMachineTest {

    @Test
    void forwardChain() {
        assertThat(SettlementStateMachine.next("executed")).contains("matched");
        assertThat(SettlementStateMachine.next("matched")).contains("confirmed");
        assertThat(SettlementStateMachine.next("confirmed")).contains("settling");
        assertThat(SettlementStateMachine.next("settling")).contains("settled");
        assertThat(SettlementStateMachine.next("settled")).isEmpty();
    }

    @Test
    void terminalDetection() {
        assertThat(SettlementStateMachine.isTerminal("settled")).isTrue();
        assertThat(SettlementStateMachine.isTerminal("failed")).isTrue();
        assertThat(SettlementStateMachine.isTerminal("EXECUTED")).isFalse();
    }

    @Test
    void caseInsensitive() {
        assertThat(SettlementStateMachine.next("EXECUTED")).isEqualTo(Optional.of("matched"));
    }
}