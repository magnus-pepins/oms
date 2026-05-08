package com.balh.oms.ingress;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

/**
 * JSON for {@code GET|PATCH /internal/v1/runtime-flags/global_halt}.
 *
 * @param globalHalt when {@code true}, control rejects all orders with {@code RISK_KILL_SWITCH}.
 * @param updatedAt last write time from {@code oms_runtime_flags}, or null if the row does not exist yet
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record GlobalHaltResponse(boolean globalHalt, Instant updatedAt) {

    public static GlobalHaltResponse absent() {
        return new GlobalHaltResponse(false, null);
    }
}
