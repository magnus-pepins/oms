package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class FixSodPolicyEngineTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void alwaysModeNeverSkips() {
        OmsConfig cfg = new OmsConfig();
        cfg.getFix().setRouteStateSodPolicyMode(FixSodPolicyEngine.MODE_ALWAYS);
        FixSodPolicyEngine engine = new FixSodPolicyEngine(cfg, objectMapper);
        assertThat(engine.skipReasonAt(Instant.parse("2026-05-09T12:00:00Z"))).isEmpty();
    }

    @Test
    void weekdaysSkipsSaturdayUtc() {
        OmsConfig cfg = new OmsConfig();
        cfg.getFix().setRouteStateSodPolicyMode(FixSodPolicyEngine.MODE_WEEKDAYS);
        cfg.getFix().setRouteStateSodPolicyZoneId("UTC");
        FixSodPolicyEngine engine = new FixSodPolicyEngine(cfg, objectMapper);
        // 2026-05-09 is Saturday
        Optional<String> skip = engine.skipReasonAt(Instant.parse("2026-05-09T12:00:00Z"));
        assertThat(skip).contains("weekend");
    }

    @Test
    void weekdaysAllowsFridayUtc() {
        OmsConfig cfg = new OmsConfig();
        cfg.getFix().setRouteStateSodPolicyMode(FixSodPolicyEngine.MODE_WEEKDAYS);
        cfg.getFix().setRouteStateSodPolicyZoneId("UTC");
        FixSodPolicyEngine engine = new FixSodPolicyEngine(cfg, objectMapper);
        // 2026-05-08 is Friday
        assertThat(engine.skipReasonAt(Instant.parse("2026-05-08T18:00:00Z"))).isEmpty();
    }

    @Test
    void regionCalendarBlockedDate() {
        OmsConfig cfg = new OmsConfig();
        cfg.getFix().setRouteStateSodPolicyMode(FixSodPolicyEngine.MODE_REGION_CALENDAR);
        cfg.getFix().setRouteStateSodPolicyZoneId("UTC");
        cfg.getFix()
                .setRouteStateSodPolicyCalendarJson(
                        "{\"activeRegionId\":\"default\",\"regions\":{\"default\":{\"allowedWeekdays\":[1,2,3,4,5,6,7],"
                                + "\"blockedDates\":[\"2026-05-08\"],\"forcedDates\":[]}}}");
        FixSodPolicyEngine engine = new FixSodPolicyEngine(cfg, objectMapper);
        Optional<String> skip = engine.skipReasonAt(Instant.parse("2026-05-08T12:00:00Z"));
        assertThat(skip).contains("blocked_date");
    }

    @Test
    void regionCalendarForcedDateOverridesWeekdayFilter() {
        OmsConfig cfg = new OmsConfig();
        cfg.getFix().setRouteStateSodPolicyMode(FixSodPolicyEngine.MODE_REGION_CALENDAR);
        cfg.getFix().setRouteStateSodPolicyZoneId("UTC");
        cfg.getFix()
                .setRouteStateSodPolicyCalendarJson(
                        "{\"activeRegionId\":\"default\",\"regions\":{\"default\":{\"allowedWeekdays\":[1,2,3],"
                                + "\"blockedDates\":[],\"forcedDates\":[\"2026-05-09\"]}}}");
        FixSodPolicyEngine engine = new FixSodPolicyEngine(cfg, objectMapper);
        // Saturday but forced
        assertThat(engine.skipReasonAt(Instant.parse("2026-05-09T12:00:00Z"))).isEmpty();
    }

    @Test
    void regionCalendarNotAllowedWeekday() {
        OmsConfig cfg = new OmsConfig();
        cfg.getFix().setRouteStateSodPolicyMode(FixSodPolicyEngine.MODE_REGION_CALENDAR);
        cfg.getFix().setRouteStateSodPolicyZoneId("UTC");
        cfg.getFix()
                .setRouteStateSodPolicyCalendarJson(
                        "{\"activeRegionId\":\"default\",\"regions\":{\"default\":{\"allowedWeekdays\":[6,7],"
                                + "\"blockedDates\":[],\"forcedDates\":[]}}}");
        FixSodPolicyEngine engine = new FixSodPolicyEngine(cfg, objectMapper);
        // Friday = 5, not in [6,7]
        Optional<String> skip = engine.skipReasonAt(Instant.parse("2026-05-08T12:00:00Z"));
        assertThat(skip).contains("not_allowed_weekday");
    }
}
