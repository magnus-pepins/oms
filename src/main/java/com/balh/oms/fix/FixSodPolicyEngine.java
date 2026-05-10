package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Optional calendar / region gate for {@link FixRouteStateSodScheduler}. Default mode {@code always}
 * preserves historical behaviour (every cron tick runs SOD).
 */
@Component
@ConditionalOnProperty(name = "oms.fix.route-state-sod-enabled", havingValue = "true")
public class FixSodPolicyEngine {

    private static final Logger log = LoggerFactory.getLogger(FixSodPolicyEngine.class);

    public static final String MODE_ALWAYS = "always";
    public static final String MODE_WEEKDAYS = "weekdays";
    public static final String MODE_REGION_CALENDAR = "region_calendar";

    private final OmsConfig omsConfig;
    private final ObjectMapper objectMapper;

    public FixSodPolicyEngine(OmsConfig omsConfig, ObjectMapper objectMapper) {
        this.omsConfig = omsConfig;
        this.objectMapper = objectMapper;
    }

    /**
     * @return empty if the SOD job should run; otherwise a short tag for {@code oms_fix_route_state_sod_skipped_total}.
     */
    public Optional<String> skipReasonAt(Instant now) {
        OmsConfig.Fix fix = omsConfig.getFix();
        String mode = fix.getRouteStateSodPolicyMode().toLowerCase(Locale.ROOT);
        if (MODE_ALWAYS.equals(mode)) {
            return Optional.empty();
        }
        if (MODE_WEEKDAYS.equals(mode)) {
            return skipReasonWeekdays(now, fix);
        }
        if (MODE_REGION_CALENDAR.equals(mode)) {
            return skipReasonRegionCalendar(now, fix);
        }
        log.warn("Unknown oms.fix.route-state-sod-policy-mode '{}'; running SOD", mode);
        return Optional.empty();
    }

    private Optional<String> skipReasonWeekdays(Instant now, OmsConfig.Fix fix) {
        ZonedDateTime zdt = now.atZone(zoneId(fix));
        DayOfWeek d = zdt.getDayOfWeek();
        if (d == DayOfWeek.SATURDAY || d == DayOfWeek.SUNDAY) {
            return Optional.of("weekend");
        }
        return Optional.empty();
    }

    private ZoneId zoneId(OmsConfig.Fix fix) {
        try {
            return ZoneId.of(fix.getRouteStateSodPolicyZoneId());
        } catch (DateTimeException e) {
            log.warn("Invalid oms.fix.route-state-sod-policy-zone-id '{}', using UTC", fix.getRouteStateSodPolicyZoneId());
            return ZoneId.of("UTC");
        }
    }

    private Optional<String> skipReasonRegionCalendar(Instant now, OmsConfig.Fix fix) {
        String json = fix.getRouteStateSodPolicyCalendarJson();
        if (json == null || json.isBlank() || "{}".equals(json.trim())) {
            log.warn(
                    "oms.fix.route-state-sod-policy-mode={} but calendar JSON is empty; running SOD (fail-open)",
                    MODE_REGION_CALENDAR);
            return Optional.empty();
        }
        try {
            SodCalendarDocument doc = objectMapper.readValue(json, SodCalendarDocument.class);
            String regionId =
                    doc.activeRegionId == null || doc.activeRegionId.isBlank() ? "default" : doc.activeRegionId.trim();
            RegionCalendar region = null;
            if (doc.regions != null) {
                region = doc.regions.get(regionId);
                if (region == null && doc.regions.containsKey("default")) {
                    region = doc.regions.get("default");
                }
            }
            if (region == null) {
                log.warn("SOD calendar has no region '{}' (and no 'default'); running SOD (fail-open)", regionId);
                return Optional.empty();
            }
            ZoneId zone = zoneId(fix);
            LocalDate today = now.atZone(zone).toLocalDate();
            String todayStr = today.toString();
            if (region.forcedDates != null && region.forcedDates.contains(todayStr)) {
                return Optional.empty();
            }
            if (region.blockedDates != null && region.blockedDates.contains(todayStr)) {
                return Optional.of("blocked_date");
            }
            List<Integer> allowed = region.allowedWeekdays;
            if (allowed != null && !allowed.isEmpty()) {
                int dow = today.getDayOfWeek().getValue();
                if (!allowed.contains(dow)) {
                    return Optional.of("not_allowed_weekday");
                }
            }
            return Optional.empty();
        } catch (Exception e) {
            log.warn("Failed to parse SOD region calendar JSON; running SOD (fail-open)", e);
            return Optional.empty();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class SodCalendarDocument {
        public String activeRegionId;
        public Map<String, RegionCalendar> regions;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class RegionCalendar {
        public List<Integer> allowedWeekdays;
        public List<String> blockedDates;
        public List<String> forcedDates;
    }
}
