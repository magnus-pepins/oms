package com.balh.oms.fixin.persistence;

import java.util.regex.Pattern;

/** Strips password (554) and username (553) tags from FIX text for operator display. */
public final class FixInMessageRedaction {

    private static final Pattern SENSITIVE_TAG = Pattern.compile("\u0001(553|554)=[^\u0001]*");

    private FixInMessageRedaction() {}

    public static String redact(String fixText) {
        if (fixText == null || fixText.isEmpty()) {
            return fixText;
        }
        return SENSITIVE_TAG.matcher(fixText).replaceAll("\u0001$1=[REDACTED]");
    }
}
