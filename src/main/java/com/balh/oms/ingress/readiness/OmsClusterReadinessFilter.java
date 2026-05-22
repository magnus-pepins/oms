package com.balh.oms.ingress.readiness;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Gates cluster-mutating ingress POSTs until readiness counter is READY (Phase 2.3). */
public final class OmsClusterReadinessFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterReadinessFilter.class);

    private static final Set<String> ALLOWED_METHODS = Set.of("GET", "HEAD", "OPTIONS");

    private static final Set<String> ALWAYS_ALLOWED_PREFIXES = Set.of("/actuator");

    private static final Pattern ORDER_CREATE = Pattern.compile("^/internal/v1/orders$");

    private static final Pattern ORDER_CANCEL =
            Pattern.compile("^/internal/v1/orders/[^/]+/cancel$");

    private static final Pattern ORDER_REPLACE =
            Pattern.compile("^/internal/v1/orders/[^/]+/replace$");

    private static final Pattern ADMIN_FORCE_CANCEL =
            Pattern.compile("^/internal/v1/admin/orders/[^/]+/force-cancel$");

    private static final Pattern FIX_MASS_CANCEL =
            Pattern.compile("^/internal/v1/fix/mass-cancel-request$");

    private final OmsClusterReadinessReader reader;
    private final ObjectMapper objectMapper;
    private final int retryAfterSeconds;

    public OmsClusterReadinessFilter(
            OmsClusterReadinessReader reader, ObjectMapper objectMapper, int retryAfterSeconds) {
        if (retryAfterSeconds <= 0) {
            throw new IllegalArgumentException("retryAfterSeconds must be > 0");
        }
        this.reader = reader;
        this.objectMapper = objectMapper;
        this.retryAfterSeconds = retryAfterSeconds;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (!isGated(request)) {
            chain.doFilter(request, response);
            return;
        }
        ReadinessSnapshot s = reader.snapshot();
        if (s.isReady()) {
            chain.doFilter(request, response);
            return;
        }
        rejectAsClusterNotReady(request, response, s);
    }

    static boolean isGatedFor(String method, String path) {
        if (method == null || path == null || ALLOWED_METHODS.contains(method.toUpperCase())) {
            return false;
        }
        for (String prefix : ALWAYS_ALLOWED_PREFIXES) {
            if (path.equals(prefix) || path.startsWith(prefix + "/")) {
                return false;
            }
        }
        return ORDER_CREATE.matcher(path).matches()
                || ORDER_CANCEL.matcher(path).matches()
                || ORDER_REPLACE.matcher(path).matches()
                || ADMIN_FORCE_CANCEL.matcher(path).matches()
                || FIX_MASS_CANCEL.matcher(path).matches();
    }

    private boolean isGated(HttpServletRequest req) {
        return isGatedFor(req.getMethod(), req.getRequestURI());
    }

    private void rejectAsClusterNotReady(
            HttpServletRequest request, HttpServletResponse response, ReadinessSnapshot s)
            throws IOException {
        log.warn(
                "rejecting {} {} — OMS cluster not ready: status={} counterValue={}",
                request.getMethod(),
                request.getRequestURI(),
                s.status(),
                s.counterValue());

        response.setStatus(HttpStatus.SERVICE_UNAVAILABLE.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setHeader(HttpHeaders.RETRY_AFTER, Integer.toString(retryAfterSeconds));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", "OMS_CLUSTER_NOT_READY");
        body.put(
                "message",
                "OMS cluster is not ready to accept admission commands; retry after a few seconds."
                        + " See oms/docs/runbooks/oms-cluster-recovery-incident.md.");
        body.put("retryAfterSeconds", retryAfterSeconds);
        body.put("readinessStatus", s.status().name());
        body.put("counterValue", s.counterValue());
        objectMapper.writeValue(response.getWriter(), body);
    }
}
