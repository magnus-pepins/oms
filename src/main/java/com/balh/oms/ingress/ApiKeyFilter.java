package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Trivial internal API key gate for the {@code /internal/v1/**} surface.
 *
 * <p>Slice 1: shared secret in env. Slice 2+: rotate to mTLS or a service-mesh
 * identity. See {@code oms/docs/decisions.md}.
 *
 * <p>Public endpoints under {@code /actuator/**} stay open (probes/Prometheus
 * scrape).
 */
@Configuration
public class ApiKeyFilter {

    public static final String HEADER = "X-OMS-Internal-Key";

    @Bean
    public FilterRegistrationBean<InternalKeyFilterImpl> filter(OmsConfig config) {
        var bean = new FilterRegistrationBean<>(new InternalKeyFilterImpl(config));
        bean.addUrlPatterns("/internal/v1/*");
        bean.setName("oms-internal-api-key");
        return bean;
    }

    static class InternalKeyFilterImpl extends OncePerRequestFilter {
        private final OmsConfig config;

        InternalKeyFilterImpl(OmsConfig config) {
            this.config = config;
        }

        @Override
        protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
                throws ServletException, IOException {
            String expected = config.getHttp().getInternalApiKey();
            String actual = req.getHeader(HEADER);
            if (expected == null || expected.isBlank() || actual == null
                    || !constantTimeEquals(expected, actual)) {
                res.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                res.setContentType("application/json");
                res.getWriter().write("{\"error\":\"unauthorized\"}");
                return;
            }
            chain.doFilter(req, res);
        }

        private static boolean constantTimeEquals(String a, String b) {
            byte[] ab = a.getBytes(StandardCharsets.UTF_8);
            byte[] bb = b.getBytes(StandardCharsets.UTF_8);
            return MessageDigest.isEqual(ab, bb);
        }
    }
}
