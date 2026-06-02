package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Manual FX hedge controller.
 *
 * <p>{@code GET /hedge/hooks-status} stays as the legacy advisory probe so the
 * earlier trading-desk health pill keeps working. {@code POST /hedge/submit}
 * is the real endpoint backing the trading-desk FX console hedge ticket,
 * delegating to {@link FxHedgeService} which writes the audit row and posts
 * the corresponding Ledger transaction. {@code GET /hedge/recent} returns
 * the last N hedge actions for the desk console / beard surveillance feed.
 */
@RestController
@RequestMapping("/internal/v1/fx")
public class FxHedgeHooksController {

    private final OmsConfig omsConfig;
    private final FxHedgeService hedgeService;
    private final Counter hedgeProbe;

    public FxHedgeHooksController(
            OmsConfig omsConfig,
            FxHedgeService hedgeService,
            MeterRegistry registry) {
        this.omsConfig = omsConfig;
        this.hedgeService = hedgeService;
        this.hedgeProbe = Counter.builder("oms.fx.hedge_hook.probe")
                .description("FX hedge hook status probe invocations")
                .register(registry);
    }

    @GetMapping("/hedge/hooks-status")
    public ResponseEntity<Map<String, Object>> hooksStatus() {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_module_disabled"));
        }
        if (!fx.isHedgeHooksEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_hedge_hooks_disabled"));
        }
        hedgeProbe.increment();
        return ResponseEntity.ok(
                Map.of("paused", false, "perPairKillSwitch", Map.of(), "schemaVersion", 1, "financeGated", true));
    }

    public record HedgeBody(
            String actionKey,
            String submittedBy,
            String pair,
            String side,
            String tier,
            String quoteId,
            BigDecimal baseAmount,
            String baseNostroId,
            String quoteNostroId,
            String description,
            String exposure
    ) {}

    @PostMapping("/hedge/submit")
    public ResponseEntity<Map<String, Object>> submit(@RequestBody HedgeBody body) {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_module_disabled"));
        }
        if (!fx.isHedgeHooksEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_hedge_hooks_disabled"));
        }
        if (body == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "missing_body"));
        }
        try {
            Map<String, Object> result = hedgeService.submit(new FxHedgeService.HedgeRequest(
                    body.actionKey(),
                    body.submittedBy(),
                    body.pair(),
                    body.side(),
                    body.tier(),
                    body.quoteId(),
                    body.baseAmount(),
                    body.baseNostroId(),
                    body.quoteNostroId(),
                    body.description(),
                    body.exposure()));
            return ResponseEntity.status(HttpStatus.CREATED).body(result);
        } catch (IllegalArgumentException e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", "invalid_request");
            err.put("message", e.getMessage());
            return ResponseEntity.badRequest().body(err);
        } catch (IllegalStateException e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", "service_unavailable");
            err.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(err);
        }
    }

    @GetMapping("/hedge/recent")
    public ResponseEntity<Map<String, Object>> recent(
            @RequestParam(name = "limit", required = false, defaultValue = "50") int limit) {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_module_disabled"));
        }
        List<Map<String, Object>> rows = hedgeService.recent(limit);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("limit", limit);
        body.put("count", rows.size());
        body.put("actions", rows);
        return ResponseEntity.ok(body);
    }
}
