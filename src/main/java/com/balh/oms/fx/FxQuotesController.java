package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * FX quote endpoint. The legacy {@code GET /quotes} stub is preserved for
 * backward compatibility with the bootstrap-phase clients; the new {@code POST
 * /quote} talks to {@link FxQuoteService} and returns a real tier-aware quote
 * backed by the fx_pair_markups grid.
 */
@RestController
@RequestMapping("/internal/v1/fx")
public class FxQuotesController {

    private final OmsConfig omsConfig;
    private final Clock clock;
    private final FxQuoteService quoteService;

    public FxQuotesController(OmsConfig omsConfig, Clock clock, FxQuoteService quoteService) {
        this.omsConfig = omsConfig;
        this.clock = clock;
        this.quoteService = quoteService;
    }

    /** Legacy stub kept until the trading-desk migrates to {@code POST /quote}. */
    @GetMapping("/quotes")
    public ResponseEntity<Map<String, Object>> quotes() {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_module_disabled"));
        }
        if (!fx.isQuoteStubEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_quote_stub_disabled"));
        }
        String asOf = clock.instant().toString();
        List<Map<String, String>> rows =
                List.of(Map.of("pair", "EURUSD", "bid", "1.0500", "ask", "1.0502", "asOf", asOf));
        return ResponseEntity.ok(
                Map.of("schemaVersion", fx.getQuoteStubSchemaVersion(), "source", "stub", "quotes", rows));
    }

    /** Mid panel for the trading-desk treasury page header. */
    @GetMapping("/mids")
    public ResponseEntity<Map<String, Object>> mids() {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_module_disabled"));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("asOf", clock.instant().toString());
        body.put("source", "internal-mid-stub");
        Map<String, String> mids = new LinkedHashMap<>();
        quoteService.stubMids().forEach((k, v) -> mids.put(k, v.toPlainString()));
        body.put("mids", mids);
        return ResponseEntity.ok(body);
    }

    public record QuoteRequest(String pair, String tier) {}

    @PostMapping("/quote")
    public ResponseEntity<Map<String, Object>> quote(@RequestBody QuoteRequest body) {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_module_disabled"));
        }
        if (body == null || body.pair() == null || body.pair().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "missing_pair"));
        }
        try {
            return ResponseEntity.ok(quoteService.quote(body.pair(), body.tier()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", "no_mid", "message", e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(Map.of("error", "no_markup", "message", e.getMessage()));
        }
    }
}
