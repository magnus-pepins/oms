package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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
    private final ObjectProvider<OmsFxCustomerQuotePublisher> customerQuotePublisher;
    private final ObjectProvider<OmsFxMidSubscriber> midSubscriber;

    public FxQuotesController(
            OmsConfig omsConfig,
            Clock clock,
            FxQuoteService quoteService,
            ObjectProvider<OmsFxCustomerQuotePublisher> customerQuotePublisher,
            ObjectProvider<OmsFxMidSubscriber> midSubscriber) {
        this.omsConfig = omsConfig;
        this.clock = clock;
        this.quoteService = quoteService;
        this.customerQuotePublisher = customerQuotePublisher;
        this.midSubscriber = midSubscriber;
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

    /**
     * Ops/diagnostic surface for {@link OmsFxCustomerQuotePublisher}.
     * Returns {@code enabled=false} when the publisher bean is not wired.
     */
    @GetMapping("/customer-quote/status")
    public ResponseEntity<Map<String, Object>> customerQuoteStatus() {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_module_disabled"));
        }
        OmsFxCustomerQuotePublisher pub = customerQuotePublisher.getIfAvailable();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("asOf", clock.instant().toString());
        if (pub == null) {
            body.put("enabled", false);
            return ResponseEntity.ok(body);
        }
        OmsFxCustomerQuotePublisher.PublisherStatus s = pub.status();
        body.put("enabled", true);
        body.put("tiers", s.tiers());
        body.put("markupCacheSize", s.markupCacheSize());
        body.put("markupCacheLoadedAtMs", s.markupCacheLoadedAtMs());
        body.put("mqttConnected", s.mqttConnected());
        body.put("publishTickPeriodMs", s.publishTickPeriodMs());
        body.put("maxMidAgeMs", s.maxMidAgeMs());
        body.put("lastSuccessfulPublishAtMs", s.lastSuccessfulPublishAtMs());
        body.put("lastStaleMidSkipAtMs", s.lastStaleMidSkipAtMs());
        OmsFxMidSubscriber sub = midSubscriber.getIfAvailable();
        Map<String, Object> upstreamBody = new LinkedHashMap<>();
        if (sub == null) {
            upstreamBody.put("enabled", false);
        } else {
            OmsFxMidSubscriber.SubscriberStatus ss = sub.status();
            upstreamBody.put("enabled", true);
            upstreamBody.put("mqttConnected", ss.mqttConnected());
            upstreamBody.put("pairsKnown", ss.pairsKnown());
            upstreamBody.put("lastTickAtMs", ss.lastTickAtMs());
            upstreamBody.put("stalenessMs", ss.stalenessMs());
        }
        body.put("upstream", upstreamBody);
        FxMarkupOverridesService.OverridesStatus o = s.overrides();
        Map<String, Object> overridesBody = new LinkedHashMap<>();
        overridesBody.put("cachedRowCount", o.cachedRowCount());
        overridesBody.put("activeCount", o.activeCount());
        overridesBody.put("nextExpiryEpochMs", o.nextExpiryEpochMs());
        overridesBody.put("cacheLoadedAtMs", o.cacheLoadedAtMs());
        body.put("overrides", overridesBody);
        return ResponseEntity.ok(body);
    }

    public record QuoteRequest(String pair, String tier) {}

    /**
     * Read-only list of the active {@code fx_pair_markups} grid powering the
     * beard-admin FX Markups operator page. Filters (pair, tier) are
     * optional and case-insensitive; both null returns the full grid. The
     * response is a flat array of rows so the UI can group on the client
     * side. See {@link FxQuoteService#listMarkups} for the SQL.
     *
     * <p>Read-only for v1 — the surface is intentionally a view-only audit
     * pane. Writes still go through Flyway / SQL; an editor follow-up is
     * planned for v1.5 (plans/oms-multi-currency-invest-accounts.md §8.9).
     */
    @GetMapping("/markups")
    public ResponseEntity<Map<String, Object>> markups(
            @RequestParam(value = "pair", required = false) String pair,
            @RequestParam(value = "tier", required = false) String tier) {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_module_disabled"));
        }
        List<Map<String, Object>> rows = quoteService.listMarkups(pair, tier);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("asOf", clock.instant().toString());
        body.put("count", rows.size());
        body.put("rows", rows);
        return ResponseEntity.ok(body);
    }

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
        } catch (FxQuoteStaleException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", "no_mid", "message", e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body(Map.of("error", "no_markup", "message", e.getMessage()));
        }
    }
}
