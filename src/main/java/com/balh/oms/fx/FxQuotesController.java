package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.util.List;
import java.util.Map;

/**
 * M3 Track 1: versioned stub quotes for integration before production price ingress.
 */
@RestController
@RequestMapping("/internal/v1/fx")
public class FxQuotesController {

    private final OmsConfig omsConfig;
    private final Clock clock;

    public FxQuotesController(OmsConfig omsConfig, Clock clock) {
        this.omsConfig = omsConfig;
        this.clock = clock;
    }

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
}
