package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/internal/v1/fx")
public class FxNostroSnapshotController {

    private final FxNostroSnapshotService fxNostroSnapshotService;
    private final FxNostroCorrespondentService correspondentService;
    private final FxSuspenseSnapshotService fxSuspenseSnapshotService;
    private final FxRetailNostroSnapshotService fxRetailNostroSnapshotService;

    public FxNostroSnapshotController(
            FxNostroSnapshotService fxNostroSnapshotService,
            FxNostroCorrespondentService correspondentService,
            FxSuspenseSnapshotService fxSuspenseSnapshotService,
            FxRetailNostroSnapshotService fxRetailNostroSnapshotService) {
        this.fxNostroSnapshotService = fxNostroSnapshotService;
        this.correspondentService = correspondentService;
        this.fxSuspenseSnapshotService = fxSuspenseSnapshotService;
        this.fxRetailNostroSnapshotService = fxRetailNostroSnapshotService;
    }

    @GetMapping("/scale/status")
    public ResponseEntity<Map<String, Object>> scaleStatus() {
        return ResponseEntity.ok(correspondentService.status());
    }

    @GetMapping("/nostro/snapshot")
    public ResponseEntity<Map<String, Object>> snapshot() {
        try {
            return ResponseEntity.ok(fxNostroSnapshotService.buildSnapshot());
        } catch (IllegalStateException e) {
            String code = e.getMessage();
            if ("fx_nostro_read_disabled".equals(code)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", code));
            }
            if ("ledger_client_unavailable".equals(code) || "fx_nostro_balance_ids_empty".equals(code)) {
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", code));
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", code));
        }
    }

    @GetMapping("/suspense/snapshot")
    public ResponseEntity<Map<String, Object>> suspenseSnapshot() {
        try {
            return ResponseEntity.ok(fxSuspenseSnapshotService.buildSnapshot());
        } catch (IllegalStateException e) {
            String code = e.getMessage();
            if ("fx_suspense_read_disabled".equals(code)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", code));
            }
            if ("ledger_client_unavailable".equals(code)
                    || "fx_suspense_currencies_empty".equals(code)) {
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", code));
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", code));
        }
    }

    @GetMapping("/retail/snapshot")
    public ResponseEntity<Map<String, Object>> retailSnapshot() {
        try {
            return ResponseEntity.ok(fxRetailNostroSnapshotService.buildSnapshot());
        } catch (IllegalStateException e) {
            String code = e.getMessage();
            if ("fx_retail_nostro_read_disabled".equals(code)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", code));
            }
            if ("ledger_client_unavailable".equals(code)
                    || "fx_retail_nostro_currencies_empty".equals(code)) {
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", code));
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", code));
        }
    }
}
