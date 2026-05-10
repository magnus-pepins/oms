package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.OrdersRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * Internal desk snapshot (bounded list). Disabled unless {@code oms.desk.snapshot-enabled=true}.
 */
@RestController
@RequestMapping("/internal/v1/desk/orders")
public class DeskSnapshotController {

    private final OmsConfig omsConfig;
    private final OrdersRepository ordersRepository;

    public DeskSnapshotController(OmsConfig omsConfig, OrdersRepository ordersRepository) {
        this.omsConfig = omsConfig;
        this.ordersRepository = ordersRepository;
    }

    public record DeskSnapshotResponse(List<OrdersRepository.DeskSnapshotRow> orders, Instant minReceived, int limit) {}

    @GetMapping("/snapshot")
    public ResponseEntity<?> snapshot(@RequestParam(name = "limit", required = false) Integer limitRaw) {
        if (!omsConfig.getDesk().isSnapshotEnabled()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "desk_snapshot_disabled", "message", "Set OMS_DESK_SNAPSHOT_ENABLED=true for bounded desk reads"));
        }
        int max = omsConfig.getDesk().getSnapshotMaxLimit();
        int lim = limitRaw == null ? Math.min(50, max) : Math.min(Math.max(1, limitRaw), max);
        int hours = omsConfig.getDesk().getSnapshotMaxAgeHours();
        Instant min = Instant.now().minus(hours, ChronoUnit.HOURS);
        var rows = ordersRepository.findDeskSnapshot(min, lim);
        return ResponseEntity.ok(new DeskSnapshotResponse(rows, min, lim));
    }
}
