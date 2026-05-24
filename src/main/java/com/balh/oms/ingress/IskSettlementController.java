package com.balh.oms.ingress;

import com.balh.oms.settlement.IskKu30ExportService;
import com.balh.oms.settlement.IskValuationSnapshotService;
import java.time.LocalDate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Internal ISK valuation + KU30 draft export (Phase E — gap plan §5.10). */
@RestController
@RequestMapping("/internal/v1/settlement/isk")
public class IskSettlementController {

    public record ValuationCaptureResponse(LocalDate quarterStart, int accountsCaptured) {}

    public record Ku30ExportResponse(int taxYear, int accountCount, String aggregateJson) {}

    private final IskValuationSnapshotService valuationSnapshots;
    private final IskKu30ExportService ku30Export;

    public IskSettlementController(
            IskValuationSnapshotService valuationSnapshots, IskKu30ExportService ku30Export) {
        this.valuationSnapshots = valuationSnapshots;
        this.ku30Export = ku30Export;
    }

    @PostMapping("/valuation-snapshots/capture")
    public ResponseEntity<ValuationCaptureResponse> captureValuationSnapshots(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate asOf) {
        LocalDate effective = asOf == null ? LocalDate.now() : asOf;
        int n = valuationSnapshots.captureQuarterStub(effective);
        return ResponseEntity.ok(
                new ValuationCaptureResponse(IskValuationSnapshotService.quarterStartFor(effective), n));
    }

    @PostMapping("/ku30-export/{taxYear}")
    public ResponseEntity<Ku30ExportResponse> exportKu30Draft(@PathVariable int taxYear) {
        IskKu30ExportService.ExportResult result = ku30Export.buildDraft(taxYear);
        return ResponseEntity.ok(
                new Ku30ExportResponse(result.taxYear(), result.accountCount(), result.aggregateJson()));
    }
}
