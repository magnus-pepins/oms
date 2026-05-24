package com.balh.oms.ledger;

import java.util.Map;

public interface LedgerMetadataClient {

    void patchBalanceMetadata(String balanceId, Map<String, Object> patch) throws LedgerMetadataException;

    final class LedgerMetadataException extends Exception {
        public LedgerMetadataException(String message) {
            super(message);
        }

        public LedgerMetadataException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
