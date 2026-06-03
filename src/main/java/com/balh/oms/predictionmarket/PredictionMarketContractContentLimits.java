package com.balh.oms.predictionmarket;

/** Catalog copy limits for prediction-market contracts (operator + retail display). */
public final class PredictionMarketContractContentLimits {

    public static final int MAX_DESCRIPTION_CHARS = 8_000;
    public static final int MAX_RESOLUTION_CRITERIA_CHARS = 4_000;
    public static final int MAX_REFERENCE_LINKS = 5;
    public static final int MAX_REFERENCE_LINK_LABEL_CHARS = 120;
    public static final int MAX_REFERENCE_LINK_URL_CHARS = 2_048;

    private PredictionMarketContractContentLimits() {}
}
