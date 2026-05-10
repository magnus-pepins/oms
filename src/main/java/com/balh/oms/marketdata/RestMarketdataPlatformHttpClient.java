package com.balh.oms.marketdata;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.util.UriComponentsBuilder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

public final class RestMarketdataPlatformHttpClient implements MarketdataPlatformHttpClient {

    private static final String API_KEY_HEADER = "X-Marketdata-Key";

    private final RestClient http;
    private final OmsConfig config;
    private final ObjectMapper objectMapper;

    public RestMarketdataPlatformHttpClient(RestClient http, OmsConfig config, ObjectMapper objectMapper) {
        this.http = http;
        this.config = config;
        this.objectMapper = objectMapper;
    }

    @Override
    public Set<String> fetchInstrumentSymbols() {
        var md = config.getMarketdata();
        String path = normalizePath(md.getInstrumentsHttpPath());
        ResponseEntity<String> res =
                http.get()
                        .uri(path)
                        .header(API_KEY_HEADER, md.getApiKey())
                        .retrieve()
                        .toEntity(String.class);
        if (!res.getStatusCode().is2xxSuccessful() || res.getBody() == null) {
            return Set.of();
        }
        return parseInstrumentSymbols(res.getBody());
    }

    @Override
    public Optional<MarketdataNbboQuote> fetchNbbo(String instrumentSymbol) {
        if (instrumentSymbol == null || instrumentSymbol.isBlank()) {
            return Optional.empty();
        }
        var md = config.getMarketdata();
        String path = normalizePath(md.getNbboHttpPath());
        String uri =
                UriComponentsBuilder.fromPath(path).queryParam("symbol", instrumentSymbol.trim()).build().encode().toUriString();
        try {
            ResponseEntity<String> res =
                    http.get()
                            .uri(uri)
                            .header(API_KEY_HEADER, md.getApiKey())
                            .retrieve()
                            .toEntity(String.class);
            if (!res.getStatusCode().is2xxSuccessful() || res.getBody() == null) {
                return Optional.empty();
            }
            return parseNbbo(res.getBody());
        } catch (RestClientException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    Set<String> parseInstrumentSymbols(String body) {
        try {
            JsonNode root = objectMapper.readTree(body);
            LinkedHashSet<String> out = new LinkedHashSet<>();
            if (root.isArray()) {
                for (JsonNode n : root) {
                    addSymbolNode(out, n);
                }
            } else if (root.isObject()) {
                if (root.has("symbols") && root.get("symbols").isArray()) {
                    for (JsonNode n : root.get("symbols")) {
                        addSymbolNode(out, n);
                    }
                }
                if (root.has("instruments") && root.get("instruments").isArray()) {
                    for (JsonNode n : root.get("instruments")) {
                        if (n.isTextual()) {
                            addSymbolText(out, n.asText());
                        } else if (n.isObject()) {
                            JsonNode s = n.get("symbol");
                            if (s != null && s.isTextual()) {
                                addSymbolText(out, s.asText());
                            }
                        }
                    }
                }
            }
            return Set.copyOf(out);
        } catch (Exception e) {
            return Set.of();
        }
    }

    private static void addSymbolNode(Set<String> out, JsonNode n) {
        if (n.isTextual()) {
            addSymbolText(out, n.asText());
        } else if (n.isObject()) {
            JsonNode s = n.get("symbol");
            if (s != null && s.isTextual()) {
                addSymbolText(out, s.asText());
            }
        }
    }

    private static void addSymbolText(Set<String> out, String raw) {
        if (raw == null) {
            return;
        }
        String t = raw.trim().toUpperCase(Locale.ROOT);
        if (!t.isEmpty()) {
            out.add(t);
        }
    }

    Optional<MarketdataNbboQuote> parseNbbo(String body) {
        try {
            JsonNode root = objectMapper.readTree(body);
            BigDecimal bid = readDecimal(root, "bid");
            BigDecimal ask = readDecimal(root, "ask");
            if (bid == null || ask == null || bid.compareTo(BigDecimal.ZERO) <= 0 || ask.compareTo(BigDecimal.ZERO) <= 0) {
                return Optional.empty();
            }
            Instant asOf = Instant.now();
            if (root.has("asOf") && root.get("asOf").isTextual()) {
                try {
                    asOf = Instant.parse(root.get("asOf").asText());
                } catch (Exception ignored) {
                    // keep now
                }
            }
            return Optional.of(new MarketdataNbboQuote(bid, ask, asOf));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static BigDecimal readDecimal(JsonNode root, String field) {
        if (!root.has(field)) {
            return null;
        }
        JsonNode n = root.get(field);
        if (n.isNumber()) {
            return n.decimalValue();
        }
        if (n.isTextual()) {
            try {
                return new BigDecimal(n.asText().trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static String normalizePath(String path) {
        String p = path == null ? "" : path.trim();
        if (p.isEmpty()) {
            return "/";
        }
        return p.startsWith("/") ? p : "/" + p;
    }
}
