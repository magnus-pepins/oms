package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixSymbolMapperTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void parseSymbolMap_emptyOrBrace_returnsEmpty() {
        assertThat(FixSymbolMapper.parseSymbolMap("", OBJECT_MAPPER)).isEmpty();
        assertThat(FixSymbolMapper.parseSymbolMap("  {}  ", OBJECT_MAPPER)).isEmpty();
    }

    @Test
    void constructor_buildsCaseInsensitiveKeys() {
        OmsConfig cfg = new OmsConfig();
        cfg.getFix().setSymbolMapJson("{\"aapl\":\"BRK.A\"}");
        FixSymbolMapper mapper = new FixSymbolMapper(cfg, OBJECT_MAPPER);
        assertThat(mapper.toVenueSymbol("AAPL")).isEqualTo("BRK.A");
        assertThat(mapper.toVenueSymbol("aapl")).isEqualTo("BRK.A");
    }

    @Test
    void toVenueSymbol_unmapped_returnsTrimmedOriginal() {
        OmsConfig cfg = new OmsConfig();
        cfg.getFix().setSymbolMapJson("{\"MSFT\":\"MSFT.NMS\"}");
        FixSymbolMapper mapper = new FixSymbolMapper(cfg, OBJECT_MAPPER);
        assertThat(mapper.toVenueSymbol("  AAPL  ")).isEqualTo("AAPL");
    }

    @Test
    void parseSymbolMap_nonObject_throws() {
        assertThatThrownBy(() -> FixSymbolMapper.parseSymbolMap("[]", OBJECT_MAPPER))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("JSON object");
    }

    @Test
    void parseSymbolMap_nonStringValue_throws() {
        assertThatThrownBy(() -> FixSymbolMapper.parseSymbolMap("{\"AAPL\":1}", OBJECT_MAPPER))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("JSON strings");
    }

    @Test
    void parseSymbolMap_invalidJson_throws() {
        assertThatThrownBy(() -> FixSymbolMapper.parseSymbolMap("{not json", OBJECT_MAPPER))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("valid JSON");
    }

    @Test
    void parseSymbolMap_returnsUnmodifiableView() {
        Map<String, String> m = FixSymbolMapper.parseSymbolMap("{\"X\":\"Y\"}", OBJECT_MAPPER);
        assertThatThrownBy(() -> m.put("Z", "W")).isInstanceOf(UnsupportedOperationException.class);
    }
}
