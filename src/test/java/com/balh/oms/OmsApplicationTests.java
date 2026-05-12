package com.balh.oms;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
class OmsApplicationTests extends AbstractPostgresIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void contextLoads() {
        // Asserts that Spring Boot wiring is healthy: Flyway runs, beans are
        // resolvable, the no-op publisher and chronicle journal are present.
    }

    @Test
    void actuatorInfo_includesOmsTopology() throws Exception {
        mockMvc.perform(get("/actuator/info"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$['oms-topology']['control.postgres-write-path']").exists())
                .andExpect(jsonPath("$['oms-topology']['spring.active-profiles']").value("test"));
    }
}
