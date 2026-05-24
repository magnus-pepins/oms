package com.balh.oms.fixin;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.fixin.persistence.FixInSessionRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import javax.sql.DataSource;

/** Starts FIX-in acceptor after {@code @Sql} seed (acceptor auto-start is off in ITs). */
abstract class FixInWireItAcceptorSupport extends com.balh.oms.AbstractPostgresIntegrationTest {

    private FixAcceptorManager acceptor;

    @org.springframework.beans.factory.annotation.Autowired
    OmsConfig omsConfig;

    @org.springframework.beans.factory.annotation.Autowired
    FixInApplication fixInApplication;

    @org.springframework.beans.factory.annotation.Autowired
    FixInSessionRepository fixInSessionRepository;

    @org.springframework.beans.factory.annotation.Autowired
    DataSource dataSource;

    @BeforeEach
    void startFixInAcceptorAfterSeed() {
        if (acceptor == null) {
            acceptor = new FixAcceptorManager(
                    omsConfig, fixInApplication, fixInSessionRepository, dataSource);
            acceptor.start();
        }
    }

    @AfterEach
    void stopFixInAcceptor() {
        if (acceptor != null) {
            acceptor.stop();
            acceptor = null;
        }
    }
}
