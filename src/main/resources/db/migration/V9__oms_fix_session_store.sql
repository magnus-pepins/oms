-- QuickFIX/J JdbcStore tables (slice 4+ master plan 6.4). DDL aligned with QuickFIX/J PostgreSQL scripts
-- (oms_fix_sessions / oms_fix_messages — names passed via JdbcStoreSessionsTableName / JdbcStoreMessagesTableName).

CREATE TABLE IF NOT EXISTS oms_fix_sessions (
    beginstring      CHAR(8)     NOT NULL,
    sendercompid     VARCHAR(64) NOT NULL,
    sendersubid      VARCHAR(64) NOT NULL,
    senderlocid      VARCHAR(64) NOT NULL,
    targetcompid     VARCHAR(64) NOT NULL,
    targetsubid      VARCHAR(64) NOT NULL,
    targetlocid      VARCHAR(64) NOT NULL,
    session_qualifier VARCHAR(64) NOT NULL,
    creation_time    TIMESTAMP   NOT NULL,
    incoming_seqnum  INTEGER     NOT NULL,
    outgoing_seqnum  INTEGER     NOT NULL,
    PRIMARY KEY (
        beginstring,
        sendercompid,
        sendersubid,
        senderlocid,
        targetcompid,
        targetsubid,
        targetlocid,
        session_qualifier
    )
);

CREATE TABLE IF NOT EXISTS oms_fix_messages (
    beginstring       CHAR(8)     NOT NULL,
    sendercompid      VARCHAR(64) NOT NULL,
    sendersubid       VARCHAR(64) NOT NULL,
    senderlocid       VARCHAR(64) NOT NULL,
    targetcompid      VARCHAR(64) NOT NULL,
    targetsubid       VARCHAR(64) NOT NULL,
    targetlocid       VARCHAR(64) NOT NULL,
    session_qualifier VARCHAR(64) NOT NULL,
    msgseqnum         INTEGER     NOT NULL,
    message           TEXT        NOT NULL,
    PRIMARY KEY (
        beginstring,
        sendercompid,
        sendersubid,
        senderlocid,
        targetcompid,
        targetsubid,
        targetlocid,
        session_qualifier,
        msgseqnum
    )
);

COMMENT ON TABLE oms_fix_sessions IS 'QuickFIX/J JdbcStore session seq state; separate from orders SoR.';
COMMENT ON TABLE oms_fix_messages IS 'QuickFIX/J JdbcStore persisted messages for resend; separate from orders SoR.';
