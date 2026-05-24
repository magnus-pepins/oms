package com.balh.oms.fixin.persistence;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Fetches redacted FIX text from QuickFIX/J {@code oms_fix_messages} when audit row has a store ref. */
@Repository
public class FixInQuickFixRawMessageRepository {

    private static final Pattern RAW_REF =
            Pattern.compile("^QFJ:FIX\\.4\\.4:([^:]+):([^:]+):(\\d+)$");

    private static final String SELECT_MESSAGE = """
            SELECT message FROM oms_fix_messages
             WHERE beginstring = 'FIX.4.4'
               AND sendercompid = :sender_comp_id
               AND targetcompid = :target_comp_id
               AND msgseqnum = :msg_seq_num
               AND session_qualifier = ''
               AND sendersubid = ''
               AND senderlocid = ''
               AND targetsubid = ''
               AND targetlocid = ''
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public FixInQuickFixRawMessageRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public static String rawStoreRef(String omsSenderCompId, String clientTargetCompId, int msgSeqNum) {
        return "QFJ:FIX.4.4:" + omsSenderCompId + ":" + clientTargetCompId + ":" + msgSeqNum;
    }

    public Optional<String> findRedactedByRawStoreRef(String rawStoreRef) {
        Matcher m = RAW_REF.matcher(rawStoreRef == null ? "" : rawStoreRef.trim());
        if (!m.matches()) {
            return Optional.empty();
        }
        String sender = m.group(1);
        String target = m.group(2);
        int seq = Integer.parseInt(m.group(3));
        try {
            String raw = jdbc.queryForObject(
                    SELECT_MESSAGE,
                    new MapSqlParameterSource()
                            .addValue("sender_comp_id", sender)
                            .addValue("target_comp_id", target)
                            .addValue("msg_seq_num", seq),
                    String.class);
            return Optional.of(FixInMessageRedaction.redact(raw));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }
}
