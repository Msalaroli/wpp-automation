package com.apijavaspring.wpp_automation.persistence;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class ChatwootInboundQueueRepository {

    private final NamedParameterJdbcTemplate jdbc;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int enqueueChatwootInboundMirror(UUID inboxId) {
        return jdbc.update(
                """
                INSERT INTO ops.chatwoot_inbound_queue (
                    inbox_id,
                    status,
                    run_after
                )
                VALUES (
                    :inboxId,
                    'pendente',
                    now()
                )
                ON CONFLICT (inbox_id) DO NOTHING
                """,
                new MapSqlParameterSource().addValue("inboxId", inboxId)
        );
    }
}
