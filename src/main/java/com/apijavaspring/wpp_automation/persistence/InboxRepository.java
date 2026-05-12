package com.apijavaspring.wpp_automation.persistence;

import com.apijavaspring.wpp_automation.web.dto.InboundEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class InboxRepository {

    private final NamedParameterJdbcTemplate jdbc;

    @Transactional
    public UUID saveInboxAndEnqueueJob(InboundEvent e) {

        var params = new MapSqlParameterSource()
                .addValue("waId", e.waId())
                .addValue("messageId", e.messageId())
                .addValue("messageType", e.messageType())
                .addValue("textBody", e.textBody())
                .addValue("mediaId", e.mediaId())
                .addValue("mimeType", e.mimeType())
                .addValue("mediaSha256", e.mediaSha256())
                .addValue("tsSeconds", e.userTimestamp())
                .addValue("rawPayload", e.rawPayloadJson());

        UUID inboxId = jdbc.query(
                """
                INSERT INTO ops.wpp_inbox
                  (wa_id, message_id, direction, message_type, text_body, media_id, mime_type, media_sha256,
                   received_at, raw_payload, status, error_message, created_at)
                VALUES
                  (:waId, :messageId, 'in', :messageType, :textBody, :mediaId, :mimeType, :mediaSha256,
                   COALESCE(to_timestamp(:tsSeconds), now()),
                   CAST(:rawPayload AS jsonb), 'pendente', null, now())
                ON CONFLICT (message_id) DO NOTHING
                RETURNING id
                """,
                params,
                rs -> rs.next() ? (UUID) rs.getObject("id") : null
        );


        if (inboxId == null) {
            inboxId = jdbc.queryForObject(
                    "SELECT id FROM ops.wpp_inbox WHERE message_id = :messageId",
                    new MapSqlParameterSource().addValue("messageId", e.messageId()),
                    UUID.class
            );
        }

        if (inboxId == null) {
            throw new IllegalStateException("Inbox id not found for message_id=" + e.messageId());
        }

        jdbc.update(
                """
                INSERT INTO ops.wpp_jobs
                  (inbox_id, wa_id, job_type, status, run_after, attempts, last_error, locked_at, locked_by, created_at, updated_at)
                VALUES
                  (:inboxId, :waId, 'process_inbox', 'pendente', now(), 0, null, null, null, now(), now())
                ON CONFLICT (inbox_id, job_type) DO NOTHING
                """,
                new MapSqlParameterSource()
                        .addValue("inboxId", inboxId)
                        .addValue("waId", e.waId())
        );

        return inboxId;
    }

}
