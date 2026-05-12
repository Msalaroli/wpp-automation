package com.apijavaspring.wpp_automation.persistence;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class DocUploadRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public UUID insertPendingWhatsAppUpload(
            String reservationId,
            String tipo,
            String sourceMessageId,
            String sourceMediaId,
            String mimeType,
            String sha256
    ) {
        var params = new MapSqlParameterSource()
                .addValue("reservationId", reservationId)
                .addValue("tipo", tipo)
                .addValue("pathStorage", null)
                .addValue("sha256", sha256)
                .addValue("sourceMessageId", sourceMessageId)
                .addValue("sourceMediaId", sourceMediaId)
                .addValue("mimeType", mimeType);

        UUID id = jdbc.query(
                """
                INSERT INTO ops.doc_uploads
                  (reservation_id, tipo, path_storage, hash_sha256, source_message_id, source_media_id, mime_type, created_at, expires_at, deleted_at)
                VALUES
                  (:reservationId, :tipo, :pathStorage, :sha256, :sourceMessageId, :sourceMediaId, :mimeType, now(), null, null)
                ON CONFLICT (source_message_id) DO NOTHING
                RETURNING id
                """,
                params,
                rs -> rs.next() ? (UUID) rs.getObject("id") : null
        );

        if (id != null) return id;

        return jdbc.queryForObject(
                """
                SELECT id
                FROM ops.doc_uploads
                WHERE source_message_id = :sourceMessageId
                """,
                new MapSqlParameterSource().addValue("sourceMessageId", sourceMessageId),
                UUID.class
        );
    }
}