package com.apijavaspring.wpp_automation.persistence;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class N8nJobRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public void enqueueDownloadMediaJob(UUID uploadId, String reservationId, String waId, String payloadJson) {
        jdbc.update(
                """
                INSERT INTO ops.n8n_jobs
                  (job_type, ref_table, ref_id, reservation_id, wa_id, payload, status, attempts, run_after, created_at, updated_at)
                VALUES
                  ('download_media', 'doc_uploads', :refId, :reservationId, :waId, CAST(:payload AS jsonb),
                   'pendente', 0, now(), now(), now())
                """,
                new MapSqlParameterSource()
                        .addValue("refId", uploadId)
                        .addValue("reservationId", reservationId)
                        .addValue("waId", waId)
                        .addValue("payload", payloadJson == null ? "{}" : payloadJson)
        );
    }

    public void enqueueCollectingDocsTextJob(UUID inboxId, String reservationId, String waId, String payloadJson) {
        jdbc.update(
                """
                INSERT INTO ops.n8n_jobs
                  (job_type, ref_table, ref_id, reservation_id, wa_id, payload, status, attempts, run_after, created_at, updated_at)
                VALUES
                  ('collecting_docs_text', 'wpp_inbox', :refId, :reservationId, :waId, CAST(:payload AS jsonb),
                   'pendente', 0, now(), now(), now())
                ON CONFLICT (job_type, ref_table, ref_id) DO NOTHING
                """,
                new MapSqlParameterSource()
                        .addValue("refId", inboxId)
                        .addValue("reservationId", reservationId)
                        .addValue("waId", waId)
                        .addValue("payload", payloadJson == null ? "{}" : payloadJson)
        );
    }

    public void enqueueUnknownReservationLookupJob(UUID inboxId, String waId, String payloadJson) {
        jdbc.update(
                """
                INSERT INTO ops.n8n_jobs
                  (job_type, ref_table, ref_id, reservation_id, wa_id, payload,
                   status, attempts, run_after, created_at, updated_at)
                VALUES
                  ('unknown_reservation_lookup',
                   'wpp_inbox',
                   :refId,
                   NULL,
                   :waId,
                   CAST(:payload AS jsonb),
                   'pendente',
                   0,
                   now(),
                   now(),
                   now())
                ON CONFLICT (job_type, ref_table, ref_id) DO NOTHING
                """,
                new MapSqlParameterSource()
                        .addValue("refId", inboxId)
                        .addValue("waId", waId)
                        .addValue("payload", payloadJson == null ? "{}" : payloadJson)
        );
    }

    public void enqueueFaqIaOpenJob(String reservationId, String waId, String payloadJson) {
        try {
            jdbc.update(
                    """
                    INSERT INTO ops.n8n_jobs
                      (job_type, ref_table, ref_id, reservation_id, wa_id, payload,
                       status, attempts, run_after, created_at, updated_at)
                    VALUES
                      ('faq_ia_inbound',
                       'conversations',
                       gen_random_uuid(),
                       :reservationId,
                       :waId,
                       CAST(:payload AS jsonb),
                       'pendente',
                       0,
                       now(),
                       now(),
                       now())
                    """,
                    new MapSqlParameterSource()
                            .addValue("reservationId", reservationId)
                            .addValue("waId", waId)
                            .addValue("payload", payloadJson == null ? "{}" : payloadJson)
            );

        } catch (org.springframework.dao.DuplicateKeyException e) {

            // Já existe job faq IA aberto → apenas "cutuca" o existente
            jdbc.update(
                    """
                    UPDATE ops.n8n_jobs
                    SET updated_at = now()
                    WHERE job_type = 'faq_ia_inbound'
                      AND wa_id = :waId
                      AND status IN ('pendente','processing')
                    """,
                    new MapSqlParameterSource().addValue("waId", waId)
            );
        }
    }

    public void enqueueCommercialCommandJob(UUID inboxId, String waId, String payloadJson) {
        jdbc.update(
                """
                INSERT INTO ops.n8n_jobs
                  (job_type, ref_table, ref_id, reservation_id, wa_id, payload,
                   status, attempts, run_after, created_at, updated_at)
                VALUES
                  ('commercial_command',
                   'wpp_inbox',
                   :refId,
                   NULL,
                   :waId,
                   CAST(:payload AS jsonb),
                   'pendente',
                   0,
                   now(),
                   now(),
                   now())
                ON CONFLICT (job_type, ref_table, ref_id) DO NOTHING
                """,
                new MapSqlParameterSource()
                        .addValue("refId", inboxId)
                        .addValue("waId", waId)
                        .addValue("payload", payloadJson == null ? "{}" : payloadJson)
        );
    }


}