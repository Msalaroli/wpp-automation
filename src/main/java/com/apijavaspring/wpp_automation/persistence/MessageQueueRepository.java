package com.apijavaspring.wpp_automation.persistence;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class MessageQueueRepository {

    private final NamedParameterJdbcTemplate jdbc;

    /**
     * Enfileira um comando para o n8n montar e enviar.
     * - channel = wa_id digits (sem +)
     * - message_type = estado/comando (ex: collecting_docs, unknown_reservation)
     * - payload = jsonb (pode ser {} se o n8n só usa os campos da tabela)
     */
    public void enqueueRealtimeEvent(String reservationId, String waDigits, String holderName, String messageType, String payloadJson) {
        String payload = (payloadJson == null || payloadJson.isBlank()) ? "{}" : payloadJson;

        jdbc.update(
                """
                INSERT INTO ops.message_queue
                  (reservation_id, channel, recipient, template, payload, status, attempts, provider_msg_id, last_error,
                   created_at, message_type, next_retry_at, last_attempt_at, sent_at, queue_name)
                VALUES
                  (:reservationId, :channel, :recipient, null, CAST(:payload AS jsonb), 'pendente', 0, null, null,
                   now(), :messageType, now(), null, null, 'realtime')
                """,
                new MapSqlParameterSource()
                        .addValue("reservationId", reservationId)
                        .addValue("channel", waDigits)
                        .addValue("recipient", holderName)
                        .addValue("messageType", messageType)
                        .addValue("payload", payload)
        );
    }
}
