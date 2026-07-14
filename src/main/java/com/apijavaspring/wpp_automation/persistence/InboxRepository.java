package com.apijavaspring.wpp_automation.persistence;

import com.apijavaspring.wpp_automation.config.WppAutomationProperties;
import com.apijavaspring.wpp_automation.core.PhoneNumberVariants;
import com.apijavaspring.wpp_automation.web.dto.InboundEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

@Repository
@RequiredArgsConstructor
@Slf4j
public class InboxRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final ChatwootInboundQueueRepository chatwootInboundQueueRepository;
    private final ConversationRepository conversationRepository;
    private final WppAutomationProperties properties;

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

        if (shouldEnqueueChatwootInbound(e)) {
            enqueueChatwootInboundMirrorSafely(inboxId);
        } else {
            log.debug("Deferring Chatwoot inbound mirror until FAQ/IA enrichment inboxId={} messageType={}",
                    inboxId, e.messageType());
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

    private boolean shouldEnqueueChatwootInbound(InboundEvent event) {
        if (notBlank(event.textBody())) {
            return true;
        }

        if (!isAudioOrImage(event.messageType())) {
            return true;
        }

        var conversation = conversationRepository.findByWaIdVariants(
                PhoneNumberVariants.brazilianVariants(event.waId())
        );

        return conversation == null || !"faq_ia".equals(conversation.state());
    }

    private static boolean isAudioOrImage(String messageType) {
        return "audio".equalsIgnoreCase(messageType)
                || "image".equalsIgnoreCase(messageType);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private void enqueueChatwootInboundMirrorSafely(UUID inboxId) {
        if (!properties.getChatwoot().isMirrorInboundEnabled()) {
            log.debug("Chatwoot inbound mirror disabled inboxId={}", inboxId);
            return;
        }

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    enqueueChatwootInboundMirrorNow(inboxId);
                }
            });
            return;
        }

        enqueueChatwootInboundMirrorNow(inboxId);
    }

    private void enqueueChatwootInboundMirrorNow(UUID inboxId) {
        try {
            int inserted = chatwootInboundQueueRepository.enqueueChatwootInboundMirror(inboxId);
            log.debug("Chatwoot inbound mirror enqueue inboxId={} inserted={}", inboxId, inserted);
        } catch (Exception ex) {
            log.warn("Failed to enqueue Chatwoot inbound mirror inboxId={}", inboxId, ex);
        }
    }
}
