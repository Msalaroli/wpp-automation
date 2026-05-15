package com.apijavaspring.wpp_automation.worker;

import com.apijavaspring.wpp_automation.config.WppAutomationProperties;
import com.apijavaspring.wpp_automation.persistence.ConversationRepository;
import com.apijavaspring.wpp_automation.persistence.DocUploadRepository;
import com.apijavaspring.wpp_automation.persistence.MessageQueueRepository;
import com.apijavaspring.wpp_automation.persistence.N8nJobRepository;
import com.apijavaspring.wpp_automation.persistence.ReservationRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class WppJobWorker {

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    private final WppAutomationProperties properties;
    private final ReservationRepository reservationRepository;
    private final ConversationRepository conversationRepository;
    private final MessageQueueRepository messageQueueRepository;

    private final DocUploadRepository docUploadRepository;
    private final N8nJobRepository n8nJobRepository;

    @Value("${app.worker.enabled:true}")
    private boolean enabled;

    @Value("${app.worker.poll-delay-ms:250}")
    private long pollDelayMs;

    @Value("${app.worker.threads:2}")
    private int threads;

    @Value("${app.worker.locked-by:wpp-automation}")
    private String lockedBy;

    private volatile boolean stopping;
    private ExecutorService executor;

    @PostConstruct
    public void start() {
        if (!enabled) return;
        executor = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            executor.submit(this::runLoop);
        }
    }

    @PreDestroy
    public void stop() {
        stopping = true;
        if (executor == null) return;

        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.warn("WPP job worker did not terminate after shutdownNow");
                }
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void runLoop() {
        while (!stopping) {
            try {
                boolean didWork = processOneJob();
                if (!didWork) Thread.sleep(pollDelayMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                try { Thread.sleep(500); } catch (InterruptedException ignored) { return; }
            }
        }
    }

    @Transactional
    public boolean processOneJob() {
        var job = pickNextJob();
        if (job == null) return false;

        UUID inboxId = job.inboxId();

        try {
            var inbox = loadInbox(inboxId);

            // Comercial: intercepta antes de criar/atualizar conversa de hóspede
            if (isCommercialNumber(job.waId())) {
                handleCommercialCommand(job.waId(), inbox);

                markInboxProcessed(inboxId);
                markJobOk(job.jobId());

                return true;
            }

            // 1) sempre atualiza janela e last_user_message_at
            upsertConversation(job.waId(), inbox.receivedAtEpochSeconds());

            // Human handoff: se atendimento humano está ativo, não empilha nada
            var currentConv = conversationRepository.findByWaId(job.waId());
            if (currentConv != null && currentConv.humanHandoff()) {
                if (handleExpiredOrOrphanedConversation(job.waId(), currentConv)) {
                    currentConv = conversationRepository.findByWaId(job.waId());
                } else {
                    markInboxProcessed(inboxId);
                    markJobOk(job.jobId());
                    return true;
                }
            }

            // Pós-checkout expirado: não automatiza mais nada, exceto nova reserva no mesmo WhatsApp
            if (currentConv != null && "post_checkout_expired".equals(safe(currentConv.state()))) {
                handleExpiredOrOrphanedConversation(job.waId(), currentConv);
                currentConv = conversationRepository.findByWaId(job.waId());
            }

            if (currentConv != null && isExpiredOrOrphaned(currentConv)) {
                if (handleExpiredOrOrphanedConversation(job.waId(), currentConv)) {
                    currentConv = conversationRepository.findByWaId(job.waId());
                }
            }

            // 2) identifica reserva se ainda não identificou
            handleIdentifyAndStartFlow(job.waId());

            // 3) carrega conversa atualizada e executa regras por estado
            var conv = conversationRepository.findByWaId(job.waId());
            if (conv != null) {
                handleUnknownReservationInbound(conv, inbox);
                handleDocsInbound(conv, inbox);
                handleFaqIaInbound(conv, inbox);
            }

            // 4) marca inbox e job como ok
            markInboxProcessed(inboxId);
            markJobOk(job.jobId());

            return true;

        } catch (Exception e) {
            markInboxError(inboxId, e);
            handleJobError(job.jobId(), e);
            return true;
        }
    }

    private boolean handleExpiredOrOrphanedConversation(String waIdRaw, ConversationRepository.Conversation conv) {
        var ref = reservationRepository.findCurrentOrFutureByHolderPhone(waIdRaw);
        if (ref == null) {
            return resetToUnknownReservation(waIdRaw);
        }
        if (safe(ref.reservationId()).equals(safe(conv.reservationId()))) {
            return false;
        }

        boolean changed = conversationRepository.replaceReservationAndResetState(
                waIdRaw,
                ref.reservationId(),
                ref.listingId(),
                "collecting_docs_init"
        );

        if (changed) {
            messageQueueRepository.enqueueRealtimeEvent(
                    ref.reservationId(),
                    digitsOnly(waIdRaw),
                    ref.holderName(),
                    "collecting_docs_init",
                    "{}"
            );
        }

        return changed;
    }

    private boolean resetToUnknownReservation(String waIdRaw) {
        boolean changed = conversationRepository.resetToUnknownReservation(waIdRaw);
        if (changed) {
            messageQueueRepository.enqueueRealtimeEvent(
                    null,
                    digitsOnly(waIdRaw),
                    null,
                    "unknown_reservation",
                    "{}"
            );
        }
        return changed;
    }

    private boolean isExpiredOrOrphaned(ConversationRepository.Conversation conv) {
        if ("post_checkout_expired".equals(safe(conv.state()))) return true;
        if (!isReactivationCandidateState(conv.state()) && !conv.humanHandoff()) return false;
        return !reservationRepository.isReservationOperational(
                conv.reservationId(),
                usesPostCheckoutSupportWindow(conv)
        );
    }

    private boolean usesPostCheckoutSupportWindow(ConversationRepository.Conversation conv) {
        return conv.humanHandoff() || "faq_ia".equals(safe(conv.state()));
    }

    private boolean isReactivationCandidateState(String state) {
        return switch (safe(state)) {
            case "collecting_docs_init", "collecting_docs_waiting", "faq_ia" -> true;
            default -> false;
        };
    }

    private void handleCommercialCommand(String waId, InboxRow inbox) {
        if (!"text".equals(safe(inbox.messageType()))) return;

        String payloadJson = payloadJson(
                "wa_id", waId,
                "inbox_id", inbox.id(),
                "message_id", inbox.messageId(),
                "text", inbox.textBody()
        );

        n8nJobRepository.enqueueCommercialCommandJob(
                inbox.id(),
                waId,
                payloadJson
        );
    }

    private boolean isCommercialNumber(String waIdRaw) {
        Set<String> incomingVariants = phoneVariants(waIdRaw);

        for (String allowed : properties.getCommercial().getAllowedNumbers()) {
            Set<String> allowedVariants = phoneVariants(allowed);

            for (String variant : incomingVariants) {
                if (allowedVariants.contains(variant)) {
                    return true;
                }
            }
        }

        return false;
    }

    private void handleIdentifyAndStartFlow(String waIdRaw) {
        String waDigits = digitsOnly(waIdRaw);

        var conv = conversationRepository.findByWaId(waIdRaw);
        if (conv == null) return;

        if (conv.humanHandoff()) return;

        if (conv.reservationId() != null && !conv.reservationId().isBlank()) {
            return;
        }

        var ref = reservationRepository.findCurrentOrFutureByHolderPhone(waIdRaw);
        if (ref == null) ref = reservationRepository.findCurrentOrFutureByHolderPhone("+" + waDigits);
        if (ref == null) ref = reservationRepository.findCurrentOrFutureByHolderPhone(waDigits);

        if (ref == null) {
            boolean changed = conversationRepository.setStateIfChanged(waIdRaw, "unknown_reservation");
            if (changed) {
                messageQueueRepository.enqueueRealtimeEvent(
                        null,
                        waDigits,
                        null,
                        "unknown_reservation",
                        "{}"
                );
            }
            return;
        }

        boolean changed = conversationRepository.setReservationAndStateIfChanged(
                waIdRaw,
                ref.reservationId(),
                ref.listingId(),
                "collecting_docs_init"
        );

        if (changed) {
            messageQueueRepository.enqueueRealtimeEvent(
                    ref.reservationId(),
                    waDigits,
                    ref.holderName(),
                    "collecting_docs_init",
                    "{}"
            );
        }
    }

    private void handleUnknownReservationInbound(ConversationRepository.Conversation conv, InboxRow inbox) {
        if (conv.humanHandoff()) return;
        if (!"unknown_reservation".equals(safe(conv.state()))) return;

        if (!"text".equals(safe(inbox.messageType()))) {
            return;
        }

        String payloadJson = payloadJson(
                "wa_id", conv.waId(),
                "inbox_id", inbox.id(),
                "message_id", inbox.messageId(),
                "text", inbox.textBody()
        );

        n8nJobRepository.enqueueUnknownReservationLookupJob(
                inbox.id(),
                conv.waId(),
                payloadJson
        );
    }

    private void handleDocsInbound(ConversationRepository.Conversation conv, InboxRow inbox) {
        if (conv.humanHandoff()) return;

        if (!"collecting_docs_waiting".equals(safe(conv.state()))) return;

        if (conv.reservationId() == null || conv.reservationId().isBlank()) return;

        if (notBlank(inbox.mediaId())) {

            String tipo = switch (safe(inbox.messageType())) {
                case "image" -> "imagem";
                case "document" -> "documento";
                case "audio" -> "audio";
                case "video" -> "video";
                default -> "midia";
            };

            UUID uploadId = docUploadRepository.insertPendingWhatsAppUpload(
                    conv.reservationId(),
                    tipo,
                    inbox.messageId(),
                    inbox.mediaId(),
                    inbox.mimeType(),
                    inbox.mediaSha256()
            );

            String payloadJson = payloadJson(
                    "upload_id", uploadId,
                    "reservation_id", conv.reservationId(),
                    "wa_id", conv.waId(),
                    "message_id", inbox.messageId(),
                    "media_id", inbox.mediaId(),
                    "mime_type", inbox.mimeType(),
                    "sha256", inbox.mediaSha256()
            );

            n8nJobRepository.enqueueDownloadMediaJob(
                    uploadId,
                    conv.reservationId(),
                    conv.waId(),
                    payloadJson
            );

            return;
        }

        if ("text".equals(safe(inbox.messageType()))) {
            String payloadJson = payloadJson(
                    "reservation_id", conv.reservationId(),
                    "wa_id", conv.waId(),
                    "inbox_id", inbox.id(),
                    "message_id", inbox.messageId(),
                    "text", inbox.textBody()
            );

            n8nJobRepository.enqueueCollectingDocsTextJob(
                    inbox.id(),
                    conv.reservationId(),
                    conv.waId(),
                    payloadJson
            );
        }
    }

    private void handleFaqIaInbound(ConversationRepository.Conversation conv, InboxRow inbox) {
        if (conv.humanHandoff()) return;
        if (!"faq_ia".equals(safe(conv.state()))) return;

        String reservationId = conv.reservationId();

        String payloadJson = payloadJson(
                "reservation_id", reservationId,
                "wa_id", conv.waId()
        );

        n8nJobRepository.enqueueFaqIaOpenJob(
                reservationId,
                conv.waId(),
                payloadJson
        );
    }

    private static String digitsOnly(String s) {
        if (s == null) return "";
        return s.replaceAll("\\D", "");
    }

    private static Set<String> phoneVariants(String raw) {
        LinkedHashSet<String> out = new LinkedHashSet<>();

        String d = digitsOnly(raw);
        if (d.isBlank()) return out;

        out.add(d);

        if (d.startsWith("55")) {
            String local = d.substring(2);
            out.add(local);

            if (local.matches("^\\d{2}9\\d{8}$")) {
                String withoutNine = local.substring(0, 2) + local.substring(3);
                out.add(withoutNine);
                out.add("55" + withoutNine);
            }

            if (local.matches("^\\d{2}\\d{8}$")) {
                String withNine = local.substring(0, 2) + "9" + local.substring(2);
                out.add(withNine);
                out.add("55" + withNine);
            }

            return out;
        }

        if (d.matches("^\\d{2}9\\d{8}$")) {
            String withoutNine = d.substring(0, 2) + d.substring(3);
            out.add(withoutNine);
            out.add("55" + d);
            out.add("55" + withoutNine);
        }

        if (d.matches("^\\d{2}\\d{8}$")) {
            String withNine = d.substring(0, 2) + "9" + d.substring(2);
            out.add(withNine);
            out.add("55" + d);
            out.add("55" + withNine);
        }

        return out;
    }

    private static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    private String payloadJson(Object... fields) {
        LinkedHashMap<String, String> payload = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            payload.put((String) fields[i], stringify(fields[i + 1]));
        }

        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to build n8n job payload", e);
        }
    }

    private static String stringify(Object value) {
        if (value == null) return "";
        return value.toString();
    }

    private JobRow pickNextJob() {
        List<JobRow> rows = jdbc.query(
                """
                UPDATE ops.wpp_jobs j
                SET status = 'processando',
                    locked_at = now(),
                    locked_by = :lockedBy,
                    updated_at = now()
                FROM (
                    SELECT id
                    FROM ops.wpp_jobs
                    WHERE status IN ('pendente','erro')
                      AND job_type = 'process_inbox'
                      AND run_after <= now()
                      AND attempts < 5
                    ORDER BY created_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                ) next_job
                WHERE j.id = next_job.id
                RETURNING j.id, j.inbox_id, j.wa_id, j.attempts
                """,
                new MapSqlParameterSource().addValue("lockedBy", lockedBy),
                (rs, n) -> new JobRow(
                        (UUID) rs.getObject("id"),
                        (UUID) rs.getObject("inbox_id"),
                        rs.getString("wa_id"),
                        rs.getInt("attempts")
                )
        );

        if (rows.isEmpty()) return null;
        return rows.get(0);
    }

    @Scheduled(fixedDelay = 300_000)
    public void reapStuckJobs() {
        int affected = jdbc.update(
                """
                UPDATE ops.wpp_jobs
                SET status = CASE WHEN attempts + 1 >= 5 THEN 'falha_final' ELSE 'erro' END,
                    attempts = attempts + 1,
                    locked_at = null,
                    locked_by = null,
                    run_after = CASE
                        WHEN attempts + 1 >= 5 THEN run_after
                        ELSE now() + make_interval(secs => 10 * (attempts + 1))
                    END,
                    updated_at = now()
                WHERE status = 'processando'
                  AND locked_at < now() - interval '5 minutes'
                """,
                new MapSqlParameterSource()
        );

        if (affected > 0) {
            log.warn("WPP job reaper affectedJobs={}", affected);
        }
    }

    private InboxRow loadInbox(UUID inboxId) {
        return jdbc.queryForObject(
                """
                SELECT id, wa_id, message_id, message_type, text_body,
                       media_id, mime_type, media_sha256, received_at
                FROM ops.wpp_inbox
                WHERE id = :id
                """,
                new MapSqlParameterSource().addValue("id", inboxId),
                (rs, n) -> new InboxRow(
                        (UUID) rs.getObject("id"),
                        rs.getString("wa_id"),
                        rs.getString("message_id"),
                        rs.getString("message_type"),
                        rs.getString("text_body"),
                        rs.getString("media_id"),
                        rs.getString("mime_type"),
                        rs.getString("media_sha256"),
                        rs.getObject("received_at", java.time.OffsetDateTime.class)
                )
        );
    }

    private void upsertConversation(String waId, long receivedAtEpochSeconds) {
        jdbc.update(
                """
                INSERT INTO ops.conversations
                  (wa_id, state, window_open_until, last_user_message_at, human_handoff, created_at, updated_at, version)
                VALUES
                  (:waId, 'aguardando_inicio', to_timestamp(:ts) + interval '24 hours', to_timestamp(:ts), false, now(), now(), 0)
                ON CONFLICT (wa_id) DO UPDATE
                SET last_user_message_at = GREATEST(
                      COALESCE(ops.conversations.last_user_message_at, to_timestamp(0)),
                      EXCLUDED.last_user_message_at
                    ),
                    window_open_until = GREATEST(
                      COALESCE(ops.conversations.last_user_message_at, to_timestamp(0)),
                      EXCLUDED.last_user_message_at
                    ) + interval '24 hours',
                    updated_at = now(),
                    version = ops.conversations.version + 1
                """,
                new MapSqlParameterSource()
                        .addValue("waId", waId)
                        .addValue("ts", receivedAtEpochSeconds)
        );
    }

    private void markInboxProcessed(UUID inboxId) {
        jdbc.update(
                """
                UPDATE ops.wpp_inbox
                SET status = 'processado'
                WHERE id = :id
                """,
                new MapSqlParameterSource().addValue("id", inboxId)
        );
    }

    private void markInboxError(UUID inboxId, Exception e) {
        Throwable root = e;
        while (root.getCause() != null) root = root.getCause();
        String msg = root.getClass().getSimpleName() + ": " + (root.getMessage() == null ? "" : root.getMessage());

        jdbc.update(
                """
                UPDATE ops.wpp_inbox
                SET status = 'erro',
                    error_message = left(:err, 1000)
                WHERE id = :id
                """,
                new MapSqlParameterSource()
                        .addValue("id", inboxId)
                        .addValue("err", msg)
        );
    }

    private void markJobOk(UUID jobId) {
        jdbc.update(
                """
                UPDATE ops.wpp_jobs
                SET status = 'ok',
                    locked_at = null,
                    locked_by = null,
                    updated_at = now()
                WHERE id = :id
                """,
                new MapSqlParameterSource().addValue("id", jobId)
        );
    }

    private void handleJobError(UUID jobId, Exception e) {
        jdbc.update(
                """
                UPDATE ops.wpp_jobs
                SET status = CASE WHEN attempts + 1 >= 5 THEN 'falha_final' ELSE 'erro' END,
                    attempts = attempts + 1,
                    last_error = left(:err, 1000),
                    locked_at = null,
                    locked_by = null,
                    run_after = now() + make_interval(secs => 10 * (attempts + 1)),
                    updated_at = now()
                WHERE id = :id
                """,
                new MapSqlParameterSource()
                        .addValue("id", jobId)
                        .addValue("err", e.getClass().getSimpleName() + ": " + (e.getMessage() == null ? "" : e.getMessage()))
        );
    }

    private record JobRow(UUID jobId, UUID inboxId, String waId, int attempts) {}

    private record InboxRow(
            UUID id,
            String waId,
            String messageId,
            String messageType,
            String textBody,
            String mediaId,
            String mimeType,
            String mediaSha256,
            java.time.OffsetDateTime receivedAt
    ) {
        long receivedAtEpochSeconds() {
            return receivedAt.toEpochSecond();
        }
    }
}
