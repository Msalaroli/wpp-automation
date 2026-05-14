package com.apijavaspring.wpp_automation.persistence;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class ConversationRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public Conversation findByWaId(String waId) {
        List<Conversation> rows = jdbc.query(
                """
                SELECT wa_id, reservation_id, listing_id, state,
                       window_open_until, last_user_message_at, last_bot_message_at,
                       human_handoff, created_at, updated_at, version
                FROM ops.conversations
                WHERE wa_id = :waId
                """,
                new MapSqlParameterSource().addValue("waId", waId),
                (rs, n) -> new Conversation(
                        rs.getString("wa_id"),
                        rs.getString("reservation_id"),
                        rs.getString("listing_id"),
                        rs.getString("state"),
                        rs.getTimestamp("window_open_until") == null ? null : rs.getTimestamp("window_open_until").toInstant(),
                        rs.getTimestamp("last_user_message_at") == null ? null : rs.getTimestamp("last_user_message_at").toInstant(),
                        rs.getTimestamp("last_bot_message_at") == null ? null : rs.getTimestamp("last_bot_message_at").toInstant(),
                        rs.getBoolean("human_handoff"),
                        rs.getTimestamp("created_at").toInstant(),
                        rs.getTimestamp("updated_at").toInstant(),
                        rs.getInt("version")
                )
        );

        return rows.isEmpty() ? null : rows.get(0);
    }

    // Mantidos (podem ser úteis depois)
    public void setReservationAndState(String waId, String reservationId, String listingId, String state) {
        jdbc.update(
                """
                UPDATE ops.conversations
                SET reservation_id = COALESCE(reservation_id, :reservationId),
                    listing_id     = COALESCE(listing_id, :listingId),
                    state          = :state,
                    updated_at     = now(),
                    version        = version + 1
                WHERE wa_id = :waId
                """,
                new MapSqlParameterSource()
                        .addValue("waId", waId)
                        .addValue("reservationId", reservationId)
                        .addValue("listingId", listingId)
                        .addValue("state", state)
        );
    }

    public void setState(String waId, String state) {
        jdbc.update(
                """
                UPDATE ops.conversations
                SET state = :state,
                    updated_at = now(),
                    version = version + 1
                WHERE wa_id = :waId
                """,
                new MapSqlParameterSource()
                        .addValue("waId", waId)
                        .addValue("state", state)
        );
    }

    // QA-level: idempotência por transição (retorna true só se mudou)
    public boolean setStateIfChanged(String waId, String newState) {
        int updated = jdbc.update(
                """
                UPDATE ops.conversations
                SET state = :newState,
                    updated_at = now(),
                    version = version + 1
                WHERE wa_id = :waId
                  AND state <> :newState
                """,
                new MapSqlParameterSource()
                        .addValue("waId", waId)
                        .addValue("newState", newState)
        );
        return updated == 1;
    }

    public boolean setReservationAndStateIfChanged(String waId, String reservationId, String listingId, String newState) {
        int updated = jdbc.update(
                """
                UPDATE ops.conversations
                SET reservation_id = COALESCE(reservation_id, :reservationId),
                    listing_id     = COALESCE(listing_id, :listingId),
                    state          = :newState,
                    updated_at     = now(),
                    version        = version + 1
                WHERE wa_id = :waId
                  AND state <> :newState
                """,
                new MapSqlParameterSource()
                        .addValue("waId", waId)
                        .addValue("reservationId", reservationId)
                        .addValue("listingId", listingId)
                        .addValue("newState", newState)
        );
        return updated == 1;
    }

    public boolean replaceReservationAndResetState(String waId, String reservationId, String listingId, String newState) {
        int updated = jdbc.update(
                """
                UPDATE ops.conversations
                SET reservation_id = :reservationId,
                    listing_id     = :listingId,
                    state          = :newState,
                    human_handoff  = false,
                    updated_at     = now(),
                    version        = version + 1
                WHERE wa_id = :waId
                  AND reservation_id IS DISTINCT FROM :reservationId
                """,
                new MapSqlParameterSource()
                        .addValue("waId", waId)
                        .addValue("reservationId", reservationId)
                        .addValue("listingId", listingId)
                        .addValue("newState", newState)
        );
        return updated == 1;
    }

    public record Conversation(
            String waId,
            String reservationId,
            String listingId,
            String state,
            Instant windowOpenUntil,
            Instant lastUserMessageAt,
            Instant lastBotMessageAt,
            boolean humanHandoff,
            Instant createdAt,
            Instant updatedAt,
            int version
    ) {}
}
