package com.apijavaspring.wpp_automation.persistence;

import com.apijavaspring.wpp_automation.core.PhoneNumberVariants;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class ReservationRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public ReservationRef findByHolderPhone(String waIdRaw) {
        List<String> variants = PhoneNumberVariants.brazilianVariants(waIdRaw);
        if (variants.isEmpty()) return null;

        var rows = jdbc.query(
                """
                SELECT reservation_id, listing_id, holder_name, holder_phone,
                       status_coleta
                FROM public.stays_reservas
                WHERE regexp_replace(holder_phone, '\\D', '', 'g') IN (:phoneDigitsList)
                ORDER BY
                  CASE
                    WHEN checkin_date >= now() THEN 0
                    ELSE 1
                  END,
                  ABS(EXTRACT(EPOCH FROM (checkin_date - now())))
                LIMIT 1
                """,
                new MapSqlParameterSource()
                        .addValue("phoneDigitsList", variants),
                (rs, n) -> new ReservationRef(
                        rs.getString("reservation_id"),
                        rs.getString("listing_id"),
                        rs.getString("holder_name"),
                        rs.getString("holder_phone"),
                        rs.getString("status_coleta")
                )
        );

        return rows.isEmpty() ? null : rows.get(0);
    }

    public ReservationRef findCurrentOrFutureByHolderPhone(String waIdRaw) {
        List<String> variants = PhoneNumberVariants.brazilianVariants(waIdRaw);
        if (variants.isEmpty()) return null;

        var rows = jdbc.query(
                """
                SELECT reservation_id, listing_id, holder_name, holder_phone,
                       status_coleta
                FROM public.stays_reservas
                WHERE regexp_replace(holder_phone, '\\D', '', 'g') IN (:phoneDigitsList)
                  AND checkout_date >= current_date
                ORDER BY
                  CASE
                    WHEN checkin_date <= now() AND checkout_date >= current_date THEN 0
                    WHEN checkin_date >= now() THEN 1
                    ELSE 2
                  END,
                  ABS(EXTRACT(EPOCH FROM (checkin_date - now())))
                LIMIT 1
                """,
                new MapSqlParameterSource()
                        .addValue("phoneDigitsList", variants),
                (rs, n) -> new ReservationRef(
                        rs.getString("reservation_id"),
                        rs.getString("listing_id"),
                        rs.getString("holder_name"),
                        rs.getString("holder_phone"),
                        rs.getString("status_coleta")
                )
        );

        return rows.isEmpty() ? null : rows.get(0);
    }

    public boolean isReservationOperational(String reservationId, boolean allowPostCheckoutSupportWindow) {
        if (reservationId == null || reservationId.isBlank()) return false;

        Boolean operational = jdbc.queryForObject(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM public.stays_reservas
                    WHERE reservation_id = :reservationId
                      AND (
                        (:allowPostCheckoutSupportWindow = true AND checkout_date >= current_date - 1)
                        OR (:allowPostCheckoutSupportWindow = false AND checkout_date >= current_date)
                      )
                )
                """,
                new MapSqlParameterSource()
                        .addValue("reservationId", reservationId)
                        .addValue("allowPostCheckoutSupportWindow", allowPostCheckoutSupportWindow),
                Boolean.class
        );

        return Boolean.TRUE.equals(operational);
    }

    public record ReservationRef(
            String reservationId,
            String listingId,
            String holderName,
            String holderPhone,
            String statusColeta
    ) {}
}
