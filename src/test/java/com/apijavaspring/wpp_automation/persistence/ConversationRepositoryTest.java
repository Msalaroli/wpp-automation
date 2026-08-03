package com.apijavaspring.wpp_automation.persistence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConversationRepositoryTest {

    @Mock
    private NamedParameterJdbcTemplate jdbc;

    @Captor
    private ArgumentCaptor<MapSqlParameterSource> parametersCaptor;

    @Test
    void normalizesValidReservationIdByTrimmingExternalSpaces() {
        assertThat(
                ConversationRepository.normalizeReservationId(" MY56J ")
        ).isEqualTo("MY56J");
    }

    @Test
    void normalizesAbsentOrInvalidReservationIdsToNull() {
        assertThat(
                ConversationRepository.normalizeReservationId(null)
        ).isNull();
        assertThat(
                ConversationRepository.normalizeReservationId("")
        ).isNull();
        assertThat(
                ConversationRepository.normalizeReservationId("   ")
        ).isNull();
        assertThat(
                ConversationRepository.normalizeReservationId("null")
        ).isNull();
        assertThat(
                ConversationRepository.normalizeReservationId(" NULL ")
        ).isNull();
    }

    @Test
    void considersReservationIdsEqualIgnoringCaseAndExternalSpaces() {
        assertThat(
                ConversationRepository.sameReservationId("MY56J", "MY56J")
        ).isTrue();
        assertThat(
                ConversationRepository.sameReservationId("my56j", "MY56J")
        ).isTrue();
        assertThat(
                ConversationRepository.sameReservationId(" MY56J ", "MY56J")
        ).isTrue();
    }

    @Test
    void considersDifferentValidReservationIdsDifferent() {
        assertThat(
                ConversationRepository.sameReservationId("MY55J", "MY56J")
        ).isFalse();
    }

    @Test
    void distinguishesValidReservationFromInvalidReservation() {
        assertThat(
                ConversationRepository.sameReservationId("MY56J", null)
        ).isFalse();
        assertThat(
                ConversationRepository.sameReservationId("MY56J", "")
        ).isFalse();
        assertThat(
                ConversationRepository.sameReservationId("MY56J", "null")
        ).isFalse();
    }

    @Test
    void considersTwoInvalidReservationIdsEquivalent() {
        assertThat(
                ConversationRepository.sameReservationId(null, "")
        ).isTrue();
        assertThat(
                ConversationRepository.sameReservationId(" null ", "   ")
        ).isTrue();
    }

    @Test
    void sendsTrimmedReservationIdToJdbcAndReturnsTrueWhenOneRowIsUpdated() {
        when(jdbc.update(anyString(), any(MapSqlParameterSource.class)))
                .thenReturn(1);

        ConversationRepository repository = new ConversationRepository(jdbc);

        boolean changed = repository.replaceReservationAndResetState(
                "5571999999999",
                " MY56J ",
                "listing-1",
                "collecting_docs_init"
        );

        verify(jdbc).update(anyString(), parametersCaptor.capture());
        assertThat(parametersCaptor.getValue().getValue("reservationId"))
                .isEqualTo("MY56J");
        assertThat(changed).isTrue();
    }

    @Test
    void sendsInvalidReservationAsNullAndReturnsFalseWhenNoRowIsUpdated() {
        when(jdbc.update(anyString(), any(MapSqlParameterSource.class)))
                .thenReturn(0);

        ConversationRepository repository = new ConversationRepository(jdbc);

        boolean changed = repository.setReservationAndStateIfChanged(
                "5571999999999",
                " null ",
                "listing-1",
                "collecting_docs_init"
        );

        verify(jdbc).update(anyString(), parametersCaptor.capture());
        assertThat(parametersCaptor.getValue().getValue("reservationId"))
                .isNull();
        assertThat(changed).isFalse();
    }
}
