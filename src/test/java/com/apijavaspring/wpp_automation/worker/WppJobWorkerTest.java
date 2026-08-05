package com.apijavaspring.wpp_automation.worker;

import com.apijavaspring.wpp_automation.persistence.MessageQueueRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WppJobWorkerTest {

    private RecordingMessageQueueRepository messageQueueRepository;
    private WppJobWorker worker;

    @BeforeEach
    void setUp() {
        messageQueueRepository = new RecordingMessageQueueRepository();
        worker = new WppJobWorker(
                null,
                null,
                null,
                null,
                null,
                messageQueueRepository,
                null,
                null
        );
    }

    @Test
    void faqIaTransitionDoesNotEnqueueRealtimeEvent() {
        worker.enqueueRealtimeTransitionIfApplicable(
                true,
                "MY56J",
                "5571999999999",
                "Hospede Teste",
                "faq_ia"
        );

        assertThat(messageQueueRepository.callCount).isZero();
    }

    @Test
    void collectingDocsInitTransitionEnqueuesRealtimeEvent() {
        worker.enqueueRealtimeTransitionIfApplicable(
                true,
                "MY56J",
                "5571999999999",
                "Hospede Teste",
                "collecting_docs_init"
        );

        assertThat(messageQueueRepository.callCount).isEqualTo(1);
        assertThat(messageQueueRepository.reservationId).isEqualTo("MY56J");
        assertThat(messageQueueRepository.waDigits).isEqualTo("5571999999999");
        assertThat(messageQueueRepository.holderName).isEqualTo("Hospede Teste");
        assertThat(messageQueueRepository.messageType).isEqualTo("collecting_docs_init");
        assertThat(messageQueueRepository.payloadJson).isEqualTo("{}");
    }

    @Test
    void unchangedConversationDoesNotEnqueueRealtimeEvent() {
        worker.enqueueRealtimeTransitionIfApplicable(
                false,
                "MY56J",
                "5571999999999",
                "Hospede Teste",
                "collecting_docs_init"
        );

        assertThat(messageQueueRepository.callCount).isZero();
    }

    @Test
    void otherStateKeepsExistingRealtimeEnqueueBehavior() {
        worker.enqueueRealtimeTransitionIfApplicable(
                true,
                "MY56J",
                "5571999999999",
                "Hospede Teste",
                "unknown_reservation"
        );

        assertThat(messageQueueRepository.callCount).isEqualTo(1);
        assertThat(messageQueueRepository.messageType).isEqualTo("unknown_reservation");
    }

    private static final class RecordingMessageQueueRepository extends MessageQueueRepository {

        private int callCount;
        private String reservationId;
        private String waDigits;
        private String holderName;
        private String messageType;
        private String payloadJson;

        private RecordingMessageQueueRepository() {
            super(null);
        }

        @Override
        public void enqueueRealtimeEvent(
                String reservationId,
                String waDigits,
                String holderName,
                String messageType,
                String payloadJson
        ) {
            callCount++;
            this.reservationId = reservationId;
            this.waDigits = waDigits;
            this.holderName = holderName;
            this.messageType = messageType;
            this.payloadJson = payloadJson;
        }
    }
}
