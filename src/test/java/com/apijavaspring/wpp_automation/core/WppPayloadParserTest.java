package com.apijavaspring.wpp_automation.core;

import com.apijavaspring.wpp_automation.web.dto.InboundEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WppPayloadParserTest {

    private static final String MESSAGE_ID = "wamid.original-contact-message-id";

    private final WppPayloadParser parser = new WppPayloadParser();

    @Test
    void parsesContactWithFormattedNameAndOnePhone() {
        InboundEvent event = parseSingleContact("""
                {
                  "name": {
                    "formatted_name": "Andrea Silva",
                    "first_name": "Andrea",
                    "last_name": "Silva"
                  },
                  "phones": [
                    {
                      "phone": "+55 71 99999-9999",
                      "wa_id": "5571999999999",
                      "type": "CELL"
                    }
                  ]
                }
                """);

        assertThat(event.messageType()).isEqualTo("contacts");
        assertThat(event.textBody()).isEqualTo("""
                Contato compartilhado:
                Nome: Andrea Silva
                Celular: +55 71 99999-9999""");
        assertThat(event.messageId()).isEqualTo(MESSAGE_ID);
        assertThat(event.mediaId()).isNull();
        assertThat(event.mimeType()).isNull();
        assertThat(event.mediaSha256()).isNull();
    }

    @Test
    void parsesContactWithMultiplePhonesInOriginalOrder() {
        InboundEvent event = parseSingleContact("""
                {
                  "name": {"formatted_name": "Andrea Silva"},
                  "phones": [
                    {"phone": "+55 71 99999-9999"},
                    {"phone": "+55 71 98888-8888"}
                  ]
                }
                """);

        assertThat(event.textBody()).isEqualTo("""
                Contato compartilhado:
                Nome: Andrea Silva
                Celulares:
                - +55 71 99999-9999
                - +55 71 98888-8888""");
    }

    @Test
    void parsesMultipleContactsInOriginalOrder() {
        List<InboundEvent> events = parseContacts("""
                [
                  {
                    "name": {"formatted_name": "Andrea Silva"},
                    "phones": [
                      {"phone": "+55 71 99999-9999"}
                    ]
                  },
                  {
                    "name": {"formatted_name": "João Souza"},
                    "phones": [
                      {"phone": "+55 71 98888-8888"},
                      {"phone": "+55 11 97777-7777"}
                    ]
                  }
                ]
                """);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).textBody()).isEqualTo("""
                Contatos compartilhados:

                Contato 1:
                Nome: Andrea Silva
                Celular: +55 71 99999-9999

                Contato 2:
                Nome: João Souza
                Celulares:
                - +55 71 98888-8888
                - +55 11 97777-7777""");
    }

    @Test
    void assemblesNameWhenFormattedNameIsMissing() {
        InboundEvent event = parseSingleContact("""
                {
                  "name": {
                    "first_name": "Andrea",
                    "middle_name": "Maria",
                    "last_name": "Silva"
                  },
                  "phones": [{"phone": "+55 71 99999-9999"}]
                }
                """);

        assertThat(event.textBody()).contains("Nome: Andrea Maria Silva");
    }

    @Test
    void usesNotInformedWhenContactHasNoName() {
        InboundEvent event = parseSingleContact("""
                {
                  "phones": [{"phone": "+55 71 99999-9999"}]
                }
                """);

        assertThat(event.textBody()).contains("Nome: Não informado");
    }

    @Test
    void usesWaIdWhenPhoneIsMissing() {
        InboundEvent event = parseSingleContact("""
                {
                  "name": {"formatted_name": "Andrea Silva"},
                  "phones": [{"wa_id": "5571999999999"}]
                }
                """);

        assertThat(event.textBody()).contains("Celular: 5571999999999");
    }

    @Test
    void usesNotInformedWhenContactHasNoPhone() {
        InboundEvent event = parseSingleContact("""
                {
                  "name": {"formatted_name": "Andrea Silva"}
                }
                """);

        assertThat(event.textBody()).contains("Celular: Não informado");
    }

    @Test
    void preservesRepeatedAndDifferentlyFormattedPhonesInOriginalOrder() {
        InboundEvent event = parseSingleContact("""
                {
                  "name": {"formatted_name": "Andrea Silva"},
                  "phones": [
                    {"phone": "+55 71 99999-9999"},
                    {"phone": "5571999999999"},
                    {"phone": "+55 71 99999-9999"}
                  ]
                }
                """);

        assertThat(event.textBody()).isEqualTo("""
                Contato compartilhado:
                Nome: Andrea Silva
                Celulares:
                - +55 71 99999-9999
                - 5571999999999
                - +55 71 99999-9999""");
    }

    @Test
    void sanitizesControlCharactersWithoutRemovingFormatterLineBreaks() {
        InboundEvent event = parseSingleContact("""
                {
                  "name": {
                    "formatted_name": "Andrea\\n\\r\\t\u0085   Silva"
                  },
                  "phones": [
                    {"phone": "+55\\t71\\n99999-9999"}
                  ]
                }
                """);

        assertThat(event.textBody()).isEqualTo("""
                Contato compartilhado:
                Nome: Andrea Silva
                Celular: +55 71 99999-9999""");
        assertThat(event.textBody()).doesNotContain("\r", "\t", "");
        assertThat(event.textBody()).contains("\nNome:", "\nCelular:");
    }

    @Test
    void ignoresWebhookContainingOnlyStatuses() {
        String payload = """
                {
                  "object": "whatsapp_business_account",
                  "entry": [
                    {
                      "id": "WABA_ID",
                      "changes": [
                        {
                          "field": "messages",
                          "value": {
                            "messaging_product": "whatsapp",
                            "statuses": [
                              {
                                "id": "wamid.status-id",
                                "status": "delivered",
                                "timestamp": "178545"
                              }
                            ]
                          }
                        }
                      ]
                    }
                  ]
                }
                """;

        List<InboundEvent> events = parser.parseAll(
                payload.getBytes(StandardCharsets.UTF_8)
        );

        assertThat(events).isEmpty();
    }

    @Test
    void preservesExistingTextMessageBehavior() {
        String payload = webhookWithMessage("""
                {
                  "from": "557591709466",
                  "id": "wamid.original-text-id",
                  "timestamp": "178545",
                  "type": "text",
                  "text": {
                    "body": "Mensagem comum"
                  }
                }
                """);

        List<InboundEvent> events = parser.parseAll(
                payload.getBytes(StandardCharsets.UTF_8)
        );

        assertThat(events).hasSize(1);

        InboundEvent event = events.get(0);
        assertThat(event.waId()).isEqualTo("557591709466");
        assertThat(event.messageId()).isEqualTo("wamid.original-text-id");
        assertThat(event.messageType()).isEqualTo("text");
        assertThat(event.textBody()).isEqualTo("Mensagem comum");
        assertThat(event.mediaId()).isNull();
        assertThat(event.mimeType()).isNull();
        assertThat(event.mediaSha256()).isNull();
    }

    private InboundEvent parseSingleContact(String contactJson) {
        List<InboundEvent> events = parseContacts("[" + contactJson + "]");

        assertThat(events).hasSize(1);
        return events.get(0);
    }

    private List<InboundEvent> parseContacts(String contactsJson) {
        String message = """
                {
                  "from": "557591709466",
                  "id": "%s",
                  "timestamp": "178545",
                  "type": "contacts",
                  "contacts": %s
                }
                """.formatted(MESSAGE_ID, contactsJson);

        String payload = webhookWithMessage(message);
        return parser.parseAll(payload.getBytes(StandardCharsets.UTF_8));
    }

    private String webhookWithMessage(String messageJson) {
        return """
                {
                  "object": "whatsapp_business_account",
                  "entry": [
                    {
                      "id": "WABA_ID",
                      "changes": [
                        {
                          "field": "messages",
                          "value": {
                            "messaging_product": "whatsapp",
                            "metadata": {
                              "display_phone_number": "557100000000",
                              "phone_number_id": "PHONE_NUMBER_ID"
                            },
                            "contacts": [
                              {
                                "profile": {
                                  "name": "Remetente"
                                },
                                "wa_id": "557591709466"
                              }
                            ],
                            "messages": [%s]
                          }
                        }
                      ]
                    }
                  ]
                }
                """.formatted(messageJson);
    }
}
