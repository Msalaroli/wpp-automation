package com.apijavaspring.wpp_automation.core;

import com.apijavaspring.wpp_automation.web.dto.InboundEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

@Slf4j
@Component
public class WppPayloadParser {

    private final ObjectMapper om = new ObjectMapper();

    public List<InboundEvent> parseAll(byte[] rawBody) {
        List<InboundEvent> out = new ArrayList<>();
        try {
            JsonNode root = om.readTree(rawBody);

            JsonNode entries = root.path("entry");
            if (!entries.isArray()) return out;

            for (JsonNode entry : entries) {
                JsonNode changes = entry.path("changes");
                if (!changes.isArray()) continue;

                for (JsonNode change : changes) {
                    String field = change.path("field").asText("");
                    if (!"messages".equals(field)) continue;

                    JsonNode value = change.path("value");
                    if (value.isMissingNode() || value.isNull()) continue;

                    String waFromContacts = value.path("contacts").path(0).path("wa_id").asText(null);

                    JsonNode messages = value.path("messages");
                    if (!messages.isArray()) continue;

                    for (JsonNode msg : messages) {
                        String messageId = msg.path("id").asText(null);
                        String type = msg.path("type").asText(null);
                        String from = msg.path("from").asText(null);

                        if (messageId == null || type == null) continue;

                        String waId = firstNonNull(from, waFromContacts);
                        if (waId == null) continue;

                        String normalizedWaId = onlyDigits(waId);

                        String textBody = null;
                        String mediaId = null;
                        String mimeType = null;
                        String mediaSha256 = null;
                        int sharedContacts = 0;

                        if ("text".equals(type)) {
                            textBody = msg.path("text").path("body").asText(null);
                        } else if ("image".equals(type) || "document".equals(type) || "video".equals(type) || "audio".equals(type)) {
                            JsonNode mediaNode = msg.path(type);
                            mediaId = mediaNode.path("id").asText(null);
                            mimeType = mediaNode.path("mime_type").asText(null);
                            mediaSha256 = mediaNode.path("sha256").asText(null);
                        } else if ("contacts".equals(type)) {
                            JsonNode sharedContactsNode = msg.path("contacts");
                            sharedContacts = sharedContactsNode.isArray()
                                    ? sharedContactsNode.size()
                                    : 0;
                            textBody = formatSharedContacts(sharedContactsNode);
                        } else {
                            log.warn("WPP unsupported message type={}", type);
                            continue;
                        }

                        Long ts = null;
                        String tsStr = msg.path("timestamp").asText(null);
                        if (tsStr != null && !tsStr.isBlank()) {
                            try {
                                ts = Long.parseLong(tsStr);
                            } catch (NumberFormatException ignored) {}
                        }

                        out.add(new InboundEvent(
                                normalizedWaId,
                                messageId,
                                type,
                                textBody,
                                mediaId,
                                mimeType,
                                mediaSha256,
                                ts,
                                root.toString()
                        ));

                        if ("contacts".equals(type)) {
                            log.info(
                                    "WPP message parsed type=contacts sharedContacts={}",
                                    sharedContacts
                            );
                        }
                    }
                }
            }

            return out;
        } catch (Exception e) {
            return out;
        }
    }

    private static String formatSharedContacts(JsonNode contacts) {
        int contactCount = contacts.isArray() ? contacts.size() : 0;
        boolean multipleContacts = contactCount > 1;

        StringBuilder text = new StringBuilder(
                multipleContacts
                        ? "Contatos compartilhados:"
                        : "Contato compartilhado:"
        );

        if (contactCount == 0) {
            return text
                    .append("\nNome: Não informado")
                    .append("\nCelular: Não informado")
                    .toString();
        }

        for (int contactIndex = 0; contactIndex < contactCount; contactIndex++) {
            JsonNode contact = contacts.path(contactIndex);

            if (multipleContacts) {
                text.append("\n\nContato ")
                        .append(contactIndex + 1)
                        .append(":\n");
            } else {
                text.append("\n");
            }

            text.append("Nome: ")
                    .append(extractContactName(contact));

            List<String> phones = extractContactPhones(contact);
            if (phones.isEmpty()) {
                text.append("\nCelular: Não informado");
            } else if (phones.size() == 1) {
                text.append("\nCelular: ")
                        .append(phones.get(0));
            } else {
                text.append("\nCelulares:");
                for (String phone : phones) {
                    text.append("\n- ").append(phone);
                }
            }
        }

        return text.toString();
    }

    private static String extractContactName(JsonNode contact) {
        JsonNode name = contact.path("name");
        String formattedName = sanitizeExtractedValue(
                name.path("formatted_name").asText(null)
        );

        if (notBlank(formattedName)) {
            return formattedName;
        }

        StringJoiner assembledName = new StringJoiner(" ");
        addIfPresent(
                assembledName,
                sanitizeExtractedValue(name.path("first_name").asText(null))
        );
        addIfPresent(
                assembledName,
                sanitizeExtractedValue(name.path("middle_name").asText(null))
        );
        addIfPresent(
                assembledName,
                sanitizeExtractedValue(name.path("last_name").asText(null))
        );

        String fallbackName = assembledName.toString();
        return notBlank(fallbackName) ? fallbackName : "Não informado";
    }

    private static List<String> extractContactPhones(JsonNode contact) {
        JsonNode phones = contact.path("phones");
        if (!phones.isArray()) {
            return List.of();
        }

        List<String> extractedPhones = new ArrayList<>();

        for (JsonNode phone : phones) {
            String number = sanitizeExtractedValue(
                    phone.path("phone").asText(null)
            );

            if (!notBlank(number)) {
                number = sanitizeExtractedValue(
                        phone.path("wa_id").asText(null)
                );
            }

            if (notBlank(number)) {
                extractedPhones.add(number);
            }
        }

        return extractedPhones;
    }

    private static String sanitizeExtractedValue(String value) {
        if (value == null) {
            return null;
        }

        StringBuilder sanitized = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            sanitized.append(Character.isISOControl(current) ? ' ' : current);
        }

        return sanitized.toString()
                .replaceAll("\\s+", " ")
                .trim();
    }

    private static void addIfPresent(StringJoiner joiner, String value) {
        if (notBlank(value)) {
            joiner.add(value);
        }
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static String onlyDigits(String s) {
        return s == null ? null : s.replaceAll("\\D+", "");
    }

    private static String firstNonNull(String a, String b) {
        return a != null ? a : b;
    }
}
