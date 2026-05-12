package com.apijavaspring.wpp_automation.core;

import com.apijavaspring.wpp_automation.web.dto.InboundEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

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

                        if ("text".equals(type)) {
                            textBody = msg.path("text").path("body").asText(null);
                        } else if ("image".equals(type) || "document".equals(type) || "video".equals(type) || "audio".equals(type)) {
                            JsonNode mediaNode = msg.path(type);
                            mediaId = mediaNode.path("id").asText(null);
                            mimeType = mediaNode.path("mime_type").asText(null);
                            mediaSha256 = mediaNode.path("sha256").asText(null);
                        } else {
                            // tipos não suportados: ignora (mas não quebra)
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
                    }
                }
            }

            return out;
        } catch (Exception e) {
            return out;
        }
    }

    private static String onlyDigits(String s) {
        return s == null ? null : s.replaceAll("\\D+", "");
    }

    private static String firstNonNull(String a, String b) {
        return a != null ? a : b;
    }
}
