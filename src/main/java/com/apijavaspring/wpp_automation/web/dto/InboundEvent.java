package com.apijavaspring.wpp_automation.web.dto;

public record InboundEvent(
        String waId,
        String messageId,
        String messageType,
        String textBody,
        String mediaId,
        String mimeType,
        String mediaSha256,
        Long userTimestamp,
        String rawPayloadJson
) {}
