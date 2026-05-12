package com.apijavaspring.wpp_automation.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.meta")
public record MetaWebhookProperties(
        String verifyToken,
        String appSecret,
        boolean signatureEnabled
) {
}
