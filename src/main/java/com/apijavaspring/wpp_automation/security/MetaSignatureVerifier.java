package com.apijavaspring.wpp_automation.security;

import com.apijavaspring.wpp_automation.config.MetaWebhookProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

@Component
@RequiredArgsConstructor
public class MetaSignatureVerifier {

    private final MetaWebhookProperties props;

    public boolean isEnabled() {
        return props.signatureEnabled();
    }

    public boolean verify(byte[] rawBody, String header) {
        if (!isEnabled()) return true;

        if (header == null || header.isBlank()) return false;
        if (!header.startsWith("sha256=")) return false;

        String providedHex = header.substring("sha256=".length()).trim();
        String expectedHex = hmacSha256Hex(props.appSecret(), rawBody);

        return constantTimeEqualsIgnoreCase(providedHex, expectedHex);
    }

    private static String hmacSha256Hex(String secret, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] out = mac.doFinal(data);
            return toHex(out);
        } catch (Exception e) {
            return "";
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    private static boolean constantTimeEqualsIgnoreCase(String a, String b) {
        if (a == null || b == null) return false;
        if (a.length() != b.length()) return false;

        int result = 0;
        for (int i = 0; i < a.length(); i++) {
            char ca = Character.toLowerCase(a.charAt(i));
            char cb = Character.toLowerCase(b.charAt(i));
            result |= (ca ^ cb);
        }
        return result == 0;
    }
}
