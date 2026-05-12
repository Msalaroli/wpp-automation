package com.apijavaspring.wpp_automation.web;

import com.apijavaspring.wpp_automation.core.WppPayloadParser;
import com.apijavaspring.wpp_automation.persistence.InboxRepository;
import com.apijavaspring.wpp_automation.web.dto.InboundEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.apijavaspring.wpp_automation.security.MetaSignatureVerifier;
import org.springframework.http.HttpStatus;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/webhooks/whatsapp")
public class WhatsAppWebhookController {

    private final WppPayloadParser parser;
    private final InboxRepository inboxRepository;
    private final MetaSignatureVerifier signatureVerifier;


    @Value("${app.meta.verify-token}")
    private String verifyToken;

    @GetMapping(produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> verify(
            @RequestParam(name = "hub.mode", required = false) String mode,
            @RequestParam(name = "hub.verify_token", required = false) String token,
            @RequestParam(name = "hub.challenge", required = false) String challenge
    ) {
        log.info("WPP webhook GET verify: mode={} tokenPresent={} tokenMatches={} challengePresent={}",
                mode,
                token != null && !token.isBlank(),
                token != null && verifyToken.equals(token),
                challenge != null && !challenge.isBlank()
        );

        if ("subscribe".equals(mode) && verifyToken.equals(token) && challenge != null) {
            return ResponseEntity.ok(challenge);
        }
        return ResponseEntity.status(403).body("forbidden");
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> inbound(
            @RequestHeader(name = "X-Hub-Signature-256", required = false) String sig,
            @RequestBody byte[] rawBody
    ) {
        int bytes = rawBody == null ? 0 : rawBody.length;

        log.info(
                "WPP webhook POST received bytes={} signaturePresent={} signatureEnabled={}",
                bytes,
                sig != null,
                signatureVerifier.isEnabled()
        );

        if (!signatureVerifier.verify(rawBody, sig)) {
            log.warn("WPP webhook POST invalid signature");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("invalid signature");
        }

        var events = parser.parseAll(rawBody);
        log.info("WPP webhook POST parsedEvents={}", events.size());

        if (events.isEmpty()) {
            log.info("WPP webhook POST ignored (no events)");
            return ResponseEntity.ok("ignored");
        }

        for (var e : events) {
            inboxRepository.saveInboxAndEnqueueJob(e);
        }

        log.info("WPP webhook POST processed events={}", events.size());
        return ResponseEntity.ok("ok");
    }

}
