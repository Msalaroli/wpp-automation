package com.apijavaspring.wpp_automation.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@ConfigurationProperties(prefix = "app")
public class WppAutomationProperties {

    private Commercial commercial = new Commercial();
    private Chatwoot chatwoot = new Chatwoot();

    public static class Commercial {

        private List<String> allowedNumbers = new ArrayList<>();

        public List<String> getAllowedNumbers() {
            return allowedNumbers;
        }

        public void setAllowedNumbers(List<String> allowedNumbers) {
            this.allowedNumbers = allowedNumbers;
        }
    }

    public static class Chatwoot {

        private boolean mirrorInboundEnabled = true;

        public boolean isMirrorInboundEnabled() {
            return mirrorInboundEnabled;
        }

        public void setMirrorInboundEnabled(boolean mirrorInboundEnabled) {
            this.mirrorInboundEnabled = mirrorInboundEnabled;
        }
    }
}
