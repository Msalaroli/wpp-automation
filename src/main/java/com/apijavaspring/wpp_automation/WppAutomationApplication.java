package com.apijavaspring.wpp_automation;

import com.apijavaspring.wpp_automation.config.WppAutomationProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.apijavaspring.wpp_automation.config.MetaWebhookProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableConfigurationProperties({
		MetaWebhookProperties.class,
		WppAutomationProperties.class
})
@SpringBootApplication
public class WppAutomationApplication {

	public static void main(String[] args) {
		SpringApplication.run(WppAutomationApplication.class, args);
	}

}
