package com.decodetamination.messageoutboxer;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties("message-outboxer.kafka.producer")
public class KafkaProducerProperties {

    private String bootstrapServers;
    private int batchSize;
    private int lingerMs;
}
