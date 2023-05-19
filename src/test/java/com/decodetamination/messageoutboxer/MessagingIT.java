package com.decodetamination.messageoutboxer;

import com.decodetamination.testapp.TestApplication;
import com.decodetamination.testapp.TestApplication.MessageCollector;
import com.decodetamination.testapp.TestApplication.MessageOutboxerCallerService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest(
        classes = TestApplication.class,
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "message-outboxer.kafka.producer.bootstrapServers=${spring.embedded.kafka.brokers}",
                "message-outboxer.kafka.producer.batchSize=1000",
                "message-outboxer.kafka.producer.lingerMs=1000",
                "message-outboxer.outboxing.delay-ms=1000",
                "spring.datasource.driverClassName=org.testcontainers.jdbc.ContainerDatabaseDriver",
                "spring.datasource.url=jdbc:tc:postgresql:15.3:////test-app?serverTimezone=UTC&TC_REUSABLE=true",
                "spring.kafka.consumer.properties.spring.json.trusted.packages=*"
        })
@EmbeddedKafka(partitions = 1)
@Testcontainers
public class MessagingIT {

    @Autowired
    private MessageCollector messageCollector;

    @Autowired
    private MessageOutboxerCallerService messageOutboxerCallerService;

    @Autowired
    private KafkaTemplateRegistry kafkaTemplateRegistry;

    @Autowired
    private OutboxingConfigurationRegistry outboxingConfigurationRegistry;

    @Test
    public void messageIsOutboxed() {
        TestApplication.SomePayload payload = new TestApplication.SomePayload(UUID.randomUUID().toString(), "name");

        messageOutboxerCallerService.produceMessage(payload);

        await().atMost(5L, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() ->
                        assertThat(messageCollector.poll())
                                .isNotEmpty()
                                .get()
                                .matches(payload::equals));
    }

    @Test
    public void unknownTopicThrowsKafkaTemplateNotFoundException() {
        assertThatThrownBy(() -> kafkaTemplateRegistry.getTemplate(RandomStringUtils.random(10)))
                .isInstanceOf(KafkaTemplateNotFoundException.class);
    }

    @Test
    public void unknownMessageSourceThrowsOutboxingConfigurationNotFoundException() {
        assertThatThrownBy(() -> outboxingConfigurationRegistry.get(RandomMessage.class))
                .isInstanceOf(OutboxingConfigurationNotFoundException.class);
    }

    public static class RandomMessage {
    }
}
