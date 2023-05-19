package com.decodetamination.messageoutboxer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class KafkaTemplateRegistry {

    private final KafkaProducerProperties kafkaProducerProperties;
    private final Map<String, KafkaTemplate<?, ?>> kafkaTemplates = new HashMap<>();

    public KafkaTemplateRegistry(KafkaProducerProperties kafkaProducerProperties,
                                 List<OutboxingConfiguration> outboxingConfigurations) {

        this.kafkaProducerProperties = kafkaProducerProperties;
        outboxingConfigurations.stream()
                .map(OutboxingConfiguration::topicConfiguration)
                .forEach(topicConfiguration -> kafkaTemplates.computeIfAbsent(
                        topicConfiguration.topicName(),
                        it -> new KafkaTemplate<>(createProducerFactory(topicConfiguration))));
    }

    public KafkaTemplate<?, ?> getTemplate(String topic) {
        return Optional.ofNullable(kafkaTemplates.get(topic))
                .orElseThrow(() -> new KafkaTemplateNotFoundException(topic));
    }

    private ProducerFactory<?, ?> createProducerFactory(TopicConfiguration topicConfiguration) {

        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerProperties.getBootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, topicConfiguration.messageKeySerializerClass());
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, topicConfiguration.messageValueSerializerClass());
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProducerProperties.getBatchSize());
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProducerProperties.getLingerMs());
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        topicConfiguration.producerPropertiesCustomizer().accept(configProps);

        return new DefaultKafkaProducerFactory<>(configProps);
    }
}
