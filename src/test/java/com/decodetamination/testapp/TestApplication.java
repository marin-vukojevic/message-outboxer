package com.decodetamination.testapp;

import com.decodetamination.messageoutboxer.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.function.Function;

@EnableMessageOutboxing
@SpringBootApplication
public class TestApplication {

    private static final String TOPIC_NAME = "test_topic";

    @Autowired
    private KafkaProducerProperties kafkaProducerProperties;

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(TOPIC_NAME)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, SomePayload>> testListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, SomePayload> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(kafkaConsumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setPollTimeout(1000);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }

    private ConsumerFactory<String, SomePayload> kafkaConsumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("spring.json.trusted.packages", "*");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    public static record SomePayload(String id, String name) {
    }

    @Component
    @RequiredArgsConstructor
    public static class SomePayloadOutboxingConfiguration implements OutboxingConfiguration<SomePayload, String> {

        private final SomePayloadTopicConfiguration somePayloadTopicConfiguration;

        @Override
        public Class<SomePayload> supports() {
            return SomePayload.class;
        }

        @Override
        public Function<SomePayload, String> keyExtractor() {
            return SomePayload::name;
        }

        @Override
        @SneakyThrows
        public byte[] serializeMessage(SomePayload message) {
            return new ObjectMapper().writeValueAsBytes(message);
        }

        @Override
        @SneakyThrows
        public SomePayload deserializeMessage(byte[] serialized) {
            return new ObjectMapper().readValue(serialized, SomePayload.class);
        }

        @Override
        public TopicConfiguration topicConfiguration() {
            return somePayloadTopicConfiguration;
        }

    }

    @Component
    public static class SomePayloadTopicConfiguration implements TopicConfiguration {

        @Override
        public String topicName() {
            return TOPIC_NAME;
        }

        @Override
        public Class<? extends Serializer> messageValueSerializerClass() {
            return JsonSerializer.class;
        }

        @Override
        public Class<? extends Serializer> messageKeySerializerClass() {
            return StringSerializer.class;
        }

    }

    @Component
    @AllArgsConstructor
    public static class MessageOutboxerCallerService {

        private final MessageOutboxerService messageOutboxerService;

        @Transactional
        public void produceMessage(SomePayload payload) {
            messageOutboxerService.saveToOutbox(payload);
        }
    }

    @Slf4j
    @Component
    @AllArgsConstructor
    public static class MessageListener {

        private final MessageCollector messageCollector;

        @KafkaListener(topics = {TOPIC_NAME}, containerFactory = "testListenerContainerFactory")
        public void process(List<ConsumerRecord<String, SomePayload>> consumerRecords, Acknowledgment acknowledgment) {
            consumerRecords.forEach(it -> messageCollector.add(it.value()));
            acknowledgment.acknowledge();
        }
    }

    @Component
    public static class MessageCollector {

        private final Queue<SomePayload> collected = new ArrayDeque<>();

        public void add(SomePayload message) {
            collected.add(message);
        }

        public Optional<SomePayload> poll() {
            return Optional.ofNullable(collected.poll());
        }
    }
}
