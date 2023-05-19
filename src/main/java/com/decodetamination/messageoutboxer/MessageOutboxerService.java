package com.decodetamination.messageoutboxer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageOutboxerService {

    private final MessageRepository messageRepository;
    private final OutboxingConfigurationRegistry outboxingConfigurationRegistry;
    private final KafkaTemplateRegistry kafkaTemplateRegistry;

    @Transactional(propagation = Propagation.MANDATORY)
    public <T> void saveToOutbox(Collection<T> message) {
        message.forEach(this::saveToOutbox);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public <T> void saveToOutbox(T payload) {
        OutboxingConfiguration<T, ?> outboxingConfiguration = outboxingConfigurationRegistry.get(payload.getClass());
        Message<T> message = toMessage(payload, outboxingConfiguration);
        messageRepository.save(message);
    }

    private <T> Message<T> toMessage(T payload, OutboxingConfiguration<T, ?> outboxingConfiguration) {
        return new Message(
                null,
                payload.getClass(),
                outboxingConfiguration.topicConfiguration().topicName(),
                outboxingConfiguration.serializeMessage(payload));
    }

    @Scheduled(fixedDelayString = "${message-outboxer.outboxing.delay-ms}")
    @SchedulerLock(name = "MessageOutboxerService_scheduledSendFromMessageOutbox", lockAtMostFor = "PT10S")
    public void scheduledSendFromMessageOutbox() {
        List<Message<?>> messages = messageRepository.getAll();
        log.debug("Found {} messages in message outbox, sending...", messages.size());

        messages.forEach(this::sendMessage);
    }

    @SneakyThrows
    private void sendMessage(Message<?> message) {
        KafkaTemplate<?, ?> kafkaTemplate = kafkaTemplateRegistry.getTemplate(message.getTopic());

        Object deserialized = getMessageSource(message);
        org.springframework.messaging.Message<?> kafkaMessage = createMessage(message, deserialized);

        kafkaTemplate.send(kafkaMessage).whenComplete((result, exception) -> {
            if (exception == null) {
                messageRepository.delete(message);
            }
        });
    }

    private Object getMessageSource(Message<?> message) {
        OutboxingConfiguration<?, ?> outboxingConfiguration = outboxingConfigurationRegistry.get(message.getClazz());
        return outboxingConfiguration.deserializeMessage(message.getSerialized());
    }

    private org.springframework.messaging.Message<?> createMessage(Message<?> message, Object deserialized) {

        MessageBuilder<Object> messageBuilder = createMessageBuilder(message, deserialized);
        setCustomHeaders(messageBuilder, outboxingConfigurationRegistry.get(message.getClazz()));
        return messageBuilder.build();
    }

    private MessageBuilder<Object> createMessageBuilder(Message<?> message, Object deserialized) {

        return MessageBuilder
                .withPayload(deserialized)
                .setHeader(
                        KafkaHeaders.KEY,
                        outboxingConfigurationRegistry.get(message.getClazz()).keyExtractor().apply(deserialized))
                .setHeader(KafkaHeaders.TOPIC, message.getTopic());
    }

    private void setCustomHeaders(MessageBuilder<Object> messageBuilder,
                                  OutboxingConfiguration<?, ?> outboxingConfiguration) {

        outboxingConfiguration.customHeaders().forEach(messageBuilder::setHeader);
    }

}
