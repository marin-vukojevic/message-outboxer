package com.decodetamination.messageoutboxer;

public class KafkaTemplateNotFoundException extends RuntimeException {

    public KafkaTemplateNotFoundException(String topic) {
        super("No kafka template found for " + topic);
    }
}
