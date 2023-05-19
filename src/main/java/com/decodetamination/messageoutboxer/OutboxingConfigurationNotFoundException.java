package com.decodetamination.messageoutboxer;

public class OutboxingConfigurationNotFoundException extends RuntimeException {

    public OutboxingConfigurationNotFoundException(Class<?> clazz) {
        super("No outboxing configuration found for " + clazz.getName());
    }
}
