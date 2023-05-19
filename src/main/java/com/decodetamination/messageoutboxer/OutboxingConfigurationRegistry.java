package com.decodetamination.messageoutboxer;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class OutboxingConfigurationRegistry {

    private final Map<Class, OutboxingConfiguration> outboxingConfigurations;

    public OutboxingConfigurationRegistry(List<OutboxingConfiguration> outboxingConfigurations) {
        this.outboxingConfigurations = outboxingConfigurations.stream()
                .collect(Collectors.toMap(OutboxingConfiguration::supports, Function.identity()));
    }

    public <T> OutboxingConfiguration get(Class<T> clazz) {
        return Optional.ofNullable(outboxingConfigurations.get(clazz))
                .orElseThrow(() -> new OutboxingConfigurationNotFoundException(clazz));
    }
}
