package com.decodetamination.messageoutboxer;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Defines kafka topic related configuration like topic name, serializers and deserializers.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public interface TopicConfiguration {

    /**
     * Defines topic name.
     *
     * @return Topic name.
     */
    String topicName();

    /**
     * Defines message key serializer class.
     *
     * @return Message key serializer class
     */
    Class<? extends Serializer> messageKeySerializerClass();

    /**
     * Defines message value serializer class.
     *
     * @return Message value serializer class
     */
    Class<? extends Serializer> messageValueSerializerClass();

    /**
     * Hook to provide custom producer properties (like schema registry).
     *
     * @return Producer properties customizer.
     */
    default Consumer<Map<String, Object>> producerPropertiesCustomizer() {
        return ignore -> {
        };
    }
}
