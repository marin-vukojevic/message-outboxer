package com.decodetamination.messageoutboxer;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * Defines outboxing configuration for class T.
 *
 * @param <T> Message type.
 * @param <K> Message key type.
 */
public interface OutboxingConfiguration<T, K> {

    /**
     * Defines class for which configuration is defined.
     *
     * @return Class for which configuration is defined.
     */
    Class<T> supports();

    /**
     * Defines key extractor for class.
     *
     * @return Key extractor for class.
     */
    Function<T, K> keyExtractor();

    /**
     * Serializes message source when storing to db.
     *
     * @param messageSource Message source to be stored in db.
     * @return Serialized message source.
     */
    byte[] serializeMessage(T messageSource);

    /**
     * Deserializes message source when loading from db.
     *
     * @param serialized Serialized message source.
     * @return Deserialized message source.
     */
    T deserializeMessage(byte[] serialized);

    /**
     * Defines kafka topic related configuration like topic name, serializers and deserializers.
     *
     * @return Topic configuration.
     */
    TopicConfiguration topicConfiguration();

    /**
     * Defines custom headers that will be set to kafka message (like type (class) information).
     *
     * @return Custom headers.
     */
    default Map<String, Object> customHeaders() {
        return Collections.emptyMap();
    }

}
