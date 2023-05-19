package com.decodetamination.messageoutboxer;

import lombok.Value;

@Value
public class Message<T> {

    Long id;
    Class<T> clazz;
    String topic;
    byte[] serialized;
}
