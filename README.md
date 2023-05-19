# Message outboxer

Message outboxer is an example of generic implementation of the 
[transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) pattern for Spring applications.

## Usage

#### Enabling
Enable outboxing by annotating your application with `@EnableMessageOutboxing`.

#### Properties
Define following properties:
```sh
message-outboxer.kafka.producer.bootstrapServers=kafkahost:port
message-outboxer.kafka.producer.batchSize=standard-kafka-spring-batch-size
message-outboxer.kafka.producer.lingerMs=standard-kafka-spring-linger-ms
message-outboxer.outboxing.delay-ms=how-often-will-outbox-be-emptied
```

#### Bean configuration
Define outbox configuration for you class (there is integration test with test application that can serve as example
how to do it).

#### Calling outboxer
Now you are ready to wire `MessageOutboxerService` into your  components and call it:
```sh
messageOutboxerService.saveToOutbox(payload);
```
Payload is instance of the class for which outbox configuration was defined.