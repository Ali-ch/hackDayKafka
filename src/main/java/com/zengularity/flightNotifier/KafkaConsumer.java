package com.zengularity.flightNotifier;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaConsumer extends KafkaConfig {

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaConsumer.class.getName());

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String TOPIC = "flight-topic";

  private final ReceiverOptions<Integer, String> receiverOptions;

  public KafkaConsumer(String bootstrapServers) {

    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "flight-consumer");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "flight-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    receiverOptions = ReceiverOptions.create(props);
  }

  public Disposable consumeMessages(String topic, CountDownLatch latch) {

    ReceiverOptions<Integer, String> options = receiverOptions.subscription(Collections.singleton(topic))
        .addAssignListener(partitions -> logger.debug("onPartitionsAssigned {}", partitions))
        .addRevokeListener(partitions -> logger.debug("onPartitionsRevoked {}", partitions));

    Flux<ReceiverRecord<Integer, String>> kafkaFlux = KafkaReceiver.create(options).receive().doOnError(e -> logger.error("receive failed", e));

    return kafkaFlux.subscribe(record -> {
      ReceiverOffset offset = record.receiverOffset();
      System.out.printf("Received message: key=%s value=%s\n",
          record.key(),
          record.value());
      offset.acknowledge();
      latch.countDown();
    });
  }

  public static void main(String[] args) throws Exception {
    int count = 20;
    CountDownLatch latch = new CountDownLatch(count);
    KafkaConsumer consumer = new KafkaConsumer(BOOTSTRAP_SERVERS);
    Disposable disposable = consumer.consumeMessages(TOPIC, latch);
    latch.await(10, TimeUnit.SECONDS);
    disposable.dispose();
  }

}
