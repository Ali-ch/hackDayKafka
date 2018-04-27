package com.zengularity.flightNotifier;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
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

public class KafkaJsonConsumer {

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaJsonProducer.class.getName());

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String TOPIC = "flight-topic";

  private final ReceiverOptions<Integer, String> receiverOptions;
  /*private final KafkaConsumer<Integer, String> kafkaConsumer;
  private final Map<String, List<PartitionInfo>> topics;*/

  public KafkaJsonConsumer(String bootstrapServers) {

    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    receiverOptions = ReceiverOptions.create(props);
    /*
    kafkaConsumer = new KafkaConsumer(props);
    topics = kafkaConsumer.listTopics();
    */
  }

  public Disposable consumeMessages(String topic) {
    /*
    kafkaConsumer.subscribe(Collections.singleton(topic));
    kafkaConsumer.poll(1000).forEach(record -> {System.out.println(record.value());});
    */
    ReceiverOptions<Integer, String> options = receiverOptions.subscription(Collections.singleton(topic))
        .addAssignListener(partitions -> logger.debug("onPartitionsAssigned {}", partitions))
        .addRevokeListener(partitions -> logger.debug("onPartitionsRevoked {}", partitions));

    Flux<ReceiverRecord<Integer, String>> kafkaFlux = KafkaReceiver.create(options).receive().doOnError(e -> logger.error("receive failed", e));

    return kafkaFlux.subscribe(record -> {
      ReceiverOffset offset = record.receiverOffset();
      System.out.printf("Received message: key=%d value=%s\n",
          record.key(),
          record.value());
      offset.acknowledge();
    });
  }

  public static void main(String[] args) {
    KafkaJsonConsumer consumer = new KafkaJsonConsumer(BOOTSTRAP_SERVERS);
    Disposable disposable = consumer.consumeMessages(TOPIC);
    disposable.dispose();
  }

}
