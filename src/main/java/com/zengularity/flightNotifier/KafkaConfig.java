package com.zengularity.flightNotifier;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KafkaConfig {

  public static KafkaSender<String, String> kafkaSender() {

    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "flight-producer");
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    SenderOptions<String, String> senderOptions = SenderOptions.create(props);

    return KafkaSender.create(senderOptions);
  }

  public static Flux<ReceiverRecord<String, String>> kafkaReceiver() {

    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "flight-consumer");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "flight-group");

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(props).subscription(Collections.singletonList("test"));

    return Flux.defer(KafkaReceiver.create(receiverOptions)::receive);
  }

}