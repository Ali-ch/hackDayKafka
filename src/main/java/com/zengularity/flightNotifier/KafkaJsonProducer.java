package com.zengularity.flightNotifier;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.HashMap;
import java.util.Map;

public class KafkaJsonProducer {
  private static final Logger logger = LoggerFactory.getLogger(KafkaJsonProducer.class.getName());

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String TOPIC = "flight-topic";

  private final KafkaSender<Integer, String> kafkaSender;


  public KafkaJsonProducer(String bootstrapServers) {

    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    SenderOptions<Integer, String> senderOptions = SenderOptions.create(props);
    kafkaSender = KafkaSender.create(senderOptions);
  }

  public void sendMessages(String topic, String json) throws InterruptedException {

    kafkaSender.send(Flux.just(SenderRecord.create(new ProducerRecord<>(topic, json), 1)))
        .doOnError(e-> logger.error("Send failed", e))
        .subscribe(record -> {
          RecordMetadata metadata = record.recordMetadata();
          System.out.printf("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s\n",
              record.correlationMetadata(),
              metadata.topic(),
              metadata.partition(),
              metadata.offset()
          );
        });
  }

  public void close() {
    kafkaSender.close();
  }

  public static void main(String[] args) throws Exception {
    KafkaJsonProducer producer = new KafkaJsonProducer(BOOTSTRAP_SERVERS);
    producer.sendMessages(TOPIC, "{'Name':'Ali'}");
    producer.close();
  }

}
