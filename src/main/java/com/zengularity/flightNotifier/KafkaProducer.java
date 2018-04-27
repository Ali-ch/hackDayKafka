package com.zengularity.flightNotifier;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

public class KafkaProducer {

  private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class.getName());

  private static final String TOPIC = "flight-topic";

  private final KafkaSender<String, String> kafkaSender;

  public KafkaProducer() {
    kafkaSender = KafkaConfig.kafkaSender();
  }

  public void sendMessages(String topic, String json) throws InterruptedException {

    kafkaSender.send(Flux.just(SenderRecord.create(new ProducerRecord<>(topic, "FlightNumber", json), 1)))
        .doOnError(e-> logger.error("Send failed", e))
        .subscribe(record -> {
          RecordMetadata metadata = record.recordMetadata();
          System.out.printf("Message %d sent successfully, topic-partition=%s-%d offset=%d\n",
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
    KafkaProducer producer = new KafkaProducer();
    producer.sendMessages(TOPIC, "{'Name':'Ali'}");
    producer.close();
  }

}
