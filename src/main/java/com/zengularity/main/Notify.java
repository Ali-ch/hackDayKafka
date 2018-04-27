package com.zengularity.main;

import com.zengularity.consumer.ApiConsumer;
import com.zengularity.flightNotifier.KafkaProducer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Notify {

  private final static KafkaProducer producer = new KafkaProducer();

  public static void main(String[] args) {

    final Disposable disposable = Flux.interval(Duration.ofSeconds(30)).
        flatMap((i) -> Mono.fromCallable(() -> ApiConsumer.consumeApi())
            .subscribeOn(Schedulers.elastic()))
        .subscribeOn(Schedulers.immediate())
        .subscribe(t -> pushToKafka(t));

    while (!disposable.isDisposed()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }

  private static void pushToKafka(List<Map> quote) {
    quote.stream().forEach((e) -> {
      try {
        // publish to Kafka
        producer.sendMessages("flight-topic", e.toString());
        Thread.sleep(1000);

      } catch (Exception e1) {
        e1.printStackTrace();
      }
    });
  }

}
