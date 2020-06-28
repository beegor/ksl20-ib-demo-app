package com.inovatrend.kafka.summit.service.fork_join;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ForkJoinConsumer implements Runnable {

    private KafkaConsumer<String, String> consumer;

    private ExecutorService executor = Executors.newFixedThreadPool(8);

    private CompletableFuture<Boolean> completion = new CompletableFuture<>();

    private boolean finished = false;

    public ForkJoinConsumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroup");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer = new KafkaConsumer<>(config);
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singleton("topic"));
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
                List<Task> tasks = records
                        .partitions()
                        .stream()
                        .map(partition -> new Task(records.records(partition)))
                        .collect(Collectors.toList());

                executor.invokeAll(tasks);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
