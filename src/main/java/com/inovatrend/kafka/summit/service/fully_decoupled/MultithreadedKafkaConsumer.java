package com.inovatrend.kafka.summit.service.fully_decoupled;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inovatrend.kafka.summit.service.ConsumerApp;
import com.inovatrend.kafka.summit.service.RecordProcessingTask;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultithreadedKafkaConsumer implements Runnable, ConsumerRebalanceListener, ConsumerApp {

    private KafkaConsumer<String, String> consumer;

    private final String topic;

    private ExecutorService executor = Executors.newFixedThreadPool(8);

    private Map<TopicPartition, Task> activeTasks = new HashMap();

    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

    private AtomicBoolean stopped = new AtomicBoolean(false);

    private long lastCommitTime = System.currentTimeMillis();

    private Logger log = LoggerFactory.getLogger(MultithreadedKafkaConsumer.class);

    //*********************************************//
    private int lastPollRecordsCount = 0;

    private int recordProcessingDurationMs = 100;

    private List<LocalDateTime> pollHistory = new ArrayList<>();

    private String consumerAppId;

    private String consumerGroup;
    //*********************************************//

    public MultithreadedKafkaConsumer(String consumerAppId,
                                      String consumerGroup,
                                      String topic,
                                      int recordProcessingDurationMs
    ) {
        this.consumerAppId = consumerAppId;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.recordProcessingDurationMs = recordProcessingDurationMs;
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "multithreaded-consumer-demo");
        consumer = new KafkaConsumer<>(config);
//        new Thread(this).start();
    }


    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singleton(topic), this);
            while (!stopped.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                updatePollMetrics(records.count());
                handleFetchedRecords(records);
                handleActiveTasks();
                commitOffsets();
            }
        } catch (WakeupException we) {
            if (!stopped.get())
                throw we;
        } finally {
            consumer.close();
        }
    }


    private void handleFetchedRecords(ConsumerRecords<String, String> records) {
        if (records.count() > 0) {
            records.partitions().forEach(partition -> {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                Task task = new Task(partitionRecords, consumerAppId, partition, recordProcessingDurationMs);
                executor.submit(task);
                activeTasks.put(partition, task);
            });
            consumer.pause(records.partitions());
        }
    }

    private void commitOffsets() {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastCommitTime > 5000) {
                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitAsync(offsetsToCommit, null);
                    offsetsToCommit.clear();
                }
                lastCommitTime = currentTimeMillis;
            }
        } catch (Exception e) {
            log.error("Failed to commit offsets!", e);
        }
    }


    private void handleActiveTasks() {
        List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
        activeTasks.forEach((partition, task) -> {
            if (task.isFinished())
                finishedTasksPartitions.add(partition);
            long offset = task.getCurrentOffset();
            if (offset > 0)
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });
        finishedTasksPartitions.forEach(partition -> {
            log.info("Removing finished task for partition: {}", partition);
            activeTasks.remove(partition);
        });
        consumer.resume(finishedTasksPartitions);
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        // 1. Stop all tasks handling records from revoked partitions
        Map<TopicPartition, Task> stoppedTask = new HashMap<>();
        for (TopicPartition partition : partitions) {
            Task task = activeTasks.remove(partition);
            if (task != null) {
                task.stop();
                stoppedTask.put(partition, task);
            }
        }

        // 2. Wait for stopped tasks to complete processing of current record
        stoppedTask.forEach((partition, task) -> {
            long offset = task.waitForCompletion();
            if (offset > 0)
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });


        // 3. collect offsets for revoked partitions
        Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
        partitions.forEach(partition -> {
            OffsetAndMetadata offset = offsetsToCommit.remove(partition);
            if (offset != null)
                revokedPartitionOffsets.put(partition, offset);
        });

        // 4. commit offsets for revoked partitions
        try {
            consumer.commitSync(revokedPartitionOffsets);
        } catch (Exception e) {
            log.warn("Failed to commit offsets for revoked partitions!");
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        consumer.resume(partitions);
    }


    //****************************************************************//
    public List<RecordProcessingTask> getActiveWorkers() {
        return new ArrayList<>(activeTasks.values());
    }

    public int getLastPollRecordsCount() {
        return this.lastPollRecordsCount;
    }

    public int getRecordProcessingDuration() {
        return this.recordProcessingDurationMs;
    }

    public void updateRecordProcessingDuration(int durationMs) {
        log.debug("Updating record processing duration: {} ms", durationMs);
        this.recordProcessingDurationMs = durationMs;
        activeTasks.values().forEach(task -> task.updateRecordProcessingDuration(durationMs));
    }

    public List<LocalDateTime> getPollHistory() {
        return new ArrayList<>(pollHistory);
    }

    @Override
    public void startConsuming() {
        new Thread(this).start();
    }

    @Override
    public void stopConsuming() {
//        log.info("Stopping consumer app {}", consumerAppId)
        stopped.set(true);
        consumer.wakeup();
    }

    private void updatePollMetrics(int recordsCount) {
        LocalDateTime now = LocalDateTime.now();
        pollHistory.add(now);
        this.lastPollRecordsCount = recordsCount;
        log.debug("Fetched {} records", lastPollRecordsCount);
    }
}
