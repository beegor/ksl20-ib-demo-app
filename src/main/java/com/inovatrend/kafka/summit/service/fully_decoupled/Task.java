package com.inovatrend.kafka.summit.service.fully_decoupled;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inovatrend.kafka.summit.service.RecordProcessingTask;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Task implements Runnable, RecordProcessingTask {

    private static Map<String, String> processedMessages = Collections.synchronizedMap(new HashMap<>());

    private final List<ConsumerRecord<String, String>> records;

    private volatile boolean stopped = false;

    private volatile boolean started = false;

    private volatile boolean finished = false;

    private final CompletableFuture<Long> completion = new CompletableFuture<>();

    private final ReentrantLock startStopLock = new ReentrantLock();

    private final AtomicLong currentOffset = new AtomicLong(-1);

    private int processedRecordsCount = 0;

    private Logger log = LoggerFactory.getLogger(Task.class);

    //*********************************************//
    private String consumerAppId;

    private TopicPartition partition;

    private int singleMsgProcessingDurationMs;
    //*********************************************//

    public Task(List<ConsumerRecord<String, String>> records, String consumerAppId, TopicPartition partition, int singleMsgProcessingDurationMs) {
        this.records = records;
        this.consumerAppId = consumerAppId;
        this.partition = partition;
        this.singleMsgProcessingDurationMs = singleMsgProcessingDurationMs;
    }


    @Override
    public void run() {
        startStopLock.lock();
        if (stopped) {
            return;
        }
        started = true;
        startStopLock.unlock();

        for (ConsumerRecord<String, String> record : records) {
            if (stopped)
                break;
            processRecord(record);
            currentOffset.set(record.offset() + 1);
            processedRecordsCount++;

            String alreadyProcessedByApp = processedMessages.get(record.value());
            if (alreadyProcessedByApp != null)
                log.warn("Duplicate processing detected! First processed by consumer app {}, then by consumer app {}: {}", alreadyProcessedByApp, consumerAppId, record.value());
            processedMessages.put(record.value(), consumerAppId);
            log.info("Total processed messages: {}", processedMessages.size());

        }
        finished = true;
        completion.complete(currentOffset.get());
    }


    private void processRecord(ConsumerRecord<String, String> record) {
        log.debug("Processing record: {}", record);
        try {
            Thread.sleep(singleMsgProcessingDurationMs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public long getCurrentOffset() {
        return currentOffset.get();
    }

    public void stop() {

        startStopLock.lock();
        this.stopped = true;
        if (!started) {
            finished = true;
            completion.complete(-1L);
        }
        startStopLock.unlock();
    }

    public long waitForCompletion() {
        try {
            return completion.get();
        } catch (InterruptedException | ExecutionException e) {
            return -1;
        }
    }

    public boolean isFinished() {
        return finished;
    }


    //*********************************************************//
    public void updateRecordProcessingDuration(int durationMs) {
        this.singleMsgProcessingDurationMs = durationMs;
        log.info("Updated singleMsgProcessingDurationMs to: ", singleMsgProcessingDurationMs);
    }

    public int getTotalRecords() {
        return records.size();
    }

    public int getProcessedRecords() {
        return processedRecordsCount;
    }

    public TopicPartition getTopicPartition() {
        return partition;
    }

    public String getId() {
        return "";
    }


}
