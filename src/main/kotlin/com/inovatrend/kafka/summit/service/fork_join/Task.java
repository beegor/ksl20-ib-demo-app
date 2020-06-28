package com.inovatrend.kafka.summit.service.fork_join;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.concurrent.Callable;

public class Task implements Callable<Boolean> {

    private final List<ConsumerRecord<String, String>> records;

    public Task(List<ConsumerRecord<String, String>> records) {
        this.records = records;
    }

    public Boolean call() {
        for (ConsumerRecord<String, String> record : records) {
            processRecord(record);
        }
        return true;
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        // do something with record
    }
}
