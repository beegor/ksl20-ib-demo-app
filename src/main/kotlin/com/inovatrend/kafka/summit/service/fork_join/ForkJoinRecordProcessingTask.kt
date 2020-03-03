package com.inovatrend.kafka.summit.service.fork_join

import com.inovatrend.kafka.summit.service.RecordProcessingTask
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicLong

class ForkJoinRecordProcessingTask(private val partition: TopicPartition,
                                   private val records: List<ConsumerRecord<String, String>>,
                                   var singleMsgProcessingDurationMs: Int) : Callable<Int>, RecordProcessingTask {

    private val log = LoggerFactory.getLogger(ForkJoinRecordProcessingTask::class.java)
    private val currentOffset = AtomicLong()
    private var processedRecordsCount = 0
    private var stopped = false


    override fun call(): Int {
        for (record in records) {
            if (stopped) break
            processRecord(record)
            currentOffset.set(record.offset())
            processedRecordsCount++
        }
        return processedRecordsCount
    }


    private fun processRecord(record: ConsumerRecord<String, String>) {
        log.info("Processing record: {}", record)
        Thread.sleep(singleMsgProcessingDurationMs.toLong())
    }

    override fun stop() {
        stopped = true
    }

    override fun updateRecordProcessingDuration(durationMs: Int) {
        this.singleMsgProcessingDurationMs = durationMs
    }

    override fun getTotalRecords() = records.size

    override fun getProcessedRecords() = processedRecordsCount

    override fun getTopicPartition() = partition

}
