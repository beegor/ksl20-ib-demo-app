package com.inovatrend.kafka.summit.service.fully_decoupled

import com.inovatrend.kafka.summit.service.RecordProcessingTask
import com.inovatrend.kafka.summit.service.RecordProcessingTaskListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong

class FullyDecoupledRecordProcessingTask(private val partition: TopicPartition,
                                         private val records: List<ConsumerRecord<String, String>>,
                                         var singleMsgProcessingDurationMs: Int,
                                         val listener: RecordProcessingTaskListener) : Runnable, RecordProcessingTask {

    private val log = LoggerFactory.getLogger(FullyDecoupledRecordProcessingTask::class.java)
    private val currentOffset = AtomicLong()
    private var processedRecordsCount = 0
    private var stopped = false
    private val completition = CompletableFuture<Boolean>()


    override fun run() {

        for (record in records) {
            if (stopped)
                break
            processRecord(record)
            currentOffset.set(record.offset() + 1)
            processedRecordsCount++
            listener.singleRecordProcessingFinished(partition, currentOffset.get())
        }
        listener.taskFinished(partition, currentOffset.get())
        completition.complete(true)
    }

    override fun stop() {
        this.stopped = true
        completition.get()
    }


    private fun processRecord(record: ConsumerRecord<String, String>) {
        log.info("Processing record: {}", record)
        Thread.sleep(singleMsgProcessingDurationMs.toLong())
    }


    override fun updateRecordProcessingDuration(durationMs: Int) {
        this.singleMsgProcessingDurationMs = durationMs
    }

    override fun getTotalRecords() = records.size

    override fun getProcessedRecords() = processedRecordsCount

    override fun getTopicPartition() = partition

}
