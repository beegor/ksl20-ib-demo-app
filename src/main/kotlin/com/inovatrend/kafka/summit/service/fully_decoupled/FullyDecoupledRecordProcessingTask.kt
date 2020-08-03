package com.inovatrend.kafka.summit.service.fully_decoupled

import com.inovatrend.kafka.summit.service.RecordProcessingTask
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


class FullyDecoupledRecordProcessingTask(private val partition: TopicPartition,
                                         private val records: List<ConsumerRecord<String, String>>,
                                         private var singleMsgProcessingDurationMs: Int
) : Runnable, RecordProcessingTask {

    private val log = LoggerFactory.getLogger(FullyDecoupledRecordProcessingTask::class.java)
    private val currentOffset = AtomicLong(-1)
    private var processedRecordsCount = 0

    @Volatile
    private var stopped = false

    @Volatile
    private var started = false

    @Volatile
    private var finished = false
    private val completion = CompletableFuture<Long>()
    private val myId = UUID.randomUUID().toString()

    private val startStopLock = ReentrantLock()

    override fun run() {

        startStopLock.withLock {
            if (stopped) {
                log.info("Task stopped before processing started: {}  partition: {}", myId, partition)
                return
            }
            started = true
        }

        for (record in records) {
            if (stopped)
                break
            processRecord(record)
            currentOffset.set(record.offset() + 1)
            processedRecordsCount++
        }
        finished = true
        completion.complete(currentOffset.get())
    }


    override fun getCurrentOffset(): Long {
        return currentOffset.get()
    }

    fun stop() {
        log.info("Stopping task: {}  partition: {}", myId, partition)
        startStopLock.withLock {
            this.stopped = true
            if (!started) {
                log.info("Task stopped while in thread pool queue: {}  partition: {}", myId, partition)
                finished = true
                completion.complete(-1)
            }
        }
        stopped = true;
    }

    fun waitForCompletion(): Long {
        return completion.get()
    }

    override fun isFinished(): Boolean {
        return finished
    }

    private fun processRecord(record: ConsumerRecord<String, String>) {
        log.debug("Processing record: {}", record)
        Thread.sleep(singleMsgProcessingDurationMs.toLong())
    }


    override fun updateRecordProcessingDuration(durationMs: Int) {
        this.singleMsgProcessingDurationMs = durationMs
    }

    override fun getTotalRecords() = records.size

    override fun getProcessedRecords() = processedRecordsCount

    override fun getTopicPartition() = partition

    override fun getId() = myId
}
