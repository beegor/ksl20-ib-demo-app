package com.inovatrend.kafka.summit.service.fully_decoupled

import com.inovatrend.kafka.summit.service.ConsumerApp
import com.inovatrend.kafka.summit.service.RecordProcessingTask
import com.inovatrend.kafka.summit.service.RecordProcessingTaskListener
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread


class FullyDecoupledConsumerApp(consumerGroup: String,
                                private val topic: String,
                                private var recordProcessingDurationMs: Int) : ConsumerApp, RecordProcessingTaskListener {

    private val consumer: KafkaConsumer<String, String>
    private val stopped = AtomicBoolean(false)
    private val executor = Executors.newFixedThreadPool(8)
    private val activeWorkers = ConcurrentHashMap<TopicPartition, RecordProcessingTask>()
    private val offsetsToCommit = Collections.synchronizedMap(mutableMapOf<TopicPartition, OffsetAndMetadata>())
    private val partitionsToResume = Collections.synchronizedList(mutableListOf<TopicPartition>())
    private var lastCommitTime = System.currentTimeMillis()
    private var lastPollRecordsCount = 0
    private val pollHistory = mutableListOf<LocalDateTime>()
    private val log = LoggerFactory.getLogger(FullyDecoupledConsumerApp::class.java)

    init {

        val config = Properties()
        config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        config[ConsumerConfig.GROUP_ID_CONFIG] = consumerGroup
        consumer = KafkaConsumer(config)
    }

    override fun startConsuming() {
        thread {
            try {
                consumer.subscribe(Collections.singleton(topic),
                        object : ConsumerRebalanceListener {
                            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) = partitionsAssigned(partitions)
                            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) = partitionsRevoked(partitions)
                        })

                while (!stopped.get()) {
                    val records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS))
                    updatePollMetrics(records.count())
                    handleFetchedRecords(records)
                    commitOffsetsIfTimeHasCome()
                    resumePartitions()
                    Thread.sleep(100)
                }
            } catch (we: WakeupException) {
                if (!stopped.get()) throw we
            } catch (e: Exception) {
                log.error("Failed to consume messages!", e)
            } finally {
                consumer.close()
            }
        }
    }

    /**
     * Called from main consumer thread.
     * Groups records by partitions, creates tasks and submits them to executor
     */
    private fun handleFetchedRecords(records: ConsumerRecords<String, String>) {
        records.partitions().forEach { partition ->
            val partitionRecords = records.records(partition)
            val worker = FullyDecoupledRecordProcessingTask(partition, partitionRecords, recordProcessingDurationMs, this)
            consumer.pause(listOf(partition))
            activeWorkers[partition] = worker
            executor.submit(worker)
        }
    }

    /**
     * Called from main consumer thread.
     * Updates last poll time and number of fetched records to local variables
     * Used for visualisation purposes
     */
    private fun updatePollMetrics(recordsCount: Int) {
        val now = LocalDateTime.now()
        pollHistory.add(now)
        this.lastPollRecordsCount = recordsCount
        log.info("Fetched {} records", lastPollRecordsCount)
    }


    private fun commitOffsetsIfTimeHasCome() {
        val currentTimeMillis = System.currentTimeMillis()
        if (currentTimeMillis - lastCommitTime > 5000) {
            synchronized(offsetsToCommit) {
                consumer.commitSync(offsetsToCommit)
            }
            lastCommitTime = currentTimeMillis
        }
    }

    private fun resumePartitions() {
        synchronized(partitionsToResume) {
            //todo check that and explain in presentation slide
            // there might be partitions that were read from, associated task finished and added partition to  partitionsToResume list,
            // but in the mean time rebalance happened and that partition might not be assigned any more, so we must not resume it
            // that's why we remove all partitions that are not currently assigned
            partitionsToResume.removeIf { !consumer.assignment().contains(it) }

            // now we can resume
            consumer.resume(partitionsToResume)
            partitionsToResume.clear()
        }
    }

    override fun singleRecordProcessingFinished(topicPartition: TopicPartition, offset: Long) {
        log.info("Single record from partition {} processing finished, offset: {}", topicPartition, offset)
        offsetsToCommit[topicPartition] = OffsetAndMetadata(offset)
    }

    override fun taskFinished(topicPartition: TopicPartition, offset: Long) {
        log.info("Task for partition {} finished", topicPartition)
        offsetsToCommit[topicPartition] = OffsetAndMetadata(offset)
        partitionsToResume.add(topicPartition)
        activeWorkers.remove(topicPartition)
    }

    override fun stopConsuming() {
        log.info("Stopping consumer app!")
        stopped.set(true)
        consumer.wakeup()
    }

    private fun partitionsRevoked(partitions: Collection<TopicPartition>) {

        log.info("Partitions revoked: {}", partitions)

        val revokedPartitionOffsets = mutableMapOf<TopicPartition, OffsetAndMetadata>()

        for (partition in activeWorkers.keys()) {
            val task = activeWorkers[partition]
            task?.stop()
            val offset = offsetsToCommit.remove(partition)
            if (offset != null)
                revokedPartitionOffsets[partition] = offset
        }

        try {
            consumer.commitSync(revokedPartitionOffsets)
        } catch (e: Exception) {
            // todo Offsets commit failed! Is there something I can do about it? Nothing, I guess.
            log.warn("Failed to commit offsets for revoked partitions!")
        }
    }

    private fun partitionsAssigned(partitions: Collection<TopicPartition>) {
        log.info("Partitions assigned: {}", partitions)
    }

    override fun getActiveWorkers() = activeWorkers.values.toList()

    override fun getLastPollRecordsCount() = this.lastPollRecordsCount

    override fun getRecordProcessingDuration() = this.recordProcessingDurationMs

    override fun updateRecordProcessingDuration(durationMs: Int) {
        log.info("Updating record processing duration: {} ms", durationMs)
        this.recordProcessingDurationMs = durationMs
        activeWorkers.values.forEach { it.updateRecordProcessingDuration(durationMs) }
    }

    override fun getPollHistory() = pollHistory.toList()


}
