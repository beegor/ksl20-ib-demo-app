package com.inovatrend.kafka.summit.service.fully_decoupled

import com.inovatrend.kafka.summit.service.ConsumerApp
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread


class FullyDecoupledConsumerApp(private val consumerAppId: String,
                                consumerGroup: String,
                                private val topic: String,
                                private var recordProcessingDurationMs: Int) : ConsumerApp {

    private val consumer: KafkaConsumer<String, String>
    private val stopped = AtomicBoolean(false)
    private val executor = Executors.newFixedThreadPool(8)

    private val activeWorkers = mutableMapOf<TopicPartition, FullyDecoupledRecordProcessingTask>()
    private val offsetsToCommit = mutableMapOf<TopicPartition, OffsetAndMetadata>()


    private var lastCommitTime = System.currentTimeMillis()
    private var lastPollRecordsCount = 0
    private val pollHistory = mutableListOf<LocalDateTime>()
    private val log = LoggerFactory.getLogger("FullyDecoupledConsumerApp-$consumerAppId")


    init {

        val config = Properties()
        config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        config[ConsumerConfig.GROUP_ID_CONFIG] = consumerGroup
        config[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 100
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
                    val records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS))
                    updatePollMetrics(records.count())
                    handleFetchedRecords(records)
                    commitOffsetsIfTimeHasCome()
                    handleActiveTasks()
                    Thread.sleep(5)
                }
            } catch (we: WakeupException) {
                if (!stopped.get())
                    throw we
            } finally {
                //handleShutdown()
                consumer.close()
            }
        }
    }


    /**
     * Called from main consumer thread.
     * Groups records by partitions, creates tasks and submits them to executor
     */
    private fun handleFetchedRecords(records: ConsumerRecords<String, String>) {
        if (records.count() > 0) {
            val partitionsToPause = mutableListOf<TopicPartition>()
            log.info("Handling fetched records")
            records.partitions().forEach { partition ->
                val partitionRecords = records.records(partition)
                val worker = FullyDecoupledRecordProcessingTask(partition, partitionRecords, recordProcessingDurationMs)
                partitionsToPause.add(partition)
                executor.submit(worker)
                activeWorkers[partition] = worker
                log.info("Task {} submitted for partition {} by consumer app {}", worker.getId(), partition, consumerAppId)
            }
            consumer.pause(partitionsToPause)
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
        log.debug("Fetched {} records", lastPollRecordsCount)
    }


    override fun stopConsuming() {
        log.info("Stopping consumer app {}", consumerAppId)
        stopped.set(true)
        consumer.wakeup()
    }


    private fun commitOffsetsIfTimeHasCome() {
        try {
            val currentTimeMillis = System.currentTimeMillis()
            if (currentTimeMillis - lastCommitTime > 5000) {
                if (offsetsToCommit.isNotEmpty()) {
                    consumer.commitSync(offsetsToCommit)
                    log.info("Committed offsets periodically: {}", offsetsToCommit.keys)
                    offsetsToCommit.clear()
                }
                lastCommitTime = currentTimeMillis
            }
        } catch (e: Exception) {
            log.error("Failed to commit offsets!", e)
        }
    }


    private fun handleActiveTasks() {
        val finishedTasksPartitions = mutableListOf<TopicPartition>()
        activeWorkers.forEach { (partition, task) ->
            if (task.isFinished())
                finishedTasksPartitions.add(partition)
            val offset = task.getCurrentOffset()
            if (offset > 0)
                offsetsToCommit[partition] = OffsetAndMetadata(offset)
        }
        finishedTasksPartitions.forEach { activeWorkers.remove(it) }
        consumer.resume(finishedTasksPartitions)
    }


    private fun partitionsRevoked(partitions: Collection<TopicPartition>) {

        log.info("Partitions revoked: {}", partitions)
        val stoppedTask = mutableMapOf<TopicPartition, FullyDecoupledRecordProcessingTask>()

        for (partition in partitions) {
            val task = activeWorkers.remove(partition)
            if (task != null) {
                log.info("Stopping task {} for partition {}", task.getId(), task.getTopicPartition())
                task.stop()
                stoppedTask[partition] = task
            }
        }

        stoppedTask.forEach { (partition, task) ->
            val offset = task.waitForCompletion()
            log.info("Task {} / {} completed with offset: {}", task.getId(), task.getTopicPartition(), offset)
            if (offset > 0)
                offsetsToCommit[partition] = OffsetAndMetadata(offset)
        }


        val revokedPartitionOffsets = mutableMapOf<TopicPartition, OffsetAndMetadata>()
        for (partition in partitions) {
            val offset = offsetsToCommit.remove(partition)
            if (offset != null)
                revokedPartitionOffsets[partition] = offset
        }
        try {
            log.info("Committing offsets on revoke: {}", revokedPartitionOffsets)
            consumer.commitSync(revokedPartitionOffsets)
        } catch (e: Exception) {
            // todo Offsets commit failed! Is there something I can do about it? Nothing, I guess.
            log.warn("Failed to commit offsets for revoked partitions!")
        }
    }

    private fun partitionsAssigned(partitions: Collection<TopicPartition>) {
        log.info("Partitions assigned: {}", partitions)
        // because  some partitions might be paused some time before then revoked by previous rebalance, so they never been resumed on consumer.
        // now they are assigned again but they are still paused, so we need  to make sure we resume them
        consumer.resume(partitions)
    }

    override fun getActiveWorkers() = activeWorkers.values.toList()

    override fun getLastPollRecordsCount() = this.lastPollRecordsCount

    override fun getRecordProcessingDuration() = this.recordProcessingDurationMs

    override fun updateRecordProcessingDuration(durationMs: Int) {
        log.debug("Updating record processing duration: {} ms", durationMs)
        this.recordProcessingDurationMs = durationMs
        activeWorkers.values.forEach { it.updateRecordProcessingDuration(durationMs) }
    }

    override fun getPollHistory() = pollHistory.toList()


}
