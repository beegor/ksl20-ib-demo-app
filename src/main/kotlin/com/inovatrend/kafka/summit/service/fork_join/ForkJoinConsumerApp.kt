package com.inovatrend.kafka.summit.service.fork_join

import com.inovatrend.kafka.summit.service.ConsumerApp
import com.inovatrend.kafka.summit.service.RecordProcessingTask
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
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

class ForkJoinConsumerApp(consumerGroup: String,
                          private val topic: String,
                          private var recordProcessingDurationMs: Int) : ConsumerApp {

    private val consumer: KafkaConsumer<String, String>
    private val stopped = AtomicBoolean(false)
    private val executor = Executors.newWorkStealingPool(8)
    private val activeWorkers = mutableListOf<RecordProcessingTask>()
    private var lastPollRecordsCount = 0
    private val pollHistory = mutableListOf<LocalDateTime>()
    private val log = LoggerFactory.getLogger(ForkJoinConsumerApp::class.java)

    init {
        val config = Properties()
        config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.GROUP_ID_CONFIG] = consumerGroup
        config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        config[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG] = 2000
        consumer = KafkaConsumer(config)
    }

    override fun startConsuming() {
        thread {
            try {
                consumer.subscribe(Collections.singleton(topic))
                while (!stopped.get()) {
                    updatePollMetrics()
                    val records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS))
                    this.lastPollRecordsCount = records.count()
                    log.debug("Fetched {} records", lastPollRecordsCount)
                    val tasks = records.partitions().map { partition ->
                        val partitionRecords = records.records(partition)
                        val worker = ForkJoinRecordProcessingTask(partition, partitionRecords, recordProcessingDurationMs)
                        worker.apply { activeWorkers.add(this) }
                    }
                    log.debug("Invoking executors start, tasks : {}", tasks.size)
                    executor.invokeAll(tasks)
                    log.debug("Invoking executors finish")
                    activeWorkers.clear()
                }
            } catch (we: WakeupException) {
                if (!stopped.get()) throw we
            } catch (e: Exception) {
                log.error("Failed to consume messages!", e)
            }
            finally {
                consumer.close()
            }
        }
    }

    private fun updatePollMetrics() {
        val now = LocalDateTime.now()
        pollHistory.add(now)
    }


    override fun stopConsuming() {
        stopped.set(true)
        consumer.wakeup()
    }


    override fun getActiveWorkers() = activeWorkers.toList()

    override fun getLastPollRecordsCount() = this.lastPollRecordsCount

    override fun getRecordProcessingDuration() = this.recordProcessingDurationMs

    override fun updateRecordProcessingDuration(durationMs: Int) {
        log.debug("Updating record processing duration: {} ms", durationMs)
        this.recordProcessingDurationMs = durationMs
        activeWorkers.forEach { it.updateRecordProcessingDuration(durationMs) }
    }

    override fun getPollHistory() = pollHistory.toList()


}
