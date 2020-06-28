package com.inovatrend.kafka.summit.web


import com.inovatrend.kafka.summit.ConsumerAppType
import com.inovatrend.kafka.summit.service.ConsumerApp
import com.inovatrend.kafka.summit.service.fork_join.ForkJoinConsumerApp
import com.inovatrend.kafka.summit.service.fully_decoupled.FullyDecoupledConsumerApp
import com.inovatrend.kafka.summit.service.fully_decoupled.MultithreadedKafkaConsumer
import com.inovatrend.kafka.summit.web.data.ConsumerAppInfo
import com.inovatrend.kafka.summit.web.data.ConsumingStateData
import com.inovatrend.kafka.summit.web.data.PollInfo
import com.inovatrend.kafka.summit.web.data.WorkerInfo
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger


private const val timeFrameDurationMS = 100L
private const val timeLineLengthMS = 10_000L


@RestController
@CrossOrigin("*")
@RequestMapping("/consumer-app")
class ConsumerAppsController {

    private val log = LoggerFactory.getLogger(ConsumerAppsController::class.java)
    var consumerApps = mutableMapOf<String, ConsumerApp>()
    private val idGenerator = AtomicInteger(1)


    @GetMapping("/list-types")
    fun chooseConsumerApp(model: Model): List<ConsumerAppType> {
        return ConsumerAppType.values().asList()
    }


    @GetMapping("/list")
    fun listConsumers(): Collection<ConsumerAppInfo> {
        return consumerApps.map {
            val impl = getConsumerAppImpl(it.value)
            ConsumerAppInfo(impl, it.key)
        }
    }

    private fun getConsumerAppImpl(app: ConsumerApp): ConsumerAppType {
        val impl = when (app) {
            is ForkJoinConsumerApp -> ConsumerAppType.FORK_JOIN
            is FullyDecoupledConsumerApp, is MultithreadedKafkaConsumer -> ConsumerAppType.FULLY_DECOUPLED
            else -> throw RuntimeException("Unknown ConsumerApp implementation!")
        }
        return impl
    }


    @GetMapping("/start")
    fun startConsumerApp(@RequestParam impl: ConsumerAppType): Map<String, String> {

        val consumerAppId = idGenerator.getAndIncrement().toString()
        val consumerApp: ConsumerApp
        when (impl) {
            ConsumerAppType.FORK_JOIN -> {
                log.debug("Starting FORK JOIN implementation")
                consumerApp = ForkJoinConsumerApp("ksl20-demo", "ksl20-input-topic", 1000)
                consumerApp.startConsuming()
            }
            ConsumerAppType.FULLY_DECOUPLED -> {
                log.debug("Starting FULLY DECOUPLED implementation")
//                consumerApp = FullyDecoupledConsumerApp(consumerAppId, "ksl20-demo", "ksl20-input-topic", 1000)
                consumerApp = MultithreadedKafkaConsumer(consumerAppId, "ksl20-demo", "ksl20-input-topic", 10)
                consumerApp.startConsuming()
            }
        }

        consumerApps[consumerAppId] = consumerApp
        return mapOf(Pair("result", "OK"), Pair("consumerAppId", consumerAppId))
    }


    @GetMapping("/stop/{consumerAppId}")
    fun stopConsumerApp(@PathVariable consumerAppId: String): Boolean {
        val consumerApp = this.consumerApps.remove(consumerAppId)
        consumerApp?.stopConsuming()
        return consumerApp != null
    }


    var endTime = LocalDateTime.now()
    @Scheduled(fixedRate = timeFrameDurationMS)
    fun updateEndTime() {
        endTime = endTime.plusNanos(timeFrameDurationMS * 1_000_000)
    }

    @GetMapping("/poll-history/{consumerId}")
    fun getPollHistory(@PathVariable consumerId: String): List<PollInfo> {

        val consumerApp = consumerApps[consumerId] ?: throw ResponseStatusException(HttpStatus.NOT_FOUND)
        val pollHistory = consumerApp.getPollHistory() ?: listOf()
        val pollMap = mutableListOf<PollInfo>()
        val frames = timeLineLengthMS / timeFrameDurationMS
        var end = LocalDateTime.from(endTime)
        for (i in 0..frames) {
            val tfDurationNanos = timeFrameDurationMS * 1_000_000
            val start = end.minusNanos(tfDurationNanos)
            val hasPolls = pollHistory.any { (it == start || it.isAfter(start)) && it.isBefore(end) }
            if (hasPolls) {
                val halfTime = end.minusNanos(tfDurationNanos / 2)
                pollMap.add(PollInfo(start, end, 1))
                pollMap.add(PollInfo(halfTime, end, 0))
            } else pollMap.add(PollInfo(start, end, 0))
            end = end.minusNanos(timeFrameDurationMS * 1_000_000)
        }

        return pollMap.reversed()
    }



    @GetMapping("/state/{consumerId}")
    fun getWorkersInfo(@PathVariable consumerId: String): ConsumingStateData {
        val consumerApp = consumerApps[consumerId] ?: throw ResponseStatusException(HttpStatus.NOT_FOUND)
        val workers = consumerApp.getActiveWorkers() ?: listOf()
        val lastPollRecordsCount = consumerApp.getLastPollRecordsCount() ?: 0
        return ConsumingStateData(
                consumerId,
                getConsumerAppImpl(consumerApp),
                workers.map { WorkerInfo(it) },
                lastPollRecordsCount, consumerApp.getRecordProcessingDuration())
    }


    @GetMapping("/record-processing-duration-set/{consumerId}")
    fun setRecordProcessingDuration(@PathVariable consumerId: String, @RequestParam durationMs: Int): Boolean {
        val consumerApp = consumerApps[consumerId] ?: throw ResponseStatusException(HttpStatus.NOT_FOUND)
        consumerApp.updateRecordProcessingDuration(durationMs)
        return true
    }

}
