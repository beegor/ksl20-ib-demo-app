package com.inovatrend.kafka.summit.web

import com.inovatrend.kafka.summit.ConsumerAppImpl
import com.inovatrend.kafka.summit.service.ConsumerApp
import com.inovatrend.kafka.summit.service.SampleProducer
import com.inovatrend.kafka.summit.service.fork_join.ForkJoinConsumerApp
import com.inovatrend.kafka.summit.service.fully_decoupled.FullyDecoupledConsumerApp
import com.inovatrend.kafka.summit.web.data.ConsumingStateData
import com.inovatrend.kafka.summit.web.data.PollInfo
import com.inovatrend.kafka.summit.web.data.WorkerInfo
import org.slf4j.LoggerFactory
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId

@RestController
@CrossOrigin("*")
class VisualisationController {

    private val log = LoggerFactory.getLogger(VisualisationController::class.java)

    var consumerApp: ConsumerApp? = null
    var producer: SampleProducer? = null

    private val timeFrameDurationMS = 100L
    private val timeLineLengthMS = 10_000L


    @GetMapping("/list-apps")
    fun chooseConsumerApp(model:Model): List<ConsumerAppImpl> {
        return ConsumerAppImpl.values().asList()
    }

    @GetMapping("/start-app")
    fun startConsumerApp(@RequestParam impl: ConsumerAppImpl): Map<String, String> {
        consumerApp?.stopConsuming()
        producer?.stopProducing()

        when (impl) {
            ConsumerAppImpl.FORK_JOIN -> {
                log.info("Starting FORK JOIN implementation")
                consumerApp = ForkJoinConsumerApp("ksl20-demo", "ksl20-input-topic", 1000)
                producer = SampleProducer("ksl20-input-topic", 1)
                consumerApp?.startConsuming()
                producer?.startProducing()
            }
            ConsumerAppImpl.FULLY_DECOUPLED -> {
                log.info("Starting FULLY DECOUPLED implementation")
                consumerApp = FullyDecoupledConsumerApp("ksl20-demo", "ksl20-input-topic", 1000)
                producer = SampleProducer("ksl20-input-topic", 1)
                consumerApp?.startConsuming()
                producer?.startProducing()
            }

        }
        return mapOf(Pair("result", "OK"))
    }



    @GetMapping("/poll-history")
    fun getPollHistory() : List<PollInfo> {

        val pollHistory = consumerApp?.getPollHistory() ?: listOf()
        val pollMap = mutableListOf<PollInfo>()
        val frames = timeLineLengthMS / timeFrameDurationMS
        var end = LocalDateTime.ofInstant( Instant.ofEpochMilli ((System.currentTimeMillis() / 30L) * 30L), ZoneId.systemDefault())
        for (i in 0..frames) {
            val tfDurationNanos = timeFrameDurationMS * 1_000_000
            val start = end.minusNanos(tfDurationNanos)
            val hasPolls = pollHistory.any{ (it == start || it.isAfter(start)) && it.isBefore(end)}
            if(hasPolls) {
                val halfTime = end.minusNanos(tfDurationNanos / 2)
                pollMap.add(PollInfo(start, end, 1))
                pollMap.add(PollInfo(halfTime, end, 0))
            }
            else pollMap.add(PollInfo(start, end, 0))
            end = end.minusNanos(timeFrameDurationMS * 1_000_000)
        }

        return pollMap.reversed()
    }

    @GetMapping("/consuming-state-data")
    fun getWorkersInfo() : ConsumingStateData {
        val workers = consumerApp?.getActiveWorkers() ?: listOf()
        val lastPollRecordsCount = consumerApp?.getLastPollRecordsCount() ?: 0
        return ConsumingStateData(workers.map { WorkerInfo(it) }, lastPollRecordsCount, producer?.produceSpeedMsgPerSec, consumerApp?.getRecordProcessingDuration())
    }


    @GetMapping("/producer-speed-set")
    fun setProducerSpeed(@RequestParam msgPerSec: Int) : Boolean {
        producer?.updateSpeed(msgPerSec)
        return true
    }


    @GetMapping("/consumer-record-processing-duration-set")
    fun setRecordProcessingDuration(@RequestParam durationMs: Int) : Boolean {
        consumerApp?.updateRecordProcessingDuration(durationMs)
        return true
    }

}
