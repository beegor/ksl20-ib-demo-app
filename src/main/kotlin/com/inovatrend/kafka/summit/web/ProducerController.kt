package com.inovatrend.kafka.summit.web

import com.inovatrend.kafka.summit.service.SampleProducer
import com.inovatrend.kafka.summit.web.data.ProducerInfo
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import java.util.concurrent.atomic.AtomicInteger

@RestController
@CrossOrigin("*")
@RequestMapping("/producer")
class ProducerController {

    var producers = mutableMapOf<String, SampleProducer>()
    private val idGenerator = AtomicInteger(1);

    private val log = LoggerFactory.getLogger(ProducerController::class.java)


    @GetMapping("/start")
    fun startProducer(topic: String): ProducerInfo {

        val producer = SampleProducer(topic, 0)
        val id = idGenerator.getAndIncrement().toString()
        producers[id] = producer
        producer.startProducing()
        log.info("Started producer with id: {}", id)
        return ProducerInfo(id, topic, producer.produceSpeedMsgPerSec)
    }


    @GetMapping("/list")
    fun listProducers(): Collection<ProducerInfo> {
        return producers.map {
            ProducerInfo(it.key, it.value.topic, it.value.produceSpeedMsgPerSec)
        }
    }


    @GetMapping("/speed-set/{producerId}")
    fun setProducerSpeed(@PathVariable producerId: String, @RequestParam msgPerSec: Int): Boolean {
        val producer = producers[producerId] ?: throw ResponseStatusException(HttpStatus.NOT_FOUND)
        producer.updateSpeed(msgPerSec)
        return true
    }

}
