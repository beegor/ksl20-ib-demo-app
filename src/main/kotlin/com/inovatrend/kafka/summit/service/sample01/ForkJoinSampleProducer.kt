package com.inovatrend.kafka.summit.service.sample01

import com.inovatrend.kafka.summit.service.SampleProducer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import kotlin.math.log

class ForkJoinSampleProducer (private val topic: String, var produceSpeedMsgPerSec: Int = 1) : SampleProducer {

    private val stopped = AtomicBoolean(false)
    private val producer: KafkaProducer<String, String>

    private val log = LoggerFactory.getLogger(ForkJoinSampleProducer::class.java)

    init {
        val config = Properties()
        config[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        config[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        config[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        producer = KafkaProducer(config)
    }


    override fun startProducing() {
        thread {
            while (!stopped.get()){
                val speedMsgPerSec = produceSpeedMsgPerSec
                if (speedMsgPerSec > 0) {
                    val message = "Message - ${LocalDateTime.now()}"
                    log.info("Producing message: {}", message)
                    producer.send(ProducerRecord(topic, null, message)).get()
                    val delay = getDelay(speedMsgPerSec)
                    log.info("Producer delay: {}", delay)
                    Thread.sleep(delay)
                }
                else Thread.sleep(1000)
            }
            producer.close()
        }
    }


    override fun stopProducing() {
        stopped.set(true)
    }

    override fun updateSpeed(msgPerSec: Int) {
        log.info("Updating producer speed to: {}", msgPerSec)
        this.produceSpeedMsgPerSec = msgPerSec
    }

    private fun getDelay(speedMsgPerSec: Int): Long {
        return 1000L / speedMsgPerSec
    }


}
