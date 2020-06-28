package com.inovatrend.kafka.summit.service

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class SimpleProducer(val topic: String, var produceSpeedMsgPerSec: Int = 1) {

    private val stopped = AtomicBoolean(false)
    private val producer: KafkaProducer<String, String>

    private val log = LoggerFactory.getLogger(SimpleProducer::class.java)

    init {
        val config = Properties()
        config[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        config[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        config[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        producer = KafkaProducer(config)
    }


    fun startProducing() {
        thread {
            while (!stopped.get()) {
                try {
                    produceMessage()
                } catch (e: Exception) {
                    log.error("Failed to produce message!", e)
                }
            }
            producer.close()
        }
    }

    private fun produceMessage() {
        val speedMsgPerSec = produceSpeedMsgPerSec
        if (speedMsgPerSec > 0) {
            val start = System.currentTimeMillis()
            val message = "Message - ${System.nanoTime()} - ${UUID.randomUUID()}"
            log.debug("Producing message: {}", message)
            producer.send(ProducerRecord(topic, null, message)).get()
            val delay = getDelay(speedMsgPerSec)

            val duration = System.currentTimeMillis() - start
            val sleepTime = delay - duration
            log.debug("Producer delay: {}, sleepTime: {}", delay, sleepTime)
            if (sleepTime > 0)
                Thread.sleep(sleepTime)
        } else Thread.sleep(1000)
    }


    fun stopProducing() {
        stopped.set(true)
    }

    fun updateSpeed(msgPerSec: Int) {
        this.produceSpeedMsgPerSec = msgPerSec
    }

    private fun getDelay(speedMsgPerSec: Int): Long {
        return 1000L / speedMsgPerSec
    }


}
