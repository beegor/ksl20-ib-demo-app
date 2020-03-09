package com.inovatrend.kafka.summit

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class KSL20DemoApplication

fun main(args: Array<String>) {
    runApplication<KSL20DemoApplication>(*args)
}
