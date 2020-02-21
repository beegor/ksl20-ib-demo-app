package com.inovatrend.kafka.summit

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class PresentationAppApplication

fun main(args: Array<String>) {
	runApplication<PresentationAppApplication>(*args)
}
