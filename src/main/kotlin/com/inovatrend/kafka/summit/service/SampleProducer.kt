package com.inovatrend.kafka.summit.service

interface SampleProducer {

    fun startProducing()

    fun stopProducing()

    fun updateSpeed(msgPerSec:Int)
}
