package com.inovatrend.kafka.summit.service

import java.time.LocalDateTime

interface ConsumerApp {

    /**
     * Starts main consumer thread
     */
    fun startConsuming()

    /**
     * Initiate stopping and block until stopped
     */
    fun stopConsuming()

    /**
     * Returns a list of workers that are currently processing records
     */
    fun getActiveWorkers() : List<RecordProcessingTask>

    fun getLastPollRecordsCount(): Int

    fun getPollHistory(): List<LocalDateTime>

    fun getRecordProcessingDuration(): Int

    fun updateRecordProcessingDuration(durationMs: Int)
}
