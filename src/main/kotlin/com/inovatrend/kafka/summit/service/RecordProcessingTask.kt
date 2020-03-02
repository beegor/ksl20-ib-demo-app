package com.inovatrend.kafka.summit.service

import org.apache.kafka.common.TopicPartition

interface RecordProcessingTask {
    fun getTotalRecords(): Int
    fun getProcessedRecords(): Int
    fun getTopicPartition(): TopicPartition
    fun updateRecordProcessingDuration(durationMs: Int)
}
