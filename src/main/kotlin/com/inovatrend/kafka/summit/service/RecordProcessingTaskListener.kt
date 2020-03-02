package com.inovatrend.kafka.summit.service

import org.apache.kafka.common.TopicPartition

interface RecordProcessingTaskListener {

    fun singleRecordProcessingFinished(topicPartition: TopicPartition, offset: Long)

    fun taskFinished(topicPartition: TopicPartition, offset: Long)

}
