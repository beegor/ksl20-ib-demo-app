package com.inovatrend.kafka.summit.service.fully_decoupled

import com.inovatrend.kafka.summit.service.RecordProcessingTaskListener
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

class RebalanceListener(recordProcessingTaskListener: RecordProcessingTaskListener) : ConsumerRebalanceListener {

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {

    }

    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {

    }
}
