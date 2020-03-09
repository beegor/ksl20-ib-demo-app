package com.inovatrend.kafka.summit.web.data

import com.inovatrend.kafka.summit.ConsumerAppType

class ConsumingStateData(
        val consumerAppId: String,
        val consumerAppType: ConsumerAppType,
        val workerInfos: List<WorkerInfo>,
        val lastPollRecordsCount: Int,
        val recordProcessingDurationMs: Int?)
