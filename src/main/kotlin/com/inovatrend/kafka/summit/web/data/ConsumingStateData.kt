package com.inovatrend.kafka.summit.web.data

class ConsumingStateData(val workerInfos: List<WorkerInfo>,
                         val lastPollRecordsCount: Int,
                         val producerSpeedMsgPerSec: Int?,
                         val recordProcessingDurationMs: Int?)
