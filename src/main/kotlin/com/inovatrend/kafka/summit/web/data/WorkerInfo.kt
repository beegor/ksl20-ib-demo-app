package com.inovatrend.kafka.summit.web.data

import com.inovatrend.kafka.summit.service.RecordProcessingTask

class WorkerInfo(val totalRecords: Int, val processedRecords: Int, val threadName: String) {

    constructor(worker: RecordProcessingTask) : this(worker.getTotalRecords(), worker.getProcessedRecords(), "${worker.getTopicPartition().partition()}")

}
