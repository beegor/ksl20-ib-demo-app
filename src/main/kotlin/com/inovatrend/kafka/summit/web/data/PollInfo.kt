package com.inovatrend.kafka.summit.web.data

import java.time.LocalDateTime

class PollInfo(val startTime: LocalDateTime, val endTime: LocalDateTime, var pollCount: Int)
