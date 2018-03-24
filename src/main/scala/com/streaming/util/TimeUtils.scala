package com.streaming.util

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object TimeUtils {
    val YYYYMMDDMMHHSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMdd_HH")

    /**
      * Get timestamp of a formatted time string.
      * @param time formatted time string
      * @return timestamp
      */
    def getTimestamp(time: String) = {
        YYYYMMDDMMHHSS_FORMAT.parse(time).getTime
    }

    /**
      * Transform timestamp to a formatted string yyyyMMdd_HH.
      * @param timestamp input timestamp
      * @return
      */
    def transform(timestamp: String) = {
        TARGET_FORMAT.format(new Date(getTimestamp(timestamp)))
    }
}
