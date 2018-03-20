package com.streaming.spark

import org.apache.hadoop.hbase.util.Bytes
import util.HBaseUtils

import scala.collection.mutable.ListBuffer

object ClickCountDAO {

    final val TABLE_CLICK_COUNT_BY_HOUR: String = "category_clickcount"
    final val TABLE_CLICK_COUNT_BY_REFERRER: String = "category_referrer_clickcount"
    final val CF: String = "info"
    final val QUALIFIER: String = "click_count"

    /**
      * Update click count.
      * @param list a list of ClickCount
      */
    def increase(list: ListBuffer[ClickCount], dataType: String) : Unit = {
        val tableName = dataType match {
            case "hour" => TABLE_CLICK_COUNT_BY_HOUR
            case "referrer" => TABLE_CLICK_COUNT_BY_REFERRER
        }
        val table = HBaseUtils.getInstance().getTable(tableName)
        list.foreach(clickCount => {
            table.incrementColumnValue(Bytes.toBytes(clickCount.categoryId), Bytes.toBytes(CF),
                                        Bytes.toBytes(QUALIFIER), clickCount.clickCount)
        })
    }
}
