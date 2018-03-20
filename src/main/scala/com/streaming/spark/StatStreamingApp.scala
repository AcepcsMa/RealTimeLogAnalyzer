package com.streaming.spark

import com.streaming.util.TimeUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object StatStreamingApp {

    final val DURATION: Int = 3
    final val KAFKA_ADDRESS: String = "localhost:9092"
    final val TOPIC = Array("test")
    final val CHANNEL_PREFIX: String = "/channel"
    final val STAT_BY_HOUR: String = "hour"
    final val STAT_BY_REFERRER_HOUR = "referrer"

    def main(args: Array[String]) : Unit = {

        // get spark context
        val streamingContext = new StreamingContext("local[*]", "StatStreamingApp", Seconds(DURATION))

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> KAFKA_ADDRESS,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "test",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        // create a stream from kafka
        val logs = KafkaUtils.createDirectStream[String, String](
            streamingContext,
            PreferConsistent,
            Subscribe[String, String](TOPIC, kafkaParams)
        ).map(log => log.value())

        // clean raw logs and build ClickLog instances
        var cleanLogs = logs.map(line => {
            var terms = line.split("\t")
            var path = terms(2).split(" ")(1)
            var categoryId = if (path.startsWith(CHANNEL_PREFIX)) path.split("/")(2).toInt else -1
            ClickLog(terms(0), TimeUtils.transform(terms(1)), categoryId, terms(3), terms(4).toInt)
        }).filter(log => log.categoryId != -1)

        cleanLogs.print()

        // click count of each channel by hour
        cleanLogs.map(log => {
            (log.time + "_" + log.categoryId, 1)
        }).reduceByKey(_ + _).foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val clickCounts = new ListBuffer[ClickCount]
                partition.foreach(pair => {
                    clickCounts.append(ClickCount(pair._1, pair._2))
                })
                ClickCountDAO.increase(clickCounts, STAT_BY_HOUR)
            })
        })

        streamingContext.start()
        streamingContext.awaitTermination()
    }
}
