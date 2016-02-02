package com.example

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

object ApacheLogAnalyzerStreaming {
  val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)

  val computeRunningSum = (values: Seq[Long], state: Option[Long]) => {
    val currentCount = values.foldLeft(0L)(_ + _)
    val previousCount = state.getOrElse(0L)
    Some(currentCount + previousCount)
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming Total in Scala")
    val sc = new SparkContext(sparkConf)

    val streamingContext = new StreamingContext(sc, SLIDE_INTERVAL)

    // NOTE: Checkpointing must be enabled to use updateStateByKey.
    streamingContext.checkpoint("/tmp/log-analyzer-streaming-total-scala")

    val logLinesDStream = streamingContext.socketTextStream("localhost", 9999)

    val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine)

    val ipAddressDStream = accessLogsDStream
      .map(log => (log.ipAddress, 1L))
      .reduceByKey(_ + _)
      .updateStateByKey(computeRunningSum)
      .filter(_._2 > 10)
      .map(_._1)
    ipAddressDStream.saveAsTextFiles("/tmp/steam")
    /*
    ipAddressDStream.foreachRDD(rdd => {
      val ipAddresses = rdd.take(100)
      println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")
    })
    */

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
