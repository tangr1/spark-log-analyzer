package com.example

import org.apache.hadoop.io.compress.{GzipCodec, CompressionCodec}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object LogAnalyzer {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Log Analyzer in Scala")
    val sc = new SparkContext(sparkConf)

    val logFile = args(0)

    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine)

    /*
    val ipCount = accessLogs.map(log => (log.ipAddress, 1)).reduceByKey(_ + _)
    println("IP Address Count: %s".format(ipCount.count()))

    val ipAddresses = accessLogs
      .map(log => (log.ipAddress, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 10)
      .map(_._1)
      .take(100)
     println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

    val userCount = accessLogs.map(log => (log.userId, 1)).reduceByKey(_ + _)
    println("User Count: %s".format(userCount.count()))
    */

    val users = accessLogs
      .map(log => (log.userId, DateTime.parse(log.dateTime, DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")).getMillis / 1000))
      .groupByKey()
      //.filter(_._2.size > 1)
    users.saveAsTextFile("/tmp/text", classOf[GzipCodec])
    //println(s"""User > 2 times: ${users.mkString("[", ",", "]")}""")

    sc.stop()
  }
}
