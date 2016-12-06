package com.ctofunds.dd.newsdog

import org.apache.spark.{SparkConf, SparkContext}

object DailyQualityUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("newsdog")
    val sc = new SparkContext(sparkConf)
    val read = args(1).toInt
    val duration = args(2).toInt
    val day = "7." + args(0).split("/").last.split("_").last.toInt.toString

    val durationResult = sc.textFile(args(0))
      .map(entry => entry.split(","))
      .map(entry => (entry(1).toInt, entry(2).toInt))
      .reduceByKey(_ + _)
      .filter(_._2 >= duration)
      .cache

    val count = sc.textFile(args(0))
      .map(entry => entry.split(","))
      .map(entry => (entry(1).toInt, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= read)
      .join(durationResult)
      .count

    println(Array(day, count.toString).mkString(","))

    sc.stop()
  }
}
