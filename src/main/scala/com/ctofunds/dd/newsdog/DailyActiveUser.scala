package com.ctofunds.dd.newsdog

import org.apache.spark.{SparkConf, SparkContext}

object DailyActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("newsdog")
    val sc = new SparkContext(sparkConf)

    val count = sc.textFile(args(0))
      .map(entry => entry.split(","))
      .map(entry => entry(1).toInt)
      .distinct
      .count

    println(Array("7." + args(1), count.toString).mkString(","))

    /*
    val result = sc.textFile(args(0))
      .map(entry => entry.split(","))
      .map(entry => (entry(0), entry(1)))
      .distinct
      .map(entry => (entry._1, 1))
      .reduceByKey(_ + _)
      .map(entry => (entry._1.toInt, entry._2.toInt))
      .collect

    result.sortBy(_._1)
      .foreach(entry => println(Array(entry._1, entry._2).mkString(",")))
     */

    sc.stop()
  }
}
