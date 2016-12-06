package com.ctofunds.dd.newsdog

import org.apache.spark.{SparkConf, SparkContext}

object WeeklyActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("newsdog")
    val sc = new SparkContext(sparkConf)
    val begin = args(1).toInt
    val end = args(2).toInt

    println(sc.textFile(args(0))
      .map(entry => entry.split(","))
      .filter(entry => entry(0).toInt >= begin && entry(0).toInt <= end)
      .map(entry => entry(1))
      .distinct
      .count)

    sc.stop()
  }
}
