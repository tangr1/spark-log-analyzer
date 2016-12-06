package com.ctofunds.dd.newsdog

import org.apache.spark.{SparkConf, SparkContext}

object DailyRetentionRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("newsdog")
    val sc = new SparkContext(sparkConf)
    val base = args(0).toInt
    val count = args(1).toInt

    val newUsers = sc.textFile("./data/newsdog/hindi_new_user.csv")
      .map(entry => entry.split(","))
      .filter(entry => entry(0).toInt == base)
      .map(entry => (entry(1).toInt, 1))
      .cache

    val result = new Array[String](count)
    for (i <- base + 1 until count + base + 1) {
      var activeFile = "./data/newsdog/hindi_refresh_" + i
      if (i < 10) {
        activeFile = "./data/newsdog/hindi_refresh_0" + i
      }
      val activeCount = sc.textFile(activeFile)
        .map(entry => entry.split(","))
        .map(entry => (entry(1).toInt, 1))
        .distinct
        .join(newUsers)
        .count
      result(i - base - 1) = "%.2f".format(activeCount * 100.0 / newUsers.count)
    }
    println(result.mkString(","))

    sc.stop()
  }
}
