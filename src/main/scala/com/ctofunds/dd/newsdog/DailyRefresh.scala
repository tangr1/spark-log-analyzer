package com.ctofunds.dd.newsdog

import org.apache.spark.{SparkConf, SparkContext}

object DailyRefresh {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("newsdog")
    val sc = new SparkContext(sparkConf)

    val dau = sc.textFile("./data/newsdog/en_dau.csv")
      .map(entry => entry.split(","))
      .collect

    var count = 0

    for ( item <- dau ) {
      if (item(0).toInt == args(1).toInt) {
        count = item(1).toInt
      }
    }

    val result = sc.textFile(args(0))
      .map(entry => entry.split(","))
      .map(entry => (args(1).toInt, 1))
      .reduceByKey(_ + _)
      .collect

    println("7." + result(0)._1 + "," + "%.2f".format(result(0)._2 / (count * 1.0)))

    sc.stop()
  }
}
