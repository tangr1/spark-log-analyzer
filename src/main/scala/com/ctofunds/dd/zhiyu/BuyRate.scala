package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object BuyRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-rr")
    val sc = new SparkContext(sparkConf)
    val baseUserFile = args(0)

    val baseUsers = sc.textFile(baseUserFile)
      .map(_.split(","))
      .filter(entry => !entry(6).isEmpty)
      .map(entry => (entry(1), 1))
      .reduceByKey((x, y) => x)
      .cache
    val result = new Array[String](args.length)
    result(0) = args(0).split("/").last
    for (i <- 1 until args.length) {
      val orderUsers = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => !entry(6).isEmpty)
        .map(entry => (entry(1), 1))
        .join(baseUsers)
        .map(_._1)
        .distinct
      result(i) = "%.2f%%".format(orderUsers.count * 100.0 / baseUsers.count)
    }
    println(result.mkString(","))

    sc.stop()
  }
}