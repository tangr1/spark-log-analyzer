package com.ctofunds.dd.quanmin

import org.apache.spark.{SparkConf, SparkContext}

object AndroidRetentionRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("quanmin")
    val sc = new SparkContext(sparkConf)

    val allNewUsers = sc.textFile(args(0))
      .map(_.split(","))
      .map(entry => (entry(1), 1))
      .reduceByKey((x, y) => x)
      .cache
    val newUsers = sc.textFile(args(1))
      .map(_.split(","))
      .filter(entry => entry(7) == "1")
      .map(entry => (entry(0), 1))
      .reduceByKey((x, y) => x)
      .join(allNewUsers)
      .cache
    val result = new Array[String](args.length)
    result(0) = args(0).split("/").last
    result(1) = newUsers.count.toString
    for (i <- 2 until args.length) {
      val retentionUsers = sc.textFile(args(i))
        .map(_.split(","))
        .map(entry => (entry(0), 1))
        .join(newUsers)
        .map(_._1)
        .distinct
      result(i) = "%.2f%%".format(retentionUsers.count * 100.0 / newUsers.count)
    }
    println(result.mkString(","))

    sc.stop()
  }
}