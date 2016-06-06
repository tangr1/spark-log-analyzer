package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object RetentionRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-rr")
    val sc = new SparkContext(sparkConf)
    val newUserFile = args(0)

    val newUsers = sc.textFile(newUserFile)
      .map(_.split(","))
      .filter(entry => entry(2) != "0")
      .map(entry => (entry(1), 1))
      .reduceByKey((x, y) => x)
      .cache
    val result = new Array[String](args.length)
    result(0) = args(0).split("/").last
    for (i <- 1 until args.length) {
      val retentionUsers = sc.textFile(args(i))
        .map(_.split(","))
        .map(entry => (entry(1), 1))
        .join(newUsers)
        .map(_._1)
        .distinct
      result(i) = "%.2f%%".format(retentionUsers.count * 100.0 / newUsers.count)
    }
    println(result.mkString(","))

    sc.stop()
  }
}