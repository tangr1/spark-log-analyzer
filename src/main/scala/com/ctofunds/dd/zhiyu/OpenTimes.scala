package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object OpenTimes {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-ot")
    val sc = new SparkContext(sparkConf)

    for (i <- args.indices) {
      val sessionCount = sc.textFile(args(i))
        .map(_.split(","))
        .map(entry => entry(0))
        .distinct
        .count
      val cookieCount = sc.textFile(args(i))
        .map(_.split(","))
        .map(entry => entry(1))
        .distinct
        .count
      val saudiSessionCount = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => entry(3) == "102358")
        .map(entry => entry(0))
        .distinct
        .count
      val saudiCookieCount = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => entry(3) == "102358")
        .map(entry => entry(1))
        .distinct
        .count
      println(Array(args(i).split("/").last,
        sessionCount.toFloat / cookieCount.toFloat,
        saudiSessionCount.toFloat / saudiCookieCount.toFloat).mkString(","))
    }

    sc.stop()
  }
}
