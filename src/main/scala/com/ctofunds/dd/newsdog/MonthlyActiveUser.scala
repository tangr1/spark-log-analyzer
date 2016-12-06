package com.ctofunds.dd.newsdog

import org.apache.spark.{SparkConf, SparkContext}

object MonthlyActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("newsdog")
    val sc = new SparkContext(sparkConf)

    println(sc.textFile(args(0))
      .map(entry => entry.split(","))
      .map(entry => entry(1))
      .distinct
      .count)

    sc.stop()
  }
}
