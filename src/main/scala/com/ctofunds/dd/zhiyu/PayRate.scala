package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object PayRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-rr")
    val sc = new SparkContext(sparkConf)

    val orders = sc.textFile(args(0))
      .map((_, 1))
      .cache
    for (i <- 1 until args.length) {
      /*
      val newUsers = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => entry(2) != "0")
        .map(entry => (entry(1), 1))
        .distinct
        .cache
      println(newUsers.count)
        */
      val orderUsers = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => !entry(6).isEmpty)
        .map(entry => (entry(6), entry(1)))
        .distinct
        .cache
      println(orderUsers.count)
      println(orderUsers.join(orders).count)
      //println(orderUsers.join(newUsers).count)
      //println(orderUsers.join(newUsers).join(orders).count)
    }

    sc.stop()
  }
}