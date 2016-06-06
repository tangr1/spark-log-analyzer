package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object GoodsRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-rr")
    val sc = new SparkContext(sparkConf)

    for (i <- args.indices) {
      val newUsers = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => entry(2) != "0")
        .map(entry => (entry(1), 1))
        .distinct
        .cache
      println(newUsers.count)
      val goodsUsers = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => !entry(8).isEmpty)
        .map(entry => (entry(1), 1))
        .distinct
        .cache
      println(goodsUsers.join(newUsers).count)
    }

    sc.stop()
  }
}