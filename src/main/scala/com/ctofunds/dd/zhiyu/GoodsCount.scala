package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object GoodsCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-rr")
    val sc = new SparkContext(sparkConf)

    for (i <- args.indices) {
      val cached = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => !entry(5).isEmpty)
        .map(entry => (entry(1), entry(5)))
        .distinct
        .map(entry => (entry._1, 1))
        .reduceByKey(_ + _)
        .cache
      val count = cached.count
      val sum = cached
        .map(_._2)
        .sum()
      val saudiCached = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => !entry(5).isEmpty && entry(3) == "102358")
        .map(entry => (entry(1), entry(5)))
        .distinct
        .map(entry => (entry._1, 1))
        .reduceByKey(_ + _)
        .cache
      val saudiCount = saudiCached.count
      val saudiSum = saudiCached
        .map(_._2)
        .sum()
      println(Array(args(i).split("/").last,
        sum.toFloat / count.toFloat,
        saudiSum.toFloat / saudiCount.toFloat).mkString(","))
    }

    sc.stop()
  }
}