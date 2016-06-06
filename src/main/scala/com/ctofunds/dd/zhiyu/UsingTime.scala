package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object UsingTime {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-ut")
    val sc = new SparkContext(sparkConf)

    for (i <- args.indices) {
      val cached = sc.textFile(args(i))
        .map(_.split(","))
        .map(entry => (entry(0), entry(7)))
        .groupByKey()
        .map(entry => (entry._1, entry._2.toArray.sortBy(x => x.toInt)))
        .cache
      val time = cached
        .filter(_._2.length >= 2)
        .map(entry => entry._2.last.toInt - entry._2(0).toInt)
        .sum.toFloat / cached.count.toFloat
      val saudiCached = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => entry(3) == "102358")
        .map(entry => (entry(0), entry(7)))
        .groupByKey()
        .map(entry => (entry._1, entry._2.toArray.sortBy(x => x.toInt)))
        .cache
      val saudiTime = saudiCached
        .filter(_._2.length >= 2)
        .map(entry => entry._2.last.toInt - entry._2(0).toInt)
        .sum.toFloat / saudiCached.count.toFloat
      println(Array(args(i).split("/").last, time / 60.0, saudiTime / 60.0).mkString(","))
    }

    sc.stop()
  }
}
