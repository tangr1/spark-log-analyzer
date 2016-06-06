package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap

object SourceDist {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-rr")
    val sc = new SparkContext(sparkConf)

    val map = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(4) != "0.00")
      .map(entry => (entry(0).stripPrefix("\"").stripSuffix("\""), entry(1)))
      .distinct
      .map(entry => (entry._1, 1))
      .reduceByKey(_ + _)
      .collectAsMap()

    var sum = 0
    for ((k, v) <- map) {
      sum += v
    }
    var top20 = 0
    val sorted = ListMap(map.toSeq.sortWith(_._2 > _._2):_*).take(20)
    for ((k, v) <- sorted) {
      top20 += v
    }
    println(sorted.keys.mkString(","))
    println(sorted.values.mkString(","))
    println(sum - top20)

    sc.stop()
  }
}