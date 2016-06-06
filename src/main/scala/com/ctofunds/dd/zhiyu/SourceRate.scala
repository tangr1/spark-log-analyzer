package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap

object SourceRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-rr")
    val sc = new SparkContext(sparkConf)

    val cached = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(2) != "0.00")
      .map(entry => (entry(0).stripPrefix("\"").stripSuffix("\""), entry(1).stripPrefix("\"").stripSuffix("\""), entry(3), entry(4)))
      .cache
    val activate = cached
      .map(entry => entry._2)
      .collect
      .toSet
    println(activate.size)
    val activateMap = cached
      .map(entry => (entry._1, 1))
      .reduceByKey(_ + _)
      .collectAsMap
    val registerMap = cached
      .filter(_._3 != "0.00")
      .map(entry => (entry._1, entry._2))
      .filter(entry => activate.contains(entry._2))
      .map(entry => (entry._1, 1))
      .reduceByKey(_ + _)
      .collectAsMap
    var sum = 0
    registerMap.foreach(sum += _._2)
    println(sum)
    val orderMap = cached
      .filter(_._4 != "0.00")
      .map(entry => (entry._1, entry._2))
      .filter(entry => activate.contains(entry._2))
      .map(entry => (entry._1, 1))
      .reduceByKey(_ + _)
      .collectAsMap
    val sortedMap = ListMap(activateMap.toSeq.sortWith(_._2 > _._2):_*).take(10)
    for ((k, v) <- sortedMap) {
      println(k + " " + v + " " + registerMap(k) + " " + orderMap(k) + " " + registerMap(k).toFloat * 100.0 / v.toFloat
        + " " + orderMap(k).toFloat * 100.0 / v.toFloat)
    }

    sc.stop()
  }
}