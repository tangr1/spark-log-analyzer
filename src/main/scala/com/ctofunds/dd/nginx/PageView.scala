package com.ctofunds.dd.nginx

import org.apache.spark.{SparkConf, SparkContext}

object PageView {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("nginx")
    val sc = new SparkContext(sparkConf)

    val count = sc.textFile(args(0))
      .map(Line.parseLine)
      .map(entry => entry.ip)
      .distinct()
      .count()
    println(count)

    sc.stop()
  }
}