package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object Source {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-rr")
    val sc = new SparkContext(sparkConf)

    for (i <- args.indices) {
      val cached = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => entry.length > 10)
        .map(entry => (entry(10), entry(0)))
        .distinct
        .map(entry => (entry._1, 1))
        .reduceByKey(_ + _)
        .foreach(entry => println(Array(entry._1, entry._2).mkString(",")))
    }

    sc.stop()
  }
}