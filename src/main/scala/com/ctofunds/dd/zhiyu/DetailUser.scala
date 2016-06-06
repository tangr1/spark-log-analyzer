package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object DetailUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-au")
    val sc = new SparkContext(sparkConf)

    for (i <- args.indices) {
      val logFile = args(i)
      val rrd = sc.textFile(logFile)
      val cached = rrd
        .map(_.split(","))
        .filter(entry => entry(5) != "")
        .map(entry => (entry(1), entry(3)))
        .cache
      val total = cached
        .map(_._1)
        .distinct
        .count
      val sa = cached
        .filter(_._2 == "102358")
        .map(_._1)
        .distinct
        .count
      println(Array(args(i).split("/").last, total, sa).mkString(","))
    }

    sc.stop()
  }
}
