package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object ActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-au")
    val sc = new SparkContext(sparkConf)

    for (i <- args.indices) {
      val logFile = args(i)
      val rrd = sc.textFile(logFile)
      val cached = rrd
        .map(_.split(","))
        .map(entry => (entry(1), entry(3)))
      val total = cached
        .map(entry => entry._1)
        .distinct
        .count
      val sa = cached
        .filter(_._2 == "102358")
        .map(entry => entry._1)
        .distinct
        .count
      println(Array(args(i).split("/").last, total, sa).mkString(","))
    }

    sc.stop()
  }
}
