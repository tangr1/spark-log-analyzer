package com.ctofunds.dd.quanmin

import org.apache.spark.{SparkConf, SparkContext}

object Order {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("quanmin")
    val sc = new SparkContext(sparkConf)

    for (i <- args.indices) {
      val logFile = args(i)
      sc.textFile(logFile)
        .map(_.split(","))
        .map(entry => (entry(0), entry(1).toInt))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(20)
        .foreach(println)
    }

    sc.stop()
  }
}
