package com.ctofunds.dd.quanmin

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Gift {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("quanmin")
    val sc = new SparkContext(sparkConf)
    val test = Array(1571965, 2096369, 2272662, 2272677, 1735235, 5683, 157, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 170, 1735340, 2984187)

    for (i <- args.indices) {
      val logFile = args(i)
      sc.textFile(logFile)
        .map(_.split(","))
        .map(entry => (entry(1).toInt, entry(2).toInt))
        .filter(entry => !(test contains entry._1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(20)
        .foreach(println)
    }

    sc.stop()
  }
}
