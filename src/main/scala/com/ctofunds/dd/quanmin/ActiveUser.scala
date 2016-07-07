package com.ctofunds.dd.quanmin

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object ActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("quanmin")
    val sc = new SparkContext(sparkConf)

    println("日期,所有,PC,移动,安卓,苹果")
    for (i <- args.indices) {
      val logFile = args(i)
      val cached = sc.textFile(logFile)
        .map(_.split(","))
        .map(entry => (entry(0), entry(6)))
        .persist(StorageLevel.DISK_ONLY)
      val one = cached
        .filter(_._2 == "1")
        .map(_._1)
        .distinct
        .count
      val two = cached
        .filter(_._2 == "2")
        .map(_._1)
        .distinct
        .count
      val three = cached
        .filter(_._2 == "3")
        .map(_._1)
        .distinct
        .count
      val four = cached
        .filter(_._2 == "4")
        .map(_._1)
        .distinct
        .count
      println(Array(args(i).split("/").last, one + two + three + four, one, two, three, four).mkString(","))
    }

    sc.stop()
  }
}
