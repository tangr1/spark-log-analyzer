package com.ctofunds.dd.quanmin

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LoginUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("quanmin")
    val sc = new SparkContext(sparkConf)

    def isAllDigits(x: String) = x forall Character.isDigit

    println("日期,所有,PC,移动,安卓,苹果")
    for (i <- args.indices) {
      val logFile = args(i)
      val cached = sc.textFile(logFile)
        .map(_.split(","))
        .filter(entry => isAllDigits(entry(5)) && entry(5).toInt != -1 && isAllDigits(entry(4)) && entry(4).toInt > 30)
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
        .filter(entry => entry._2 == "3" || entry._2 == "5")
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
