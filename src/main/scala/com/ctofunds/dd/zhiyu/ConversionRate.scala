package com.ctofunds.dd.zhiyu

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object ConversionRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-au")
    val sc = new SparkContext(sparkConf)

    var count = 0L
    var registerCount = 0L
    var detailCount = 0L
    var orderCount = 0L
    var saudiCount = 0L
    var saudiRegisterCount = 0L
    var saudiDetailCount = 0L
    var saudiOrderCount = 0L
    for (i <- args.indices) {
      val logFile = args(i)
      val rrd = sc.textFile(logFile)
      val cached = rrd
        .map(_.split(","))
        .map(entry => (entry(1), entry(2), entry(3), entry(9), entry(5), entry(6)))
        .persist(StorageLevel.DISK_ONLY)
      val base = cached
        .filter(_._2 != "0")
        .map(entry => (entry._1, 1))
        .distinct
        .persist(StorageLevel.DISK_ONLY)
      count += base.count
      registerCount += cached
        .filter(entry => !entry._4.isEmpty && entry._4 != "0")
        .map(entry => (entry._1, 1))
        .distinct
        .join(base)
        .count
      detailCount += cached
        .filter(_._5 != "")
        .map(entry => (entry._1, 1))
        .distinct
        .join(base)
        .count
      orderCount += cached
        .filter(_._6 != "")
        .map(entry => (entry._1, 1))
        .distinct
        .join(base)
        .count
      val saudiBase = cached
        .filter(entry => entry._2 != "0" && entry._3.toInt == 102358)
        .map(entry => (entry._1, 1))
        .distinct
        .persist(StorageLevel.DISK_ONLY)
      saudiCount += saudiBase.count
      saudiRegisterCount += cached
        .filter(entry => !entry._4.isEmpty && entry._4 != "0" && entry._3.toInt == 102358)
        .map(entry => (entry._1, 1))
        .distinct
        .join(saudiBase)
        .count
      saudiDetailCount += cached
        .filter(entry => entry._3.toInt == 102358 && entry._5 != "")
        .map(entry => (entry._1, 1))
        .distinct
        .join(saudiBase)
        .count
      saudiOrderCount += cached
        .filter(entry => entry._3.toInt == 102358 && entry._6 != "")
        .map(entry => (entry._1, 1))
        .distinct
        .join(saudiBase)
        .count
    }
    println(registerCount.toFloat * 100.0 / count.toFloat)
    println(detailCount.toFloat * 100.0 / count.toFloat)
    println(orderCount.toFloat * 100.0 / count.toFloat)
    println(saudiRegisterCount.toFloat * 100.0 / saudiCount.toFloat)
    println(saudiDetailCount.toFloat * 100.0 / saudiCount.toFloat)
    println(saudiOrderCount.toFloat * 100.0 / saudiCount.toFloat)

    sc.stop()
  }
}
