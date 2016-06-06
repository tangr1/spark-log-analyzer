package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object QualityUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-ut")
    val sc = new SparkContext(sparkConf)

    for (i <- args.indices) {
      val timeUsers = sc.textFile(args(i))
        .map(_.split(","))
        .map(entry => (entry(1), entry(7)))
        .groupByKey()
        .map(entry => (entry._1, entry._2.toArray.sortBy(x => x.toInt)))
        .filter(entry => entry._2.length >= 2 && entry._2.last.toInt - entry._2(0).toInt > 300)
        .map(entry => (entry._1, 1))
        .cache
      val countUsers = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => !entry(5).isEmpty)
        .map(entry => (entry(1), entry(5)))
        .distinct
        .map(entry => (entry._1, 1))
        .reduceByKey(_ + _)
        .filter(_._2 > 10)
        .cache
      val saudiTimeUsers = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => entry(3) == "102358")
        .map(entry => (entry(1), entry(7)))
        .groupByKey()
        .map(entry => (entry._1, entry._2.toArray.sortBy(x => x.toInt)))
        .filter(entry => entry._2.length >= 2 && entry._2.last.toInt - entry._2(0).toInt > 300)
        .map(entry => (entry._1, 1))
        .cache
      val saudiCountUsers = sc.textFile(args(i))
        .map(_.split(","))
        .filter(entry => !entry(5).isEmpty && entry(3) == "102358")
        .map(entry => (entry(1), entry(5)))
        .distinct
        .map(entry => (entry._1, 1))
        .reduceByKey(_ + _)
        .filter(_._2 > 10)
        .cache
      println(Array(args(i).split("/").last,
        countUsers.join(timeUsers).count,
        saudiCountUsers.join(saudiTimeUsers).count).mkString(","))
    }

    sc.stop()
  }
}
