package com.ctofunds.dd.newsdog

import com.ctofunds.dd.util.Util
import org.apache.spark.{SparkConf, SparkContext}

object WeeklyRetentionRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("newsdog")
    val sc = new SparkContext(sparkConf)
    val newUserFile = "./data/newsdog/hindi_new_user.csv"
    val base = args(0).toInt
    val week = args(1).toInt
    val newUsers = sc.textFile(newUserFile)
      .map(entry => entry.split(","))
      .filter(entry => entry(0).toInt > (base - 1) * 7 && entry(0).toInt <= base * 7)
      .map(entry => entry(1).toInt)
      .distinct
      .map(entry => (entry, 1))
      .cache
    val array = new Array[String](7)
    for (i <- week * 7 + 1 until (week + 1) * 7 + 1) {
      if (i < 10) {
        array(i - week * 7 - 1) = "./data/newsdog/hindi_refresh_0" + i
      } else {
        array(i - week * 7 - 1) = "./data/newsdog/hindi_refresh_" + i
      }
    }
    val activeUsers = sc.textFile(array.mkString(","))
      .map(entry => entry.split(","))
      .map(entry => (entry(1).toInt, 1))
      .distinct
      .cache
    println(Util.percent(newUsers.join(activeUsers).count, newUsers.count))

    sc.stop()
  }
}