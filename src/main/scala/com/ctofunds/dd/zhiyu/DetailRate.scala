package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object DetailRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-au")
    val sc = new SparkContext(sparkConf)

    val reg_users = sc.textFile(args(0))
      .map((_, 1))
    val result = sc.textFile(args(1))
        .map(_.split(","))
        .filter(entry => entry(5) != "")
        .map(entry => (entry(8), 1))
        .distinct
        .join(reg_users)
        .count
    println(result)

    sc.stop()
  }
}
