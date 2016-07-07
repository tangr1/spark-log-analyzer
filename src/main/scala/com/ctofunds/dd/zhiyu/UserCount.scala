package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object UserCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-ut")
    val sc = new SparkContext(sparkConf)

    println(sc.textFile(args(0))
      .distinct()
      .count)

    sc.stop()
  }
}
