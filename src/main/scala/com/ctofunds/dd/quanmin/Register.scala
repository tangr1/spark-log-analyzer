package com.ctofunds.dd.quanmin

import org.apache.spark.{SparkConf, SparkContext}

object Register {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("quanmin")
    val sc = new SparkContext(sparkConf)

    val a = sc.textFile(args(0))
      .map(_.split("%5D="))
      .map(entry => entry(1))
      .map(_.split("&p%"))
      .map(entry => entry(0))
      .distinct
      .collect

    val b = sc.textFile(args(1))
      .map(_.split(","))
      .map(entry => entry(0))
      .distinct
      .collect

    println(a.length)
    println(b.length)
    b.filter(entry => !(a contains entry)).foreach(println)

    sc.stop()
  }
}
