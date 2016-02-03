package com.ctofunds.dd

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RetentionRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-rr")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile("/tmp/dnu")

    println(file.collect().mkString("\n"))

    sc.stop()
  }
}
