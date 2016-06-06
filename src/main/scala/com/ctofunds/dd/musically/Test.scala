package com.ctofunds.dd.musically

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-test")
    val sc = new SparkContext(sparkConf)
    // HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    sc.parallelize(List(1, 2, 3)).saveAsTextFile(args(0))
    sc.stop()
  }
}