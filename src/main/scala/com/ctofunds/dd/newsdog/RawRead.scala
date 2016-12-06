package com.ctofunds.dd.newsdog

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.{SparkConf, SparkContext}

object RawRead {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("newsdog")
    val sc = new SparkContext(sparkConf)

    sc.textFile(args(0))
      .map(ReadLine.parseLine)
      .saveAsTextFile(args(1), classOf[BZip2Codec])

    sc.stop()
  }
}
