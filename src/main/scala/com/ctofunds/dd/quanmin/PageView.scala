package com.ctofunds.dd.quanmin

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.{SparkConf, SparkContext}

object PageView {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("quanmin")
    val sc = new SparkContext(sparkConf)

    sc.textFile(args(0))
      .map(Line.parseLine)
      .map(entry => Array(entry.ip, entry.time, entry.method, entry.path, entry.status, entry.bytes, entry.device).mkString(","))
      .saveAsTextFile(args(1), classOf[BZip2Codec])

    sc.stop()
  }
}