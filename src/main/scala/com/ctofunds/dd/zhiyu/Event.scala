package com.ctofunds.dd.zhiyu

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.{SparkConf, SparkContext}

object Event {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-e")
    val sc = new SparkContext(sparkConf)


    sc.textFile(args(0))
      .map(_.split("\\^A\\^R"))
      .filter(entry => entry(1) == "600" && entry.length > 9 )
      // 0 type, 1 action, 2 goods id, 3 session id
      .map(entry => entry(2))
      .distinct
      .foreach(println)
      //.map(entry => Array(entry(2), entry(3), entry(4), entry(7)).mkString(","))
      //.saveAsTextFile(args(1), classOf[BZip2Codec])
    sc.stop()
  }
}