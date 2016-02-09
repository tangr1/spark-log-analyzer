package com.ctofunds.dd

import org.apache.spark.{SparkConf, SparkContext}

object DailyActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dau")
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val logFile = args(0)
    val usCode = 6252001
    val rrd = sc.textFile(logFile)

    val context = rrd
      .map(Line.parseLine)
    val count = context.count
    val usCount = context.filter(_.country == usCode).count
    val nonUsCount = context.filter(_.country != usCode).count
    val iosCount = context.filter(_.ios != -1).count
    val androidCount = context.filter(_.android != -1).count
    println("活跃用户数:\t" + count)
    println("美国活跃用户数:\t%s (%.2f%%)".format(usCount, usCount * 100.0 / count))
    println("非美活跃用户数:\t%s (%.2f%%)".format(nonUsCount, nonUsCount * 100.0 / count))
    println("苹果活跃用户数:\t%s (%.2f%%)".format(iosCount, iosCount * 100.0 / count))
    println("安卓活跃用户数:\t%s (%.2f%%)".format(androidCount, androidCount * 100.0 / count))

    sc.stop()
  }
}
