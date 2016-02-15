package com.ctofunds.dd

import org.apache.spark.{SparkConf, SparkContext}

object ActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dau")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val usCode = 6252001

    for (i <- 2 until args.length) {
      val logFile = args(i)
      val rrd = sc.textFile(logFile)
      val context = rrd
        .map(Line.parseLine)
        .cache
      val count = context.count
      val usIosCount = context.filter(x => x.country == usCode && x.ios != -1).count
      val usAndroidCount = context.filter(x => x.country == usCode && x.android != -1).count
      val usCount = context.filter(_.country == usCode).count
      val nonUsIosCount = context.filter(x => x.country != usCode && x.ios != -1).count
      val nonUsAndroidCount = context.filter(x => x.country != usCode && x.android != -1).count
      val nonUsCount = context.filter(_.country != usCode).count
      val iosCount = context.filter(_.ios != -1).count
      val androidCount = context.filter(_.android != -1).count
      println("=== %s Active User ===".format(logFile))
      println("US iOS user:\t\t%s (%.2f%%)".format(usIosCount, usIosCount * 100.0 / count))
      println("Non US iOS user:\t%s (%.2f%%)".format(nonUsIosCount, nonUsIosCount * 100.0 / count))
      println("US Android user:\t%s (%.2f%%)".format(usAndroidCount, usAndroidCount * 100.0 / count))
      println("Non US Android user:\t%s (%.2f%%)".format(nonUsAndroidCount, nonUsAndroidCount * 100.0 / count))
      println("US user:\t\t%s (%.2f%%)".format(usCount, usCount * 100.0 / count))
      println("Non US user:\t\t%s (%.2f%%)".format(nonUsCount, nonUsCount * 100.0 / count))
      println("iOS user:\t\t%s (%.2f%%)".format(iosCount, iosCount * 100.0 / count))
      println("Android user:\t\t%s (%.2f%%)".format(androidCount, androidCount * 100.0 / count))
      println("Active user:\t\t" + count)
      println(" | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) | %s |"
        .format(
          usIosCount, usIosCount * 100.0 / count, nonUsIosCount, nonUsIosCount * 100.0 / count,
          usAndroidCount, usAndroidCount * 100.0 / count, nonUsAndroidCount, nonUsAndroidCount * 100.0 / count,
          usCount, usCount * 100.0 / count, nonUsCount, nonUsCount * 100.0 / count,
          iosCount, iosCount * 100.0 / count, androidCount, androidCount * 100.0 / count,
          count
        ))
    }

    sc.stop()
  }
}
