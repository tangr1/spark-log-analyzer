package com.ctofunds.dd.musically

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.{SparkConf, SparkContext}

object RawRequestPath {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-drp")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val logFile = args(2)
    val outFile = args(3)
    val logs = sc.textFile(logFile)

    logs
      .map(_.split("\""))
      .map(row => (if (row.length > 49) row(51) else " ") + "," + row(23) + "," + row(19) + "," + row(31))
      .saveAsTextFile(outFile, classOf[BZip2Codec])

    sc.stop()
  }
}
