package com.ctofunds.dd

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.{SparkConf, SparkContext}

object NewRawActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-drp")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val logFile = args(2)
    val outFile = args(3)
    val logs = sc.textFile(logFile)

    logs
      .map(_.split("\""))
      .filter(row => row.length > 49 && row(31) == "200")
      .map(row => (row(51),  row(11).trim + "," + row(39)))
      .reduceByKey((x, y) => x)
      .saveAsTextFile(outFile, classOf[BZip2Codec])

    sc.stop()
  }
}
