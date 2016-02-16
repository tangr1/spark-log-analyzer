package com.ctofunds.dd

import org.apache.spark.{SparkConf, SparkContext}

object EventAnalysis {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dea")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)

    for (i <- 2 until args.length) {
      val logFile = args(i)
      val result = sc.textFile(logFile)
        .map(row => (row.split("\"")(9), 1))
        .reduceByKey(_ + _)
        .sortByKey()
        .collect
      println(s"=== $logFile Event Analysis ===")
      println(result.mkString("\n"))
      result.foreach(item => print(" | %s".format(item._2)))
      println(" |")
    }

    sc.stop()
  }
}
