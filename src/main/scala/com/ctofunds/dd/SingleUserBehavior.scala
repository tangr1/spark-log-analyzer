package com.ctofunds.dd

import org.apache.spark.{SparkConf, SparkContext}
import com.github.nscala_time.time.Imports._

object SingleUserBehavior {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-sub")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val logFile = args(2)
    val userName = args(3)
    val logs = sc.textFile(logFile)

    val result = logs
      .map(_.split("\""))
      .filter(row => row.length > 49 && row(51) == userName)
      .map(row => (DateTime.parse(row(15), formatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")),
        row(23) + " | " + row(19) + " | " + row(31) + " | " + row(11).trim + " | " + row(39)))
      .sortByKey(ascending = true)
      .map(row => row._1.toString("yyyy-MM-dd HH:mm:ss") + " | " + row._2)
      .collect

    println(result.mkString("\n"))

    sc.stop()
  }
}
