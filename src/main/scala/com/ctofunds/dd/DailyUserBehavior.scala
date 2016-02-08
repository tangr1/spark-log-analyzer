package com.ctofunds.dd

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DailyUserBehavior {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dau")
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val logFile = args(0)
    val sqlContext = new SQLContext(sc)
    val logs = sqlContext.read.json(logFile)
    logs.registerTempTable("dub")

    val activeUsers = sqlContext
      .sql("SELECT requestUser, requestPath FROM dub WHERE requestPath = '/rest/v2/users/active' " +
        "and responseCode = '200' and requestUser IS NOT NULL")
      .map(row => (row.getString(0), row.getString(1)))
      .reduceByKey((x, y) => x)

    val userCount = activeUsers.count()
    println(userCount)

    val result = sqlContext
      .sql("SELECT requestUser, requestPath FROM dub WHERE responseCode = '200' and requestUser IS NOT NULL")
      .map(row => (row.getString(0), row.getString(1)))
      .join(activeUsers)
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .map(x => (x._2, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, (x._2 * 100.0 / userCount).toFloat))
      .collect

    println(result.mkString("\n"))

    sc.stop()
  }
}
