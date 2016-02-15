package com.ctofunds.dd

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object UserBehavior {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dau")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val logFile = args(2)
    val sqlContext = new SQLContext(sc)
    val logs = sqlContext.read.json(logFile)
    logs.registerTempTable("dub")

    val activeUsers = sqlContext
      .sql("SELECT requestUser FROM dub WHERE requestPath = '/rest/v2/users/active' " +
        "and responseCode = '200' and requestUser IS NOT NULL")
      .map(row => (row.getString(0), 1))
      .distinct()

    val userCount = activeUsers.count()
    println(userCount)

    val cachedResult = sqlContext
      .sql("SELECT requestUser FROM dub WHERE responseCode = '200' and requestUser IS NOT NULL")
      .map(row => (row.getString(0), 1))
      .join(activeUsers)
      .mapValues(_ => 1L)
      .reduceByKey(_ + _)
      .map(x => (x._2, 1))
      .reduceByKey(_ + _)
      .cache()

    val result = cachedResult
      .map(x => (x._1, (x._2 * 100.0 / userCount).formatted("%.2f%%")))
      .sortByKey()
      .collect

    println(result.mkString("\n"))

    sc.stop()
  }
}
