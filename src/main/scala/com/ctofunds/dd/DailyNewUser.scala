package com.ctofunds.dd

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DailyNewUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dnu")
    val sc = new SparkContext(sparkConf)
    val logFile = args(0)
    val outFile = args(1)
    val sqlContext = new SQLContext(sc)
    val logs = sqlContext.read.json(logFile)
    logs.registerTempTable("dnu")

    sqlContext
      .sql("SELECT extProps.userName FROM dnu WHERE eventType = 'USER_CREATED'")
      .map(row => (row.getString(0), 1))
      .reduceByKey(_ + _)
      .keys
      .saveAsTextFile(outFile, classOf[GzipCodec])

    sc.stop()
  }
}
