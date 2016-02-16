package com.ctofunds.dd

import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RawNewUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dnu")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val logFile = args(2)
    val outFile = args(3)
    val sqlContext = new SQLContext(sc)
    val logs = sqlContext.read.json(logFile)
    logs.registerTempTable("dnu")

    sqlContext
      .sql("SELECT extProps.userName FROM dnu WHERE eventType = 'USER_CREATED'")
      .map(row => (row.getString(0), 1))
      .reduceByKey(_ + _)
      .keys
      .saveAsTextFile(outFile, classOf[BZip2Codec])

    sc.stop()
  }
}
