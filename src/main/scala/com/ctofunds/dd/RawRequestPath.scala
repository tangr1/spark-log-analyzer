package com.ctofunds.dd

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RawRequestPath {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-drp")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val logFile = args(2)
    val outFile = args(3)
    val sqlContext = new SQLContext(sc)
    val logs = sqlContext.read.json(logFile)
    logs.registerTempTable("drp")

    sqlContext
      .sql("SELECT requestUser, requestPath FROM drp WHERE responseCode = '200' AND requestUser IS NOT NULL " +
        "AND requestUser NOT IN('-1','-2','-3')")
      .map(row => row.getString(0) + "," + row.getString(1))
      .saveAsTextFile(outFile, classOf[BZip2Codec])

    sc.stop()
  }
}
