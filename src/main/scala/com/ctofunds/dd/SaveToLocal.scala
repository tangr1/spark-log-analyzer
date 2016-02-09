package com.ctofunds.dd

import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.DatabaseReader.Builder
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SaveToLocal {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dau")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(sc.hadoopConfiguration)
    val logFile = args(0)
    val outFile = args(1)
    val sqlContext = new SQLContext(sc)
    val logs = sqlContext.read.json(logFile)
    logs.registerTempTable("dau")

    object Reader extends Serializable {
      val database = getClass.getResourceAsStream("/geoip.mmdb")
      val reader: DatabaseReader = new Builder(database).build()
    }

    sqlContext
      .sql("SELECT requestUser, requestIp, userAgent FROM dau WHERE requestPath = '/rest/v2/users/active' " +
        "and responseCode = '200' and requestUser IS NOT NULL")
      .map(row => (row.getString(0), Array(Reader.reader.country(InetAddress.getByName(row.getString(1).stripSuffix(" ")))
        .getCountry.getGeoNameId, row.getString(2).indexOf("iOS"), row.getString(2).indexOf("Android")).mkString(":")))
      .reduceByKey((x, y) => x)
      .saveAsTextFile(outFile, classOf[BZip2Codec])

    sc.stop()
  }
}
