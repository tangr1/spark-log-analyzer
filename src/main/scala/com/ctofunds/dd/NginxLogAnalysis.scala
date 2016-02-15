package com.ctofunds.dd

import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.DatabaseReader.Builder
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object NginxLogAnalysis {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dau")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val logFile = args(2)
    val outFile = args(3)
    val sqlContext = new SQLContext(sc)
    val logs = sqlContext.read.json(logFile)
    logs.registerTempTable("dau")

    object Reader extends Serializable {
      val database = getClass.getResourceAsStream("/geoip.mmdb")
      val reader: DatabaseReader = new Builder(database).build()
      def parse(ip: String): Int = {
        try {
          reader.country(InetAddress.getByName(ip)).getCountry.getGeoNameId
        } catch {
          // 可能有极少数IP不在数据库里面, 这里统一归属到美国, 造成的误差应该极小
          case ex: Exception => 6252001
        }
      }
    }

    sqlContext
      .sql("SELECT requestUser, requestIp, userAgent FROM dau WHERE requestPath = '/rest/v2/users/active' " +
        "and responseCode = '200' and requestUser IS NOT NULL")
      .map(row => (row.getString(0), Array(Reader.parse(row.getString(1).stripSuffix(" ")),
        row.getString(2).indexOf("iOS"), row.getString(2).indexOf("Android")).mkString(":")))
      .reduceByKey((x, y) => x)
      .saveAsTextFile(outFile, classOf[BZip2Codec])

    sc.stop()
  }
}
