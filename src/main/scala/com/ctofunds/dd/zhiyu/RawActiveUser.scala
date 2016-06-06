package com.ctofunds.dd.zhiyu

import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.DatabaseReader.Builder
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.{SparkConf, SparkContext}

object RawActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-pv")
    val sc = new SparkContext(sparkConf)

    object Reader extends Serializable {
      val database = getClass.getResourceAsStream("/geoip.mmdb")
      val reader: DatabaseReader = new Builder(database).build()
      def parse(ip: String): Int = {
        try {
          reader.country(InetAddress.getByName(ip)).getCountry.getGeoNameId
        } catch {
          case ex: Exception => -1
        }
      }
    }

    /*
    object Timer extends Serializable {
      val formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
      def parse(datetime: String): Int = {
        (formatter.parseDateTime(datetime).getMillis / 1000).toInt
      }
    }
    */

    // HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val count = sc.textFile(args(0))
      .map(_.split("\\^A\\^R"))
      //.filter(entry => entry(1) == "600" && entry.length > 26 )
      .filter(entry => entry(1) == "600" && entry.length > 26)
      .filter(entry => entry(4) != "0")
      // 0 session id, 1 cookie id, 2 is new, 3 country, 4 mtc, 5 goods id, 6 order id, 7 timestamp, 8 user id, 9 is register
      .map(entry => entry(3))
      .distinct
      .count
    println(count)

    sc.stop()
  }
}