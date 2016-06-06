package com.ctofunds.dd.zhiyu

import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.DatabaseReader.Builder
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.{SparkConf, SparkContext}

object Temp {
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
    println(sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(1).toLong >= 1451606400 && entry(1).toLong < 1454284800)
      .map(entry => (entry(0), 1))
      .distinct
      .count)
      //.map(entry => entry(8).split("%7C")(0))
      //.distinct
      //.foreach(println)
      //.filter(entry => entry(1) == "600" && entry.length > 26)
      //.map(entry => (entry(24), 1))
      //.reduceByKey(_ + _)
      //.foreach(println)
      // 0 session id, 1 cookie id, 2 is new, 3 country, 4 mtc, 5 goods id, 6 order id, 7 timestamp, 8 user id, 9 is register, 10 source
      //.filter(entry => entry(8).split("%7C").length > 3)
      //.map(entry => entry(8).split("%7C")(0))
      //.map(entry => Array(entry(2), entry(3), entry(4), Reader.parse(entry(5)), entry(26), entry(21), entry(23), entry(25).toFloat.toInt, entry(15), entry(24), entry(9).split("%7C")(0)).mkString(","))
      //.saveAsTextFile(args(1), classOf[BZip2Codec])
    sc.stop()
  }
}