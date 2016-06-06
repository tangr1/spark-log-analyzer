package com.ctofunds.dd.zhiyu

import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.DatabaseReader.Builder
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Sorting

object RegisterUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-au")
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

    val rrd = sc.textFile(args(0))
    println(rrd
      .map(_.split(","))
      .map(entry => Reader.parse(entry(1)))
      .filter(_ == 102358)
      .count)

    /*
    val cached = rrd
      .map(_.split(","))
      .map(entry => (entry(0), Reader.parse(entry(1))))
      .cache
    val all = cached
      .map(entry => (entry._1, 1))
      .reduceByKey(_ + _)
      .collectAsMap
    val saudi = cached
      .filter(_._2 == 102358)
      .map(entry => (entry._1, 1))
      .reduceByKey(_ + _)
      .collectAsMap
    var array: Array[String] = Array()
    for ((k,v) <- all) {
      array = array :+ k + "," + v + "," + saudi(k)
    }
    Sorting.stableSort(array, (e1: String, e2: String) => e1.split('-')(1).split(',')(0).toInt < e2.split('-')(1).split(',')(0).toInt)
    println(array.mkString("\n"))
    */

    sc.stop()
  }
}
