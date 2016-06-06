package com.ctofunds.dd.zhiyu

import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.DatabaseReader.Builder
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap

object RegisterUserAll {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-au")
    val sc = new SparkContext(sparkConf)

    object Reader extends Serializable {
      val database = getClass.getResourceAsStream("/geoip.mmdb")
      val reader: DatabaseReader = new Builder(database).build()
      def parse(ip: String): String = {
        try {
          reader.country(InetAddress.getByName(ip)).getCountry.getName
        } catch {
          case ex: Exception => "unknown"
        }
      }
    }

    val map = sc.textFile(args(0))
      .map(_.split(","))
      .map(entry => (Reader.parse(entry(1)), 1))
      .reduceByKey(_ + _)
      .collectAsMap()
    var sum = 0
    for ((k, v) <- map) {
      sum += v
    }
    var top10 = 0
    val sorted = ListMap(map.toSeq.sortWith(_._2 > _._2):_*).take(10)
    for ((k, v) <- sorted) {
      top10 += v
    }
    println(sorted.keys.mkString(","))
    println(sorted.values.mkString(","))
    println("Other," + (sum - top10))
    println(sum)
    println(top10)

    sc.stop()
  }
}
