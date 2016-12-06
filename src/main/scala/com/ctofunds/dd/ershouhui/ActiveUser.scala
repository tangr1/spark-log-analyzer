package com.ctofunds.dd.ershouhui

import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader.Builder
import com.maxmind.geoip2.DatabaseReader
import org.apache.spark.{SparkConf, SparkContext}
import org.uaparser.scala.Parser

object ActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ershouhui")
    val sc = new SparkContext(sparkConf)

    object AgentParser extends Serializable {
      def parse(agent: String): String = {
          Parser.get.parse(agent).userAgent.family
      }
    }

    object Reader extends Serializable {
      val database = getClass.getResourceAsStream("/geoip-city.mmdb")
      val reader: DatabaseReader = new Builder(database).build()
      def parse(ip: String): String = {
        reader.city(InetAddress.getByName(ip)).getCity.getName
      }
    }

    /*
    val cache = sc.textFile(args(0))
      .map(_.split(" "))
      .map(entry => (entry(8), 1))
      .cache
      */

    /*
    println(cache
      .map(_._1)
      .distinct
      .count)
      */

    //cache.map(_._2).distinct.foreach(println)

    /*
    cache.reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(20)
      .foreach(println)
      */

    /*
  cache
    .reduceByKey(_ + _)
    //.filter(entry => entry._2 == 1)
    .map(_._1)
    .filter(entry => Reader.parse(entry) == "Beijing")
    .foreach(println)
    .distinct
    .map(entry => (Reader.parse(entry), 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .take(20)
    .foreach(println)
    */

    println(sc.textFile(args(0))
      .map(_.split(" "))
      .map(entry => (entry(8), entry(9).indexOf("spider")))
      .filter(entry => entry._2 != -1)
      .map(_._1)
      .count)
    println(sc.textFile(args(0))
      .map(_.split(" "))
      .map(entry => (entry(8), entry(9).indexOf("Spider")))
      .filter(entry => entry._2 != -1)
      .map(_._1)
      .count)
    println(sc.textFile(args(0))
      .map(_.split(" "))
      .map(entry => entry(8))
      .count)
    sc.stop()
  }
}
