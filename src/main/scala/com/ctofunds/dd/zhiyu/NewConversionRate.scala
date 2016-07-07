package com.ctofunds.dd.zhiyu
import java.net.InetAddress

import com.ctofunds.dd.util.Util
import com.maxmind.geoip2.DatabaseReader.Builder
import com.maxmind.geoip2.DatabaseReader
import org.apache.spark.{SparkConf, SparkContext}

object NewConversionRate {
  def main(args: Array[String]) {
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

    val sparkConf = new SparkConf().setAppName("zhiyu-au")
    val sc = new SparkContext(sparkConf)

    val active = sc.textFile("./data/zhiyu/active/" + args(0))
      .distinct
      .count
    val detail = sc.textFile("./data/zhiyu/detail/" + args(0))
      .distinct
      .count
    val open = sc.textFile("./data/zhiyu/new/" + args(0))
      .distinct
      .count
    val order = sc.textFile("./data/zhiyu/order/" + args(0))
      .distinct
      .count
    val quality = sc.textFile("./data/zhiyu/quality/" + args(0))
      .distinct
      .count
    val register = sc.textFile("./data/zhiyu/register_users.csv")
      .map(_.split(","))
      .filter(entry => entry(0) == args(0))
      //.map(entry => Reader.parse(entry(1)))
      .count
    val pay = sc.textFile("./data/zhiyu/pay_users.csv")
      .map(_.split(","))
      .filter(entry => entry(0) == args(0))
      //.map(entry => Reader.parse(entry(1)))
      .count
    val array = Array(
      args(0),
      Util.percent(open, active),
      Util.percent(register, active),
      Util.percent(detail, active),
      Util.percent(quality, active),
      Util.percent(order, active),
      Util.percent(pay, active)
    )
    println(array.mkString(","))

    sc.stop()
  }
}
