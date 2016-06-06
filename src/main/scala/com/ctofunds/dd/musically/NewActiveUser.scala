package com.ctofunds.dd.musically

import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.DatabaseReader.Builder
import org.apache.spark.{SparkConf, SparkContext}

object NewActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dau")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val usCode = 6252001

    object Reader extends Serializable {
      val database = getClass.getResourceAsStream("/geoip.mmdb")
      val reader: DatabaseReader = new Builder(database).build()
      def parse(ip: String): Int = {
        try {
          reader.country(InetAddress.getByName(ip)).getCountry.getGeoNameId
        } catch {
          // 可能有极少数IP不在数据库里面, 这里统一归属到美国, 造成的误差应该极小
          case ex: Exception => usCode + 1
        }
      }
    }

    for (i <- 2 until args.length) {
      val logFile = args(i)
      val rrd = sc.textFile(logFile)
      val context = rrd
        .map(_.split(","))
        .map(row => (row(0), Reader.parse(row(1)), row(2).indexOf("iOS"), row(2).indexOf("Android")))
        .cache
      val count = context.count
      val usIosCount = context.filter(x => x._2 == usCode && x._3 != -1).count
      val usAndroidCount = context.filter(x => x._2 == usCode && x._4 != -1).count
      val usCount = context.filter(_._2 == usCode).count
      val nonUsIosCount = context.filter(x => x._2 != usCode && x._3 != -1).count
      val nonUsAndroidCount = context.filter(x => x._2 != usCode && x._4 != -1).count
      val nonUsCount = context.filter(_._2 != usCode).count
      val iosCount = context.filter(_._3 != -1).count
      val androidCount = context.filter(_._4 != -1).count
      println("=== %s Active User ===".format(logFile))
      println("iOS user:\t\t%s (%.2f%%)".format(iosCount, iosCount * 100.0 / count))
      println("Android user:\t\t%s (%.2f%%)".format(androidCount, androidCount * 100.0 / count))
      println("US user:\t\t%s (%.2f%%)".format(usCount, usCount * 100.0 / count))
      println("Non US user:\t\t%s (%.2f%%)".format(nonUsCount, nonUsCount * 100.0 / count))
      println("Active user:\t\t" + count)
      println("US iOS user:\t\t%s (%.2f%%)".format(usIosCount, usIosCount * 100.0 / count))
      println("Non US iOS user:\t%s (%.2f%%)".format(nonUsIosCount, nonUsIosCount * 100.0 / count))
      println("US Android user:\t%s (%.2f%%)".format(usAndroidCount, usAndroidCount * 100.0 / count))
      println("Non US Android user:\t%s (%.2f%%)".format(nonUsAndroidCount, nonUsAndroidCount * 100.0 / count))
      println(" %s | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) | %s (%.2f%%) |"
        .format(
          count,
          iosCount, iosCount * 100.0 / count, androidCount, androidCount * 100.0 / count,
          usCount, usCount * 100.0 / count, nonUsCount, nonUsCount * 100.0 / count,
          usIosCount, usIosCount * 100.0 / count, nonUsIosCount, nonUsIosCount * 100.0 / count,
          usAndroidCount, usAndroidCount * 100.0 / count, nonUsAndroidCount, nonUsAndroidCount * 100.0 / count
        ))
    }

    sc.stop()
  }
}
