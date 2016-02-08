package com.ctofunds.dd

import org.apache.spark.{SparkConf, SparkContext}

object DailyActiveUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dau")
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val logFile = args(0)
    val usCode = 6252001
    val rrd = sc.textFile(logFile)

    val context = rrd
      .map(Line.parseLine)
    println("活跃用户数: " + context.count())
    println("美国活跃用户数: " + context.filter(_.country == usCode).count())
    println("非美国活跃用户数: " + context.filter(_.country != usCode).count())
    println("iOS活跃用户数: " + context.filter(_.ios != -1).count())
    println("Android活跃用户数: " + context.filter(_.android != -1).count())

    /*
    val sqlContext = new SQLContext(sc)
    val logs = sqlContext.read.json(logFile)
    val usCode = 6252001
    logs.registerTempTable("dau")

    object Reader extends Serializable {
      val database = getClass.getResourceAsStream("/geoip.mmdb")
      val reader: DatabaseReader = new Builder(database).build()
    }

    val userCount = sqlContext
      .sql("SELECT requestUser, COUNT(*) FROM dau WHERE requestPath = '/rest/v2/users/active' " +
        "and responseCode = '200' and requestUser IS NOT NULL GROUP BY requestUser")
      .map(row => (row.getString(0), row.getLong(1)))
      .count()
    println(userCount)

    val newUserCount = sqlContext
      .sql("SELECT COUNT(*) FROM dau WHERE requestPath = '/rest/v2/users/register' and responseCode = '200'")
      .map(row => row.getLong(0))
      .first()
    println(newUserCount)
    val ips = sqlContext
      .sql("SELECT requestUser, requestIp FROM dau WHERE requestPath = '/rest/v2/users/active' " +
        "and responseCode = '200' AND requestUser IS NOT NULL AND requestIp IS NOT NULL")
      .map(row => (row.getString(0), Reader.reader.country(InetAddress.getByName(row.getString(1).stripSuffix(" ")))
        .getCountry.getGeoNameId))
      .reduceByKey((x, y) => x)
      .values.cache()
    println(ips.count())
    println(ips.filter(_ == usCode).count())
    println(ips.filter(_ != usCode).count())

    val agents = sqlContext
      .sql("SELECT requestUser FROM dau WHERE requestPath = '/rest/v2/users/active' " +
        "and responseCode = '200' AND requestUser IS NOT NULL AND userAgent LIKE '%iOS%'")
      .map(row => (row.getString(0), 1))
      .reduceByKey((x, y) => x)
    println(agents.count())
    */

    sc.stop()
  }
}
