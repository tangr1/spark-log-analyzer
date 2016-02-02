package com.example

import java.io.File
import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object NginxLogAnalyzer {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Log Analyzer SQL in Scala")
    val sc = new SparkContext(sparkConf)
    val database = new File("./GeoIP2-City.mmdb")
    val reader = new DatabaseReader.Builder(database).build
    val logFile = args(0)
    val sqlContext = new SQLContext(sc)
    val logs = sqlContext.read.json(logFile)
    logs.registerTempTable("musically")

    /*
    val userCount = sqlContext
      .sql("SELECT requestUser, COUNT(*) FROM musically WHERE requestPath = '/rest/v2/users/active' " +
        "and responseCode = '200' GROUP BY requestUser")
      .map(row => (row.getString(0), row.getLong(1)))
      .count()
    println(userCount)
    val newUserCount = sqlContext
      .sql("SELECT COUNT(*) FROM musically WHERE requestPath = '/rest/v2/users/register' and responseCode = '200'")
      .map(row => row.getLong(0))
      .first()
    println(newUserCount)
    */
    val userCount = sqlContext
      .sql("SELECT requestUser, requestIp FROM musically WHERE requestPath = '/rest/v2/users/active' " +
        "and responseCode = '200' GROUP BY requestUser")
      .map(row => (row.getString(0), reader.country(InetAddress.getByName(row.getString(1))).getCountry.getName))
      .collect()
    println(s"""userCount : ${userCount.mkString("[", ",", "]")}""")

    /*
    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine).toDF()
    accessLogs.registerTempTable("logs")
    sqlContext.cacheTable("logs")

    // Calculate statistics based on the content size.
    val contentSizeStats = sqlContext
      .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
      .first()
    println("Content Size Avg: %s, Min: %s, Max: %s".format(
      contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
      contentSizeStats(2),
      contentSizeStats(3)))

    // Compute Response Code to Count.
    val responseCodeToCount = sqlContext
      .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 1000")
      .map(row => (row.getInt(0), row.getLong(1)))
      .collect()
    println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

    // Any IPAddress that has accessed the server more than 10 times.
    val ipAddresses =sqlContext
      .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 1000")
      .map(row => row.getString(0))
      .collect()
    println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

    val topEndpoints = sqlContext
      .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()
    println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
    */

    sc.stop()
  }
}
