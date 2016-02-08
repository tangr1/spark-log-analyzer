package com.ctofunds.dd

import org.apache.spark.{SparkConf, SparkContext}

object RetentionRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-rr")
    val sc = new SparkContext(sparkConf)
    val newUserFile = args(0)
    val activeUserFile = args(1)

    val newUsers = sc.textFile(newUserFile)
      .map(line => (line, 1))
    println("新用户数: " + newUsers.count)
    val retentionUsers= sc.textFile(activeUserFile)
      .map(Line.parseLine)
      .map(line => (line.user, 1))
      .join(newUsers)
    println("留存用户数: " + retentionUsers.count)
    println("留存率: " + retentionUsers.count * 100.0 / newUsers.count)

    sc.stop()
  }
}
