package com.ctofunds.dd

import org.apache.spark.{SparkConf, SparkContext}

object RetentionRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-rr")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val newUserFile = args(2)
    val activeUserFile = args(3)

    val newUsers = sc.textFile(newUserFile)
      .map(line => (line, 1))
    println("新用户数:\t%s".format(newUsers.count))
    val retentionUsers= sc.textFile(activeUserFile)
      .map(Line.parseLine)
      .map(line => (line.user, 1))
      .join(newUsers)
    println("留存用户数:\t%s".format(retentionUsers.count))
    println("留存率:\t\t%.2f%%".format(retentionUsers.count * 100.0 / newUsers.count))

    sc.stop()
  }
}
