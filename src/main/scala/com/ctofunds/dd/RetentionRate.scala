package com.ctofunds.dd

import org.apache.spark.{SparkConf, SparkContext}

object RetentionRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-rr")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val newUserFile = args(2)
    val activeUserFile = args(3)
    val usCode = 6252001

    val newUsers = sc.textFile(newUserFile).map(line => (line, 1)).cache
    val activeUsers = sc.textFile(activeUserFile)
        .map(Line.parseLine)
        .cache
    val usUsers = newUsers.join(activeUsers.filter(_._2.country == usCode))
      .join(newUsers)
    val nonUsUsers = newUsers.join(activeUsers.filter(_._2.country != usCode))
      .join(newUsers)
    print(newUsers.count)
    for (i <- 4 until args.length) {
      val retentionUsers = sc.textFile(args(i))
        .map(Line.parseLine)
        .map(line => (line._1, 1))
        .join(newUsers)
      print(" | %.2f%%".format(retentionUsers.count * 100.0 / newUsers.count))
    }
    print(" |\n")
    print(usUsers.count)
    for (i <- 4 until args.length) {
      val retentionUsers = sc.textFile(args(i))
        .map(Line.parseLine)
        .map(line => (line._1, 1))
        .join(usUsers)
      print(" | %.2f%%".format(retentionUsers.count * 100.0 / usUsers.count))
    }
    print(" |\n")
    print(nonUsUsers.count)
    for (i <- 4 until args.length) {
      val retentionUsers = sc.textFile(args(i))
        .map(Line.parseLine)
        .map(line => (line._1, 1))
        .join(nonUsUsers)
      print(" | %.2f%%".format(retentionUsers.count * 100.0 / nonUsUsers.count))
    }
    print(" |\n")

    sc.stop()
  }
}