package com.ctofunds.dd

import org.apache.spark.{SparkConf, SparkContext}

object RetentionRate {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-rr")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val newUserFile = args(2)

    val newUsers = sc.textFile(newUserFile)
      .map(line => (line, 1))
      .cache
    print(newUsers.count)
    for (i <- 3 until args.length) {
      val retentionUsers = sc.textFile(args(i))
        .map(Line.parseLine)
        .map(line => (line._1, 1))
        .join(newUsers)
      print(" | %.2f%%".format(retentionUsers.count * 100.0 / newUsers.count))
      /*
      println("%s retention".format(args(i)))
      println("Retention user:\t%s".format(retentionUsers.count))
      println("Retention rate:\t%.2f%%".format(retentionUsers.count * 100.0 / newUsers.count))
      */
    }
    print(" |\n")

    sc.stop()
  }
}