package com.ctofunds.dd.musically

import org.apache.spark.{SparkConf, SparkContext}

object UserBehavior {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dau")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val activeUserFile = args(2)
    val requestPathFile = args(3)

    val userCount = sc.textFile(activeUserFile)
      .map(Line.parseLine)
      .map(line => (line._1, 1))
      .count
    val rawResult = sc.textFile(requestPathFile)
      .map(line => (line.split(",")(0), 1))
      .filter(_._1 != " ")
      .mapValues(_ => 1L)
      .reduceByKey(_ + _)
      .cache
    val topUsers = rawResult
      .sortBy(_._2, ascending = false)
      .take(10)
    println(topUsers.mkString("\n"))
    val distribution = rawResult
      .map(x => (x._2, 1))
      .reduceByKey(_ + _)
      .cache
    /*
    val result = distribution
      .sortByKey()
      .map(x => (x._1, "%s %.2f%%".format(x._2, x._2 * 100.0 / userCount)))
      .collect
    println(result.mkString("\n"))
    */
    print(userCount)
    print(" | %.2f%%".format(distribution.filter(x => x._1 == 1).values.sum * 100.0 / userCount))
    print(" | %.2f%%".format(distribution.filter(x => x._1 == 2).values.sum * 100.0 / userCount))
    print(" | %.2f%%".format(distribution.filter(x => x._1 == 3).values.sum * 100.0 / userCount))
    print(" | %.2f%%".format(distribution.filter(x => x._1 == 4).values.sum * 100.0 / userCount))
    print(" | %.2f%%".format(distribution.filter(x => x._1 == 5).values.sum * 100.0 / userCount))
    print(" | %.2f%%".format(distribution.filter(x => x._1 > 5 && x._1 < 11).values.sum * 100.0 / userCount))
    print(" | %.2f%%".format(distribution.filter(x => x._1 > 10 && x._1 < 101).values.sum * 100.0 / userCount))
    print(" | %.2f%%".format(distribution.filter(x => x._1 > 100 && x._1 < 1001).values.sum * 100.0 / userCount))
    print(" | %.2f%%".format(distribution.filter(x => x._1 > 1000 && x._1 < 10001).values.sum * 100.0 / userCount))
    print(" | %.2f%%".format(distribution.filter(x => x._1 > 10000).values.sum * 100.0 / userCount))
    print(" |")

    sc.stop()
  }
}
