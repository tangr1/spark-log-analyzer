package com.ctofunds.dd.newsdog

import com.ctofunds.dd.util.Util
import org.apache.spark.{SparkConf, SparkContext}

object CategoryDist {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("newsdog")
    val sc = new SparkContext(sparkConf)

    val categories = sc.textFile(args(0))
      .map(entry => entry.split(","))
      .filter(entry => entry.size == 2)
      .map(entry => (entry(0).toInt, entry(1)))
      .cache

    println(categories.count)

    val result = sc.textFile(args(1))
      .map(entry => entry.split(","))
      .map(entry => (entry(2).toInt, 1))
      .join(categories)
      .map(entry => (entry._2._2, 1))
      .groupByKey()
    /*
      .reduceByKey(_ + _)
      .collect

    val sum = result.map(_._2).sum

    result.sortBy(_._2)
      .reverse
      .foreach(entry => println(Array(entry._1, Util.ratio(entry._2, 10000, 2) + "万次", Util.percent(entry._2, sum) + "%").mkString(",")))
    */

    sc.stop()
  }
}