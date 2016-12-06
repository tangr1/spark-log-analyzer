package com.ctofunds.dd.newsdog

import org.apache.spark.{SparkConf, SparkContext}

object ArticleDist {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("newsdog")
    val sc = new SparkContext(sparkConf)

    val articleFile = "./data/newsdog/hindi_article.csv"
    val articles = sc.textFile(articleFile)
      .map(entry => entry.split(","))
      .map(entry => (entry(0).toInt, entry(1)))
      .cache

    val result = sc.textFile(args(0))
      .map(entry => entry.split(","))
      .map(entry => (entry(2).toInt, 1))
      .join(articles)
      .map(entry => (entry._2._2, 1))
      .reduceByKey(_ + _)
      .collect

    result.sortBy(_._2)
      .foreach(entry => println(Array(entry._1, entry._2).mkString(",")))

    sc.stop()
  }
}