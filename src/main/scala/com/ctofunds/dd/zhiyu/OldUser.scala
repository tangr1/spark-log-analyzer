package com.ctofunds.dd.zhiyu

import org.apache.spark.{SparkConf, SparkContext}

object OldUser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("zhiyu-au")
    val sc = new SparkContext(sparkConf)

    val one = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(1).toLong >= 1451606400 && entry(1).toLong < 1454284800)
      .map(entry => (entry(0), 1))
      .distinct
      .cache
    val two = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(1).toLong >= 1454284800 && entry(1).toLong < 1456790400)
      .map(entry => (entry(0), 1))
      .distinct
      .cache
    val three = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(1).toLong >= 1456790400 && entry(1).toLong < 1459468800)
      .map(entry => (entry(0), 1))
      .distinct
      .cache
    val four = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(1).toLong >= 1459468800 && entry(1).toLong < 1462060800)
      .map(entry => (entry(0), 1))
      .distinct
      .cache
    val five = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(1).toLong >= 1462060800 && entry(1).toLong < 1464739200)
      .map(entry => (entry(0), 1))
      .distinct
      .cache
    val saudiOne = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(1).toLong >= 1451606400 && entry(1).toLong < 1454284800 && entry(2).toInt == 1876)
      .map(entry => (entry(0), 1))
      .distinct
      .cache
    val saudiTwo = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(1).toLong >= 1454284800 && entry(1).toLong < 1456790400 && entry(2).toInt == 1876)
      .map(entry => (entry(0), 1))
      .distinct
      .cache
    val saudiThree = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(1).toLong >= 1456790400 && entry(1).toLong < 1459468800 && entry(2).toInt == 1876)
      .map(entry => (entry(0), 1))
      .distinct
      .cache
    val saudiFour = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(1).toLong >= 1459468800 && entry(1).toLong < 1462060800 && entry(2).toInt == 1876)
      .map(entry => (entry(0), 1))
      .distinct
      .cache
    val saudiFive = sc.textFile(args(0))
      .map(_.split(","))
      .filter(entry => entry(1).toLong >= 1462060800 && entry(1).toLong < 1464739200 && entry(2).toInt == 1876)
      .map(entry => (entry(0), 1))
      .distinct
      .cache
    println(Array("2月", two.count, two.join(one).count, saudiTwo.count, saudiTwo.join(saudiOne).count).mkString(","))
    println(Array("3月", three.count, three.join(two).count, saudiThree.count, saudiThree.join(saudiTwo).count).mkString(","))
    println(Array("4月", four.count, four.join(three).count, saudiFour.count, saudiFour.join(saudiThree).count).mkString(","))
    println(Array("5月", five.count, five.join(four).count, saudiFive.count, saudiFive.join(saudiFour).count).mkString(","))
    /*
    println(two.join(one).count.toFloat * 100.0 / two.count.toFloat)
    println(three.join(two).count.toFloat * 100.0 / three.count.toFloat)
    println(four.join(three).count.toFloat * 100.0 / four.count.toFloat)
    println(five.join(four).count.toFloat * 100.0 / five.count.toFloat)
    */
  }
}
