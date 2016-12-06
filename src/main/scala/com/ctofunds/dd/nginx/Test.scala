package com.ctofunds.dd.nginx

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("nginx")
    val sc = new SparkContext(sparkConf)
    sc.parallelize()

    def parseLine(line: String): (String, (Long, Double)) = {
      val PATTERN = """^\{\"name\": \"(.*)\", \"value\": (.*), \"timestamp\": (.*)\}$""".r
      val res = PATTERN.findFirstMatchIn(line)
      if (res.isEmpty) {
        throw new RuntimeException("Cannot parse line: " + line)
      }
      val m = res.get
      (m.group(1), (m.group(3).toLong, m.group(2).toDouble))
    }

    object Handler extends Serializable {
      def handle(entry: (String, Iterable[(Long, Double)])): Map[Long, Double] = {
        val set = entry._2.map(_._1 / 10).toSet
        var map = entry._2.toMap
        for (i <- 146971080 until 146971441) {
          if (!set.contains(i)) {
            map += (i.toLong * 10L -> 0.0)
          }
        }
        // Logic
        map
      }
    }

    sc.textFile("./data/tmp.json")
      .map(parseLine)
      .groupByKey()
      .foreach(entry => Handler.handle(entry))

    sc.stop()
  }
}