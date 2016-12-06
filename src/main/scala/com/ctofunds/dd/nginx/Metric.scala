package com.ctofunds.dd.nginx

import org.uaparser.scala.{Parser}

case class Metric(name: String, value: Double, timestamp: Long)

object Metric {
  val PATTERN = """^\{\"name\": \"(.*)\", \"value\": (.*), \"timestamp\": (.*)\}$""".r

  def parseLine(log: String): Metric = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {
      throw new RuntimeException("Cannot parse line: " + log)
    }
    val m = res.get
    Metric(m.group(1), m.group(2).toDouble, m.group(3).toLong)
  }
}
