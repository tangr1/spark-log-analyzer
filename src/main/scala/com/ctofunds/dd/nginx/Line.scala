package com.ctofunds.dd.nginx

import org.uaparser.scala.{Client, Parser}

case class Line(ip: String, time: String, request: String, status: Int, bytes: Int, referer: String, agent: Client, forward: String) {
}

object Line {
  val PATTERN = """^(\d{1,3}\.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(.*)\] \"(.*)\" (\d+) (\d+) \"(.*)\" \"(.*)\" \"(.*)\"""".r

  def parseLine(log: String): Line = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {
      throw new RuntimeException("Cannot parse line: " + log)
    }
    val m = res.get
    Line(m.group(1), m.group(2), m.group(3), m.group(4).toInt, m.group(5).toInt, m.group(6), Parser.get.parse(m.group(7)), m.group(8))
  }
}
