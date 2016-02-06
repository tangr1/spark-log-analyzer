package com.ctofunds.dd

case class Line(user: String, country: Int, ios: Int, android: Int) {
}

object Line {
  val PATTERN = """^\((\S+),(\d+):(-?\d+):(-?\d+)\)""".r

  def parseLine(log: String): Line = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {
      throw new RuntimeException("Cannot parse line: " + log)
    }
    val m = res.get
    Line(m.group(1), m.group(2).toInt, m.group(3).toInt, m.group(4).toInt)
  }
}

