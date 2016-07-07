package com.ctofunds.dd.quanmin

import org.joda.time.format.DateTimeFormat

case class Line(ip: String, time: Int, method: String, path: String, status: Int, bytes: Int, device: Int) {
}

object Line {
  //val PATTERN = """^(\d{1,3}\.\d{1,3}.\d{1,3}.\d{1,3}) - - \[(.*)\] \"(\w{3,6}) (.*) \w{0,4}/\d\.\d\" (\d+) (\d+) "(.*)" "(.*)" (.*)""".r

  def getDeviceType(agent: String): Int = {
    if (agent.indexOf("QuanMinTV") == -1) {
      if (agent.indexOf("Mobile") != -1) {
        2
      } else if (agent.indexOf("Android") != -1) {
        3
      } else {
        1
      }
    } else {
      4
    }
  }

  object Timer extends Serializable {
    val formatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")
    def parse(datetime: String): Int = {
      (formatter.parseDateTime(datetime).getMillis / 1000).toInt
    }
  }

  def parseLine(log: String): Line = {
    /*
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {
      throw new RuntimeException("Cannot parse line: " + log)
    }
    val m = res.get
    Line(m.group(1), Timer.parse(m.group(2)), m.group(3), m.group(4), m.group(5).toInt, m.group(6).toInt, getDeviceType(m.group(8)), m.group(9))
    */
    val fields = log.split(" \\| ")
    var length = 0
    if (fields(6) != "-") {
        length = fields(6).toInt
    }
    if (fields(2).split(" ").length != 3) {
      Line(fields(0), Timer.parse(fields(1)), "", "", fields(3).toInt, length, getDeviceType(fields(8)))
    } else {
      Line(fields(0), Timer.parse(fields(1)), fields(2).split(" ")(0), fields(2).split(" ")(1), fields(3).toInt, length, getDeviceType(fields(8)))
    }
  }
}
