package com.ctofunds.dd.zhiyu

case class PageViewLine(tid: String, sid: String, kid: String, lnew: String) {
}

object Line {
  def parseLine(log: String): (PageViewLine) = {
    val result = log.split("\\^A\\^R")
    if (result.isEmpty) {
      throw new RuntimeException("Cannot parse line: " + log)
    }
    PageViewLine(result(1), result(2), result(3), result(4))
  }
}

