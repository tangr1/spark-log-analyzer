package com.ctofunds.dd.newsdog

object ReadLine {
  val PATTERN = """^\[2016-0\d-(\d{2}).*\{\"user_seq_id\": (\d+), \"source": \"\w+\", \"article_seq_id": (\d+)""".r

  def parseLine(log: String): String = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {
      throw new RuntimeException("Cannot parse line: " + log)
    }
    val m = res.get
    Array(m.group(1).toInt, m.group(2), m.group(3)).mkString(",")
  }
}
