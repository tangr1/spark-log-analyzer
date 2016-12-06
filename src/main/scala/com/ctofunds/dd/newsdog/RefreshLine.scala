package com.ctofunds.dd.newsdog

object RefreshLine {
  val PATTERN = """^\[2016-0\d-(\d{2}).*\{\"category": \"\w+\", \"source": \"\w+\", \"user_seq_id": (\d+), \"article_seq_ids\": \[(.*)\]""".r

  def parseLine(log: String): String = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {
      throw new RuntimeException("Cannot parse line: " + log)
    }
    val m = res.get
    Array(m.group(1).toInt, m.group(2), m.group(3).split(", ").size).mkString(",")
  }
}
