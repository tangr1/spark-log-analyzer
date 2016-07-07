package com.ctofunds.dd.util

object Util {
  def percent(numerator: Int, denominator: Int): Double = {
    BigDecimal(numerator.toDouble * 100.0 / denominator.toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def percent(numerator: Long, denominator: Long): Double = {
    BigDecimal(numerator.toDouble * 100.0 / denominator.toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}
