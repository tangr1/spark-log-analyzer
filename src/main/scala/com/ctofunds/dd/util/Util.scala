package com.ctofunds.dd.util

object Util {
  def ratio(numerator: Int, denominator: Int, scale: Int): Double = {
    BigDecimal(numerator.toDouble / denominator.toDouble).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def percent(numerator: Int, denominator: Int): Double = {
    BigDecimal(numerator.toDouble * 100.0 / denominator.toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def percent(numerator: Long, denominator: Long): Double = {
    BigDecimal(numerator.toDouble * 100.0 / denominator.toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}
