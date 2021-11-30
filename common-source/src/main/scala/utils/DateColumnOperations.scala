package utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{date_format, from_unixtime, to_date}

object DateColumnOperations {
  def createFilterBetweenCodMonths(codMonth: Column, fromCodMonth: String, toCodMonth :String): Column =
    (codMonth >= fromCodMonth) && (codMonth <= toCodMonth)

  def createFilterBetweenDates(date: Column, fromCodMonth: String, toCodMonth :String): Column = {
    val codMonthFromTimestamp = generateCodMonthFromDate(date)
    createFilterBetweenCodMonths(codMonthFromTimestamp, fromCodMonth, toCodMonth)
  }

  def generateDateFromTimestamp(timestamp: Column): Column =
    to_date(from_unixtime(timestamp/1000,"yyyy-MM-dd"))

  def generateCodMonthFromDate(date: Column): Column =
    date_format(date, "yyyyMM")
}
