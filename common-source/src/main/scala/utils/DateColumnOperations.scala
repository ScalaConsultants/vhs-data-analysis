package utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object DateColumnOperations {
  def createFilterBetweenCodMonths(codMonth: Column, fromCodMonth: String, toCodMonth :String): Column =
    (codMonth >= fromCodMonth) && (codMonth <= toCodMonth)

  def createFilterBetweenDates(date: Column, fromCodMonth: String, toCodMonth :String): Column = {
    val codMonthFromDate = generateCodMonthFromDate(date)
    createFilterBetweenCodMonths(codMonthFromDate, fromCodMonth, toCodMonth)
  }

  def generateDateTimeFromTimestamp(timestamp: Column): Column = from_unixtime(timestamp/1000,"yyyy-MM-dd HH:mm:ss")

  def generateDateFromDateTime(datetime: Column): Column = to_date(datetime,"yyyy-MM-dd HH:mm:ss")

  def generateDateFromTimestamp(timestamp: Column): Column = generateDateFromDateTime(generateDateTimeFromTimestamp(timestamp))

  def generateCodMonthFromDate(date: Column): Column =
    date_format(date, "yyyyMM")

  def generatePartOfDayFromDateTime(datetime: Column, timezone: Column): Column = {
    val datetimeWithTimezone = from_utc_timestamp(datetime, timezone)
    val hourOfDay = hour(datetimeWithTimezone)
    when((hourOfDay>=5) && (hourOfDay<12), lit("morning"))
      .when((hourOfDay>=12) && (hourOfDay<17), lit("afternoon"))
      .when((hourOfDay>=17) && (hourOfDay<21), lit("evening"))
      .when((hourOfDay>=21) || (hourOfDay<5), lit("night"))
  }
}