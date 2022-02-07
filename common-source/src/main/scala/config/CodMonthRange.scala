package config

import scala.util.matching.Regex

final case class CodMonthRange(fromDate: String, toDate: String)

object CodMonthRange {
  val codMonthRegex: Regex = "([0-9]{6})".r

  def createFromOneMonth(oneMonth: String): CodMonthRange = {
    CodMonthRange(oneMonth, oneMonth)
  }

  def getDateRangeFromOpts(mapOpts: Map[String, String]): Either[String, CodMonthRange] = {
    val (fromDateOpt, toDateOpt) = (mapOpts.get("fromDate"), mapOpts.get("toDate"))
    (fromDateOpt, toDateOpt) match {
      case (Some(codMonthRegex(fromCodMonth)), Some(codMonthRegex(toCodMonth))) =>  Right(CodMonthRange(fromCodMonth, toCodMonth))
      case _ => Left("invalid dateRange format")
    }
  }
}