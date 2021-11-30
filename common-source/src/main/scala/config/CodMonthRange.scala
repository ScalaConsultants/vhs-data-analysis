package config

import scala.util.matching.Regex

final case class CodMonthRange(fromDate: String, toDate: String)

object CodMonthRange {
  val codMonthRegex: Regex = "([0-9]{6})".r
}