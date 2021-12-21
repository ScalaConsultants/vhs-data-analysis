package utils

import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.Column
import config.{Behavior, Daily}
import utils.DataAnalyzerConstants._

object DataAnalyzerUtil {
  def transformPartOfDayToNumber(partOfDay: Column): Column = {
    when(partOfDay === "morning", lit(0))
      .when(partOfDay === "afternoon", lit(1))
      .when(partOfDay === "evening", lit(2))
      .when(partOfDay === "night", lit(3))
  }

  def getPartitionSourceFromBehavior(bh: Behavior): String = bh match {
    case Daily => DAILY_PARTITION
    case _ => MONTHLY_PARTITION
  }

  def getFileNameFromBehavior(bh: Behavior): String = bh match {
    case Daily => DAILY_PLAYER_FILE
    case _ => MONTHLY_PLAYER_FILE
  }
}
