package utils

import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.{Column, DataFrame}

object DataEnricherUtil {

  import DataEnricherConstants._

  def isNewPlayer: Column = col("action") === PLAYER_SIGNUP

  def isPlayerOrganic: Column = col("attribution") === PLAYER_ORGANIC

  def isLevelCompleted: Column = col("status") === LEVEL_COMPLETED

  def isLevelStarted: Column = col("status") === LEVEL_STARTED

  def isAdWatched: Column = col("status") === ADD_WATCHED

  def isAdIgnored: Column = col("status") === ADD_IGNORED

  def isAdProposed: Column = col("status") === ADD_PROPOSED

  def isPurchaseDone: Column = col("status") === PURCHASE_DONE

  def isPurchaseRejected: Column = col("status") === PURCHASE_REJECTED

  def isPurchaseCanceled: Column = col("status") === PURCHASE_CANCELED

  def getHourlyResultPath(outputPath: String): String = s"$outputPath/$FILENAME_HOURLY_RESULT"

  def getDailyResultPath(outputPath: String): String = s"$outputPath/$FILENAME_DAILY_RESULT"

  def getMonthlyResultPath(outputPath: String): String = s"$outputPath/$FILENAME_MONTHLY_RESULT"

  def getDefaultTimezoneIfNull(timezone: Column): Column = coalesce(timezone, lit(DEFAULT_UTC_TIMEZONE))

  def saveDataframeData(dfData: DataFrame, partitions: Seq[String], path: String): Unit =
    dfData
      .write
      .mode("overwrite")
      .partitionBy(partitions: _*)
      .parquet(path)
}
