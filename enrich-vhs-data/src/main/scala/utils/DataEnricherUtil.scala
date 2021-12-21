package utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

object DataEnricherUtil {

  import DataEnricherConstants._

  def isNewPlayer: Column = col("action") === PLAYER_SIGNUP

  def isPlayerOrganic: Column = col("attribution") === PLAYER_ORGANIC

  def isLevelCompleted: Column = col("status") === LEVEL_COMPLETED

  def isLevelStarted: Column = col("status") === LEVEL_STARTED

  def isAddWatched: Column = col("status") === ADD_WATCHED

  def isAddIgnored: Column = col("status") === ADD_IGNORED

  def isAddProposed: Column = col("status") === ADD_PROPOSED

  def isPurchaseDone: Column = col("status") === PURCHASE_DONE

  def isPurchaseRejected: Column = col("status") === PURCHASE_REJECTED

  def isPurchaseCanceled: Column = col("status") === PURCHASE_CANCELED

  def getDailyResultPath(outputPath: String): String = s"$outputPath/$FILENAME_DAILY_RESULT"

  def getMonthlyResultPath(outputPath: String): String = s"$outputPath/$FILENAME_MONTHLY_RESULT"

  def saveEnrichedData(enrichedData: DataFrame, partitions: Seq[String], path: String): Unit =
    enrichedData
      .write
      .mode("overwrite")
      .partitionBy(partitions: _*)
      .parquet(path)
}
