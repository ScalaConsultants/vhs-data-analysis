package utils

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object DataEnricherUtil {
  import utils.DataEnricherConstants._

  def createFilterBetweenMonths(date: Column, fromCodMonth: String, toCodMonth :String): Column = {
    val codMonthFromTimestamp = generateCodMonthFromDate(date)
    (codMonthFromTimestamp >= fromCodMonth) && (codMonthFromTimestamp <= toCodMonth)
  }

  def generateDateFromTimestamp(timestamp: Column): Column =
    to_date(from_unixtime(timestamp/1000,"yyyy-MM-dd"))

  def generateCodMonthFromDate(date: Column): Column =
    date_format(date, "yyyyMM")

  def isNewPlayer: Column = col("action") === PLAYER_SIGNUP

  def isPlayerOrganic: Column = col("attribution") === PLAYER_ORGANIC

  def isLevelCompleted: Column = col("status") === LEVEL_COMPLETED

  def isLevelStarted: Column = col("status") === LEVEL_STARTED

  def isAddWatched: Column = col("status") === ADD_WATCHED

  def isAddIgnored: Column = col("status") === ADD_IGNORED

  def isAddProposed: Column = col("status") === ADD_PROPOSED

  def isPurchaseDone: Column = col("status")=== PURCHASE_DONE

  def isPurchaseRejected: Column = col("status")=== PURCHASE_REJECTED

  def isPurchaseCanceled: Column = col("status")=== PURCHASE_CANCELED

  def cleanPlayersData(playerInfoDf: DataFrame): DataFrame = {
    val playersEvents = playerInfoDf.where(col("attribution").isNotNull)
    playersEvents.select(
      trim(col("userId")) as "userId",
      trim(col("gameId")) as "gameId",
      trim(col("attribution")) as "attribution",
      trim(col("action")) as "action",
      col("date")
    )
  }

  def cleanLevelsData(playerBehaviorDf: DataFrame): DataFrame = {
    val levelsEvents = playerBehaviorDf.where(col("levelId").isNotNull)
    levelsEvents.select(
      trim(col("userId")) as "userId",
      trim(col("gameId")) as "gameId",
      trim(col("levelId")) as "levelId",
      col("levelDifficulty"),
      col("levelProgress"),
      trim(col("status")) as "status",
      col("date")
    )
  }

  def cleanAddsData(playerBehaviorDf: DataFrame): DataFrame = {
    val addsEvents = playerBehaviorDf.where(col("adType").isNotNull)
    addsEvents.select(
      trim(col("userId")) as "userId",
      trim(col("gameId")) as "gameId",
      col("placementId"),
      trim(col("adType")) as "adType",
      trim(col("status")) as "status",
      col("date")
    )
  }

  def cleanPurchasesData(playerBehaviorDf: DataFrame): DataFrame = {
    val purchasesEvents =  playerBehaviorDf.where(col("amount").isNotNull)
    purchasesEvents.select(
      trim(col("userId")) as "userId",
      trim(col("gameId")) as "gameId",
      trim(col("itemId")) as "itemId",
      trim(col("category")) as "category",
      col("amount"),
      trim(col("currency")) as "currency",
      trim(col("status")) as "status",
      col("date")
    )
  }

  def enrichPlayersData(playersCleanedDf: DataFrame): DataFrame =
    playersCleanedDf
      .where(isNewPlayer)
      .groupBy("userId","gameId")
      .agg(
        max(when(isPlayerOrganic, lit(1)).otherwise(lit(0))) as "flagOrganic",
        min("date") as "dateRegistration"
      )

  def enrichLevelsData(levelsCleanedDf: DataFrame): DataFrame =
    levelsCleanedDf
      .groupBy("userId", "gameId", "date")
      .agg(
        sum(when(isLevelCompleted, lit(1)).otherwise(lit(0))) as "numLevelsCompleted",
        sum(when(isLevelStarted, lit(1)).otherwise(lit(0))) as "numLevelsStarted"
      )

  def enrichAddsData(addsCleanedDf: DataFrame): DataFrame =
    addsCleanedDf
      .groupBy("userId", "gameId", "date")
      .agg(
        sum(when(isAddWatched, lit(1)).otherwise(lit(0))) as "numAddsWatched",
        sum(when(isAddIgnored, lit(1)).otherwise(lit(0))) as "numAddsIgnored",  //-- Bug in the game
        sum(when(isAddProposed, lit(1)).otherwise(lit(0))) as "numAddsProposed"  //-- Not implemented yet in the game
      )

  def enrichPurchasesData(purchasesCleanedDf: DataFrame): DataFrame =
    purchasesCleanedDf
      .groupBy("userId", "gameId", "date")
      .agg(
        sum(when(isPurchaseDone, lit(1)).otherwise(lit(0))) as "numPurchasesDone",
        sum(when(isPurchaseDone, col("amount")).otherwise(lit(0))) as "amountPurchasesDoneDol",
        sum(when(isPurchaseRejected, lit(1)).otherwise(lit(0))) as "numPurchasesRejected",
        sum(when(isPurchaseRejected, col("amount")).otherwise(lit(0))) as "amountPurchasesRejectedDol",
        sum(when(isPurchaseCanceled, lit(1)).otherwise(lit(0))) as "numPurchasesCanceled",
        sum(when(isPurchaseCanceled, col("amount")).otherwise(lit(0))) as "amountPurchasesCanceledDol"
      )
}

object DataEnricherConstants {
  val PLAYER_SIGNUP = "SIGN_UP"
  val PLAYER_ORGANIC = "organic"

  val LEVEL_COMPLETED = "COMPLETED"
  val LEVEL_STARTED = "STARTED"

  val ADD_WATCHED = "WATCHED"
  val ADD_IGNORED = "IGNORED"
  val ADD_PROPOSED = "PROPOSED"

  val PURCHASE_DONE = "DONE"
  val PURCHASE_REJECTED = "REJECTED"
  val PURCHASE_CANCELED = "CANCELED"
}