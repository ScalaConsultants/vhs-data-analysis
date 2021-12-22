import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import utils.DateColumnOperations._
import utils.DataEnricherUtil._
import config._

object DataEnricherLogic extends Logging {

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
      generatePartOfDayFromDateTime(col("datetime"), getDefaultTimezoneIfNull(col("timezone"))) as "partOfDay",
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
      generatePartOfDayFromDateTime(col("datetime"), getDefaultTimezoneIfNull(col("timezone"))) as "partOfDay",
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
      generatePartOfDayFromDateTime(col("datetime"), getDefaultTimezoneIfNull(col("timezone"))) as "partOfDay",
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
      .groupBy("userId", "gameId", "date", "partOfDay")
      .agg(
        sum(when(isLevelCompleted, lit(1)).otherwise(lit(0))) as "numLevelsCompleted",
        sum(when(isLevelStarted, lit(1)).otherwise(lit(0))) as "numLevelsStarted"
      )

  def enrichAddsData(addsCleanedDf: DataFrame): DataFrame =
    addsCleanedDf
      .groupBy("userId", "gameId", "date", "partOfDay")
      .agg(
        sum(when(isAddWatched, lit(1)).otherwise(lit(0))) as "numAddsWatched",
        sum(when(isAddIgnored, lit(1)).otherwise(lit(0))) as "numAddsIgnored",  //-- Bug in the game
        sum(when(isAddProposed, lit(1)).otherwise(lit(0))) as "numAddsProposed"  //-- Not implemented yet in the game
      )

  def enrichPurchasesData(purchasesCleanedDf: DataFrame): DataFrame =
    purchasesCleanedDf
      .groupBy("userId", "gameId", "date", "partOfDay")
      .agg(
        sum(when(isPurchaseDone, lit(1)).otherwise(lit(0))) as "numPurchasesDone",
        sum(when(isPurchaseDone, col("amount")).otherwise(lit(0))) as "amountPurchasesDoneDol",
        sum(when(isPurchaseRejected, lit(1)).otherwise(lit(0))) as "numPurchasesRejected",
        sum(when(isPurchaseRejected, col("amount")).otherwise(lit(0))) as "amountPurchasesRejectedDol",
        sum(when(isPurchaseCanceled, lit(1)).otherwise(lit(0))) as "numPurchasesCanceled",
        sum(when(isPurchaseCanceled, col("amount")).otherwise(lit(0))) as "amountPurchasesCanceledDol"
      )

  def enrichVHSDataPerDay(playerInfoData: DataFrame,
                          playerBehaviorData: DataFrame): DataFrame = {

    val playersCleanedDf = cleanPlayersData(playerInfoData)
    val levelsCleanedDf = cleanLevelsData(playerBehaviorData)
    val addsCleanedDf = cleanAddsData(playerBehaviorData)
    val purchasesCleanedDf = cleanPurchasesData(playerBehaviorData)

    val playersEnrichDataDf = enrichPlayersData(playersCleanedDf)
    val levelsEnrichDataDf = enrichLevelsData(levelsCleanedDf)
    val addsEnrichDataDf = enrichAddsData(addsCleanedDf)
    val purchasesEnrichDataDf = enrichPurchasesData(purchasesCleanedDf)

    playersEnrichDataDf.show(5)
    levelsEnrichDataDf.show(5)
    addsEnrichDataDf.show(5)
    purchasesEnrichDataDf.show(5)

    val enrichedPlayersDataDf = levelsEnrichDataDf
      .join(playersEnrichDataDf, Seq("userId", "gameId"), "left_outer")
      .join(addsEnrichDataDf, Seq("userId", "gameId", "date", "partOfDay"), "left_outer")
      .join(purchasesEnrichDataDf, Seq("userId", "gameId", "date", "partOfDay"), "left_outer")

    val enrichedDataPerDayDf = enrichedPlayersDataDf.select(
      col("userId"),
      col("gameId"),
      coalesce(col("flagOrganic"), lit(0)) as "flagOrganic",
      col("dateRegistration"),
      coalesce(col("numLevelsCompleted"), lit(0)) as "numLevelsCompleted",
      coalesce(col("numLevelsStarted"), lit(0)) as "numLevelsStarted",
      coalesce(col("numAddsWatched"), lit(0)) as "numAddsWatched",
      coalesce(col("numAddsIgnored"), lit(0)) as "numAddsIgnored",
      coalesce(col("numAddsProposed"), lit(0)) as "numAddsProposed",
      coalesce(col("numPurchasesDone"), lit(0)) as "numPurchasesDone",
      coalesce(col("amountPurchasesDoneDol"), lit(0)) as "amountPurchasesDoneDol",
      coalesce(col("numPurchasesRejected"), lit(0)) as "numPurchasesRejected",
      coalesce(col("amountPurchasesRejectedDol"), lit(0)) as "amountPurchasesRejectedDol",
      coalesce(col("numPurchasesCanceled"), lit(0)) as "numPurchasesCanceled",
      coalesce(col("amountPurchasesCanceledDol"), lit(0)) as "amountPurchasesCanceledDol",
      col("partOfDay"),
      col("date")
    )

    enrichedDataPerDayDf
  }

  def enrichVHSDataPerMonth(enrichedDataPerDayDf: DataFrame): DataFrame = {
    val enrichedDataPerMonthDf = enrichedDataPerDayDf
      .withColumn("codMonth", generateCodMonthFromDate(col("date")))
      .groupBy("userId", "gameId", "codMonth", "partOfDay")
      .agg(
        max("flagOrganic") as "flagOrganic",
        min("dateRegistration") as "dateRegistration",
        sum("numLevelsCompleted") as "numLevelsCompleted",
        sum("numLevelsStarted") as "numLevelsStarted",
        sum("numAddsWatched") as "numAddsWatched",
        sum("numAddsIgnored") as "numAddsIgnored",
        sum("numAddsProposed") as "numAddsProposed",
        sum("numPurchasesDone") as "numPurchasesDone",
        sum("amountPurchasesDoneDol") as "amountPurchasesDoneDol",
        sum("numPurchasesRejected") as "numPurchasesRejected",
        sum("amountPurchasesRejectedDol") as "amountPurchasesRejectedDol",
        sum("numPurchasesCanceled") as "numPurchasesCanceled",
        sum("amountPurchasesCanceledDol") as "amountPurchasesCanceledDol"
      )

    enrichedDataPerMonthDf
  }

  def enrichVHSDataPerMonth(playerInfoData: DataFrame,
                            playerBehaviorData: DataFrame): DataFrame = {

    val enrichedDataPerDayDf = enrichVHSDataPerDay(playerInfoData, playerBehaviorData)

    enrichVHSDataPerMonth(enrichedDataPerDayDf)
  }

  def enrichVHSData(behavior: Behavior,
                    playerInfoData: DataFrame,
                    playerBehaviorData: DataFrame,
                    outputPath: String): Unit =
    behavior match {
      case Daily =>
        val vhsEnrichedPerDayDf = enrichVHSDataPerDay(playerInfoData, playerBehaviorData)
        saveEnrichedData(vhsEnrichedPerDayDf, Seq("date", "partOfDay"), getDailyResultPath(outputPath))
        vhsEnrichedPerDayDf.show(15)
        log.info(s"EnrichedDataPerDay count: ${vhsEnrichedPerDayDf.count()}")
      case Monthly =>
        val vhsEnrichedPerMonthDf = enrichVHSDataPerMonth(playerInfoData, playerBehaviorData)
        saveEnrichedData(vhsEnrichedPerMonthDf, Seq("codMonth", "partOfDay"), getMonthlyResultPath(outputPath))
        vhsEnrichedPerMonthDf.show(15)
        log.info(s"EnrichedDataPerMonth count: ${vhsEnrichedPerMonthDf.count()}")
      case Both =>
        val vhsEnrichedPerDayDf = enrichVHSDataPerDay(playerInfoData, playerBehaviorData).cache()
        val vhsEnrichedPerMonthDf = enrichVHSDataPerMonth(vhsEnrichedPerDayDf)
        saveEnrichedData(vhsEnrichedPerDayDf, Seq("date", "partOfDay"), getDailyResultPath(outputPath))
        saveEnrichedData(vhsEnrichedPerMonthDf, Seq("codMonth", "partOfDay"), getMonthlyResultPath(outputPath))
        vhsEnrichedPerDayDf.show(15)
        vhsEnrichedPerMonthDf.show(15)
        log.info(s"EnrichedDataPerDay count: ${vhsEnrichedPerDayDf.count()} ,  enrichedDataPerMonth count: ${vhsEnrichedPerMonthDf.count()}")
    }
}