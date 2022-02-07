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
      col("timezone") as "timezone",
      getHourOfDayFromDateTime(col("datetime"), col("timezone")) as "hourOfDay",
      col("date")
    )
  }

  def cleanAdsData(playerBehaviorDf: DataFrame): DataFrame = {
    val adsEvents = playerBehaviorDf.where(col("adType").isNotNull)
    adsEvents.select(
      trim(col("userId")) as "userId",
      trim(col("gameId")) as "gameId",
      col("placementId"),
      trim(col("adType")) as "adType",
      trim(col("status")) as "status",
      col("timezone") as "timezone",
      getHourOfDayFromDateTime(col("datetime"), col("timezone")) as "hourOfDay",
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
      col("timezone") as "timezone",
      getHourOfDayFromDateTime(col("datetime"), col("timezone")) as "hourOfDay",
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
      .groupBy("userId", "gameId", "date", "hourOfDay")
      .agg(
        sum(when(isLevelCompleted, lit(1)).otherwise(lit(0))) as "numLevelsCompleted",
        sum(when(isLevelStarted, lit(1)).otherwise(lit(0))) as "numLevelsStarted"
      )

  def enrichAdsData(adsCleanedDf: DataFrame): DataFrame =
    adsCleanedDf
      .groupBy("userId", "gameId", "date", "hourOfDay")
      .agg(
        sum(when(isAdWatched, lit(1)).otherwise(lit(0))) as "numAdsWatched",
        sum(when(isAdIgnored, lit(1)).otherwise(lit(0))) as "numAdsIgnored",  //-- Bug in the game
        sum(when(isAdProposed, lit(1)).otherwise(lit(0))) as "numAdsProposed"  //-- Not implemented yet in the game
      )

  def enrichPurchasesData(purchasesCleanedDf: DataFrame): DataFrame =
    purchasesCleanedDf
      .groupBy("userId", "gameId", "date", "hourOfDay")
      .agg(
        sum(when(isPurchaseDone, lit(1)).otherwise(lit(0))) as "numPurchasesDone",
        sum(when(isPurchaseDone, col("amount")).otherwise(lit(0))) as "amountPurchasesDoneDol",
        sum(when(isPurchaseRejected, lit(1)).otherwise(lit(0))) as "numPurchasesRejected",
        sum(when(isPurchaseRejected, col("amount")).otherwise(lit(0))) as "amountPurchasesRejectedDol",
        sum(when(isPurchaseCanceled, lit(1)).otherwise(lit(0))) as "numPurchasesCanceled",
        sum(when(isPurchaseCanceled, col("amount")).otherwise(lit(0))) as "amountPurchasesCanceledDol"
      )

  def enrichVHSDataPerHourOfDay(playerInfoData: DataFrame,
                           playerBehaviorData: DataFrame): DataFrame = {

    val playersCleanedDf = cleanPlayersData(playerInfoData)
    val levelsCleanedDf = cleanLevelsData(playerBehaviorData)
    val adsCleanedDf = cleanAdsData(playerBehaviorData)
    val purchasesCleanedDf = cleanPurchasesData(playerBehaviorData)

    saveDataframeData(playersCleanedDf, Seq("date"), "data/output/raw-data/playerEvent")
    saveDataframeData(levelsCleanedDf, Seq("date"), "data/output/raw-data/levelEvent")
    saveDataframeData(adsCleanedDf, Seq("date"), "data/output/raw-data/adEvent")
    saveDataframeData(purchasesCleanedDf, Seq("date"), "data/output/raw-data/purchaseEvent")

    val playersEnrichDataDf = enrichPlayersData(playersCleanedDf)
    val levelsEnrichDataDf = enrichLevelsData(levelsCleanedDf)
    val adsEnrichDataDf = enrichAdsData(adsCleanedDf)
    val purchasesEnrichDataDf = enrichPurchasesData(purchasesCleanedDf)

    val enrichedPlayersDataDf = levelsEnrichDataDf
      .join(playersEnrichDataDf, Seq("userId", "gameId"), "left_outer")
      .join(adsEnrichDataDf, Seq("userId", "gameId", "date", "hourOfDay"), "left_outer")
      .join(purchasesEnrichDataDf, Seq("userId", "gameId", "date", "hourOfDay"), "left_outer")

    val enrichedDataPerHourOfDayDf = enrichedPlayersDataDf.select(
      col("userId"),
      col("gameId"),
      coalesce(col("flagOrganic"), lit(0)) as "flagOrganic",
      col("dateRegistration"),
      coalesce(col("numLevelsCompleted"), lit(0)) as "numLevelsCompleted",
      coalesce(col("numLevelsStarted"), lit(0)) as "numLevelsStarted",
      coalesce(col("numAdsWatched"), lit(0)) as "numAdsWatched",
      coalesce(col("numAdsIgnored"), lit(0)) as "numAdsIgnored",
      coalesce(col("numAdsProposed"), lit(0)) as "numAdsProposed",
      coalesce(col("numPurchasesDone"), lit(0)) as "numPurchasesDone",
      coalesce(col("amountPurchasesDoneDol"), lit(0)) as "amountPurchasesDoneDol",
      coalesce(col("numPurchasesRejected"), lit(0)) as "numPurchasesRejected",
      coalesce(col("amountPurchasesRejectedDol"), lit(0)) as "amountPurchasesRejectedDol",
      coalesce(col("numPurchasesCanceled"), lit(0)) as "numPurchasesCanceled",
      coalesce(col("amountPurchasesCanceledDol"), lit(0)) as "amountPurchasesCanceledDol",
      col("hourOfDay"),
      generatePartOfDayFromHour(col("hourOfDay")) as "partOfDay",
      dayofweek(col("date")) as "dayOfWeek",
      col("date")
    )

    enrichedDataPerHourOfDayDf
  }
  def enrichVHSDataPerDay(enrichVHSDataPerHourOfDay: DataFrame): DataFrame = {
    val enrichedDataPerMonthDf = enrichVHSDataPerHourOfDay
      .groupBy("userId", "gameId", "date", "partOfDay")
      .agg(
        max("flagOrganic") as "flagOrganic",
        min("dateRegistration") as "dateRegistration",
        sum("numLevelsCompleted") as "numLevelsCompleted",
        sum("numLevelsStarted") as "numLevelsStarted",
        sum("numAdsWatched") as "numAdsWatched",
        sum("numAdsIgnored") as "numAdsIgnored",
        sum("numAdsProposed") as "numAdsProposed",
        sum("numPurchasesDone") as "numPurchasesDone",
        sum("amountPurchasesDoneDol") as "amountPurchasesDoneDol",
        sum("numPurchasesRejected") as "numPurchasesRejected",
        sum("amountPurchasesRejectedDol") as "amountPurchasesRejectedDol",
        sum("numPurchasesCanceled") as "numPurchasesCanceled",
        sum("amountPurchasesCanceledDol") as "amountPurchasesCanceledDol",
        max("dayOfWeek") as "dayOfWeek",
      )

    enrichedDataPerMonthDf
  }

  def enrichVHSDataPerDay(playerInfoData: DataFrame,
                            playerBehaviorData: DataFrame): DataFrame = {

    val vhsEnrichedPerHourOfDayDf = enrichVHSDataPerHourOfDay(playerInfoData, playerBehaviorData)
    enrichVHSDataPerDay(vhsEnrichedPerHourOfDayDf)
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
        sum("numAdsWatched") as "numAdsWatched",
        sum("numAdsIgnored") as "numAdsIgnored",
        sum("numAdsProposed") as "numAdsProposed",
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
        val vhsEnrichedPerHourOfDayDf = enrichVHSDataPerHourOfDay(playerInfoData, playerBehaviorData).cache()
        val vhsEnrichedPerDayDf = enrichVHSDataPerDay(vhsEnrichedPerHourOfDayDf)
        saveDataframeData(vhsEnrichedPerHourOfDayDf, Seq("date", "hourOfDay"), getHourlyResultPath(outputPath))
        saveDataframeData(vhsEnrichedPerDayDf, Seq("date", "partOfDay"), getDailyResultPath(outputPath))
        vhsEnrichedPerDayDf.orderBy(desc("date")).show(15)
        log.info(s"EnrichedDataPerDay count: ${vhsEnrichedPerDayDf.count()}")
      case Monthly =>
        val vhsEnrichedPerMonthDf = enrichVHSDataPerMonth(playerInfoData, playerBehaviorData)
        saveDataframeData(vhsEnrichedPerMonthDf, Seq("codMonth", "partOfDay"), getMonthlyResultPath(outputPath))
        vhsEnrichedPerMonthDf.orderBy(desc("date")).show(15)
        log.info(s"EnrichedDataPerMonth count: ${vhsEnrichedPerMonthDf.count()}")
      case Both =>
        val vhsEnrichedPerHourOfDayDf = enrichVHSDataPerHourOfDay(playerInfoData, playerBehaviorData).cache()
        val vhsEnrichedPerDayDf = enrichVHSDataPerDay(vhsEnrichedPerHourOfDayDf)
        val vhsEnrichedPerMonthDf = enrichVHSDataPerMonth(vhsEnrichedPerDayDf)
        saveDataframeData(vhsEnrichedPerHourOfDayDf, Seq("date", "hourOfDay"), getHourlyResultPath(outputPath))
        saveDataframeData(vhsEnrichedPerDayDf, Seq("date", "partOfDay"), getDailyResultPath(outputPath))
        saveDataframeData(vhsEnrichedPerMonthDf, Seq("codMonth", "partOfDay"), getMonthlyResultPath(outputPath))
        vhsEnrichedPerHourOfDayDf.orderBy(desc("date")).show(15)
        vhsEnrichedPerDayDf.orderBy(desc("date")).show(15)
        vhsEnrichedPerMonthDf.orderBy(desc("codMonth")).show(15)
        log.info(s"vhsEnrichedPerHourOfDayDf count: ${vhsEnrichedPerHourOfDayDf.count()}, EnrichedDataPerDay count: ${vhsEnrichedPerDayDf.count()} ,  enrichedDataPerMonth count: ${vhsEnrichedPerMonthDf.count()}")
    }
}