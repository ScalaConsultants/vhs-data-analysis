import config.{AppConfig, MongoReaderConfig}
import model.events.EventsSchema
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import reader.mongo.MongoReader
import utils.SchemaOperations._

object VHSDataEnricher extends Logging {

  def createFilterBetweenMonths(date: Column, fromCodMonth: String, toCodMonth :String): Column = {
    val codMonthFromTimestamp = generateCodMonthFromDate(date)
    (codMonthFromTimestamp >= fromCodMonth) && (codMonthFromTimestamp <= toCodMonth)
  }

  def generateDateFromTimestamp(timestamp: Column): Column =
    to_date(from_unixtime(timestamp/1000,"yyyy-MM-dd"))

  def generateCodMonthFromDate(date: Column): Column =
    date_format(date, "yyyyMM")

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

  def enrichVHSData(playersCleanedDf: DataFrame,
                    levelsCleanedDf: DataFrame,
                    addsCleanedDf: DataFrame,
                    purchasesCleanedDf: DataFrame): (DataFrame, DataFrame) = {
    val playersEnrichDataDf = playersCleanedDf
      .where(col("action")==="SIGN_UP")
      .groupBy("userId","gameId")
      .agg(
        max(when(col("attribution") === "organic", lit(1)).otherwise(lit(0))) as "flagOrganic",
        min("date") as "dateRegistration"
      )

    val levelsEnrichDataDf = levelsCleanedDf
      .groupBy("userId", "gameId", "date")
      .agg(
        sum(when(col("status") === "COMPLETED", lit(1)).otherwise(lit(0))) as "numLevelsCompleted",
        sum(when(col("status") === "STARTED", lit(1)).otherwise(lit(0))) as "numLevelsStarted"
      )

    val addsEnrichDataDf = addsCleanedDf
      .groupBy("userId", "gameId", "date")
      .agg(
        sum(when(col("status") === "WATCHED", lit(1)).otherwise(lit(0))) as "numAddsWatched",
        sum(when(col("status") === "IGNORED", lit(1)).otherwise(lit(0))) as "numAddsIgnored",  //-- Bug in the game
        sum(when(col("status") === "PROPOSED", lit(1)).otherwise(lit(0))) as "numAddsProposed"  //-- Not implemented yet in the game
      )

    val purchasesEnrichDataDf = purchasesCleanedDf
      .groupBy("userId", "gameId", "date")
      .agg(
        sum(when(col("status")=== "DONE", lit(1)).otherwise(lit(0))) as "numPurchasesDone",
        sum(when(col("status")=== "DONE", col("amount")).otherwise(lit(0))) as "amountPurchasesDoneDol",
        sum(when(col("status")=== "REJECTED", lit(1)).otherwise(lit(0))) as "numPurchasesRejected",
        sum(when(col("status")=== "REJECTED", col("amount")).otherwise(lit(0))) as "amountPurchasesRejectedDol",
        sum(when(col("status")=== "CANCELED", lit(1)).otherwise(lit(0))) as "numPurchasesCanceled",
        sum(when(col("status")=== "CANCELED", col("amount")).otherwise(lit(0))) as "amountPurchasesCanceledDol"
      )

    playersEnrichDataDf.show(5)
    levelsEnrichDataDf.show(5)
    addsEnrichDataDf.show(5)
    purchasesEnrichDataDf.show(5)

    val enrichedPlayersDataDf = levelsEnrichDataDf
      .join(playersEnrichDataDf, Seq("userId", "gameId"), "left_outer")
      .join(addsEnrichDataDf, Seq("userId", "gameId", "date"), "left_outer")
      .join(purchasesEnrichDataDf, Seq("userId", "gameId", "date"), "left_outer")

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
      col("date")
    )

    val enrichedDataPerMonthDf = enrichedDataPerDayDf
      .withColumn("codMonth", generateCodMonthFromDate(col("date")))
      .groupBy("userId", "gameId", "codMonth")
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

    (enrichedDataPerDayDf, enrichedDataPerMonthDf)
  }

  def main(args: Array[String]) = {
    AppConfig.load(args) match {
      case Some(AppConfig(mongoReaderConfig :MongoReaderConfig)) =>
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("VHSDataAnalyzer")
          .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
          .getOrCreate()

        val mongoReader = MongoReader(spark, mongoReaderConfig.mongoUri)

        val playerInfoSchema = mergeSchemas(EventsSchema.getAllPlayerInfoEventsSchema)

        val playerInfoData =
          mongoReader
            .read(
              mongoReaderConfig.database,
              mongoReaderConfig.collection,
              playerInfoSchema
            )
            .where(col("attribution").isNotNull)
            .withColumn("date", generateDateFromTimestamp(col("timestamp")))
            .cache()

        val playerBehaviorSchema = mergeSchemas(EventsSchema.getAllPlayerBehaviorEventsSchema)

        val playerBehaviorData =
          mongoReader
            .read(
              mongoReaderConfig.database,
              mongoReaderConfig.collection,
              playerBehaviorSchema
            )
            .where(col("levelId").isNotNull || col("adType").isNotNull || col("amount").isNotNull)
            .withColumn("date", generateDateFromTimestamp(col("timestamp")))
            .filter(createFilterBetweenMonths(col("date"), "202101", "202112"))
            .cache()

        val playersCleanedDf = cleanPlayersData(playerInfoData)
        val levelsCleanedDf = cleanLevelsData(playerBehaviorData)
        val addsCleanedDf = cleanAddsData(playerBehaviorData)
        val purchasesCleanedDf = cleanPurchasesData(playerBehaviorData)

        val (vhsEnrichedPerDayDf, vhsEnrichedPerMonthDf) = enrichVHSData(playersCleanedDf, levelsCleanedDf, addsCleanedDf, purchasesCleanedDf)

        vhsEnrichedPerDayDf
          .write
          .mode("overwrite")
          .partitionBy("date")
          .parquet("data/output/enriched-data/hd_playerbehavior")

        vhsEnrichedPerMonthDf
          .write
          .mode("overwrite")
          .partitionBy("codMonth")
          .parquet("data/output/enriched-data/hm_playerbehavior")

        vhsEnrichedPerDayDf.show(10)

        vhsEnrichedPerMonthDf.show(10)

        log.info(s"enrichedDataPerDay count: ${vhsEnrichedPerDayDf.count()} ,  enrichedDataPerMonth count: ${vhsEnrichedPerMonthDf.count()}")

        spark.stop()
      case _ =>
        log.warn("appConfig was not provided")
    }
  }
}