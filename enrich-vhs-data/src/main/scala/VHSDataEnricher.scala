import config.{AppConfig, MongoReaderConfig}
import model.events.EventsSchema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import reader.mongo.MongoReader
import utils.DataEnricherUtil._
import utils.SchemaOperations._

object VHSDataEnricher extends Logging {

  def enrichVHSData(playerInfoData: DataFrame,
                    playerBehaviorData: DataFrame): (DataFrame, DataFrame) = {

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

        val (vhsEnrichedPerDayDf, vhsEnrichedPerMonthDf) = enrichVHSData(playerInfoData, playerBehaviorData)

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