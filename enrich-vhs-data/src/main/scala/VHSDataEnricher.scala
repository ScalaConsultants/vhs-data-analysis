import config.{AppConfig, MongoReaderConfig}
import model.events.EventsSchema
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import reader.mongo.MongoReader
import utils.SchemaOperations._

object VHSDataEnricher extends Logging {

  def generateCodMonthFromTimestamp(timestamp: Column): Column = {
    date_format(to_date(from_unixtime(timestamp/1000,"yyyy-MM-dd")), "yyyyMM")
  }

  def cleanLevelsData(eventsDf: DataFrame): DataFrame = {
    val levelsEvents = eventsDf.where("levelId is not null")
    levelsEvents.select(
      trim(col("userId")) as "userId",
      trim(col("gameId")) as "gameId",
      trim(col("levelId")) as "levelId",
      col("levelDifficulty"),
      col("levelProgress"),
      trim(col("status")) as "status",
      col("codMonth")
    )
  }

  def cleanAddsData(eventsDf: DataFrame): DataFrame = {
    val addsEvents = eventsDf.where("adType is not null")
    addsEvents.select(
      trim(col("userId")) as "userId",
      trim(col("gameId")) as "gameId",
      col("placementId"),
      trim(col("adType")) as "adType",
      trim(col("status")) as "status",
      col("codMonth")
    )
  }

  def cleanPurchasesData(eventsDf: DataFrame): DataFrame = {
    val purchasesEvents =  eventsDf.filter("amount is not null")
    purchasesEvents.select(
      trim(col("userId")) as "userId",
      trim(col("gameId")) as "gameId",
      trim(col("itemId")) as "itemId",
      trim(col("category")) as "category",
      col("amount"),
      trim(col("currency")) as "currency",
      trim(col("status")) as "status",
      col("codMonth")
    )
  }

  def enrichVHSData(levelsCleanedDf: DataFrame, addsCleanedDf: DataFrame, purchasesCleanedDf: DataFrame): DataFrame = {
    val levelsEnrichDataDf = levelsCleanedDf
      .withColumn("numLevelsCompleted", when(col("status") === "COMPLETED", lit(1)).otherwise(lit(0)))
      .withColumn("numLevelsStarted", when(col("status") === "STARTED", lit(1)).otherwise(lit(0)))
      .groupBy("userId", "gameId", "codMonth")
      .agg(
        sum("numLevelsCompleted") as "numLevelsCompleted",
        sum("numLevelsStarted") as "numLevelsStarted"
      )

    val addsEnrichDataDf = addsCleanedDf
      .withColumn("numAddsWatched", when(col("status") === "WATCHED", lit(1)).otherwise(lit(0)))
      .withColumn("numAddsIgnored", when(col("status") === "IGNORED", lit(1)).otherwise(lit(0)))  //-- Bug in the game
      .withColumn("numAddsProposed", when(col("status") === "PROPOSED", lit(1)).otherwise(lit(0)))  //-- Not implemented yet in the game
      .groupBy("userId", "gameId", "codMonth")
      .agg(
        sum("numAddsWatched") as "numAddsWatched",
        sum("numAddsIgnored") as "numAddsIgnored",  //-- Bug in the game
        sum("numAddsProposed") as "numAddsProposed"  //-- Not implemented yet in the game
      )

    /*
     Need to convert amounts to USD before calculate
      amountPurchasesDoneDol
      amountPurchasesRejectedDol
      amountPurchasesCanceledDol
     */

    val purchasesEnrichDataDf = purchasesCleanedDf
      .withColumn("numPurchasesDone", when(col("status") === "DONE", lit(1)).otherwise(lit(0)))
      .withColumn("numPurchasesRejected", when(col("status") === "REJECTED", lit(1)).otherwise(lit(0)))
      .withColumn("numPurchasesCanceled", when(col("status") === "CANCELED", lit(1)).otherwise(lit(0)))
      .groupBy("userId", "gameId", "codMonth")
      .agg(
        sum("numPurchasesDone") as "numPurchasesDone",
        sum("numPurchasesRejected") as "numPurchasesRejected",
        sum("numPurchasesCanceled") as "numPurchasesCanceled"
      )

    levelsEnrichDataDf.show(5)
    addsEnrichDataDf.show(5)
    purchasesEnrichDataDf.show(5)

    val activePlayersDf = levelsEnrichDataDf.where(col("numLevelsCompleted")>0)

    val enrichedData = activePlayersDf
      .join(addsEnrichDataDf, Seq("userId", "gameId", "codMonth"))
      .join(purchasesEnrichDataDf, Seq("userId", "gameId", "codMonth"))

    enrichedData.select(
      col("userId"),
      col("gameId"),
      coalesce(col("numLevelsCompleted"), lit(0)) as "numLevelsCompleted",
      coalesce(col("numLevelsStarted"), lit(0)) as "numLevelsStarted",
      coalesce(col("numAddsWatched"), lit(0)) as "numAddsWatched",
      coalesce(col("numAddsIgnored"), lit(0)) as "numAddsIgnored",
      coalesce(col("numAddsProposed"), lit(0)) as "numAddsProposed",
      coalesce(col("numPurchasesDone"), lit(0)) as "numPurchasesDone",
      coalesce(col("numPurchasesRejected"), lit(0)) as "numPurchasesRejected",
      coalesce(col("numPurchasesCanceled"), lit(0)) as "numPurchasesCanceled",
      col("codMonth")
    )
  }

  def main(args: Array[String]) = {
    AppConfig.load(args) match {
      case Some(AppConfig(mongoReaderConfig :MongoReaderConfig)) =>
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("VHSDataAnalyzer")
          .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
          .getOrCreate()

        val mongoReader = MongoReader(spark, mongoReaderConfig.mongoUri)

        val inputSchemaFromMongo = mergeSchemas(EventsSchema.getAllEventsSchema)

        log.info(s"levelSchema: ${inputSchemaFromMongo.toString()}")
        val eventsData =
          mongoReader
            .read(
              mongoReaderConfig.database,
              mongoReaderConfig.collection,
              inputSchemaFromMongo
            )
            .where(col("levelId").isNotNull || col("adType").isNotNull || col("amount").isNotNull)
            .withColumn("codMonth", generateCodMonthFromTimestamp(col("timestamp")))
            .cache()


        val levelsCleanedDf = cleanLevelsData(eventsData)
        val addsCleanedDf = cleanAddsData(eventsData)
        val purchasesCleanedDf = cleanPurchasesData(eventsData)

        val vhsEnrichedDf = enrichVHSData(levelsCleanedDf, addsCleanedDf, purchasesCleanedDf)

        vhsEnrichedDf.show(10)

        log.info(s"enrichedData count: ${vhsEnrichedDf.count()}")

        vhsEnrichedDf.write.mode("overwrite").partitionBy("codMonth").parquet("data/output/enriched-data")

        spark.stop()
      case _ =>
        log.warn("appConfig was not provided")
    }
  }
}