import model.events.EventsSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import reader.mongo.MongoReader
import utils.SchemaOperations._
import config._

object VHSDataEnricher extends Logging {
  import utils.DateColumnOperations._
  import DataEnricherLogic._

  def main(args: Array[String]) = {
    AppConfig.loadEnrichedConfig(args) match {
      case Right(EnricherAppConfig(mongoReaderConfig :MongoReaderConfig, behavior, dateRange)) =>
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
            .withColumn("datetime", generateDateTimeFromTimestamp(col("timestamp")))
            .withColumn("date", generateDateFromDateTime(col("datetime")))
            .filter(createFilterBetweenDates(col("date"), dateRange.fromDate, dateRange.toDate))
            .cache()

        enrichVHSData(behavior, playerInfoData, playerBehaviorData, "data/output/enriched-data")

        spark.stop()
      case Right(EnricherAppConfig(_ :LocalFileReaderConfig, _, _)) =>
        log.warn("This module doesn't support localFileReader")
      case Left(exMsg) =>
        log.warn(s"Problem while loading appConfig - $exMsg")
    }
  }
}