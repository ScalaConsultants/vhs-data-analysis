import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import reader.file.LocalFileReader
import utils.DateColumnOperations._
import utils.DataAnalyzerUtil._
import config._
import methods._
import model._


object VHSDataAnalyzer extends Logging {

  def readEnrichedData(spark: SparkSession, localFileReaderConfig: LocalFileReaderConfig, behavior: Behavior, dateRange: CodMonthRange): DataFrame = {
    log.info("read vhs enriched data")

    val localFileReader = LocalFileReader(spark, localFileReaderConfig.mainPath)

    val (enrichedDataSchema, filterByPartition) = behavior match {
      case Daily => (EnrichedDataPerDay.generateSchema, createFilterBetweenDates(_, dateRange.fromDate, dateRange.toDate))
      case _ => (EnrichedDataPerMonth.generateSchema, createFilterBetweenCodMonths(_, dateRange.fromDate, dateRange.toDate))
    }
    val fileNameSource = getFileNameFromBehavior(behavior)
    val partitionSource = getPartitionSourceFromBehavior(behavior)

    val enrichedDataDf = localFileReader
      .read(
        localFileReaderConfig.folderName,
        fileNameSource,
        enrichedDataSchema
      )

    enrichedDataDf
      .select(
        col("userId"),
        col("gameId"),
        col("numLevelsCompleted"),
        col("numAddsWatched"),
        col("numPurchasesDone"),
        col("partOfDay"),
        transformPartOfDayToNumber(col("partOfDay")) as "partOfDayNumber",
        col("flagOrganic"),
        col(partitionSource)
      )
      .where(filterByPartition(col(partitionSource)))
  }

  def main(args: Array[String]): Unit = {
    AppConfig.loadAnalyzerConfig(args) match {
      case Right(AnalyzerAppConfig(localFileReaderConfig: LocalFileReaderConfig, methodAnalyzer, behavior@(Daily | Monthly), dateRange)) =>
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("VHSDataAnalyzer")
          .getOrCreate()

        log.info("read vhs enriched data")

        methodAnalyzer match {
          case ElbowAnalyzer(fromK, toK) =>
            log.info("read vhs enriched data")
            val enrichedData = readEnrichedData(spark, localFileReaderConfig, behavior, dateRange).cache()

            ElbowMethod.showClusterCostForElbowMethod(enrichedData, fromK, toK)
          case KMeansAnalyzer(k) =>
            log.info("read vhs enriched data")
            val enrichedData = readEnrichedData(spark, localFileReaderConfig, behavior, dateRange).cache()

            log.info("segmentation of vhs data")
            KMeansMethod.showAndSaveKMeansResults(enrichedData, k, getPartitionSourceFromBehavior(behavior))
          case LTVAnalyzer(attribute) =>
            attribute match {
              case LTVAttribute.Cluster => LTVMethod.calculateAndSaveLTVByCluster(spark)
              case LTVAttribute.User    =>
                val enrichedData = readEnrichedData(spark, localFileReaderConfig, Daily, dateRange).cache()
                behavior match {
                  case Daily   => LTVMethod.calculateAndSaveLTVByUserDaily(enrichedData)
                  case Monthly => LTVMethod.calculateAndSaveLTVByUserMonthly(enrichedData)
                  case _       =>          log.warn(s"LTV behaviour not supported ")
                }
            }
        }

        spark.stop()
      case Right(AnalyzerAppConfig(_: LocalFileReaderConfig, _, Both, _)) =>
        log.warn("This module doesn't support daily and monthly behavior at the same time")
      case Right(AnalyzerAppConfig(_: MongoReaderConfig, _, _, _)) =>
        log.warn("This module doesn't support mongoReader")
      case Left(exMsg) =>
        log.warn(s"Problem while loading appConfig - $exMsg")
    }
  }
}