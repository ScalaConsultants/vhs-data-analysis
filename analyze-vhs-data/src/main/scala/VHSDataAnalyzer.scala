import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import reader.file.LocalFileReader
import java.time.Year
import utils.DateColumnOperations._
import utils.DataAnalyzerUtil._
import config._
import methods._
import model._


object VHSDataAnalyzer extends Logging {
  def readPlayerEventData(spark: SparkSession, localFileReaderConfig: LocalFileReaderConfig, gameId: String, dateRange: CodMonthRange): DataFrame = {
    val localFileReader = LocalFileReader(spark, localFileReaderConfig.mainPath)

    val playerEventDataDf = localFileReader
      .read(
        "raw-data",
        "playerEvent"
      )

    val df = playerEventDataDf
      .select(
        col("userId"),
        col("gameId"),
        col("attribution"),
        col("action"),
        col("date")
      )

    df.where((col("gameId")===gameId) && createFilterBetweenDates(col("date"), dateRange.fromDate, dateRange.toDate))
  }

  def readEnrichedData(spark: SparkSession, localFileReaderConfig: LocalFileReaderConfig, behavior: Behavior, gameId: String, dateRange: CodMonthRange): DataFrame = {
    log.info("read vhs enriched data")

    val localFileReader = LocalFileReader(spark, localFileReaderConfig.mainPath)

    val enrichedDataSchema = behavior match {
      case Daily => EnrichedDataPerDay.generateSchema
      case _ => EnrichedDataPerMonth.generateSchema
    }
    val fileNameSource = getFileNameFromBehavior(behavior)
    val partitionSource = getPartitionSourceFromBehavior(behavior)

    val enrichedDataDf = localFileReader
      .read(
        localFileReaderConfig.folderName,
        fileNameSource,
        enrichedDataSchema
      )

    val df = enrichedDataDf
      .select(
        col("userId"),
        col("gameId"),
        col("numLevelsStarted"),
        col("numLevelsCompleted"),
        col("numAdsWatched"),
        col("numPurchasesDone"),
        col("partOfDay"),
        transformPartOfDayToNumber(col("partOfDay")) as "partOfDayNumber",
        col("flagOrganic"),
        col(partitionSource)
      )


    val filterByPartition = behavior match {
      case Daily =>  createFilterBetweenDates(_, dateRange.fromDate, dateRange.toDate)
      case _ =>  createFilterBetweenCodMonths(_, dateRange.fromDate, dateRange.toDate)
    }

    df.where((col("gameId")===gameId) && filterByPartition(col(partitionSource)))
  }

  def readClusterData(spark: SparkSession, localFileReaderConfig: LocalFileReaderConfig, behavior: Behavior, gameId: String,dateRange: CodMonthRange): DataFrame = {
    log.info("read cluster data")

    val localFileReader = LocalFileReader(spark, localFileReaderConfig.mainPath)


    val fileNameSource = "cluster-data"
    val partitionSource = getPartitionSourceFromBehavior(behavior)

    val enrichedDataDf = localFileReader
      .read(
        localFileReaderConfig.folderName,
        fileNameSource
      )

    val df = enrichedDataDf
      .select(
        col("userId"),
        col("gameId"),
        col("numLevelsCompleted"),
        col("numAdsWatched"),
        col("numPurchasesDone"),
        col("partOfDay"),
        transformPartOfDayToNumber(col("partOfDay")) as "partOfDayNumber",
        col("flagOrganic"),
        col("cluster"),
        col(partitionSource)
      )

    val filterByPartition = behavior match {
      case Daily => createFilterBetweenDates(_, dateRange.fromDate, dateRange.toDate)
      case _ => createFilterBetweenCodMonths(_, dateRange.fromDate, dateRange.toDate)
    }
    df.where((col("gameId")===gameId) && filterByPartition(col(partitionSource)))
  }

  def main(args: Array[String]): Unit = {
    AppConfig.loadAnalyzerConfig(args) match {
      case Right(AnalyzerAppConfig(localFileReaderConfig: LocalFileReaderConfig, methodAnalyzer, behavior@(Daily | Monthly), gameId, dateRange)) =>
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("VHSDataAnalyzer")
          .getOrCreate()

        log.info("read vhs enriched data")

        methodAnalyzer match {
          case ElbowAnalyzer(fromK, toK) =>
            log.info("read vhs enriched data")
            val enrichedData = readEnrichedData(spark, localFileReaderConfig, behavior, gameId, dateRange).cache()

            ElbowMethod.showClusterCostForElbowMethod(enrichedData, fromK, toK)
          case KMeansAnalyzer(k) =>
            log.info("read vhs enriched data")
            val enrichedData = readEnrichedData(spark, localFileReaderConfig, behavior, gameId, dateRange).cache()

            log.info("segmentation of vhs data")
            KMeansMethod.showAndSaveKMeansResults(enrichedData, k, getPartitionSourceFromBehavior(behavior))
          case LTVAnalyzer(attribute) =>
            val clusterData = readClusterData(spark, localFileReaderConfig, Daily, gameId, dateRange).cache()
            attribute match {
              case LTVAttribute.Cluster => LTVMethod.calculateAndSaveLTVByCluster(clusterData)
              case LTVAttribute.User =>
                behavior match {
                  case Daily => LTVMethod.calculateAndSaveLTVByUserDaily(clusterData)
                  case Monthly => LTVMethod.calculateAndSaveLTVByUserMonthly(clusterData)
                  case _ => log.warn(s"LTV behaviour not supported ")
                }
            }
          case Retention(startMonth, idleTime) =>
            val days = startMonth.getMonth.length(Year.isLeap(startMonth.getYear))
            val playerEventsData = readPlayerEventData(spark, localFileReaderConfig, gameId, CodMonthRange.createFromOneMonth(dateRange.fromDate))
            val enrichedDataPerDay = readEnrichedData(spark, localFileReaderConfig, Daily, gameId, dateRange)

            NRetentionMethod.calculateRetentionWithIdleTime(playerEventsData, enrichedDataPerDay, startMonth, days, idleTime)
        }
        spark.stop()
      case Right(AnalyzerAppConfig(_: LocalFileReaderConfig, _, Both, _, _)) =>
        log.warn("This module doesn't support daily and monthly behavior at the same time")
      case Right(AnalyzerAppConfig(_: MongoReaderConfig, _, _, _, _)) =>
        log.warn("This module doesn't support mongoReader")
      case Left(exMsg) =>
        log.warn(s"Problem while loading appConfig - $exMsg")
    }
  }
}