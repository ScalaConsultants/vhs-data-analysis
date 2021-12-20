import VHSDataAnalyzer.showMetrics
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import plotly._
import plotly.layout._
import plotly.Plotly._
import utils.DateColumnOperations.{createFilterBetweenCodMonths, createFilterBetweenDates}
import reader.file.LocalFileReader
import config._
import model._

object LTVAnalyzer extends Logging {
  final case class KMeansCost(kCluster: Int, costTraining: Double)

  def getModelForKMeans(kmeansInput: DataFrame, kClusters: Int, kIter: Int = 30): PipelineModel ={
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("numLevelsCompleted", "numAddsWatched", "numPurchasesDone", "partOfDay" ,"flagOrganic"))
      .setOutputCol("featureVector")

    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatures")

    val kMeans = new KMeans()
      .setSeed(1L)
      .setK(kClusters)
      .setPredictionCol("cluster")
      .setFeaturesCol("scaledFeatures")
      .setMaxIter(kIter)

    val pipeline = new Pipeline().setStages(Array(vectorAssembler, scaler, kMeans))

    pipeline.fit(kmeansInput)
  }

  def displayClusterLTV(ltv: DataFrame, clustersK: Int): Unit = {
    ltv.show()
    (0 until clustersK).foreach { clusterK =>
      val clusterResultK = ltv.where(col("cluster") === clusterK)
      clusterResultK.show(20)
    }
  }

  def plotLTV(points: Map[Int, Double] ): Unit = {

    val xCluster =  points.keys.toList
    val yCostTraining = points.values.toList

    val plot = Seq(
      Bar(xCluster, yCostTraining).withName("Cluster LTV")
    )
    val lay = Layout().withTitle("LTV method")
    plot.plot(s"plots/ltv.html", lay)
  }

  def calculateAndSaveLTV(inputKMeansDf: DataFrame, kCluster: Int): Unit  = {
    val pipelineModel = getModelForKMeans(inputKMeansDf, kCluster)

    val kmResult = pipelineModel
      .transform(inputKMeansDf)
      .cache()

    val ltv = kmResult
        .groupBy("cluster")
        .agg(count_distinct(col("userId")) as "uniqueUsers", sum("numAddsWatched").multiply(0.015) as "revenue")
        .withColumn("ltv", col("revenue").divide(col("uniqueUsers"))  )
        .select(col("cluster"), col("ltv"))


    ltv.write.mode("overwrite").partitionBy("cluster").parquet("data-models/output/ltv-data")


    displayClusterLTV(ltv, kCluster)


    plotLTV( ltv.collect().map(a => a.getInt(0) -> a.getDouble(1)).toMap)

  }

  def transformPartOfDay(partOfDay: Column): Column = {
    when(partOfDay==="morning", lit(0))
      .when(partOfDay==="afternoon", lit(1))
      .when(partOfDay==="evening", lit(2))
      .when(partOfDay==="night", lit(3))
  }

  def main(args: Array[String]): Unit = {
    AppConfig.loadAnalyzerConfig(args) match {
      case Right(AnalyzerAppConfig(localFileReaderConfig: LocalFileReaderConfig, methodAnalyzer, behavior@(Daily | Monthly), dateRange)) =>
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("LTVAnalyzer")
          .getOrCreate()

        log.info("read vhs enriched data")

        val localFileReader = LocalFileReader(spark, localFileReaderConfig.mainPath)

        val (partitionSource, fileNameSource, enrichedDataSchema, filterByPartition) = behavior match {
          case Daily => ("date", "hd_playerbehavior", EnrichedDataPerDay.generateSchema, createFilterBetweenDates(_, dateRange.fromDate, dateRange.toDate))
          case _ => ("codMonth", "hm_playerbehavior", EnrichedDataPerMonth.generateSchema, createFilterBetweenCodMonths(_, dateRange.fromDate, dateRange.toDate))
        }

        val enrichedDataDf = localFileReader
          .read(
            localFileReaderConfig.folderName,
            fileNameSource,
            enrichedDataSchema
          )

        val inputKMeansDf = enrichedDataDf
          .select(
            col("userId"),
            col("gameId"),
            col("numLevelsCompleted"),
            col("numAddsWatched"),
            col("numPurchasesDone"),
            transformPartOfDay(col("partOfDay")) as "partOfDay",
            col("flagOrganic"),
            col(partitionSource)
          )
          .where(filterByPartition(col(partitionSource)))
          .cache()

        log.info(s"segmentation of vhs data $methodAnalyzer")
        methodAnalyzer match {
          case KMeansAnalyzer(k) =>
            log.info("segmentation of vhs data")
            calculateAndSaveLTV(inputKMeansDf, k)
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