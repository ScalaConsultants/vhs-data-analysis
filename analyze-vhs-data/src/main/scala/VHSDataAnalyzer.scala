import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly._
import utils.DateColumnOperations.{createFilterBetweenCodMonths, createFilterBetweenDates}
import reader.file.LocalFileReader
import config._
import model._

object VHSDataAnalyzer extends Logging {
  final case class KMeansCost(kCluster: Int, costTraining: Double)

  def showMetrics(enrichedDf: DataFrame): Unit = {
    val metricsDf = enrichedDf.select("numLevelsCompleted", "numAddsWatched", "numPurchasesDone", "flagOrganic")
    metricsDf
      .summary("count", "min", "mean", "max")
      .show()
  }

  def printClusterCenters(model:KMeansModel): Unit = {
    val clCenters = model.clusterCenters
    println("clusters centers")
    clCenters.foreach(println)
  }

  def displayClusterMetrics(kmResult: DataFrame, clustersK: Int): Unit = {
    val clustersMetrics = kmResult
      .select(
        col("userId"),
        col("cluster")
      )
      .groupBy("cluster")
      .agg(
        countDistinct("userId") as "numberOfUsers"
      )
      .orderBy("cluster")

    clustersMetrics.show()

    (0 until clustersK).foreach { clusterK =>
      val clusterResultK = kmResult.where(col("cluster") === clusterK)
      showMetrics(clusterResultK)
      clusterResultK.show(20)
    }
  }

  def getModelForKMeans(kmeansInput: DataFrame, kClusters: Int, kIter: Int = 30): PipelineModel ={
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("numLevelsCompleted", "numAddsWatched", "numPurchasesDone", "flagOrganic"))
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

  def showClusterCostForElbowMethod(inputKMeansDf: DataFrame, fromK: Int, toK: Int): Unit = {
    val kMeansCosts = for {
      kCluster <- (fromK to toK).toList
      pipelineModel = getModelForKMeans(inputKMeansDf, kCluster)
      kmModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
      kMeansCost = kmModel.summary.trainingCost
    } yield KMeansCost(kCluster, kMeansCost)

    val xCluster = kMeansCosts.map(_.kCluster)
    val yCostTraining = kMeansCosts.map(_.costTraining)

    val plot = Seq(
      Scatter(xCluster, yCostTraining).withName("CostTraining vs k")
    )
    val lay = Layout().withTitle("Elbow method")
    plot.plot(s"plots/elbowFrom${fromK}To${toK}", lay)
  }

  def showAndSaveKMeansResults(inputKMeansDf: DataFrame, kCluster: Int, partitionSource: String): Unit  = {
    val bestKCluster = 8
    val pipelineModel = getModelForKMeans(inputKMeansDf, kCluster)
    val kmModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]

    val kmResult = pipelineModel
      .transform(inputKMeansDf)
      .cache()

    // Save model result
    pipelineModel
      .write
      .overwrite()
      .save("data-models/output/cluster-model")

    // Save enriched data with cluster
    kmResult.write.mode("overwrite").partitionBy(partitionSource).parquet("data-models/output/cluster-data")

    // Show results
    displayClusterMetrics(kmResult, bestKCluster)
    printClusterCenters(kmModel)

  }

  def main(args: Array[String]): Unit = {
    AppConfig.loadAnalyzerConfig(args) match {
      case Right(AnalyzerAppConfig(localFileReaderConfig: LocalFileReaderConfig, methodAnalyzer, behavior@(Daily | Monthly), dateRange)) =>
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("VHSDataAnalyzer")
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
            col("flagOrganic"),
            col(partitionSource)
          )
          .where(filterByPartition(col(partitionSource)))
          .cache()

        methodAnalyzer match {
          case ElbowAnalyzer(fromK, toK) =>
            showClusterCostForElbowMethod(inputKMeansDf, fromK, toK)
          case KMeansAnalyzer(k) =>
            log.info("segmentation of vhs data")
            showAndSaveKMeansResults(inputKMeansDf, k, partitionSource)
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