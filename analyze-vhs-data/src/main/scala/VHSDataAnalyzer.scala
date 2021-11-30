import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import utils.DateColumnOperations.{createFilterBetweenCodMonths, createFilterBetweenDates}
import scala.util.Random
import reader.file.LocalFileReader
import config._
import model._

object VHSDataAnalyzer extends Logging {

  def showMetrics(enrichedDf: DataFrame): Unit = {
    val metricsDf = enrichedDf.select("numLevelsCompleted","numAddsWatched", "numPurchasesDone")
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

  def main(args: Array[String]): Unit = {
    AppConfig.load(args) match {
      case Right(AppConfig(localFileReaderConfig: LocalFileReaderConfig, behavior@(Daily | Monthly), dateRange)) =>
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
            col("numPurchasesCanceled"),
            col(partitionSource)
          )
          .where(filterByPartition(col(partitionSource)))
          .cache()

        log.info("segmentation of vhs data")

        val clustersK = 4
        val iterationsK = 50
        val tolK = 1.0e-5

        val vectorAssembler = new VectorAssembler()
          .setInputCols(Array("numLevelsCompleted", "numAddsWatched", "numPurchasesDone", "numPurchasesCanceled"))
          .setOutputCol("featureVector")

        val kMeans = new KMeans()
          .setSeed(Random.nextLong())
          .setK(clustersK)
          .setPredictionCol("cluster")
          .setFeaturesCol("featureVector")
          .setMaxIter(iterationsK)
          .setTol(tolK)

        val pipeline = new Pipeline().setStages(Array(vectorAssembler, kMeans))
        val pipelineModel = pipeline.fit(inputKMeansDf)
        val kmModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
        val kmResult = pipelineModel.transform(inputKMeansDf).cache()

        // Save model result
        kmModel
          .write
          .overwrite()
          .save("data-models/output/cluster-model")

        // Save enriched data with cluster
        kmResult.write.mode("overwrite").partitionBy(partitionSource).parquet("data-models/output/cluster-data")

        displayClusterMetrics(kmResult, clustersK)
        printClusterCenters(kmModel)

        spark.stop()
      case Right(AppConfig(_: LocalFileReaderConfig, Both, _)) =>
        log.warn("This module doesn't support daily and monthly behavior at the same time")
      case Right(AppConfig(_: MongoReaderConfig, _, _)) =>
        log.warn("This module doesn't support mongoReader")
      case Left(exMsg) =>
        log.warn(s"Problem while loading appConfig - $exMsg")
    }
  }
}