import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline

import scala.util.Random

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
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("VHSDataAnalyzer")
      .getOrCreate()

    log.info("read vhs enriched data")

    val enrichedDataDf = spark
      .read
      .parquet("data/output/enriched-data")
      .cache()

    val inputKMeansDf = enrichedDataDf
      .select(
        col("userId"),
        col("gameId"),
        col("numLevelsCompleted"),
        col("numAddsWatched"),
        col("numPurchasesDone"),
        col("numPurchasesCanceled"),
        col("codMonth")
      ).cache()

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

    displayClusterMetrics(kmResult, clustersK)
    printClusterCenters(kmModel)

    // Save model result
    kmModel.save("data-models/output/cluster-model")

    // Save enriched data with cluster
    kmResult.write.mode("overwrite").partitionBy("codMonth").parquet("data-models/output/cluster-data")

    spark.stop()
  }
}