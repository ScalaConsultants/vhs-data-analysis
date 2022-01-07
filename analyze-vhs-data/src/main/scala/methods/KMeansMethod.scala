package methods

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, countDistinct}
import utils.KMeansPlotter

object KMeansMethod {

  def showMetrics(enrichedDf: DataFrame): Unit = {
    val metricsDf = enrichedDf.select("numLevelsCompleted", "numAddsWatched", "numPurchasesDone", "partOfDayNumber", "flagOrganic")
    metricsDf
      .summary("count", "min", "mean", "max")
      .show()
  }

  def printClusterCenters(model: KMeansModel): Unit = {
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
      .setInputCols(Array("numLevelsCompleted", "numAddsWatched", "numPurchasesDone", "partOfDayNumber" ,"flagOrganic"))
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

  def showAndSaveKMeansResults(inputKMeansDf: DataFrame, kCluster: Int, partitionSource: String): Unit  = {
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

    KMeansPlotter.generatePlots(kmResult)

    // Save enriched data with cluster
    kmResult
      .write
      .mode("overwrite")
      .partitionBy(partitionSource, "partOfDay")
      .parquet("data-models/output/cluster-data")

    // Show results
    displayClusterMetrics(kmResult, kCluster)
    printClusterCenters(kmModel)

  }
}
