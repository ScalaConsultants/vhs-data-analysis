import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object KMeansAccuracy {

  def main(args: Array[String]): Unit = {

    implicit val session = SparkSession.builder().getOrCreate()

    val df = session.read.parquet("data-models/output/cluster-data")

    val weights = Array(0.6D, 0.4D)
    val nc = df.select("cluster").collect().map { r =>
      r.getInt(r.fieldIndex("cluster"))
    }.distinct.length

    val ts = df.where(col("cluster")===0).randomSplit(weights)

    var trainDf = ts(0)
    var testDf = ts(1)

    for(i<-1 until nc){
      val ts = df.where(col("cluster") === i).randomSplit(weights)
      trainDf = trainDf.union(ts(0))
      testDf = testDf.union(ts(1))
    }

    def getModelForKMeans(kmeansInput: org.apache.spark.sql.DataFrame, kClusters: Int, kIter: Int = 30): PipelineModel ={
      val vectorAssembler = new VectorAssembler()
        .setInputCols(Array("numLevelsCompleted", "numAddsWatched", "numPurchasesDone", "partOfDayNumber" ,"flagOrganic"))
        .setOutputCol("featureVector2")

      val scaler = new StandardScaler()
        .setInputCol("featureVector2")
        .setOutputCol("scaledFeatures2")

      val kMeans = new KMeans()
        .setSeed(1L)
        .setK(kClusters)
        .setPredictionCol("cluster2")
        .setFeaturesCol("scaledFeatures2")
        .setMaxIter(kIter)

      val pipeline = new Pipeline().setStages(Array(vectorAssembler, scaler, kMeans))

      pipeline.fit(kmeansInput)
    }

    val p = getModelForKMeans(trainDf, nc, 30)
    val result = p.transform(testDf)
    //val model = p.stages.last.asInstanceOf[KMeansModel]

    val rows = result.collect()

    val hits = rows.count { r =>
      val i0 = r.fieldIndex("cluster")
      val i1 = r.fieldIndex("cluster2")

      val cluster = r.getInt(i0)
      val oCluster = r.getInt(i1)

      cluster == oCluster
    }

    val rate = (hits.toDouble * 100)/rows.length

    println()
    println(s"accuracy: ${rate}% nc: ${nc}")
    println()

    session.close()

  }

}
