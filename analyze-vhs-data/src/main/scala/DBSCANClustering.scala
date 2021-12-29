import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.mllib.dbscan.DBSCAN
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.sql.SparkSession
import plotly.Plotly._
import plotly._
import plotly.element._
import plotly.layout.Layout

object DBSCANClustering {

  def main(args: Array[String]): Unit = {

    implicit val session = SparkSession.builder().config("spark.master", "local").getOrCreate()

    val (src, dest, maxPointsPerPartition, eps, minPoints) =
      ("demo.txt", "result", 30, 0.0375.toFloat, 50)

    val vd = session.read.parquet("data-models/output/cluster-data").select("featureVector")

    // Normalize each Vector using $L^1$ norm.
    val normalizer = new Normalizer()
      .setInputCol("featureVector")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val transformedDF = normalizer.transform(vd).select("normFeatures").rdd.map(_.getAs[org.apache.spark.ml.linalg.Vector](0))
      .map(org.apache.spark.mllib.linalg.Vectors.fromML)

    val pca = new PCA(2)
    val principal = pca.fit(transformedDF).transform(transformedDF).toJavaRDD().rdd

    val model = DBSCAN.train(
      principal,
      eps = eps,
      minPoints = minPoints,
      maxPointsPerPartition = maxPointsPerPartition)

    val list = model.labeledPoints.map { d =>
      Tuple3(d.x, d.y, d.cluster)
    }.collect().toSeq.groupBy(_._3).map { case (c, list) =>
      Scatter(list.map(_._1), list.map(_._2)).withMode(ScatterMode(ScatterMode.Markers)).withName(c.toString)
    }.toSeq

    val lay = Layout().withTitle("DBSCAN")
    list.plot(path = "plots/dbscan/plot.html",
      lay,
      useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    session.close()
  }

}
