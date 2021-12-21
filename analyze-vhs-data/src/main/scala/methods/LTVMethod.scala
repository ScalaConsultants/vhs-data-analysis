package methods

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.layout._
import plotly.Plotly._

object LTVMethod {
  case class LTVPerCluster(ltv: Double, k: Int)

  def plotLTV(points: List[LTVPerCluster]): Unit = {

    val xCluster = points.map(_.k)
    val yLTV = points.map(_.ltv)

    val plot = Seq(
      Bar(xCluster, yLTV).withName("Cluster vs LTV")
    )
    val lay = Layout().withTitle("LTV method")
    plot.plot(s"plots/ltv.html", lay)
  }

  def calculateAndSaveLTV(inputKMeansDf: DataFrame, kCluster: Int): Unit = {
    val pipelineModel = KMeansMethod.getModelForKMeans(inputKMeansDf, kCluster)

    val kmResult = pipelineModel
      .transform(inputKMeansDf)
      .cache()

    val ltv = kmResult
      .groupBy("cluster")
      .agg(count_distinct(col("userId")) as "uniqueUsers", sum("numAddsWatched").multiply(0.015) as "revenue")
      .withColumn("ltv", col("revenue").divide(col("uniqueUsers")))
      .select(col("ltv"), col("cluster"))

    plotLTV(ltv.collect().map(a => LTVPerCluster(a.getDouble(0), a.getInt(1))).toList)

    ltv.show()
  }
}
