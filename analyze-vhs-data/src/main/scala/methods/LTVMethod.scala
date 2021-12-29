package methods

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import plotly._
import plotly.layout._
import plotly.Plotly._

object LTVMethod {
  case class LTVPerCluster(ltv: Double, k: Int)
  case class LTVPerUser(ltv: Double, user: String)

  def plotLTV(points: List[LTVPerCluster]): Unit = {

    val xCluster = points.map(_.k)
    val yLTV = points.map(_.ltv)

    val plot = Seq(
      Bar(xCluster, yLTV).withName("Cluster vs LTV")
    )
    val lay = Layout().withTitle("LTV method")
    plot.plot(s"plots/ltv.html", lay)
  }

  def plotByUser(points: List[LTVPerUser]): Unit = {

    val xCluster = points.zipWithIndex.map(_._2)
    val yLTV = points.map(_.ltv)

    val plot = Seq(
      Bar(xCluster, yLTV).withName("Cluster vs LTV")
    )
    val lay = Layout().withTitle("LTV method")
    plot.plot(s"plots/ltv.html", lay)
  }
  def calculateAndSaveLTVByCluster(sparkSession: SparkSession): Unit = {
    val kmResult = sparkSession.read.parquet("data-models/output/cluster-data")

    val ltv = kmResult
      .groupBy("cluster")
      .agg(count_distinct(col("userId")) as "uniqueUsers", sum("numAddsWatched").multiply(0.015) as "revenue")
      .withColumn("ltv", col("revenue").divide(col("uniqueUsers")))
      .select(col("ltv"), col("cluster"))

    plotLTV(ltv.collect().map(a => LTVPerCluster(a.getDouble(0), a.getInt(1))).toList)

    ltv.show()
  }

  def calculateAndSaveLTVByUser(sparkSession: SparkSession): Unit = {
    val kmResult = sparkSession.read.parquet("data-models/output/cluster-data")

    val ltv = kmResult
      .groupBy(col("userId"))
      .agg(count_distinct(col("userId")) as "uniqueUsers", sum("numAddsWatched").multiply(0.015) as "revenue")
      .withColumn("ltv", col("revenue").divide(col("uniqueUsers")))
      .select(col("ltv" ), col("userId"))

    plotByUser(ltv.collect().map(a => LTVPerUser(a.getDouble(0), a.getString(1))).toList)

    ltv.show()
  }
}
