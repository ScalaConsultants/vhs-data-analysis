package methods

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import plotly._
import plotly.layout._
import plotly.Plotly._
import plotly.element.LocalDateTime
import utils.DateColumnOperations.generateCodMonthFromDate
import java.sql.Date

object LTVMethod {
  case class LTVPerCluster(ltv: Double, k: Int)
  case class LTVPerDate(ltv: Double, date: Date)
  case class LTVPerMonth(ltv: Double, codMonth: String)

  def plotLTVByCluster(points: List[LTVPerCluster]): Unit = {

    val xCluster = points.map(_.k)
    val yLTV = points.map(_.ltv)

    val plot = Seq(
      Bar(xCluster, yLTV).withName("Cluster vs LTV")
    )
    val lay = Layout().withTitle("LTV method by Cluster")
    plot.plot(s"plots/ltvByCluster.html", lay)
  }

  def plotLTVByDate(points: List[LTVPerDate], aggOperation: String): Unit = {

    val pointsWithDateParsed = points
      .map(p => (LocalDateTime.parse(s"${p.date.toString} 00:00"), p.ltv))
      .collect{
        case (Some(date), ltv) => (date, ltv)
      }

    val xDate = pointsWithDateParsed.map(_._1)
    val yLTV = pointsWithDateParsed.map(_._2)

    val plot = Seq(
      Scatter(xDate, yLTV)
    )
    val lay = Layout().withTitle(s"Date vs $aggOperation LTV")
    plot.plot(s"plots/ltvByDate_$aggOperation.html", lay)
  }


  def plotLTVByMonth(points: List[LTVPerMonth], aggOperation: String): Unit = {

    val xCodMonth = points.map(_.codMonth)
    val yLTV = points.map(_.ltv)

    val plot = Seq(
      Scatter(xCodMonth, yLTV)
    )
    val lay = Layout().withTitle(s"CodMonth vs $aggOperation LTV")
    plot.plot(s"plots/ltvByMonth_$aggOperation.html", lay)
  }

  def calculateAndSaveLTVByCluster(sparkSession: SparkSession): Unit = {
    val kmResult = sparkSession.read.parquet("data-models/output/cluster-data")

    val ltv = kmResult
      .groupBy("cluster")
      .agg(count_distinct(col("userId")) as "uniqueUsers", sum("numAddsWatched").multiply(0.015) as "revenue")
      .withColumn("ltv", col("revenue").divide(col("uniqueUsers")))
      .select(col("ltv"), col("cluster"))

    plotLTVByCluster(ltv.collect().map(a => LTVPerCluster(a.getDouble(0), a.getInt(1))).toList)

    ltv.show()
  }

  def calculateAndSaveLTVByUserDaily(dataInput: DataFrame): Unit = {
    val lifetimeWithRevenuePerUserDf =  dataInput.groupBy("userId", "date").agg(
      count(col("partOfDay")) as "lifetime",
      sum("numAddsWatched").multiply(0.015) as "revenue"
    )

    val dauPerDayDf =  dataInput.groupBy("date").agg(count_distinct(col("numLevelsCompleted").gt(0)) as "activeUsersPerPeriod")

    val ltvDf = lifetimeWithRevenuePerUserDf
      .join(dauPerDayDf, Seq("date"), "left_outer")
      .withColumn("arpdau", col("revenue").divide(col("activeUsersPerPeriod")))
      .select(
        col("userId"),
        col("arpdau").multiply(col("lifetime")) as "ltv",
        col("date")
      )

    ltvDf
      .write
      .mode("overwrite")
      .partitionBy("date")
      .parquet("data-models/output/ltvDaily")

    val ltvByPeriodDf = ltvDf.groupBy("date").agg(
      sum("ltv") as "sumLtv",
      avg("ltv") as "avgLtv"
    ).orderBy("date")

    ltvByPeriodDf.show(50)
    val ltvByPeriodList = ltvByPeriodDf.collect()

    //Sum ltv
    plotLTVByDate(ltvByPeriodList.map(a => LTVPerDate(a.getDouble(1), a.getDate(0))).toList, "Sum")

    //Avg ltv
    plotLTVByDate(ltvByPeriodList.map(a => LTVPerDate(a.getDouble(2), a.getDate(0))).toList, "Avg")
  }


  def calculateAndSaveLTVByUserMonthly(dataInput: DataFrame): Unit = {

    val dateInputWithCodeDate = dataInput.withColumn("codeMonth", generateCodMonthFromDate(col("date")))

    val lifetimeWithRevenuePerUserDf =  dateInputWithCodeDate.groupBy("userId", "codeMonth").agg(
      count_distinct(col("date")) as "lifetime",
      sum("numAddsWatched").multiply(0.015) as "revenue"
    )
    val dauPerMonthDf =  dateInputWithCodeDate.groupBy("codeMonth").agg(count_distinct(col("numLevelsCompleted").gt(0)) as "activeUsersPerPeriod")

    val ltvDf = lifetimeWithRevenuePerUserDf
      .join(dauPerMonthDf, Seq("codeMonth"), "left_outer")
      .withColumn("arpdau", col("revenue").divide(col("activeUsersPerPeriod")))
      .select(
        col("userId"),
        col("arpdau").multiply(col("lifetime")) as "ltv",
        col("codMonth")
      )

    ltvDf
      .write
      .mode("overwrite")
      .partitionBy("codMonth")
      .parquet("data-models/output/ltvMonthly")

    val ltvByPeriodDf = ltvDf.groupBy("codMonth").agg(
      sum("ltv") as "sumLtv",
      avg("ltv") as "avgLtv"
    )

    val ltvByPeriodList = ltvByPeriodDf.collect()

    //Sum ltv
    plotLTVByMonth(ltvByPeriodList.map(a => LTVPerMonth(a.getDouble(1), a.getString(0))).toList, "Sum")

    //Avg ltv
    plotLTVByMonth(ltvByPeriodList.map(a => LTVPerMonth(a.getDouble(2), a.getString(0))).toList, "Avg")
  }
}
