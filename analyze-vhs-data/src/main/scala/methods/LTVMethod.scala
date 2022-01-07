package methods

import config.{Behavior, Both, Daily, Monthly}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import plotly._
import plotly.layout._
import plotly.Plotly._
import utils.DateColumnOperations.generateCodMonthFromDate

import java.sql.Date
import java.time.LocalDate

object LTVMethod {
  case class LTVPerCluster(ltv: Double, k: Int)
  case class LTVPerUser(ltv: Double, user: String)
  case class LTVPerDate(ltv: Double, date: Date)
  case class LTVPerMonth(ltv: Double, date: String)

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
      Bar(xCluster, yLTV).withName("User vs LTV")
    )
    val lay = Layout().withTitle("LTV method")
    plot.plot(s"plots/ltv.html", lay)
  }

  def plotByDate(points: List[LTVPerDate]): Unit = {

    val xCluster = points.zipWithIndex.map(_._2)
    val yLTV = points.map(_.ltv)

    val plot = Seq(
      Bar(xCluster, yLTV).withName("Month vs LTV")
    )
    val lay = Layout().withTitle("LTV method")
    plot.plot(s"plots/ltv.html", lay)
  }


  def plotByMonth(points: List[LTVPerMonth]): Unit = {

    val xCluster = points.zipWithIndex.map(_._2)
    val yLTV = points.map(_.ltv)

    val plot = Seq(
      Bar(xCluster, yLTV).withName("Month vs LTV")
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

  def calculateAndSaveLTVByUserDaily(dataInput: DataFrame): Unit = {

    val dateInputWithCodeDate = dataInput.withColumn("codeDate", col("date"))

    val lifetimeWithRevenuePerUserDf =  dateInputWithCodeDate.groupBy("userId", "codeDate").agg(
       lit(4).minus(count(col("partOfDay"))) as "lifetime",
      sum("numAddsWatched").multiply(0.015) as "revenue"
    )

    val dauPerDayDf =  dateInputWithCodeDate.groupBy("codeDate").agg(count_distinct(col("numLevelsCompleted").gt(0)) as "activeUsersPerPeriod")

    val ltvDf = lifetimeWithRevenuePerUserDf
      .join(dauPerDayDf, Seq("codeDate"), "left_outer")
      .withColumn("arpdau", col("revenue").divide(col("activeUsersPerPeriod")))
      .select(
        col("userId"),
        col("arpdau").multiply(col("lifetime")) as "ltv"
      )

    plotByUser(ltvDf.collect().map(a => LTVPerUser(a.getDouble(1), a.getString(0))).toList)

    ltvDf.show()
  }


  def calculateAndSaveLTVByMonth(dataInput: DataFrame): Unit = {
    val dateInputWithCodeDate = dataInput.withColumn("codeMonth", generateCodMonthFromDate(col("date")))

    val lifetimeWithRevenuePerUserDf =  dateInputWithCodeDate.groupBy("codeMonth").agg(
      count(col("date")) as "lifetime",
      sum("numAddsWatched").multiply(0.015) as "revenue"
    )

    val dauPerMonth =  dateInputWithCodeDate.groupBy("codeMonth").agg(count_distinct(col("numLevelsCompleted").gt(0)) as "activeUsersPerPeriod")

    val ltvDf = lifetimeWithRevenuePerUserDf
      .join(dauPerMonth, Seq("codeMonth"), "left_outer")
      .withColumn("arpdau", col("revenue"))
      .select(
        col("codeMonth"),
        col("arpdau").multiply(col("lifetime")) as "ltv"
      ).orderBy(col("codeMonth"))

    plotByMonth(ltvDf.collect().map(a => LTVPerMonth(a.getDouble(1), a.getString(0))).toList)

    ltvDf.show()

  }

  def calculateAndSaveLTVByDay(dataInput: DataFrame): Unit = {
    val dateInputWithCodeDate = dataInput.withColumn("codeDate", col("date"))

    val lifetimeWithRevenuePerUserDf =  dateInputWithCodeDate.groupBy("codeDate").agg(
      count(col("partOfDay")) as "lifetime",
      sum("numAddsWatched").multiply(0.015) as "revenue"
    )

    val dauPerDayDf =  dateInputWithCodeDate.groupBy("codeDate").agg(count_distinct(col("numLevelsCompleted").gt(0)) as "activeUsersPerPeriod")

    val ltvDf = lifetimeWithRevenuePerUserDf
      .join(dauPerDayDf, Seq("codeDate"), "left_outer")
      .withColumn("arpdau", col("revenue").divide(col("activeUsersPerPeriod")))
      .select(
        col("codeDate"),
        col("arpdau").multiply(col("lifetime")) as "ltv"
      ).orderBy(col("codeDate"))

    plotByDate(ltvDf.collect().map(a => LTVPerDate(a.getDouble(1), a.getDate(0))).toList)

    ltvDf.show()

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
        col("arpdau").multiply(col("lifetime")) as "ltv"
      )

    plotByUser(ltvDf.collect().map(a => LTVPerUser(a.getDouble(1), a.getString(0))).toList)

    ltvDf.show()
  }
}
