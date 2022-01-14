package methods

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import plotly._
import plotly.layout._
import plotly.Plotly._
import plotly.element.LocalDateTime
import utils.DateColumnOperations.generateCodMonthFromDate
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object LTVMethod {
  case class LTVPerCluster(ltv: Double, k: Int)
  case class LTVPerDate(ltv: Double, date: LocalDateTime)
  case class LTVPerMonth(ltv: Double, codMonth: String)

  case class LTVPointsPerDate(sumLTVPoints: List[LTVPerDate], avgLTVPoints: List[LTVPerDate])
  case class LTVPointsPerCluster(sumLTVPoints: List[LTVPerDate], avgLTVPoints: List[LTVPerDate], k: Int)

  def plotLTVByCluster(points: List[LTVPerCluster]): Unit = {

    val xCluster = points.map(_.k)
    val yLTV = points.map(_.ltv)

    val plot = Seq(
      Bar(xCluster, yLTV).withName("Cluster vs LTV")
    )
    val lay = Layout().withTitle("LTV method by Cluster")
    plot.plot(s"plots/ltvByCluster.html", lay)
  }

  def plotLTVByDate(points: List[LTVPerDate], pointsByCluster: List[(List[LTVPerDate], Int)], aggOperation: String): Unit = {
    val xDate = points.map(_.date)
    val yLTV = points.map(_.ltv)

    val ScattersByClusters = pointsByCluster.map{ case (pointsCluster, k) =>
      val xDateCluster = pointsCluster.map(_.date)
      val yLTVCluster = pointsCluster.map(_.ltv)
      Scatter(xDateCluster, yLTVCluster).withName(s"LTV cluster $k")
    }

    val plot = Seq(Scatter(xDate, yLTV).withName("LTV general"))++ScattersByClusters

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

  def calculateAndSaveLTVByCluster(kmResult: DataFrame): Unit = {
    val ltv = kmResult
      .groupBy("cluster")
      .agg(count_distinct(col("userId")) as "uniqueUsers", sum("numAddsWatched").multiply(0.015) as "revenue")
      .withColumn("ltv", col("revenue").divide(col("uniqueUsers")))
      .select(col("ltv"), col("cluster"))

    plotLTVByCluster(ltv.collect().map(a => LTVPerCluster(a.getDouble(0), a.getInt(1))).toList)

    ltv.show()
  }

  def calculateLTVByUserDaily(dataInput: DataFrame): DataFrame = {
    val lifetimeWithRevenuePerUserDf =  dataInput.groupBy("userId", "date").agg(
      count(col("partOfDay")) as "lifetime",
      avg("numAddsWatched").multiply(0.015) as "revenue"
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

    /*val df = dataInput.groupBy("userId", "date").agg(
      count(col("partOfDay")) as "lifetime",
      avg("numAddsWatched").multiply(0.015) as "revenue"
    ).withColumn("ltv", col("lifetime").multiply(col("revenue")))

    df.show(100)

    df*/
  }

  def calculateLTVPointsByDate(ltvDf: DataFrame): DataFrame = {
    val ltvByPeriodDf = ltvDf.groupBy("date").agg(
      sum("ltv") as "sumLtv",
      avg("ltv") as "avgLtv"
    ).orderBy("date")
    ltvByPeriodDf
  }

  def linearRegressionLTV(dataInput: DataFrame): Unit = {
    //val dataInput = dataInputT.where((col("ltv")>0 && col("numLevelsCompleted")>0))
    val Array(trainData, testData) = dataInput.randomSplit(Array(0.80, 0.20))

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("numLevelsCompleted", "numAddsWatched", "partOfDayNumber"))
      .setOutputCol("featureVector")

    val lr = new LinearRegression()
     // new RandomForestRegressor()
        .setFeaturesCol("featureVector")
        .setLabelCol("ltv")
        .setPredictionCol("prediction")
    .setMaxIter(80)
    /*.setRegParam(0.3)
    .setElasticNetParam(0.8)*/

    val pipeline = new Pipeline().setStages(Array(vectorAssembler, lr))

    val modelLinearRegression = pipeline.fit(trainData)

    // Make predictions.
    val predictions = modelLinearRegression.transform(testData)

    // Select example rows to display.
    // predictions.select("featureVector", "ltv", "prediction").show(15)

    val evaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("ltv")
      .setMetricName("r2")

    val metric = evaluator.evaluate(predictions)

    /*val tmpDf = predictions.withColumn("error", abs((col("ltv") - col("prediction"))/col("ltv")))

    tmpDf.where(col("error").gt(0.30)).show(20)

    tmpDf.where(col("error").lt(0.30)).show(20)

    tmpDf.select(sum(when(col("error").lt(0.30), lit(1)).otherwise(lit(0)))/count("*") as "accuracy").show()*/

    predictions.withColumn("accuracy", lit(metric)).select("accuracy").limit(1).show()
  }

  def calculateAndSaveLTVByUserDaily(dataInput: DataFrame): Unit = {
    val ltvDf = calculateLTVByUserDaily(dataInput)

    val ltvByPeriodDf = calculateLTVPointsByDate(ltvDf)

    ltvByPeriodDf.show(30)

    /*val dataInputLTV = dataInput
      .groupBy("userId", "date")
      .agg(
        sum("numLevelsCompleted") as "numLevelsCompleted",
        sum("numAddsWatched") as "numAddsWatched",
        avg("partOfDayNumber") as "partOfDayNumber"
      )
      .join(ltvDf, Seq("userId", "date"), "left_outer")*/

    val dataInputLTV = dataInput
      .groupBy("userId", "date")
      .agg(
        avg("numLevelsCompleted") as "numLevelsCompleted",
        avg("numAddsWatched") as "numAddsWatched",
        count(col("partOfDay")) as "partOfDayNumber"
      )
      .join(ltvDf, Seq("userId", "date"), "left_outer")

    linearRegressionLTV(dataInputLTV)

    /*ltvDf
      .write
      .mode("overwrite")
      .partitionBy("date")
      .parquet("data-models/output/ltvDaily")*/

    /*
        val ltvSumAndAvgPoints = ltvByPeriodDf
          .collect()
          .map { a =>
            (
              LocalDateTime.parse(s"${a.getDate(0)} 00:00"),
              a.getDouble(1),
              a.getDouble(2)
            )
          }
          .collect{
            case (Some(date), sumLTV, avgLTV) => (LTVPerDate(sumLTV, date), LTVPerDate(avgLTV, date))
          }
          .toList

        val ltvSumAndAvgPointsTotal = LTVPointsPerDate(
          ltvSumAndAvgPoints.map(_._1),
          ltvSumAndAvgPoints.map(_._2)
        )

        val kClusters = dataInput
          .groupBy("cluster")
          .count()
          .select("cluster")
          .collect()
          .map(k => k.getInt(0))
          .sorted
          .toList

        val ltvSumAndAvgPointsByClusters = kClusters.map{ k =>
          val ltvByCluster = calculateLTVByUserDaily(dataInput.where(col("cluster")===k))
          val ltvSumAndAvgPointsByCluster = calculateLTVPointsByDate(ltvByCluster)
            .collect()
            .map { a =>
              (
                LocalDateTime.parse(s"${a.getDate(0)} 00:00"),
                a.getDouble(1),
                a.getDouble(2)
              )
            }
            .collect{
              case (Some(date), sumLTV, avgLTV) => (LTVPerDate(sumLTV, date), LTVPerDate(avgLTV, date))
            }
            .toList

          LTVPointsPerCluster(
            ltvSumAndAvgPointsByCluster.map(_._1),
            ltvSumAndAvgPointsByCluster.map(_._2),
            k
          )
        }

        //Sum ltv
        plotLTVByDate(
          ltvSumAndAvgPointsTotal.sumLTVPoints,
          ltvSumAndAvgPointsByClusters.map(p => (p.sumLTVPoints, p.k)),
          "Sum"
        )

        //Avg ltv
        plotLTVByDate(
          ltvSumAndAvgPointsTotal.avgLTVPoints,
          ltvSumAndAvgPointsByClusters.map(p => (p.avgLTVPoints, p.k)),
          "Avg")*/
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