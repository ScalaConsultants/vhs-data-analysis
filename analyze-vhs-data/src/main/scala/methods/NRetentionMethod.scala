package methods

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly.Plotly.TraceOps
import plotly.Scatter
import plotly.element.LocalDateTime
import plotly.layout.Layout
import java.time.YearMonth

object NRetentionMethod {

  case class RetentionPoint(dateRetention: LocalDateTime, dayRetention: Int, retentionRate: Double)

  def formatDay(d: Int): String = {
    if((d/10)<1)
      s"0$d"
    else
      s"$d"
  }

  def calculateRetentionWithIdleTime(playerEvents: DataFrame, enrichedData: DataFrame, startMonth: YearMonth, nDays: Int, idleTime: Int): Unit = {

    val newUsersDf =  playerEvents
      .groupBy("date")
      .agg(
        count_distinct(col("userId")) as "newUsers"
      )
      .cache()

    val enrichedDataPerDay = enrichedData
      .groupBy("date", "userId")
      .agg(
        sum("numLevelsCompleted") as "numLevelsCompleted",
        sum("numLevelsStarted") as "numLevelsStarted"
      )
      .cache()

    val pivotDate = (1 to nDays).map(d => s"${startMonth.getYear}-${startMonth.getMonthValue}-${formatDay(d)}")

    val monthlyRetentionPoints = pivotDate.flatMap{ pivotDate =>
      val newUsersPerDateDf =  newUsersDf
        .where(col("date") === pivotDate )

      val listOfNewUsers = playerEvents
        .where(col("date") === pivotDate)
        .select("userId")
        .distinct()
        .collect()
        .map(a => a.getString(0))

      if(newUsersPerDateDf.count() > 0) {
        val initialNewUsers = newUsersPerDateDf.first()

        val (startDate, numberOfNewUsers) = (initialNewUsers.getDate(0) ,initialNewUsers.getLong(1))

        val dateIndex = (1 to nDays).map(i => (startDate.toLocalDate.plusDays(i), startDate.toLocalDate.plusDays(i+idleTime), i))

        val retentionPoints = dateIndex.map{
          case (dateRetention, dateRetentionWithIdleTime, iRetention) =>
            val userActivity = enrichedDataPerDay
              .where(
                ((col("date") >= dateRetention) && (col("date") <= dateRetentionWithIdleTime))
                  && (col("userId").isin(listOfNewUsers: _*))
              )
              .groupBy("userId")
              .agg(
                sum("numLevelsCompleted") as "numLevelsCompleted",
                sum("numLevelsStarted") as "numLevelsStarted"
              )


            val activePlayers = userActivity
              .where((col("numLevelsStarted") > 0) || (col("numLevelsCompleted") > 0))
              .count()

            val retentionValue = (activePlayers.toDouble / numberOfNewUsers) * 100

            RetentionPoint(LocalDateTime.parse(s"$dateRetention 00:00").get, iRetention, retentionValue)
        }.sortBy(_.dayRetention)

        if (retentionPoints.nonEmpty) {
          val xRetentionDay = retentionPoints.map(_.dayRetention)
          val yValue = retentionPoints.map(_.retentionRate)

          val plot = Scatter(xRetentionDay, yValue).withName("Retention by xDay")

          val lay = Layout().withTitle(s"Retention by xDay")
          plot.plot(s"plots/retention/retentionByxDay_pivotDate_${pivotDate}_numberOfNewUsers_$numberOfNewUsers.html", lay, useCdn = true,
            openInBrowser = false,
            addSuffixIfExists = true)
        }
        retentionPoints.toList
      }
      else {
        List.empty
      }
    }

   val monthlyPointsAvgRetention = monthlyRetentionPoints.groupBy(_.dayRetention).map{ case (k, v) => (k, v.map(_.retentionRate).toList.sum/nDays)}.toList.sortBy(_._1)

    val xRetentionDay = monthlyPointsAvgRetention.map(_._1)
    val yValue = monthlyPointsAvgRetention.map(_._2)

    val plot = Scatter(xRetentionDay, yValue).withName("Avg Retention by xDay")

    val lay = Layout().withTitle(s"Avg Retention by xDay")
    plot.plot(s"plots/avgRetentionByMonth_codMonth_${startMonth}_idleTime_$idleTime.html", lay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)
  }

}
