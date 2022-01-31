package methods

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, _}
import plotly.Plotly.TraceOps
import plotly.Scatter
import plotly.element.LocalDateTime
import plotly.layout.Layout

import java.sql.Date
import java.time.LocalDate

object NRetentionMethod {

  case class RetentionByDate(retention: Double, date: String)

  def plotRetention(points: List[RetentionByDate]): Unit = {

    val xDate = points.map(_.date)
    val yValue = points.map(_.retention)

    val plot = Scatter(xDate, yValue).withName("Retention by Date")

    val lay = Layout().withTitle(s"Retention by Date")
    plot.plot(s"plots/retentionByDate.html", lay)
  }


  def calculateRetentionByDays(dataInput: DataFrame): Unit = {

    val initialUsers =  dataInput.groupBy("date").agg(
      count_distinct(col("userId")) as "uniqueUsers"
    ).orderBy(col("date")).first().getLong(1)

    val userNDayRetention = dataInput.groupBy("date").agg(
      count_distinct(col("userId")) as "uniqueUsers"
    ).orderBy(col("date")).select(
      col("date"), col("uniqueUsers").divide(initialUsers).multiply(100L)
    )

    val result = userNDayRetention.collect().toList
    plotRetention(result.map(a => RetentionByDate(a.getDouble(1), a.getDate(0).toLocalDate.toString)))
  }

  def calculateRetentionByBracket(dataInput: DataFrame, range: Int): Unit = {

    val initialUsers =  dataInput.groupBy("date").agg(
      count_distinct(col("userId")) as "uniqueUsers"
    ).orderBy(col("date"))

    val first = initialUsers.first()

    val (initialDate, numberOfUsers) =  first.getDate(0).toLocalDate -> first.getLong(1)
    val finalDate =  initialUsers.orderBy(desc("date")).first().getDate(0).toLocalDate


    def go(lb: LocalDate, ub: LocalDate, finalDate: LocalDate): List[RetentionByDate] = {
      if(ub.isAfter(finalDate))
        Nil
      else {
        dataInput.groupBy("date").agg(
          count_distinct(col("userId")) as "uniqueUsers"
        ).orderBy(col("date")).select(
          col("date"), col("uniqueUsers").divide(numberOfUsers).multiply(100L)
        ).where(col("date").between(lb, ub))
          .collect()
          .toList
          .map(a => RetentionByDate(a.getDouble(1), s"${lb}/${ub}")) ++ go(ub.plusDays(1), ub.plusDays(1 + range), finalDate)
      }
    }

    plotRetention(go(initialDate, initialDate.plusDays(range), finalDate))

  }

  def calculateRetention2(playerEvents: DataFrame, enrichedData: DataFrame): Unit = {
    val newUsersDf =  playerEvents
      .where((col("date") >= "2021-11-29") && (col("date") <= "2021-11-30"))
      //.groupBy("date")
      .agg(
        count_distinct(col("userId")) as "newUsers"
      )
      //.orderBy(asc("date"))

    val initialNewUsers = newUsersDf.first()

    //val (initialDate, numberOfNewUsers) =  initialNewUsers.getDate(0) -> initialNewUsers.getLong(1)
    val numberOfNewUsers = initialNewUsers.getLong(0)

    val listOfNewUsers = playerEvents
      .select("userId").distinct()
      .collect()
      .map(a => a.getString(0))

    val enrichedDataPerDay = enrichedData
      .groupBy("date", "userId")
      .agg(
        sum("numLevelsCompleted") as "numLevelsCompleted",
        sum("numLevelsStarted") as "numLevelsStarted"
      )
      .where(
        ((col("numLevelsStarted")>0) || (col("numLevelsCompleted")>0))
        && (col("userId").isin(listOfNewUsers:_*))
      )

    val activePlayersPerDay = enrichedDataPerDay
      .groupBy("date")
      .agg(
        count_distinct(col("userId")) as "activePlayers"
      )


    val retentionByDate = activePlayersPerDay
      .select(
        col("date"),
        ((col("activePlayers")/numberOfNewUsers)*100) as "retention"
      )
      .orderBy("date")

    val retentionByDatePoints = retentionByDate
      .collect()
      .map { a =>
        (
          LocalDateTime.parse(s"${a.getDate(0)} 00:00"),
          a.getDouble(1)
        )
      }
      .collect{
        case (Some(date), retention) => (date, retention)
      }
      .toList

    val xDate = retentionByDatePoints.map(_._1)
    val yValue = retentionByDatePoints.map(_._2)

    val plot = Scatter(xDate, yValue).withName("Retention by Date")

    val lay = Layout().withTitle(s"Retention by Date")
    plot.plot(s"plots/retentionByDate.html", lay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    newUsersDf.show(50)

    activePlayersPerDay
      .select(
        col("date"),
        col("activePlayers") ,
        lit(numberOfNewUsers) as "newPlayers"
      )
      .orderBy("date")
      .show()
  }

}
