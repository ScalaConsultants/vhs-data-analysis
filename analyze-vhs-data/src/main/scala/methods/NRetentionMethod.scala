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

  def formatDay(d: Int): String = {
    if((d/10)<1)
      s"0$d"
    else
      s"$d"
  }

  def calculateRetention2(playerEvents: DataFrame, enrichedData: DataFrame): Unit = {
    val newUsersDf =  playerEvents
      .groupBy("date")
      .agg(
        count_distinct(col("userId")) as "newUsers"
      )
      .select(
        col("date"),
        date_add(col("date"),30) as "endDate",
        col("newUsers")
      )
      .cache()

    val enrichedDataPerDay = enrichedData
      .groupBy("date", "userId")
      .agg(
        sum("numLevelsCompleted") as "numLevelsCompleted",
        sum("numLevelsStarted") as "numLevelsStarted"
      )
      .cache()

    val pivotDate = (1 to 30).map( d => s"2021-11-${formatDay(d)}")

    pivotDate.foreach{ pivotDate =>
      val newUsersPerDateDf =  newUsersDf
        .where(col("date") === pivotDate )

      if(newUsersPerDateDf.count() > 0) {
        val initialNewUsers = newUsersPerDateDf.first()

        val (endDate, numberOfNewUsers) = initialNewUsers.getDate(1) -> initialNewUsers.getLong(2)

        val listOfNewUsers = playerEvents
          .where(col("date") === pivotDate)
          .select("userId").distinct()
          .collect()
          .map(a => a.getString(0))

        val activePlayersPerDay = enrichedDataPerDay
          .where(
            ((col("date") > pivotDate) && (col("date") <= endDate))
              &&
              ((col("numLevelsStarted") > 0) || (col("numLevelsCompleted") > 0))
              && (col("userId").isin(listOfNewUsers: _*))
          )
          .groupBy("date")
          .agg(
            count_distinct(col("userId")) as "activePlayers"
          )


        val retentionByDate = activePlayersPerDay
          .select(
            col("date"),
            ((col("activePlayers") / numberOfNewUsers) * 100) as "retention"
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
          .collect {
            case (Some(date), retention) => (date, retention)
          }
          .toList

        if (retentionByDatePoints.nonEmpty) {
          val xDate = retentionByDatePoints.map(_._1)
          val yValue = retentionByDatePoints.map(_._2)

          val plot = Scatter(xDate, yValue).withName("Retention by Date")

          val lay = Layout().withTitle(s"Retention by Date")
          plot.plot(s"plots/retention/retentionByDate_${pivotDate}_numberOfNewUsers_$numberOfNewUsers.html", lay, useCdn = true,
            openInBrowser = false,
            addSuffixIfExists = true)
        }
      }
    }
    newUsersDf
      .where(col("date")<="2021-11-30")
      .orderBy("date")
      .drop("endDate")
      .show(50)
  }

}
