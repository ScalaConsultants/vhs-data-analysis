package methods

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count_distinct, desc, initcap}
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

}
